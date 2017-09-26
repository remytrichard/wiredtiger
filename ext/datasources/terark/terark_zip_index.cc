
// terark header
#include <terark/hash_strmap.hpp>
#include <terark/fsa/dfa_mmap_header.hpp>
#include <terark/fsa/fsa_cache.hpp>
#include <terark/fsa/nest_trie_dawg.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/fsa/fsa_for_union_dfa.hpp>
// project header
#include "terark_zip_index.h"
#include "terark_zip_common.h"
#include "terark_zip_config.h"

namespace terark {

	using terark::initial_state;
	using terark::BaseDFA;
	using terark::NestLoudsTrieDAWG_SE_512;
	using terark::NestLoudsTrieDAWG_IL_256;
	using terark::NestLoudsTrieDAWG_Mixed_SE_512;
	using terark::NestLoudsTrieDAWG_Mixed_IL_256;
	using terark::NestLoudsTrieDAWG_Mixed_XL_256;
	using terark::SortableStrVec;
	using terark::MmapWholeFile;

	static terark::hash_strmap<TerarkIndex::FactoryPtr> g_TerarkIndexFactroy;
	static terark::hash_strmap<std::string>             g_TerarkIndexName;

	struct TerarkIndexHeader {
		uint8_t   magic_len;
		char      magic[19];
		char      class_name[60];

		uint32_t  reserved_80_4;
		uint32_t  header_size;
		uint32_t  version;
		uint32_t  reserved_92_4;

		uint64_t  file_size;
		uint64_t  reserved_102_24;
	};

	TerarkIndex::AutoRegisterFactory::AutoRegisterFactory(std::initializer_list<const char*> names,
														  const char* riit_name,
														  Factory* factory) {
		for (const char* name : names) {
			//  STD_INFO("AutoRegisterFactory: %s\n", name);
			g_TerarkIndexFactroy.insert_i(name, FactoryPtr(factory));
			g_TerarkIndexName.insert_i(riit_name, *names.begin());
		}
	}

	const TerarkIndex::Factory* TerarkIndex::GetFactory(fstring name) {
		size_t idx = g_TerarkIndexFactroy.find_i(name);
		if (idx < g_TerarkIndexFactroy.end_i()) {
			auto factory = g_TerarkIndexFactroy.val(idx).get();
			return factory;
		}
		return NULL;
	}

	const TerarkIndex::Factory*
	TerarkIndex::SelectFactory(const KeyStat& ks, fstring key_f, 
							   WT_SESSION* session, fstring name) {
		if (key_f == "Q" || key_f == "r") {
			uint64_t minVal, maxVal;
			wiredtiger_struct_unpack(session, ks.minKey.begin(), 
									 ks.minKey.size(), "Q", &minVal);
			wiredtiger_struct_unpack(session, ks.maxKey.begin(), 
									 ks.maxKey.size(), "Q", &maxVal);
			printf("[SelectFactory] key_format %s, min: %lld, max %lld\n", key_f.c_str(), minVal, maxVal);
			uint64_t diff = (minVal < maxVal ? maxVal - minVal : minVal - maxVal) + 1;
			if (diff < ks.numKeys * 30) {
				if (diff < (4ull << 30)) {
					return GetFactory("UintIndex");
				} else {
					return GetFactory("UintIndex_SE_512_64");
				}
			}
		}

		if (ks.sumKeyLen - ks.numKeys * ks.commonPrefixLen > 0x1E0000000) { // 7.5G
			return GetFactory("SE_512_64");
		}
		return GetFactory(name);
	}

	TerarkIndex::~TerarkIndex() {}
	TerarkIndex::Factory::~Factory() {}
	TerarkIndex::Iterator::~Iterator() {}

	unique_ptr<TerarkIndex> TerarkIndex::LoadFile(fstring fpath, const TerarkTableReaderOptions& ropt) {
		TerarkIndex::Factory* factory = NULL;
		{
			MmapWholeFile mmap(fpath);
			auto header = (const TerarkIndexHeader*)mmap.base;
			size_t idx = g_TerarkIndexFactroy.find_i(header->class_name);
			if (idx >= g_TerarkIndexFactroy.end_i()) {
				throw std::invalid_argument(
											"TerarkIndex::LoadFile(" + fpath + "): Unknown class: "
											+ header->class_name);
			}
			factory = g_TerarkIndexFactroy.val(idx).get();
		}
		return factory->LoadFile(fpath, ropt);
	}

	unique_ptr<TerarkIndex> TerarkIndex::LoadMemory(fstring mem, const TerarkTableReaderOptions& ropt) {
		auto header = (const TerarkIndexHeader*)mem.data();
		size_t idx = g_TerarkIndexFactroy.find_i(header->class_name);
		if (idx >= g_TerarkIndexFactroy.end_i()) {
			throw std::invalid_argument(
										std::string("TerarkIndex::LoadMemory(): Unknown class: ")
										+ header->class_name);
		}
		TerarkIndex::Factory* factory = g_TerarkIndexFactroy.val(idx).get();
		return factory->LoadMemory(mem, ropt);
	}

	/*
	 * Most commonly used type: NLTrie
	 */
	class NestLoudsTrieIterBase : public TerarkIndex::Iterator {
	protected:
		unique_ptr<terark::ADFA_LexIterator> m_iter;
		template<class NLTrie>
		bool Done(const NLTrie* trie, bool ok) {
			if (ok)
				m_id = trie->state_to_word_id(m_iter->word_state());
			else
				m_id = size_t(-1);
			return ok;
		}
		fstring key() const override {
			return fstring(m_iter->word());
		}
		NestLoudsTrieIterBase(terark::ADFA_LexIterator* iter)
			: m_iter(iter) {}
	};
	template<class NLTrie>
	class NestLoudsTrieIndex : public TerarkIndex {
		unique_ptr<NLTrie> m_trie;
		class MyIterator : public NestLoudsTrieIterBase {
			const NLTrie* m_trie;
		public:
			explicit MyIterator(NLTrie* trie)
				: NestLoudsTrieIterBase(trie->adfa_make_iter(initial_state))
				, m_trie(trie)
			{}
			bool SeekToFirst() override { return Done(m_trie, m_iter->seek_begin()); }
			bool SeekToLast()  override { return Done(m_trie, m_iter->seek_end()); }
			bool Seek(fstring key) override {
				return Done(m_trie, m_iter->seek_lower_bound(key));
			}
			bool Next() override { return Done(m_trie, m_iter->incr()); }
			bool Prev() override { return Done(m_trie, m_iter->decr()); }
		};
	public:
		NestLoudsTrieIndex(NLTrie* trie, const TerarkTableReaderOptions& ropt)
			: TerarkIndex(ropt), 
			  m_trie(trie) {}
		const char* Name() const override {
			auto header = (const TerarkIndexHeader*)m_trie->get_mmap().data();
			return header->class_name;
		}
		void SaveMmap(std::function<void(const void *, size_t)> write) const override {
			m_trie->save_mmap(write);
		}
		size_t Find(fstring key) const override final {
			MY_THREAD_LOCAL(terark::MatchContext, ctx);
			ctx.root = 0;
			ctx.pos = 0;
			ctx.zidx = 0;
			ctx.zbuf_state = size_t(-1);
			return m_trie->index(ctx, key);
		}
		size_t NumKeys() const override final {
			return m_trie->num_words();
		}
		size_t TotalKeySize() const override final {
			return m_trie->adfa_total_words_len();
		}
		fstring Memory() const override final {
			return m_trie->get_mmap();
		}
		Iterator* NewIterator() const override final {
			return new MyIterator(m_trie.get());
		}
		bool NeedsReorder() const override final { return true; }
		void GetOrderMap(terark::UintVecMin0& newToOld)
			const override final {
			terark::NonRecursiveDictionaryOrderToStateMapGenerator gen;
			gen(*m_trie, [&](size_t dictOrderOldId, size_t state) {
					size_t newId = m_trie->state_to_word_id(state);
					newToOld.set_wire(newId, dictOrderOldId);
				});
		}
		void BuildCache(double cacheRatio) {
			if (cacheRatio > 1e-8) {
				m_trie->build_fsa_cache(cacheRatio, NULL);
			}
		}
		/*
		 * TBD(kg): 
		 *     1. Sorted vs FixedLen; 
		 *     2. tmpLevel;
		 */
		class MyFactory : public Factory {
		public:
			void Build(NativeDataInput<InputBuffer>& reader,
					   const TerarkZipTableOptions& tzopt,
					   const TerarkTableBuilderOptions& tbo,
					   std::function<void(const void *, size_t)> write,
					   KeyStat& ks) const override {
				size_t sumPrefixLen = ks.commonPrefixLen * ks.numKeys;
				SortableStrVec keyVec;
				keyVec.m_index.reserve(ks.numKeys);
				keyVec.m_strpool.reserve(ks.sumKeyLen - sumPrefixLen);
				valvec<byte_t> keyBuf;
				for (size_t seq_id = 0; seq_id < ks.numKeys; ++seq_id) {
					reader >> keyBuf;
					keyVec.push_back(fstring(keyBuf).substr(ks.commonPrefixLen));
				}
				if (keyVec[0] > keyVec.back()) {
					std::reverse(keyVec.m_index.begin(), keyVec.m_index.end());
				}
				terark::NestLoudsTrieConfig conf;
				conf.nestLevel = tzopt.indexNestLevel;
				const size_t smallmem = 1000*1024*1024;
				const size_t myWorkMem = keyVec.mem_size();
				if (myWorkMem > smallmem) {
					// use tmp files during index building
					conf.tmpDir = tzopt.localTempDir;
					// adjust tmpLevel for linkVec, wihch is proportional to num of keys
					if (ks.numKeys > 1ul<<30) {
						// not need any mem in BFS, instead 8G file of 4G mem (linkVec)
						// this reduce 10% peak mem when avg keylen is 24 bytes
						conf.tmpLevel = 3;
					}
					else if (myWorkMem > 256ul<<20) {
						// 1G mem in BFS, swap to 1G file after BFS and before build nextStrVec
						conf.tmpLevel = 2;
					}
				}
				conf.isInputSorted = true;
				std::unique_ptr<NLTrie> trie(new NLTrie());
				trie->build_from(keyVec, conf);
				trie->save_mmap(write);
				// TBD(kg): should use save_mmap ?
				//SaveMmap(write);
			}

			unique_ptr<TerarkIndex> LoadMemory(fstring mem, 
											   const TerarkTableReaderOptions& ropt) const override {
				unique_ptr<BaseDFA>
					dfa(BaseDFA::load_mmap_user_mem(mem.data(), mem.size()));
				auto trie = dynamic_cast<NLTrie*>(dfa.get());
				if (NULL == trie) {
					throw std::invalid_argument("Bad trie class: " + ClassName(*dfa)
												+ ", should be " + ClassName<NLTrie>());
				}
				unique_ptr<TerarkIndex> index(new NestLoudsTrieIndex(trie, ropt));
				dfa.release();
				return std::move(index);
			}

			unique_ptr<TerarkIndex> LoadFile(fstring fpath,
											 const TerarkTableReaderOptions& ropt) const override {
				unique_ptr<BaseDFA> dfa(BaseDFA::load_mmap(fpath));
				auto trie = dynamic_cast<NLTrie*>(dfa.get());
				if (NULL == trie) {
					throw std::invalid_argument(
												"File: " + fpath + ", Bad trie class: " + ClassName(*dfa)
												+ ", should be " + ClassName<NLTrie>());
				}
				unique_ptr<TerarkIndex> index(new NestLoudsTrieIndex(trie, ropt));
				dfa.release();
				return std::move(index);
			}

			size_t MemSizeForBuild(const TerarkTableBuilderOptions& tbo, 
								   const KeyStat& ks) const override {
				return sizeof(SortableStrVec::SEntry) * ks.numKeys + ks.sumKeyLen
					- ks.commonPrefixLen * ks.numKeys;
			}
		};
	};

	
	/*
	 * uint64 customized
	 */ 
	template<class RankSelect>
	class TerarkUintIndex : public TerarkIndex {
	public:
		static const char* index_name;
		
		struct FileHeader : public TerarkIndexHeader
		{
			uint64_t min_value;
			uint64_t max_value;
			uint64_t index_mem_size;
			uint32_t key_length;
			/*
			 * For one huge index, we'll split it into multipart-index for the sake of RAM, 
			 * and each sub-index could have longer commonPrefix compared with ks.commonPrefix.
			 * what's more, under such circumstances, ks.commonPrefix may have been rewritten
			 * be upper-level builder to '0'. here, 
			 * common_prefix_length = sub-index.commonPrefixLen - whole-index.commonPrefixLen
			 */
			uint32_t common_prefix_length;

			FileHeader(size_t body_size) {
				memset(this, 0, sizeof *this);
				magic_len = strlen(index_name);
				strncpy(magic, index_name, sizeof magic);
				size_t name_i = g_TerarkIndexName.find_i(typeid(TerarkUintIndex<RankSelect>).name());
				strncpy(class_name, g_TerarkIndexName.val(name_i).c_str(), sizeof class_name);

				header_size = sizeof *this;
				version = 1;

				file_size = sizeof *this + body_size;
			}
		};

		class UIntIndexIterator : public TerarkIndex::Iterator {
		public:
			UIntIndexIterator(const TerarkUintIndex& index) : index_(index) {
				pos_ = size_t(-1);
				buffer_.resize_no_init(index_.commonPrefix_.size() + index_.keyLength_);
				memcpy(buffer_.data(), index_.commonPrefix_.data(), index_.commonPrefix_.size());
			}
			virtual ~UIntIndexIterator() {}

			bool SeekToFirst() override {
				m_id = 0;
				pos_ = 0;
				UpdateBuffer();
				return true;
			}
			bool SeekToLast() override {
				m_id = index_.indexSeq_.max_rank1() - 1;
				pos_ = index_.indexSeq_.size() - 1;
				UpdateBuffer();
				return true;
			}
			bool Seek(fstring target) override {
				size_t cplen = target.commonPrefixLen(index_.commonPrefix_);
				if (cplen != index_.commonPrefix_.size()) {
					assert(target.size() >= cplen);
					assert(target.size() == cplen || target[cplen] != index_.commonPrefix_[cplen]);
					if (target.size() == cplen || target[cplen] < index_.commonPrefix_[cplen]) {
						SeekToFirst();
						return true;
					} else {
						m_id = size_t(-1);
						return false;
					}
				}
				target.n -= index_.commonPrefix_.size();
				byte_t targetBuffer[8] = {};
				memcpy(targetBuffer + (8 - index_.keyLength_),
					   target.data(), std::min<size_t>(index_.keyLength_, target.size()));
				uint64_t targetValue = ReadUint64Aligned(targetBuffer, targetBuffer + 8);
				if (targetValue > index_.maxValue_) {
					m_id = size_t(-1);
					return false;
				}
				if (targetValue < index_.minValue_) {
					SeekToFirst();
					return true;
				}
				pos_ = targetValue - index_.minValue_;
				m_id = index_.indexSeq_.rank1(pos_);
				if (!index_.indexSeq_[pos_]) {
					pos_ += index_.indexSeq_.zero_seq_len(pos_);
				}
				else if (target.size() > index_.keyLength_) {
					if (pos_ == index_.indexSeq_.size() - 1) {
						m_id = size_t(-1);
						return false;
					}
					++m_id;
					pos_ = pos_ + index_.indexSeq_.zero_seq_len(pos_ + 1) + 1;
				}
				UpdateBuffer();
				return true;
			}
			bool Next() override {
				assert(m_id != size_t(-1));
				assert(index_.indexSeq_[pos_]);
				assert(index_.indexSeq_.rank1(pos_) == m_id);
				if (m_id == index_.indexSeq_.max_rank1() - 1) {
					m_id = size_t(-1);
					return false;
				} else {
					++m_id;
					pos_ = pos_ + index_.indexSeq_.zero_seq_len(pos_ + 1) + 1;
					UpdateBuffer();
					return true;
				}
			}
			bool Prev() override {
				assert(m_id != size_t(-1));
				assert(index_.indexSeq_[pos_]);
				assert(index_.indexSeq_.rank1(pos_) == m_id);
				if (m_id == 0) {
					m_id = size_t(-1);
					return false;
				} else {
					--m_id;
					/*
					 * zero_seq_ has [a, b) range, hence next() need (pos_ + 1), whereas
					 * prev() just called with (pos_) is enough
					 */
					pos_ = pos_ - index_.indexSeq_.zero_seq_revlen(pos_) - 1;
					UpdateBuffer();
					return true;
				}
			}
			fstring key() const override {
				assert(m_id != size_t(-1));
				return buffer_;
			}
		protected:
			void UpdateBuffer() {
				TerarkUintIndex::AssignUint64(index_.reader_options_.wt_session,
											  pos_ + index_.minValue_, buffer_);
			}
			size_t pos_; // offset starting from min_val
			valvec<byte_t> buffer_;
			const TerarkUintIndex& index_;
		};
		class MyFactory : public Factory {
		public:
			void Build(NativeDataInput<InputBuffer>& reader,
					   const TerarkZipTableOptions& tzopt,
					   const TerarkTableBuilderOptions& tbo,
					   std::function<void(const void *, size_t)> write,
					   KeyStat& ks) const override {
				printf("\n\n\n\n enter Uint64 Factory\n\n\n\n");
				uint64_t
					minValue = TerarkUintIndex::ReadUint64(tbo.wt_session, ks.minKey.begin()),
					maxValue = TerarkUintIndex::ReadUint64(tbo.wt_session, ks.maxKey.begin());
				if (minValue > maxValue) {
					std::swap(minValue, maxValue);
				}
				uint64_t diff = maxValue - minValue + 1;
				RankSelect indexSeq;
				valvec<byte_t> keyBuf;
				indexSeq.resize(diff);
				for (size_t seq_id = 0; seq_id < ks.numKeys; ++seq_id) {
					reader >> keyBuf;
					indexSeq.set1(TerarkUintIndex::ReadUint64(tbo.wt_session, keyBuf) - minValue);
				}
				indexSeq.build_cache(false, false);
				/*
				 * in ideal world, when it comes to TerarkIndex we should always 
				 * treat it as read-only, that's why reader_options is required.
				 * here, unfortunately we create one during Build phase...
				 */
				TerarkTableReaderOptions ropt(tbo.internal_comparator);
				unique_ptr<TerarkUintIndex<RankSelect>> ptr(new TerarkUintIndex<RankSelect>(ropt));
				ptr->isUserMemory_ = false;
				ptr->isBuilding_ = true;
				FileHeader *header = new FileHeader(indexSeq.mem_size());
				header->min_value = minValue;
				header->max_value = maxValue;
				header->index_mem_size = indexSeq.mem_size();
				header->key_length = ks.maxKeyLen;
				/*
				 * For one huge index, we'll split it into multipart-index for the sake of RAM, 
				 * and each sub-index could have longer commonPrefix compared with ks.commonPrefix.
				 * what's more, under such circumstances, ks.commonPrefix may have been rewritten
				 * be upper-level builder to '0'
				 */
				/*if (commonPrefixLen > ks.commonPrefixLen) {
					header->common_prefix_length = commonPrefixLen - ks.commonPrefixLen;
					ptr->commonPrefix_.assign(ks.minKey.data(), header->common_prefix_length);
					header->file_size += terark::align_up(header->common_prefix_length, 8);
					}*/
				ptr->header_ = header;
				ptr->indexSeq_.swap(indexSeq);
				//return ptr.release();
				ptr->SaveMmap(write);
			}
			unique_ptr<TerarkIndex> LoadMemory(fstring mem, 
											   const TerarkTableReaderOptions& ropt) const override {
				return unique_ptr<TerarkIndex>(loadImpl(mem, {}, ropt).release());
			}
			unique_ptr<TerarkIndex> LoadFile(fstring fpath,
											 const TerarkTableReaderOptions& ropt) const override {
				return unique_ptr<TerarkIndex>(loadImpl({}, fpath, ropt).release());
			}
			size_t MemSizeForBuild(const TerarkTableBuilderOptions& tbo, 
								   const KeyStat& ks) const override {
				size_t length = ks.maxKeyLen - fstring(ks.minKey).commonPrefixLen(ks.maxKey);
				uint64_t
					minValue = ReadUint64(tbo.wt_session, ks.minKey),
					maxValue = ReadUint64(tbo.wt_session, ks.maxKey);
				if (minValue > maxValue) {
					std::swap(minValue, maxValue);
				}
				uint64_t diff = maxValue - minValue + 1;
				return size_t(std::ceil(diff * 1.25 / 8));
			}
		protected:
			unique_ptr<TerarkUintIndex<RankSelect>> loadImpl(fstring mem, fstring fpath,
															 const TerarkTableReaderOptions& ropt) const {
				unique_ptr<TerarkUintIndex<RankSelect>> ptr(new TerarkUintIndex<RankSelect>(ropt));
				ptr->isUserMemory_ = false;
				ptr->isBuilding_ = false;

				if (mem.data() == nullptr) {
					MmapWholeFile(fpath).swap(ptr->file_);
					mem = {(const char*)ptr->file_.base, (ptrdiff_t)ptr->file_.size};
				} else {
					ptr->isUserMemory_ = true;
				}
				const FileHeader* header = (const FileHeader*)mem.data();

				if (mem.size() < sizeof(FileHeader)
					|| header->magic_len != strlen(index_name)
					|| strcmp(header->magic, index_name) != 0
					|| header->header_size != sizeof(FileHeader)
					|| header->version != 1
					|| header->file_size != mem.size()
					) {
					return nullptr;
				}
				size_t name_i = g_TerarkIndexName.find_i(typeid(TerarkUintIndex<RankSelect>).name());
				if (strcmp(header->class_name, g_TerarkIndexName.val(name_i).c_str()) != 0) {
					return nullptr;
				}
				ptr->header_ = header;
				ptr->minValue_ = header->min_value;
				ptr->maxValue_ = header->max_value;
				ptr->keyLength_ = header->key_length;
				/*
				  ptr->commonPrefix_.risk_set_data((char*)mem.data() + header->header_size,
													 header->common_prefix_length);
				ptr->indexSeq_.risk_mmap_from((unsigned char*)mem.data() + header->header_size
											  + terark::align_up(header->common_prefix_length, 8), header->index_mem_size);
				*/
				ptr->indexSeq_.risk_mmap_from((unsigned char*)mem.data() + header->header_size, header->index_mem_size);
				return ptr;
			}
		};
		using TerarkIndex::FactoryPtr;
		TerarkUintIndex(const TerarkTableReaderOptions& ropt) : TerarkIndex(ropt) {}
		virtual ~TerarkUintIndex() {
			if (isBuilding_) {
				delete (FileHeader*)header_;
			} else if (file_.base != nullptr || isUserMemory_) {
				indexSeq_.risk_release_ownership();
				commonPrefix_.risk_release_ownership();
			}
		}
		const char* Name() const override {
			return header_->class_name;
		}
		void SaveMmap(std::function<void(const void *, size_t)> write) const override {
			write(header_, sizeof *header_);
			if (!commonPrefix_.empty()) {
				write(commonPrefix_.data(), terark::align_up(commonPrefix_.size(), 8));
			}
			write(indexSeq_.data(), indexSeq_.mem_size());
		}
		size_t Find(fstring key) const override {
			uint64_t findValue = ReadUint64(reader_options_.wt_session, key.begin());
			printf("findval %lld, key %s\n", findValue, key);
			if (findValue < minValue_ || findValue > maxValue_) {
				return size_t(-1);
			}
			uint64_t findPos = findValue - minValue_;
			if (!indexSeq_[findPos]) {
				return size_t(-1);
			}
			return indexSeq_.rank1(findPos);
		}
		size_t NumKeys() const override {
			return indexSeq_.max_rank1();
		}
		size_t TotalKeySize() const override final {
			return (commonPrefix_.size() + keyLength_) * indexSeq_.max_rank1();
		}
		fstring Memory() const override {
			return fstring((const char*)header_, (ptrdiff_t)header_->file_size);
		}
		Iterator* NewIterator() const override {
			return new UIntIndexIterator(*this);
		}
		bool NeedsReorder() const override {
			return false;
		}
		void GetOrderMap(UintVecMin0& newToOld) const override {
			assert(false);
		}
		void BuildCache(double cacheRatio) override {
			//do nothing
		}

		static uint64_t ReadUint64(WT_SESSION* wt_session, valvec<byte_t>& buf) {
			uint64_t val;
			wiredtiger_struct_unpack(wt_session, buf.begin(), buf.size(), "Q", &val);
			return val;
		}
		static uint64_t ReadUint64(WT_SESSION* wt_session, fstring buf) {
			uint64_t val;
			wiredtiger_struct_unpack(wt_session, buf.begin(), buf.size(), "Q", &val);
			return val;
		}
		static void AssignUint64(WT_SESSION* wt_session, uint64_t i, valvec<byte_t>& buf) {
			buf.resize(8);
			wiredtiger_struct_pack(wt_session, buf.begin(), 8, "Q", i); 
		}


	protected:
		const FileHeader* header_;
		MmapWholeFile     file_;
		valvec<char>      commonPrefix_;
		RankSelect        indexSeq_;
		uint64_t          minValue_;
		uint64_t          maxValue_;
		uint32_t          keyLength_;
		bool              isUserMemory_;
		bool              isBuilding_;
		//WT_SESSION*       wt_session_;
	};
	template<class RankSelect>
	const char* TerarkUintIndex<RankSelect>::index_name = "UintIndex";

	/*
	 * index register part
	 */
	typedef NestLoudsTrieDAWG_IL_256 NestLoudsTrieDAWG_IL_256_32;
	typedef NestLoudsTrieDAWG_SE_512 NestLoudsTrieDAWG_SE_512_32;
	typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_SE_512_32> TerocksIndex_NestLoudsTrieDAWG_SE_512_32;
	typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_IL_256_32> TerocksIndex_NestLoudsTrieDAWG_IL_256_32;
	typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_SE_512> TerocksIndex_NestLoudsTrieDAWG_Mixed_SE_512;
	typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_IL_256> TerocksIndex_NestLoudsTrieDAWG_Mixed_IL_256;
	typedef NestLoudsTrieIndex<NestLoudsTrieDAWG_Mixed_XL_256> TerocksIndex_NestLoudsTrieDAWG_Mixed_XL_256;
	TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_SE_512_32, "NestLoudsTrieDAWG_SE_512", "SE_512_32", "SE_512");
	TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_IL_256_32, "NestLoudsTrieDAWG_IL_256", "IL_256_32", "IL_256", "NestLoudsTrieDAWG_IL");
	TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_Mixed_SE_512, "NestLoudsTrieDAWG_Mixed_SE_512", "Mixed_SE_512");
	TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_Mixed_IL_256, "NestLoudsTrieDAWG_Mixed_IL_256", "Mixed_IL_256");
	TerarkIndexRegister(TerocksIndex_NestLoudsTrieDAWG_Mixed_XL_256, "NestLoudsTrieDAWG_Mixed_XL_256", "Mixed_XL_256");

	typedef TerarkUintIndex<terark::rank_select_il_256_32> TerarkUintIndex_IL_256_32;
	typedef TerarkUintIndex<terark::rank_select_se_256_32> TerarkUintIndex_SE_256_32;
	typedef TerarkUintIndex<terark::rank_select_se_512_32> TerarkUintIndex_SE_512_32;
	typedef TerarkUintIndex<terark::rank_select_se_512_64> TerarkUintIndex_SE_512_64;
	TerarkIndexRegister(TerarkUintIndex_IL_256_32, "UintIndex_IL_256_32", "UintIndex");
	TerarkIndexRegister(TerarkUintIndex_SE_256_32, "UintIndex_SE_256_32");
	TerarkIndexRegister(TerarkUintIndex_SE_512_32, "UintIndex_SE_512_32");
	TerarkIndexRegister(TerarkUintIndex_SE_512_64, "UintIndex_SE_512_64");

} // namespace
