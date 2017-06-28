
// std headers
#include <future>
#include <cfloat>
// boost headers
#include <boost/scope_exit.hpp>
// rocksdb headers
#include <rocksdb/merge_operator.h>
#include <table/meta_blocks.h>
// wiredtiger headers
#include "wiredtiger.h"
// terark headers
#include <terark/util/sortable_strvec.hpp>
// project headers
#include "terark_zip_table_builder.h"
#include "trk_block_builder.h"
#include "trk_format.h"
#include "trk_meta_blocks.h"
#include "trk_table_properties.h"

namespace rocksdb {

#ifdef TERARK_ZIP_TRIAL_VERSION
	const char g_trail_rand_delete[] = "TERARK_ZIP_TRIAL_VERSION random deleted this row";
#endif

	using terark::SortableStrVec;
	using terark::byte_swap;

	std::mutex g_sumMutex;
	size_t g_sumKeyLen = 0;
	size_t g_sumValueLen = 0;
	size_t g_sumUserKeyLen = 0;
	size_t g_sumUserKeyNum = 0;
	size_t g_sumEntryNum = 0;
	long long g_lastTime = g_pf.now();


#if defined(DEBUG_TWO_PASS_ITER) && !defined(NDEBUG)

	void DEBUG_PRINT_KEY(const char* first_or_second, rocksdb::Slice key) {
		rocksdb::ParsedInternalKey ikey;
		rocksdb::ParseInternalKey(key, &ikey);
		fprintf(stderr, "DEBUG: %s pass => %s\n", first_or_second, ikey.DebugString(true).c_str());
	}

#define DEBUG_PRINT_1ST_PASS_KEY(key) DEBUG_PRINT_KEY("1st", key);
#define DEBUG_PRINT_2ND_PASS_KEY(key) DEBUG_PRINT_KEY("2nd", key);

#else

	void DEBUG_PRINT_KEY(...) {}

#define DEBUG_PRINT_1ST_PASS_KEY(...) DEBUG_PRINT_KEY(__VA_ARGS__);
#define DEBUG_PRINT_2ND_PASS_KEY(...) DEBUG_PRINT_KEY(__VA_ARGS__);

#endif

	template<class ByteArray>
	static
	Status WriteBlock(const ByteArray& blockData, WritableFileWriter* file,
					  uint64_t* offset, TerarkBlockHandle* block_handle) {
		block_handle->set_offset(*offset);
		block_handle->set_size(blockData.size());
		Status s = file->Append(SliceOf(blockData));
		if (s.ok()) {
			*offset += blockData.size();
		}
		return s;
	}

	namespace {
		struct PendingTask {
			const TerarkZipTableBuilder* tztb;
			long long startTime;
		};
		
		const WT_ITEM kTombstone = { "\x14\x14", 2, 0, NULL, 0 };
		bool IsDeleted(WT_ITEM* item) {
			return (item->size == kTombstone.size &&
				memcmp(item->data, kTombstone.data, kTombstone.size) == 0);
		}
		bool IsDeleted(const Slice& slice) {
			return (slice.size() == kTombstone.size &&
					memcmp(slice.data(), kTombstone.data, kTombstone.size) == 0);
		}
	}

	TerarkZipTableBuilder::TerarkZipTableBuilder(const TerarkZipTableOptions& tzto,
		const TerarkTableBuilderOptions& tbo,
		uint32_t column_family_id,
		WritableFileWriter* file,
		size_t key_prefixLen)
		: table_options_(tzto)
		, ioptions_(tbo)
		, key_prefixLen_(key_prefixLen)
		, chunk_state_(kJustCreated) {
		properties_.fixed_key_len = 0;
		properties_.num_data_blocks = 1;
		properties_.column_family_id = 0;
		properties_.column_family_name = "nullptr";
		properties_.comparator_name = ioptions_.internal_comparator.Name() ?
			tbo.internal_comparator.Name() : "nullptr";
		properties_.merge_operator_name = "nullptr";
		properties_.compression_name = "nullptr";
		properties_.prefix_extractor_name = "nullptr";

		isReverseBytewiseOrder_ =
			fstring(properties_.comparator_name).startsWith("rev:");

		/*if (tbo.int_tbl_prop_collector_factories) {
		  const auto& factories = *tbo.int_tbl_prop_collector_factories;
		  collectors_.resize(factories.size());
		  auto cfId = properties_.column_family_id;
		  for (size_t i = 0; i < collectors_.size(); ++i) {
		  collectors_[i].reset(factories[i]->CreateIntTblPropCollector(cfId));
		  }
		  }*/
		properties_.property_collectors_names = "[]";

		file_ = file;
		sampleUpperBound_ = randomGenerator_.max() * table_options_.sampleRatio;
		tmpValueFile_.path = tzto.localTempDir + "/Terark-XXXXXX";
		tmpValueFile_.open_temp();
		tmpKeyFile_.path = tmpValueFile_.path + ".keydata";
		tmpKeyFile_.open();
		tmpSampleFile_.path = tmpValueFile_.path + ".sample";
		tmpSampleFile_.open();
		if (table_options_.debugLevel == 4) {
			tmpDumpFile_.open(tmpValueFile_.path + ".dump", "wb+");
		}
	}

	DictZipBlobStore::ZipBuilder*
	TerarkZipTableBuilder::createZipBuilder() const {
		DictZipBlobStore::Options dzopt;
		dzopt.entropyAlgo = DictZipBlobStore::Options::EntropyAlgo(table_options_.entropyAlgo);
		dzopt.checksumLevel = table_options_.checksumLevel;
		dzopt.useSuffixArrayLocalMatch = table_options_.useSuffixArrayLocalMatch;
		return DictZipBlobStore::createZipBuilder(dzopt);
	}

	TerarkZipTableBuilder::~TerarkZipTableBuilder() {}

	uint64_t TerarkZipTableBuilder::FileSize() const {
		if (0 == offset_) {
			// for compaction caller to split file by increasing size
			auto kvLen = properties_.raw_key_size + properties_.raw_value_size;
			auto fsize = uint64_t(kvLen * table_options_.estimateCompressionRatio);
			if (terark_unlikely(histogram_.empty())) {
				return fsize;
			}
			size_t dictZipMemSize = std::min<size_t>(sampleLenSum_, INT32_MAX) * 6;
			size_t nltTrieMemSize = 0;
			for (auto& item : histogram_) {
				nltTrieMemSize = std::max(nltTrieMemSize,
										  item.stat.sumKeyLen + sizeof(SortableStrVec::SEntry) * item.stat.numKeys);
			}
			size_t peakMemSize = std::max(dictZipMemSize, nltTrieMemSize);
			if (peakMemSize < table_options_.softZipWorkingMemLimit) {
				return fsize;
			} else {
				return fsize * 5; // notify rocksdb to `Finish()` this table asap.
			}
		} else {
			return offset_;
		}
	}


	void TerarkZipTableBuilder::Add(const Slice& key, const Slice& value) {
		if (table_options_.debugLevel == 4) {
			//rocksdb::ParsedInternalKey ikey;
			//rocksdb::ParseInternalKey(key, &ikey);
			//fprintf(tmpDumpFile_.fp(), "DEBUG: 1st pass => %s / %s \n", ikey.DebugString(true).c_str(), value.ToString(true).c_str());
			fprintf(tmpDumpFile_.fp(), "DEBUG: 1st pass => %s / %s \n", key.data(), value.data());
		}
		DEBUG_PRINT_1ST_PASS_KEY(key);
		uint64_t offset = uint64_t((properties_.raw_key_size + properties_.raw_value_size)
								   * table_options_.estimateCompressionRatio);
		fstring userKey(key.data(), key.size());
		if (terark_likely(!histogram_.empty())) {
			auto& keyStat = histogram_.back().stat;
			assert((prevUserKey_ < userKey) ^ isReverseBytewiseOrder_);
			keyStat.commonPrefixLen = fstring(prevUserKey_.data(), keyStat.commonPrefixLen)
				.commonPrefixLen(userKey);
			keyStat.minKeyLen = std::min(userKey.size(), keyStat.minKeyLen);
			keyStat.maxKeyLen = std::max(userKey.size(), keyStat.maxKeyLen);
			AddPrevUserKey();
			prevUserKey_.assign(userKey);
		} else {
			if (terark_unlikely(histogram_.empty())) {
				t0 = g_pf.now();
			} else {
				AddPrevUserKey(true);
			}
			histogram_.emplace_back();
			auto& currentHistogram = histogram_.back();
			//currentHistogram.prefix.assign(userKey.data(), key_prefixLen_);
			currentHistogram.prefix.emplace_back();
			currentHistogram.stat.commonPrefixLen = userKey.size();
			currentHistogram.stat.minKeyLen = userKey.size();
			currentHistogram.stat.maxKeyLen = userKey.size();
			currentHistogram.stat.sumKeyLen = 0;
			currentHistogram.stat.numKeys = 0;
			currentHistogram.stat.minKey.assign(userKey);
			prevUserKey_.assign(userKey);
		}
		//value_.assign(value.data(), value.size());
		valueBuf_.emplace_back(value.data(), value.size());
		if (!value.empty() && randomGenerator_() < sampleUpperBound_) {
			tmpSampleFile_.writer << fstringOf(value);
			sampleLenSum_ += value.size();
		}
		//tmpValueFile_.writer.ensureWrite(valueBuf_.back().first, 8);
		tmpValueFile_.writer << fstringOf(value);

		properties_.num_entries++;
		properties_.raw_key_size += key.size();
		properties_.raw_value_size += value.size();
	}


	Status TerarkZipTableBuilder::EmptyTableFinish() {
		//INFO(ioptions_.info_log
		//	 , "TerarkZipTableBuilder::EmptyFinish():this=%p\n", this);
		offset_ = 0;
		TerarkBlockHandle emptyTableBH;
		Status s = WriteBlock(Slice("Empty"), file_, &offset_, &emptyTableBH);
		if (!s.ok()) {
			return s;
		}
		return WriteMetaData({
				{ &kTerarkEmptyTableKey                         , emptyTableBH  },
			   });
	}


	Status TerarkZipTableBuilder::Finish() {
		assert(!closed_);
		closed_ = true;

		if (histogram_.empty()) {
			return EmptyTableFinish();
		}

		AddPrevUserKey(true);
		for (auto& item : histogram_) {
			item.key.finish();
			item.value.finish();
			if (item.stat.numKeys == 1) {
				assert(item.stat.commonPrefixLen == item.stat.sumKeyLen);
				item.stat.commonPrefixLen = 0; // to avoid empty nlt trie
			}
		}
		tmpKeyFile_.complete_write();
		tmpValueFile_.complete_write();
		tmpSampleFile_.complete_write();
		{
			long long rawBytes = properties_.raw_key_size + properties_.raw_value_size;
			long long tt = g_pf.now();
			//INFO(ioptions_.info_log
			//	 , "TerarkZipTableBuilder::Finish():this=%p:  first pass time =%7.2f's, %8.3f'MB/sec\n"
			//	 , this, g_pf.sf(t0, tt), rawBytes*1.0 / g_pf.uf(t0, tt)
			//	 );
		}
		static std::mutex zipMutex;
		static std::condition_variable zipCond;
		static valvec<PendingTask> waitQueue;
		static size_t sumWaitingMem = 0;
		static size_t sumWorkingMem = 0;
		const  size_t softMemLimit = table_options_.softZipWorkingMemLimit;
		const  size_t hardMemLimit = std::max(table_options_.hardZipWorkingMemLimit, softMemLimit);
		const  size_t smallmem = table_options_.smallTaskMemory;
		const  long long myStartTime = g_pf.now();
		{
			std::unique_lock<std::mutex> zipLock(zipMutex);
			waitQueue.push_back({this, myStartTime});
		}
		auto waitForMemory = [&](size_t myWorkMem, const char* who) {
			const std::chrono::seconds waitForTime(10);
			long long now = myStartTime;
			auto shouldWait = [&]() {
				bool w;
				if (myWorkMem < softMemLimit) {
					w = (sumWorkingMem + myWorkMem >= hardMemLimit) ||
						(sumWorkingMem + myWorkMem >= softMemLimit && myWorkMem >= smallmem);
				} else {
					w = sumWorkingMem > softMemLimit / 4;
				}
				if (!w) {
					assert(!waitQueue.empty());
					now = g_pf.now();
					if (myWorkMem < smallmem) {
						return false; // do not wait
					}
					if (sumWaitingMem + sumWorkingMem < softMemLimit) {
						return false; // do not wait
					}
					if (waitQueue.size() == 1) {
						assert(this == waitQueue[0].tztb);
						return false; // do not wait
					}
					size_t minRateIdx = size_t(-1);
					double minRateVal = DBL_MAX;
					auto wq = waitQueue.data();
					for (size_t i = 0, n = waitQueue.size(); i < n; ++i) {
						double rate = myWorkMem / (0.1 + now - wq[i].startTime);
						if (rate < minRateVal) {
							minRateVal = rate;
							minRateIdx = i;
						}
					}
					if (this == wq[minRateIdx].tztb) {
						return false; // do not wait
					}
				}
				return true; // wait
			};
			std::unique_lock<std::mutex> zipLock(zipMutex);
			if (myWorkMem > smallmem / 2) { // never wait for very smallmem(SST flush)
				sumWaitingMem += myWorkMem;
				while (shouldWait()) {
					/*INFO(ioptions_.info_log
						 , "TerarkZipTableBuilder::Finish():this=%p: sumWaitingMem = %f GB, sumWorkingMem = %f GB, %s WorkingMem = %f GB, wait...\n"
						 , this, sumWaitingMem / 1e9, sumWorkingMem / 1e9, who, myWorkMem / 1e9
						 );*/
					zipCond.wait_for(zipLock, waitForTime);
				}
				/*INFO(ioptions_.info_log
					 , "TerarkZipTableBuilder::Finish():this=%p: sumWaitingMem = %f GB, sumWorkingMem = %f GB, %s WorkingMem = %f GB, waited %8.3f sec, Key+Value bytes = %f GB\n"
					 , this, sumWaitingMem / 1e9, sumWorkingMem / 1e9, who, myWorkMem / 1e9
					 , g_pf.sf(myStartTime, now)
					 , (properties_.raw_key_size + properties_.raw_value_size) / 1e9
					 );*/
				sumWaitingMem -= myWorkMem;
			}
			sumWorkingMem += myWorkMem;
		};
		// indexing is also slow, run it in parallel
		AutoDeleteFile tmpIndexFile{tmpValueFile_.path + ".index"};
		std::future<void> asyncIndexResult = 
			std::async(std::launch::async, [&]() {
					size_t fileOffset = 0;
					FileStream writer(tmpIndexFile, "wb+");
					NativeDataInput<InputBuffer> tempKeyFileReader(&tmpKeyFile_.fp);
					for (size_t i = 0; i < histogram_.size(); ++i) {
						auto& keyStat = histogram_[i].stat;
						auto factory = TerarkIndex::SelectFactory(keyStat, table_options_.indexType);
						if (!factory) {
							THROW_STD(invalid_argument,
									  "invalid indexType: %s", table_options_.indexType.c_str());
						}
						const size_t myWorkMem = factory->MemSizeForBuild(keyStat);
						waitForMemory(myWorkMem, "nltTrie");
						BOOST_SCOPE_EXIT(myWorkMem) {
							std::unique_lock<std::mutex> zipLock(zipMutex);
							assert(sumWorkingMem >= myWorkMem);
							sumWorkingMem -= myWorkMem;
							zipCond.notify_all();
						}BOOST_SCOPE_EXIT_END;

						long long t1 = g_pf.now();
						histogram_[i].keyFileBegin = fileOffset;
						factory->Build(tempKeyFileReader, table_options_, [&fileOffset, &writer](const void* data, size_t size) {
								fileOffset += size;
								writer.ensureWrite(data, size);
							}, keyStat);
						histogram_[i].keyFileEnd = fileOffset;
						assert((fileOffset - histogram_[i].keyFileBegin) % 8 == 0);
						long long tt = g_pf.now();
						/*INFO(ioptions_.info_log
							 , "TerarkZipTableBuilder::Finish():this=%p:  index pass time =%7.2f's, %8.3f'MB/sec\n"
							 , this, g_pf.sf(t1, tt), properties_.raw_key_size*1.0 / g_pf.uf(t1, tt)
							 );*/
					}
					tmpKeyFile_.close();
				});
		size_t myDictMem = std::min<size_t>(sampleLenSum_, INT32_MAX) * 6;
		waitForMemory(myDictMem, "dictZip");

		BOOST_SCOPE_EXIT(&myDictMem) {
			std::unique_lock<std::mutex> zipLock(zipMutex);
			assert(sumWorkingMem >= myDictMem);
			// if success, myDictMem is 0, else sumWorkingMem should be restored
			sumWorkingMem -= myDictMem;
			zipCond.notify_all();
		}BOOST_SCOPE_EXIT_END;

		auto waitIndex = [&]() {
			{
				std::unique_lock<std::mutex> zipLock(zipMutex);
				sumWorkingMem -= myDictMem;
			}
			myDictMem = 0; // success, set to 0
			asyncIndexResult.get();
			std::unique_lock<std::mutex> zipLock(zipMutex);
			waitQueue.trim(std::remove_if(waitQueue.begin(), waitQueue.end(),
										  [this](PendingTask x) {return this == x.tztb; }));
		};
		return ZipValueToFinish(tmpIndexFile, waitIndex);
	}

	Status
	TerarkZipTableBuilder::
	ZipValueToFinish(fstring tmpIndexFile, std::function<void()> waitIndex) {
		DebugPrepare();
		assert(histogram_.size() == 1);
		AutoDeleteFile tmpStoreFile{tmpValueFile_.path + ".zbs"};
		NativeDataInput<InputBuffer> input(&tmpValueFile_.fp);
		auto& kvs = histogram_.front();
		terark::BlobStore::Dictionary dict;
		std::unique_ptr<DictZipBlobStore::ZipBuilder> zbuilder;
		std::unique_ptr<terark::BlobStore> store;
		DictZipBlobStore::ZipStat dzstat;
		long long t3, t4;
		{
			t3 = g_pf.now();
			zbuilder = UniquePtrOf(this->createZipBuilder());
			{
				valvec<byte_t> sample;
				NativeDataInput<InputBuffer> input(&tmpSampleFile_.fp);
				size_t realsampleLenSum = 0;
				if (sampleLenSum_ < INT32_MAX) {
					for (size_t len = 0; len < sampleLenSum_; ) {
						input >> sample;
						zbuilder->addSample(sample);
						len += sample.size();
					}
					realsampleLenSum = sampleLenSum_;
				} else {
					uint64_t upperBound2 = uint64_t(randomGenerator_.max() * double(INT32_MAX) / sampleLenSum_);
					for (size_t len = 0; len < sampleLenSum_; ) {
						input >> sample;
						if (randomGenerator_() < upperBound2) {
							zbuilder->addSample(sample);
							realsampleLenSum += sample.size();
						}
						len += sample.size();
					}
				}
				tmpSampleFile_.close();
				if (0 == realsampleLenSum) { // prevent from empty
					zbuilder->addSample("Hello World!");
				}
				zbuilder->finishSample();
				zbuilder->prepare(kvs.stat.numKeys, tmpStoreFile);
			}

			BuilderWriteValues(input, kvs, [&](fstring value) {zbuilder->addRecord(value); });

			DictZipBlobStore* zstore;
			store.reset(zstore = zbuilder->finish(DictZipBlobStore::ZipBuilder::FinishFreeDict));
			dzstat = zbuilder->getZipStat();

			t4 = g_pf.now();
			dict = zbuilder->getDictionary();
		}
		DebugCleanup();
		// wait for indexing complete, if indexing is slower than value compressing
		waitIndex();
		return WriteSSTFile(t3, t4, tmpIndexFile, store.get(), dict, dzstat);
	}


	void TerarkZipTableBuilder::DebugPrepare() {
	}

	void TerarkZipTableBuilder::DebugCleanup() {
		if (tmpDumpFile_.isOpen()) {
			tmpDumpFile_.close();
		}
		tmpValueFile_.close();
	}

	void
	TerarkZipTableBuilder::BuilderWriteValues(NativeDataInput<InputBuffer>& input, 
		KeyValueStatus& kvs, std::function<void(fstring)> write) {
		auto& bzvType = kvs.type;
		bzvType.resize(kvs.stat.numKeys);
		valvec<byte_t> value;
		size_t entryId = 0;
		for (size_t recId = 0; recId < kvs.stat.numKeys; recId++) {
			/*
			  uint64_t seqType = input.load_as<uint64_t>();
			  uint64_t seqNum;
			  ValueType vType;
			  UnPackSequenceAndType(seqType, &seqNum, &vType);
			  size_t oneSeqLen = 1;
			  assert(oneSeqLen >= 1);
			  if (1 == oneSeqLen && (kTypeDeletion == vType || kTypeValue == vType)) {
			  if (kTypeValue == vType) {
			  bzvType.set0(recId, size_t(ZipValueType::kValue));
			  } else {
			  bzvType.set0(recId, size_t(ZipValueType::kDelete));
			  }
			  value.erase_all();
			  value.append((byte_t*)&seqNum, 7);
			  input.load_add(value);
			  }*/
			{
				value.erase_all();
				input.load_add(value);
				if (!IsDeleted(SliceOf(value))) {
					bzvType.set0(recId, size_t(ZipValueType::kValue));
				} else {
					bzvType.set0(recId, size_t(ZipValueType::kDelete));
				}
			}
			write(value);
			entryId += 1;
		}
		// tmpValueFile_ ignore kTypeRangeDeletion keys
		// so entryId may less than properties_.num_entries
		assert(entryId <= properties_.num_entries);
	}

	Status TerarkZipTableBuilder::WriteStore(TerarkIndex* index, terark::BlobStore* store,
		KeyValueStatus& kvs, std::function<void(const void*, size_t)> writeAppend,
		TerarkBlockHandle& dataBlock,
		long long& t5, long long& t6, long long& t7) {
		auto& keyStat = kvs.stat;
		auto& bzvType = kvs.type;
		if (index->NeedsReorder()) {
			bitfield_array<2> zvType2(keyStat.numKeys);
			terark::AutoFree<uint32_t> newToOld(keyStat.numKeys, UINT32_MAX);
			index->GetOrderMap(newToOld.p);
			t6 = g_pf.now();
			if (fstring(ioptions_.internal_comparator.Name()).startsWith("rev:")) {
				// Damn reverse bytewise order
				for (size_t newId = 0; newId < keyStat.numKeys; ++newId) {
					size_t dictOrderOldId = newToOld.p[newId];
					size_t reverseOrderId = keyStat.numKeys - dictOrderOldId - 1;
					newToOld.p[newId] = reverseOrderId;
					zvType2.set0(newId, bzvType[reverseOrderId]);
				}
			} else {
				for (size_t newId = 0; newId < keyStat.numKeys; ++newId) {
					size_t dictOrderOldId = newToOld.p[newId];
					zvType2.set0(newId, bzvType[dictOrderOldId]);
				}
			}
			t7 = g_pf.now();
			try {
				dataBlock.set_offset(offset_);
				store->reorder_zip_data(newToOld, std::ref(writeAppend));
				dataBlock.set_size(offset_ - dataBlock.offset());
			} catch (const Status& s) {
				return s;
			}
			bzvType.clear();
			bzvType.swap(zvType2);
		} else {
			if (fstring(ioptions_.internal_comparator.Name()).startsWith("rev:")) {
				bitfield_array<2> zvType2(keyStat.numKeys);
				terark::AutoFree<uint32_t> newToOld(keyStat.numKeys);
				t6 = g_pf.now();
				for (size_t newId = 0, oldId = keyStat.numKeys - 1; newId < keyStat.numKeys;
					 ++newId, --oldId) {
					newToOld.p[newId] = oldId;
					zvType2.set0(newId, bzvType[oldId]);
				}
				t7 = g_pf.now();
				try {
					dataBlock.set_offset(offset_);
					store->reorder_zip_data(newToOld, std::ref(writeAppend));
					dataBlock.set_size(offset_ - dataBlock.offset());
				} catch (const Status& s) {
					return s;
				}
				bzvType.clear();
				bzvType.swap(zvType2);
			} else {
				t7 = t6 = t5;
				try {
					dataBlock.set_offset(offset_);
					store->save_mmap(std::ref(writeAppend));
					dataBlock.set_size(offset_ - dataBlock.offset());
				} catch (const Status& s) {
					return s;
				}
			}
		}
		return Status::OK();
	}

	Status TerarkZipTableBuilder::WriteSSTFile(long long t3, long long t4,
		fstring tmpIndexFile, terark::BlobStore* zstore,
		terark::BlobStore::Dictionary dict,
		const DictZipBlobStore::ZipStat& dzstat) {
		assert(histogram_.size() == 1);
		auto& kvs = histogram_.front();
		auto& keyStat = kvs.stat;
		auto& bzvType = kvs.type;
		const size_t realsampleLenSum = dict.memory.size();
		long long rawBytes = properties_.raw_key_size + properties_.raw_value_size;
		long long t5 = g_pf.now();
		std::unique_ptr<TerarkIndex> index(TerarkIndex::LoadFile(tmpIndexFile));
		printf("index->keys: %d, keyStat->keys: %d\n", index->NumKeys(), keyStat.numKeys);
		//sleep(1000);
		assert(index->NumKeys() == keyStat.numKeys);
		Status s;
		TerarkBlockHandle dataBlock, dictBlock, indexBlock, zvTypeBlock(0, 0);
		TerarkBlockHandle commonPrefixBlock;
		{
			size_t real_size = index->Memory().size() + zstore->mem_size() + bzvType.mem_size();
			size_t block_size, last_allocated_block;
			file_->writable_file()->GetPreallocationStatus(&block_size, &last_allocated_block);
			/*INFO(ioptions_.info_log
				 , "TerarkZipTableBuilder::Finish():this=%p: old prealloc_size = %zd, real_size = %zd\n"
				 , this, block_size, real_size
				 );*/
			file_->writable_file()->SetPreallocationBlockSize(1 * 1024 * 1024 + real_size);
		}
		long long t6, t7;
		offset_ = 0;
		auto writeAppend = [&](const void* data, size_t size) {
			s = file_->Append(Slice((const char*)data, size));
			if (!s.ok()) {
				throw s;
			}
			offset_ += size;
		};
		s = WriteStore(index.get(), zstore, kvs, writeAppend, dataBlock, t5, t6, t7);
		if (!s.ok()) {
			return s;
		}

		std::string commonPrefix;
		commonPrefix.reserve(key_prefixLen_ + keyStat.commonPrefixLen);
		commonPrefix.append(kvs.prefix.data(), kvs.prefix.size());
		commonPrefix.append((const char*)prevUserKey_.data(), keyStat.commonPrefixLen);
		WriteBlock(commonPrefix, file_, &offset_, &commonPrefixBlock);
		properties_.data_size = dataBlock.size();

		s = WriteBlock(dict.memory, file_, &offset_, &dictBlock);
		if (!s.ok()) {
			return s;
		}

		s = WriteBlock(index->Memory(), file_, &offset_, &indexBlock);
		if (!s.ok()) {
			return s;
		}

		/*if (zeroSeqCount_ != bzvType.size()) {
			assert(zeroSeqCount_ < bzvType.size());
			fstring zvTypeMem(bzvType.data(), bzvType.mem_size());
			s = WriteBlock(zvTypeMem, file_, &offset_, &zvTypeBlock);
			if (!s.ok()) {
				return s;
			}
			}*/
		index.reset();
		properties_.index_size = indexBlock.size();

		WriteMetaData({
				{ dict.memory.size() ? &kTerarkZipTableValueDictBlock : NULL   , dictBlock         },
				{ &kTerarkZipTableIndexBlock                                   , indexBlock        },
				{ !zvTypeBlock.IsNull() ? &kTerarkZipTableValueTypeBlock : NULL, zvTypeBlock       },
				{ &kTerarkZipTableCommonPrefixBlock                            , commonPrefixBlock },
			});

		long long t8 = g_pf.now();
		{
			std::unique_lock<std::mutex> lock(g_sumMutex);
			g_sumKeyLen += properties_.raw_key_size;
			g_sumValueLen += properties_.raw_value_size;
			g_sumUserKeyLen += keyStat.sumKeyLen;
			g_sumUserKeyNum += keyStat.numKeys;
			g_sumEntryNum += properties_.num_entries;
		}
		/*INFO(ioptions_.info_log,
			 "TerarkZipTableBuilder::Finish():this=%p: second pass time =%7.2f's, %8.3f'MB/sec, value only(%4.1f%% of KV)\n"
			 "   wait indexing time = %7.2f's,\n"
			 "  remap KeyValue time = %7.2f's, %8.3f'MB/sec (all stages of remap)\n"
			 "    Get OrderMap time = %7.2f's, %8.3f'MB/sec (index lex order gen)\n"
			 "  rebuild zvType time = %7.2f's, %8.3f'MB/sec\n"
			 "  write SST data time = %7.2f's, %8.3f'MB/sec\n"
			 "    z-dict build time = %7.2f's, sample length = %7.3f'MB, throughput = %6.3f'MB/sec\n"
			 "    zip my value time = %7.2f's, unzip  length = %7.3f'GB\n"
			 "    zip my value throughput = %7.3f'MB/sec\n"
			 "    zip pipeline throughput = %7.3f'MB/sec\n"
			 "    entries = %zd  keys = %zd  avg-key = %.2f  avg-zkey = %.2f  avg-val = %.2f  avg-zval = %.2f\n"
			 "    UnZipSize{ index =%9.4f GB  value =%9.4f GB   all =%9.4f GB }\n"
			 "    __ZipSize{ index =%9.4f GB  value =%9.4f GB   all =%9.4f GB }\n"
			 "    UnZip/Zip{ index =%9.4f     value =%9.4f      all =%9.4f    }\n"
			 "    Zip/UnZip{ index =%9.4f     value =%9.4f      all =%9.4f    }\n"
			 "----------------------------\n"
			 "    total value len =%12.6f GB     avg =%8.3f KB (by entry num)\n"
			 "    total  key  len =%12.6f GB     avg =%8.3f KB\n"
			 "    total ukey  len =%12.6f GB     avg =%8.3f KB\n"
			 "    total ukey  num =%15.9f Billion\n"
			 "    total entry num =%15.9f Billion\n"
			 "    write speed all =%15.9f MB/sec (with    version num)\n"
			 "    write speed all =%15.9f MB/sec (without version num)"
			 , this, g_pf.sf(t3, t4)
			 , properties_.raw_value_size*1.0 / g_pf.uf(t3, t4)
			 , properties_.raw_value_size*100.0 / rawBytes

			 , g_pf.sf(t4, t5) // wait indexing time
			 , g_pf.sf(t5, t8), double(offset_) / g_pf.uf(t5, t8)

			 , g_pf.sf(t5, t6), properties_.index_size / g_pf.uf(t5, t6) // index lex walk

			 , g_pf.sf(t6, t7), keyStat.numKeys * 2 / 8 / (g_pf.uf(t6, t7) + 1.0) // rebuild zvType

			 , g_pf.sf(t7, t8), double(offset_) / g_pf.uf(t7, t8) // write SST data

			 , dzstat.dictBuildTime, realsampleLenSum / 1e6
			 , realsampleLenSum / dzstat.dictBuildTime / 1e6

			 , dzstat.dictZipTime, properties_.raw_value_size / 1e9
			 , properties_.raw_value_size / dzstat.dictZipTime / 1e6
			 , dzstat.pipelineThroughBytes / dzstat.dictZipTime / 1e6

			 , size_t(properties_.num_entries), keyStat.numKeys
			 , double(keyStat.sumKeyLen) / keyStat.numKeys
			 , double(properties_.index_size) / keyStat.numKeys
			 , double(properties_.raw_value_size) / keyStat.numKeys
			 , double(properties_.data_size) / keyStat.numKeys

			 , keyStat.sumKeyLen / 1e9, properties_.raw_value_size / 1e9, rawBytes / 1e9

			 , properties_.index_size / 1e9, properties_.data_size / 1e9, offset_ / 1e9

			 , double(keyStat.sumKeyLen) / properties_.index_size
			 , double(properties_.raw_value_size) / properties_.data_size
			 , double(rawBytes) / offset_

			 , properties_.index_size / double(keyStat.sumKeyLen)
			 , properties_.data_size / double(properties_.raw_value_size)
			 , offset_ / double(rawBytes)

			 , g_sumValueLen / 1e9, g_sumValueLen / 1e3 / g_sumEntryNum
			 , g_sumKeyLen / 1e9, g_sumKeyLen / 1e3 / g_sumEntryNum
			 , g_sumUserKeyLen / 1e9, g_sumUserKeyLen / 1e3 / g_sumUserKeyNum
			 , g_sumUserKeyNum / 1e9
			 , g_sumEntryNum / 1e9
			 , (g_sumKeyLen + g_sumValueLen) / g_pf.uf(g_lastTime, t8)
			 , (g_sumKeyLen + g_sumValueLen - g_sumEntryNum * 8) / g_pf.uf(g_lastTime, t8)
			 );*/
		return s;
	}


	Status TerarkZipTableBuilder::WriteMetaData(std::initializer_list<std::pair<const std::string*, TerarkBlockHandle> > blocks) {
		TerarkMetaIndexBuilder metaindexBuiler;
		for (const auto& block : blocks) {
			if (block.first) {
				metaindexBuiler.Add(*block.first, block.second);
			}
		}
		{
			TerarkPropertyBlockBuilder propBlockBuilder;
			propBlockBuilder.AddTableProperty(properties_);
			TerarkBlockHandle propBlock;
			Status s = WriteBlock(propBlockBuilder.Finish(), file_, &offset_, &propBlock);
			if (!s.ok()) {
				return s;
			}
			metaindexBuiler.Add(kPropertiesBlock, propBlock);
		}
		TerarkBlockHandle metaindexBlock;
		Status s = WriteBlock(metaindexBuiler.Finish(), file_, &offset_, &metaindexBlock);
		if (!s.ok()) {
			return s;
		}
		TerarkFooter footer(kTerarkZipTableMagicNumber, 0);
		footer.set_metaindex_handle(metaindexBlock);
		footer.set_index_handle(TerarkBlockHandle::NullBlockHandle());
		std::string footer_encoding;
		footer.EncodeTo(&footer_encoding);
		s = file_->Append(footer_encoding);
		if (s.ok()) {
			offset_ += footer_encoding.size();
		}
		return s;
	}

	void TerarkZipTableBuilder::Abandon() {
		closed_ = true;
		tmpKeyFile_.complete_write();
		tmpValueFile_.complete_write();
		tmpSampleFile_.complete_write();
		tmpZipDictFile_.Delete();
		tmpZipValueFile_.Delete();
	}

	void TerarkZipTableBuilder::AddPrevUserKey(bool finish) {
		// update histogram
		size_t valueLen = valueBuf_.strpool.size();
		histogram_.back().value[valueLen]++;
		valueBuf_.erase_all();

		auto& currentHistogram = histogram_.back();
		currentHistogram.key[prevUserKey_.size()]++;
		tmpKeyFile_.writer << prevUserKey_;
		currentHistogram.stat.sumKeyLen += prevUserKey_.size();
		currentHistogram.stat.numKeys++;
		if (finish) {
			currentHistogram.stat.maxKey.assign(prevUserKey_);
		}
		printf("AddPrevUserKey: %*s\n", prevUserKey_.size(), prevUserKey_.data());
	}

}
