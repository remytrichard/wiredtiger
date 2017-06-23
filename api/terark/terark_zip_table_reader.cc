// project headers
#include "terark_zip_table_reader.h"
#include "terark_zip_common.h"
// boost headers
#include <boost/scope_exit.hpp>
// rocksdb headers
//#include <table/internal_iterator.h>
//#include <table/sst_file_writer_collectors.h>
//#include <table/meta_blocks.h>
//#include <table/get_context.h>
#include "rocksdb/iterator.h"
// terark headers
#include <terark/util/crc.hpp>


namespace {
	using namespace rocksdb;

	void SharedBlockCleanupFunction(void* arg1, void* arg2) {
		delete reinterpret_cast<shared_ptr<Block>*>(arg1);
	}


	static void MmapWarmUpBytes(const void* addr, size_t len) {
		auto base = (const byte_t*)(uintptr_t(addr) & uintptr_t(~4095));
		auto size = terark::align_up((size_t(addr) & 4095) + len, 4096);
#ifdef POSIX_MADV_WILLNEED
		posix_madvise((void*)addr, len, POSIX_MADV_WILLNEED);
#endif
		for (size_t i = 0; i < size; i += 4096) {
			volatile byte_t unused = ((const volatile byte_t*)base)[i];
			(void)unused;
		}
	}
	template<class T>
	static void MmapWarmUp(const T* addr, size_t len) {
		MmapWarmUpBytes(addr, sizeof(T)*len);
	}
	static void MmapWarmUp(fstring mem) {
		MmapWarmUpBytes(mem.data(), mem.size());
	}
	template<class Vec>
	static void MmapWarmUp(const Vec& uv) {
		MmapWarmUpBytes(uv.data(), uv.mem_size());
	}
}


namespace rocksdb {

	using terark::BadCrc32cException;
	using terark::byte_swap;
	using terark::BlobStore;

	template<bool reverse>
	class TerarkZipTableIterator : public Iterator, boost::noncopyable {
	protected:
		const TerarkTableReaderOptions* table_reader_options_;
		const TerarkZipSubReader* subReader_;
		unique_ptr<TerarkIndex::Iterator> iter_;
		ParsedInternalKey       pInterKey_;
		std::string             interKeyBuf_;
		valvec<byte_t>          interKeyBuf_xx_;
		valvec<byte_t>          valueBuf_;
		Slice                   userValue_;
		ZipValueType            zValtype_;
		Status                  status_;

	public:
		TerarkZipTableIterator(const TableReaderOptions& tro
							   , const TerarkZipSubReader *subReader)
			: table_reader_options_(&tro)
			, subReader_(subReader) {
			if (subReader_ != nullptr) {
				iter_.reset(subReader_->index_->NewIterator());
				iter_->SetInvalid();
			}
			pInterKey_.user_key = Slice();
			//pInterKey_.sequence = uint64_t(-1);
			pInterKey_.type = kMaxValue;
		}

		bool Valid() const override {
			return iter_->Valid();
		}

		void SeekToFirst() override {
			if (UnzipIterRecord(IndexIterSeekToFirst())) {
				DecodeCurrKeyValue();
			}
		}

		void SeekToLast() override {
			if (UnzipIterRecord(IndexIterSeekToLast())) {
				DecodeCurrKeyValue();
			}
		}

		void SeekForPrev(const Slice& target) override {
			SeekForPrevImpl(target, &table_reader_options_->internal_comparator);
		}

		void Seek(const Slice& target) override {
			ParsedInternalKey pikey;
			if (!ParseInternalKey(target, &pikey)) {
				status_ = Status::InvalidArgument("TerarkZipTableIterator::Seek()",
												  "param target.size() < 8");
				SetIterInvalid();
				return;
			}
			SeekInternal(pikey);
		}

		void SeekInternal(const ParsedInternalKey& pikey) {
			//TryPinBuffer(interKeyBuf_xx_);
			// Damn MySQL-rocksdb may use "rev:" comparator
			size_t cplen = fstringOf(pikey.user_key).commonPrefixLen(subReader_->commonPrefix_);
			if (subReader_->commonPrefix_.size() != cplen) {
				if (pikey.user_key.size() == cplen) {
					assert(pikey.user_key.size() < subReader_->commonPrefix_.size());
					if (reverse) {
						SeekToLast();
						this->Next(); // move  to EOF
						assert(!this->Valid());
					} else {
						SeekToFirst();
					}
				} else {
					assert(pikey.user_key.size() > cplen);
					assert(pikey.user_key[cplen] != subReader_->commonPrefix_[cplen]);
					if ((byte_t(pikey.user_key[cplen]) < subReader_->commonPrefix_[cplen]) ^ reverse) {
						SeekToFirst();
					} else {
						SeekToLast();
						this->Next(); // move  to EOF
						assert(!this->Valid());
					}
				}
			} else {
				bool ok;
				int cmp; // compare(iterKey, searchKey)
				ok = iter_->Seek(fstringOf(pikey.user_key).substr(cplen));
				if (reverse) {
					if (!ok) {
						// searchKey is reverse_bytewise less than all keys in database
						iter_->SeekToLast();
						ok = iter_->Valid();
						cmp = -1;
					} else if ((cmp = SliceOf(iter_->key()).compare(SubStr(pikey.user_key, cplen))) != 0) {
						assert(cmp > 0);
						iter_->Prev();
						ok = iter_->Valid();
					}
				} else {
					cmp = 0;
					if (ok) {
						cmp = SliceOf(iter_->key()).compare(SubStr(pikey.user_key, cplen));
					}
				}
				if (UnzipIterRecord(ok)) {
					if (0 == cmp) {
						validx_ = size_t(-1);
						do {
							validx_++;
							DecodeCurrKeyValue();
							if (pInterKey_.sequence <= pikey.sequence) {
								return; // done
							}
						} while (validx_ + 1 < valnum_);
						// no visible version/sequence for target, use Next();
						// if using Next(), version check is not needed
						Next();
					} else {
						DecodeCurrKeyValue();
					}
				}
			}
		}

		void Next() override {
			assert(iter_->Valid());
			if (UnzipIterRecord(IndexIterNext())) {
				DecodeCurrKeyValue();
			}
		}

		void Prev() override {
			assert(iter_->Valid());
			if (UnzipIterRecord(IndexIterPrev())) {
				DecodeCurrKeyValue();
			}
		}

		Slice key() const override {
			assert(iter_->Valid());
			return SliceOf(interKeyBuf_xx_);
		}

		Slice value() const override {
			assert(iter_->Valid());
			return userValue_;
		}

		Status status() const override {
			return status_;
		}

	protected:
		virtual void SetIterInvalid() {
			//TryPinBuffer(interKeyBuf_xx_);
			iter_->SetInvalid();
			pInterKey_.user_key = Slice();
			pInterKey_.sequence = uint64_t(-1);
			pInterKey_.type = kMaxValue;
		}

		virtual bool IndexIterSeekToFirst() {
			//TryPinBuffer(interKeyBuf_xx_);
			if (reverse) {
				return iter_->SeekToLast();
			} else {
				return iter_->SeekToFirst();
			}
		}

		virtual bool IndexIterSeekToLast() {
			//TryPinBuffer(interKeyBuf_xx_);
			if (reverse) {
				return iter_->SeekToFirst();
			} else {
				return iter_->SeekToLast();
			}
		}

		virtual bool IndexIterPrev() {
			//TryPinBuffer(interKeyBuf_xx_);
			if (reverse) {
				return iter_->Next();
			} else {
				return iter_->Prev();
			}
		}

		virtual bool IndexIterNext() {
			//TryPinBuffer(interKeyBuf_xx_);
			if (reverse) {
				return iter_->Prev();
			} else {
				return iter_->Next();
			}
		}

		virtual void DecodeCurrKeyValue() {
			DecodeCurrKeyValueInternal();
			interKeyBuf_.assign(subReader_->commonPrefix_.data(), subReader_->commonPrefix_.size());
			AppendInternalKey(&interKeyBuf_, pInterKey_);
			interKeyBuf_xx_.assign((byte_t*)interKeyBuf_.data(), interKeyBuf_.size());
		}

		bool UnzipIterRecord(bool hasRecord) {
			if (hasRecord) {
				size_t recId = iter_->id();
				zValtype_ = ZipValueType(subReader_->type_[recId]);
				try {
					valueBuf_.erase_all();
					subReader_->store_->get_record_append(recId, &valueBuf_);
				} catch (const BadCrc32cException& ex) { // crc checksum error
					SetIterInvalid();
					status_ = Status::Corruption("TerarkZipTableIterator::UnzipIterRecord()", ex.what());
					return false;
				}
				pInterKey_.user_key = SliceOf(iter_->key());
				return true;
			} else {
				SetIterInvalid();
				return false;
			}
		}
		
		// TBD(kg): Pinn removed, where to store the actual data?
		void DecodeCurrKeyValueInternal() {
			assert(status_.ok());
			assert(iter_->id() < subReader_->index_->NumKeys());
			switch (zValtype_) {
			case ZipValueType::kValue: {
				pInterKey_.type = kTypeValue;
				userValue_ = SliceOf(fstring(valueBuf_));
				break;
			}
			case ZipValueType::kDelete: {
				pInterKey_.type = kTypeDeletion;
				// TBD(kg): use Tombstome instead?
				userValue_ = Slice();
				break;
			}
			default: {
				status_ = Status::Aborted("TerarkZipTableIterator::DecodeCurrKeyValue()",
										  "Bad ZipValueType");
				abort(); // must not goes here, if it does, it should be a bug!!
				break;
			}
		}
	};


#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
	class TerarkZipTableUint64Iterator : public TerarkZipTableIterator<false> {
	public:
		TerarkZipTableUint64Iterator(const TableReaderOptions& tro
									 , const TerarkZipSubReader *subReader)
			: TerarkZipTableIterator<false>(tro, subReader) {
		}
	protected:
		typedef TerarkZipTableIterator<false> base_t;
		using base_t::subReader_;
		using base_t::pInterKey_;
		using base_t::interKeyBuf_;
		using base_t::interKeyBuf_xx_;
		using base_t::status_;

		using base_t::SeekInternal;
		using base_t::DecodeCurrKeyValueInternal;

	public:
		void Seek(const Slice& target) override {
			ParsedInternalKey pikey;
			if (!ParseInternalKey(target, &pikey)) {
				status_ = Status::InvalidArgument("TerarkZipTableIterator::Seek()",
												  "param target.size() < 8");
				SetIterInvalid();
				return;
			}
			uint64_t u64_target;
			assert(pikey.user_key.size() == 8);
			u64_target = byte_swap(*reinterpret_cast<const uint64_t*>(pikey.user_key.data()));
			pikey.user_key = Slice(reinterpret_cast<const char*>(&u64_target), 8);
			SeekInternal(pikey);
		}
		void DecodeCurrKeyValue() override {
			DecodeCurrKeyValueInternal();
			interKeyBuf_.assign(subReader_->commonPrefix_.data(), subReader_->commonPrefix_.size());
			AppendInternalKey(&interKeyBuf_, pInterKey_);
			assert(interKeyBuf_.size() == 16);
			uint64_t *ukey = reinterpret_cast<uint64_t*>(&interKeyBuf_[0]);
			*ukey = byte_swap(*ukey);
			interKeyBuf_xx_.assign((byte_t*)interKeyBuf_.data(), interKeyBuf_.size());
		}
	};
#endif

	TerarkZipSubReader::~TerarkZipSubReader() {
		type_.risk_release_ownership();
	}

	Status
	TerarkEmptyTableReader::Open(RandomAccessFileReader* file, uint64_t file_size) {
		file_.reset(file); // take ownership
		const auto& ioptions = table_reader_options_.ioptions;
		TableProperties* props = nullptr;
		Status s = ReadTableProperties(file, file_size,
									   kTerarkZipTableMagicNumber, ioptions, &props);
		if (!s.ok()) {
			return s;
		}
		assert(nullptr != props);
		unique_ptr<TableProperties> uniqueProps(props);
		Slice file_data;
		s = file->Read(0, file_size, &file_data, nullptr);
		if (!s.ok()) {
			return s;
		}
		file_data_ = file_data;
		table_properties_.reset(uniqueProps.release());
		INFO(ioptions.info_log
			 , "TerarkZipTableReader::Open(): fsize = %zd, entries = %zd keys = 0 indexSize = 0 valueSize = 0, warm up time =      0.000'sec, build cache time =      0.000'sec\n"
			 , size_t(file_size), size_t(table_properties_->num_entries)
			 );
		return Status::OK();
	}


	Status
	TerarkZipTableReader::Open(RandomAccessFileReader* file, uint64_t file_size) {
		file_.reset(file); // take ownership
		const auto& ioptions = table_reader_options_.ioptions;
		TableProperties* props = nullptr;
		Status s = ReadTableProperties(file, file_size,
									   kTerarkZipTableMagicNumber, ioptions, &props);
		if (!s.ok()) {
			return s;
		}
		assert(nullptr != props);
		unique_ptr<TableProperties> uniqueProps(props);
		Slice file_data;
		s = file->Read(0, file_size, &file_data, nullptr);
		if (!s.ok()) {
			return s;
		}
		file_data_ = file_data;
		table_properties_.reset(uniqueProps.release());
		isReverseBytewiseOrder_ =
			fstring(ioptions.user_comparator->Name()).startsWith("rev:");
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
		isUint64Comparator_ =
			fstring(ioptions.user_comparator->Name()) == "rocksdb.Uint64Comparator";
#endif
		BlockContents valueDictBlock, indexBlock, zValueTypeBlock, commonPrefixBlock;
		s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
						  kTerarkZipTableValueDictBlock, &valueDictBlock);
		s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
						  kTerarkZipTableIndexBlock, &indexBlock);
		if (!s.ok()) {
			return s;
		}
		s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
						  kTerarkZipTableCommonPrefixBlock, &commonPrefixBlock);
		if (s.ok()) {
			subReader_.commonPrefix_.assign(commonPrefixBlock.data.data(),
											commonPrefixBlock.data.size());
		} else {
			// some error, usually is
			// Status::Corruption("Cannot find the meta block", meta_block_name)
			WARN(ioptions.info_log
				 , "Read %s block failed, treat as old SST version, error: %s\n"
				 , kTerarkZipTableCommonPrefixBlock.c_str()
				 , s.ToString().c_str());
		}
		try {
			subReader_.store_.reset(terark::BlobStore::load_from_user_memory(
				fstring(file_data.data(), props->data_size), fstringOf(valueDictBlock.data)));
		} catch (const BadCrc32cException& ex) {
			return Status::Corruption("TerarkZipTableReader::Open()", ex.what());
		}
		s = LoadIndex(indexBlock.data);
		if (!s.ok()) {
			return s;
		}
		size_t recNum = subReader_.index_->NumKeys();
		s = ReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
						  kTerarkZipTableValueTypeBlock, &zValueTypeBlock);
		if (s.ok()) {
			subReader_.type_.risk_set_data((byte_t*)zValueTypeBlock.data.data(), recNum);
		}
		long long t0 = g_pf.now();
		if (tzto_.warmUpIndexOnOpen) {
			MmapWarmUp(fstringOf(indexBlock.data));
			if (!tzto_.warmUpValueOnOpen) {
				MmapWarmUp(subReader_.store_->get_dict());
				for (fstring block : subReader_.store_->get_index_blocks()) {
					MmapWarmUp(block);
				}
			}
		}
		if (tzto_.warmUpValueOnOpen) {
			MmapWarmUp(subReader_.store_->get_mmap());
		}
		long long t1 = g_pf.now();
		subReader_.index_->BuildCache(tzto_.indexCacheRatio);
		long long t2 = g_pf.now();
		INFO(ioptions.info_log
			 , "TerarkZipTableReader::Open(): fsize = %zd, entries = %zd keys = %zd indexSize = %zd valueSize=%zd, warm up time = %6.3f'sec, build cache time = %6.3f'sec\n"
			 , size_t(file_size), size_t(table_properties_->num_entries)
			 , subReader_.index_->NumKeys()
			 , size_t(table_properties_->index_size)
			 , size_t(table_properties_->data_size)
			 , g_pf.sf(t0, t1)
			 , g_pf.sf(t1, t2)
			 );
		return Status::OK();
	}


	Status TerarkZipTableReader::LoadIndex(Slice mem) {
		auto func = "TerarkZipTableReader::LoadIndex()";
		try {
			subReader_.index_ = TerarkIndex::LoadMemory(fstringOf(mem));
		} catch (const BadCrc32cException& ex) {
			return Status::Corruption(func, ex.what());
		} catch (const std::exception& ex) {
			return Status::InvalidArgument(func, ex.what());
		}
		return Status::OK();
	}

	Iterator*
	TerarkZipTableReader::
	NewIterator(const ReadOptions& ro, Arena* arena, bool skip_filters) {
		(void)skip_filters; // unused
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
		if (isUint64Comparator_) {
			if (arena) {
				return new(arena->AllocateAligned(sizeof(TerarkZipTableUint64Iterator)))
					TerarkZipTableUint64Iterator(table_reader_options_, &subReader__);
			} else {
				return new TerarkZipTableUint64Iterator(table_reader_options_, &subReader_);
			}
		}
#endif
		if (isReverseBytewiseOrder_) {
			if (arena) {
				return new(arena->AllocateAligned(sizeof(TerarkZipTableIterator<true>)))
					TerarkZipTableIterator<true>(table_reader_options_, &subReader_);
			} else {
				return new TerarkZipTableIterator<true>(table_reader_options_, &subReader_);
			}
		} else {
			if (arena) {
				return new(arena->AllocateAligned(sizeof(TerarkZipTableIterator<false>)))
					TerarkZipTableIterator<false>(table_reader_options_, &subReader_);
			} else {
				return new TerarkZipTableIterator<false>(table_reader_options_, &subReader_);
			}
		}
	}


	TerarkZipTableReader::~TerarkZipTableReader() {}

	TerarkZipTableReader::TerarkZipTableReader(const TableReaderOptions& tro,
											   const TerarkZipTableOptions& tzto)
		: table_reader_options_(tro)
		, global_seqno_(kDisableGlobalSequenceNumber)
		, tzto_(tzto) {
		isReverseBytewiseOrder_ = false;
	}
}
