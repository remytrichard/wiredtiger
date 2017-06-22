// project headers
#include "terark_zip_table_reader.h"
#include "terark_zip_common.h"
// boost headers
#include <boost/scope_exit.hpp>
// rocksdb headers
#include <table/internal_iterator.h>
#include <table/sst_file_writer_collectors.h>
#include <table/meta_blocks.h>
#include <table/get_context.h>
// terark headers
#include <terark/util/crc.hpp>


namespace {
	using namespace rocksdb;

	// copy & modify from block_based_table_reader.cc
	SequenceNumber GetGlobalSequenceNumber(const TableProperties& table_properties,
										   Logger* info_log) {
		auto& props = table_properties.user_collected_properties;

		auto version_pos = props.find(ExternalSstFilePropertyNames::kVersion);
		auto seqno_pos = props.find(ExternalSstFilePropertyNames::kGlobalSeqno);

		if (version_pos == props.end()) {
			if (seqno_pos != props.end()) {
				// This is not an external sst file, global_seqno is not supported.
				assert(false);
				fprintf(stderr,
						"A non-external sst file have global seqno property with value %s\n",
						seqno_pos->second.c_str());
			}
			return kDisableGlobalSequenceNumber;
		}

		uint32_t version = DecodeFixed32(version_pos->second.c_str());
		if (version < 2) {
			if (seqno_pos != props.end() || version != 1) {
				// This is a v1 external sst file, global_seqno is not supported.
				assert(false);
				fprintf(stderr,
						"An external sst file with version %u have global seqno property "
						"with value %s\n",
						version, seqno_pos->second.c_str());
			}
			return kDisableGlobalSequenceNumber;
		}

		SequenceNumber global_seqno = DecodeFixed64(seqno_pos->second.c_str());

		if (global_seqno > kMaxSequenceNumber) {
			assert(false);
			fprintf(stderr,
					"An external sst file with version %u have global seqno property "
					"with value %llu, which is greater than kMaxSequenceNumber\n",
					version, (long long)global_seqno);
		}

		return global_seqno;
	}

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
	class TerarkZipTableIterator : public InternalIterator, boost::noncopyable {
	protected:
		const TableReaderOptions* table_reader_options_;
		const TerarkZipSubReader* subReader_;
		unique_ptr<TerarkIndex::Iterator> iter_;
		SequenceNumber          global_seqno_;
		ParsedInternalKey       pInterKey_;
		std::string             interKeyBuf_;
		valvec<byte_t>          interKeyBuf_xx_;
		valvec<byte_t>          valueBuf_;
		Slice                   userValue_;
		ZipValueType            zValtype_;
		size_t                  valnum_;
		size_t                  validx_;
		Status                  status_;
		PinnedIteratorsManager* pinned_iters_mgr_;
		valvec<valvec<byte_t>>  pinned_buffer_;

	public:
		TerarkZipTableIterator(const TableReaderOptions& tro
							   , const TerarkZipSubReader *subReader
							   , SequenceNumber global_seqno)
			: table_reader_options_(&tro)
			, subReader_(subReader)
			, global_seqno_(global_seqno) {
			if (subReader_ != nullptr) {
				iter_.reset(subReader_->index_->NewIterator());
				iter_->SetInvalid();
			}
			pinned_iters_mgr_ = NULL;
			TryPinBuffer(interKeyBuf_xx_);
			validx_ = 0;
			valnum_ = 0;
			pInterKey_.user_key = Slice();
			pInterKey_.sequence = uint64_t(-1);
			pInterKey_.type = kMaxValue;
		}

		void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) {
			if (pinned_iters_mgr_ && pinned_iters_mgr_ != pinned_iters_mgr) {
				pinned_buffer_.clear();
			}
			pinned_iters_mgr_ = pinned_iters_mgr;
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
				validx_ = valnum_ - 1;
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
			TryPinBuffer(interKeyBuf_xx_);
			// Damn MySQL-rocksdb may use "rev:" comparator
			size_t cplen = fstringOf(pikey.user_key).commonPrefixLen(subReader_->commonPrefix_);
			if (subReader_->commonPrefix_.size() != cplen) {
				if (pikey.user_key.size() == cplen) {
					assert(pikey.user_key.size() < subReader_->commonPrefix_.size());
					if (reverse) {
						SeekToLast();
						this->Next(); // move  to EOF
						assert(!this->Valid());
					}
					else {
						SeekToFirst();
					}
				}
				else {
					assert(pikey.user_key.size() > cplen);
					assert(pikey.user_key[cplen] != subReader_->commonPrefix_[cplen]);
					if ((byte_t(pikey.user_key[cplen]) < subReader_->commonPrefix_[cplen]) ^ reverse) {
						SeekToFirst();
					}
					else {
						SeekToLast();
						this->Next(); // move  to EOF
						assert(!this->Valid());
					}
				}
			}
			else {
				bool ok;
				int cmp; // compare(iterKey, searchKey)
				ok = iter_->Seek(fstringOf(pikey.user_key).substr(cplen));
				if (reverse) {
					if (!ok) {
						// searchKey is reverse_bytewise less than all keys in database
						iter_->SeekToLast();
						ok = iter_->Valid();
						cmp = -1;
					}
					else if ((cmp = SliceOf(iter_->key()).compare(SubStr(pikey.user_key, cplen))) != 0) {
						assert(cmp > 0);
						iter_->Prev();
						ok = iter_->Valid();
					}
				}
				else {
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
					}
					else {
						DecodeCurrKeyValue();
					}
				}
			}
		}

		void Next() override {
			assert(iter_->Valid());
			validx_++;
			if (validx_ < valnum_) {
				DecodeCurrKeyValue();
			}
			else {
				if (UnzipIterRecord(IndexIterNext())) {
					DecodeCurrKeyValue();
				}
			}
		}

		void Prev() override {
			assert(iter_->Valid());
			if (validx_ > 0) {
				validx_--;
				DecodeCurrKeyValue();
			}
			else {
				if (UnzipIterRecord(IndexIterPrev())) {
					validx_ = valnum_ - 1;
					DecodeCurrKeyValue();
				}
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

		bool IsKeyPinned() const {
			return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled();
		}
		bool IsValuePinned() const {
			return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled();
		}

	protected:
		virtual void SetIterInvalid() {
			TryPinBuffer(interKeyBuf_xx_);
			iter_->SetInvalid();
			validx_ = 0;
			valnum_ = 0;
			pInterKey_.user_key = Slice();
			pInterKey_.sequence = uint64_t(-1);
			pInterKey_.type = kMaxValue;
		}
		virtual bool IndexIterSeekToFirst() {
			TryPinBuffer(interKeyBuf_xx_);
			if (reverse)
				return iter_->SeekToLast();
			else
				return iter_->SeekToFirst();
		}
		virtual bool IndexIterSeekToLast() {
			TryPinBuffer(interKeyBuf_xx_);
			if (reverse)
				return iter_->SeekToFirst();
			else
				return iter_->SeekToLast();
		}
		virtual bool IndexIterPrev() {
			TryPinBuffer(interKeyBuf_xx_);
			if (reverse)
				return iter_->Next();
			else
				return iter_->Prev();
		}
		virtual bool IndexIterNext() {
			TryPinBuffer(interKeyBuf_xx_);
			if (reverse)
				return iter_->Prev();
			else
				return iter_->Next();
		}
		virtual void DecodeCurrKeyValue() {
			DecodeCurrKeyValueInternal();
			interKeyBuf_.assign(subReader_->commonPrefix_.data(), subReader_->commonPrefix_.size());
			AppendInternalKey(&interKeyBuf_, pInterKey_);
			interKeyBuf_xx_.assign((byte_t*)interKeyBuf_.data(), interKeyBuf_.size());
		}
		void TryPinBuffer(valvec<byte_t>& buf) {
			if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
				pinned_buffer_.push_back();
				pinned_buffer_.back().swap(buf);
			}
		}
		bool UnzipIterRecord(bool hasRecord) {
			if (hasRecord) {
				size_t recId = iter_->id();
				zValtype_ = subReader_->type_.size()
					? ZipValueType(subReader_->type_[recId])
					: ZipValueType::kZeroSeq;
				try {
					TryPinBuffer(valueBuf_);
					if (ZipValueType::kMulti == zValtype_) {
						valueBuf_.resize_no_init(sizeof(uint32_t)); // for offsets[valnum_]
					}
					else {
						valueBuf_.erase_all();
					}
					subReader_->store_->get_record_append(recId, &valueBuf_);
				}
				catch (const BadCrc32cException& ex) { // crc checksum error
					SetIterInvalid();
					status_ = Status::Corruption(
												 "TerarkZipTableIterator::UnzipIterRecord()", ex.what());
					return false;
				}
				if (ZipValueType::kMulti == zValtype_) {
					ZipValueMultiValue::decode(valueBuf_, &valnum_);
				}
				else {
					valnum_ = 1;
				}
				validx_ = 0;
				pInterKey_.user_key = SliceOf(iter_->key());
				return true;
			}
			else {
				SetIterInvalid();
				return false;
			}
		}
		void DecodeCurrKeyValueInternal() {
			assert(status_.ok());
			assert(iter_->id() < subReader_->index_->NumKeys());
			switch (zValtype_) {
			default:
				status_ = Status::Aborted("TerarkZipTableIterator::DecodeCurrKeyValue()",
										  "Bad ZipValueType");
				abort(); // must not goes here, if it does, it should be a bug!!
				break;
			case ZipValueType::kZeroSeq:
				assert(0 == validx_);
				assert(1 == valnum_);
				pInterKey_.sequence = global_seqno_;
				pInterKey_.type = kTypeValue;
				userValue_ = SliceOf(valueBuf_);
				break;
			case ZipValueType::kValue: // should be a kTypeValue, the normal case
				assert(0 == validx_);
				assert(1 == valnum_);
				// little endian uint64_t
				pInterKey_.sequence = *(uint64_t*)valueBuf_.data() & kMaxSequenceNumber;
				pInterKey_.type = kTypeValue;
				userValue_ = SliceOf(fstring(valueBuf_).substr(7));
				break;
			case ZipValueType::kDelete:
				assert(0 == validx_);
				assert(1 == valnum_);
				// little endian uint64_t
				pInterKey_.sequence = *(uint64_t*)valueBuf_.data() & kMaxSequenceNumber;
				pInterKey_.type = kTypeDeletion;
				userValue_ = Slice();
				break;
			case ZipValueType::kMulti: { // more than one value
				auto zmValue = (const ZipValueMultiValue*)(valueBuf_.data());
				assert(0 != valnum_);
				assert(validx_ < valnum_);
				Slice d = zmValue->getValueData(validx_, valnum_);
				auto snt = unaligned_load<SequenceNumber>(d.data());
				UnPackSequenceAndType(snt, &pInterKey_.sequence, &pInterKey_.type);
				d.remove_prefix(sizeof(SequenceNumber));
				userValue_ = d;
				break; }
			}
		}
	};


#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
	class TerarkZipTableUint64Iterator : public TerarkZipTableIterator<false> {
	public:
		TerarkZipTableUint64Iterator(const TableReaderOptions& tro
									 , const TerarkZipSubReader *subReader
									 , SequenceNumber global_seqno)
			: TerarkZipTableIterator<false>(tro, subReader, global_seqno) {
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

	Status TerarkZipSubReader::Get(SequenceNumber global_seqno, const ReadOptions& ro, const Slice& ikey,
								   GetContext* get_context, int flag) const {
		(void)flag;
		MY_THREAD_LOCAL(valvec<byte_t>, g_tbuf);
		ParsedInternalKey pikey;
		if (!ParseInternalKey(ikey, &pikey)) {
			return Status::InvalidArgument("TerarkZipTableReader::Get()",
										   "bad internal key causing ParseInternalKey() failed");
		}
		Slice user_key = pikey.user_key;
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
		uint64_t u64_target;
		if (flag & FlagUint64Comparator) {
			assert(pikey.user_key.size() == 8);
			u64_target = byte_swap(*reinterpret_cast<const uint64_t*>(pikey.user_key.data()));
			user_key = Slice(reinterpret_cast<const char*>(&u64_target), 8);
		}
#endif
		size_t cplen = user_key.difference_offset(commonPrefix_);
		if (commonPrefix_.size() != cplen) {
			return Status::OK();
		}
		assert(user_key.size() > prefix_.size());
		size_t recId = index_->Find(fstringOf(user_key).substr(cplen + prefix_.size()));
		if (size_t(-1) == recId) {
			return Status::OK();
		}
		auto zvType = type_.size()
			? ZipValueType(type_[recId])
			: ZipValueType::kZeroSeq;
		if (ZipValueType::kMulti == zvType) {
			g_tbuf.resize_no_init(sizeof(uint32_t));
		}
		else {
			g_tbuf.erase_all();
		}
		try {
			store_->get_record_append(recId, &g_tbuf);
		}
		catch (const terark::BadChecksumException& ex) {
			return Status::Corruption("TerarkZipTableReader::Get()", ex.what());
		}
		switch (zvType) {
		default:
			return Status::Aborted("TerarkZipTableReader::Get()", "Bad ZipValueType");
		case ZipValueType::kZeroSeq:
			get_context->SaveValue(ParsedInternalKey(pikey.user_key, global_seqno, kTypeValue),
								   Slice((char*)g_tbuf.data(), g_tbuf.size()));
			break;
		case ZipValueType::kValue: { // should be a kTypeValue, the normal case
			// little endian uint64_t
			uint64_t seq = *(uint64_t*)g_tbuf.data() & kMaxSequenceNumber;
			if (seq <= pikey.sequence) {
				get_context->SaveValue(ParsedInternalKey(pikey.user_key, seq, kTypeValue),
									   SliceOf(fstring(g_tbuf).substr(7)));
			}
			break; }
		case ZipValueType::kDelete: {
			// little endian uint64_t
			uint64_t seq = *(uint64_t*)g_tbuf.data() & kMaxSequenceNumber;
			if (seq <= pikey.sequence) {
				get_context->SaveValue(ParsedInternalKey(pikey.user_key, seq, kTypeDeletion),
									   Slice());
			}
			break; }
		case ZipValueType::kMulti: { // more than one value
			size_t num = 0;
			auto mVal = ZipValueMultiValue::decode(g_tbuf, &num);
			for (size_t i = 0; i < num; ++i) {
				Slice val = mVal->getValueData(i, num);
				SequenceNumber sn;
				ValueType valtype;
				{
					auto snt = unaligned_load<SequenceNumber>(val.data());
					UnPackSequenceAndType(snt, &sn, &valtype);
				}
				if (sn <= pikey.sequence) {
					val.remove_prefix(sizeof(SequenceNumber));
					// only kTypeMerge will return true
					bool hasMoreValue = get_context->SaveValue(
															   ParsedInternalKey(pikey.user_key, sn, valtype), val);
					if (!hasMoreValue) {
						break;
					}
				}
			}
			break; }
		}
		if (g_tbuf.capacity() > 512 * 1024) {
			g_tbuf.clear(); // free large thread local memory
		}
		return Status::OK();
	}

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
		if (table_reader_options_.env_options.use_mmap_reads) {
			s = file->Read(0, file_size, &file_data, nullptr);
			if (!s.ok())
				return s;
		}
		else {
			return Status::InvalidArgument("TerarkZipTableReader::Open()",
										   "EnvOptions::use_mmap_reads must be true");
		}
		file_data_ = file_data;
		table_properties_.reset(uniqueProps.release());
		global_seqno_ = GetGlobalSequenceNumber(*table_properties_, ioptions.info_log);
		if (global_seqno_ == kDisableGlobalSequenceNumber) {
			global_seqno_ = 0;
		}
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
		if (!s.ok())
			return s;
		}
		file_data_ = file_data;
		table_properties_.reset(uniqueProps.release());
		global_seqno_ = GetGlobalSequenceNumber(*table_properties_, ioptions.info_log);
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
		if (global_seqno_ == kDisableGlobalSequenceNumber) {
			global_seqno_ = 0;
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
		}
		catch (const BadCrc32cException& ex) {
			return Status::Corruption(func, ex.what());
		}
		catch (const std::exception& ex) {
			return Status::InvalidArgument(func, ex.what());
		}
		return Status::OK();
	}

	InternalIterator*
	TerarkZipTableReader::
	NewIterator(const ReadOptions& ro, Arena* arena, bool skip_filters) {
		(void)skip_filters; // unused
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
		if (isUint64Comparator_) {
			if (arena) {
				return new(arena->AllocateAligned(sizeof(TerarkZipTableUint64Iterator)))
					TerarkZipTableUint64Iterator(table_reader_options_, &subReader_, global_seqno_);
			} else {
				return new TerarkZipTableUint64Iterator(table_reader_options_, &subReader_, global_seqno_);
			}
		}
#endif
		if (isReverseBytewiseOrder_) {
			if (arena) {
				return new(arena->AllocateAligned(sizeof(TerarkZipTableIterator<true>)))
					TerarkZipTableIterator<true>(table_reader_options_, &subReader_, global_seqno_);
			} else {
				return new TerarkZipTableIterator<true>(table_reader_options_, &subReader_, global_seqno_);
			}
		} else {
			if (arena) {
				return new(arena->AllocateAligned(sizeof(TerarkZipTableIterator<false>)))
					TerarkZipTableIterator<false>(table_reader_options_, &subReader_, global_seqno_);
			} else {
				return new TerarkZipTableIterator<false>(table_reader_options_, &subReader_, global_seqno_);
			}
		}
	}


	Status
	TerarkZipTableReader::Get(const ReadOptions& ro, const Slice& ikey,
							  GetContext* get_context, bool skip_filters) {
		int flag = skip_filters ? TerarkZipSubReader::FlagSkipFilter : TerarkZipSubReader::FlagNone;
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
		if (isUint64Comparator_) {
			flag |= TerarkZipSubReader::FlagUint64Comparator;
		}
#endif
		return subReader_.Get(global_seqno_, ro, ikey, get_context, flag);
	}

	TerarkZipTableReader::~TerarkZipTableReader() {
	}

	TerarkZipTableReader::TerarkZipTableReader(const TableReaderOptions& tro,
											   const TerarkZipTableOptions& tzto)
		: table_reader_options_(tro)
		, global_seqno_(kDisableGlobalSequenceNumber)
		, tzto_(tzto) {
		isReverseBytewiseOrder_ = false;
	}


}
