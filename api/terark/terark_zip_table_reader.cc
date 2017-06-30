
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

// project headers
#include "terark_zip_table_builder.h"
#include "terark_zip_table_reader.h"
#include "terark_zip_common.h"
#include "trk_format.h"
#include "trk_meta_blocks.h"
#include "trk_table_properties.h"


namespace {
	using namespace rocksdb;

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
		// TBD(kg): uv.data(), uv.mem_size() ?
		MmapWarmUpBytes(uv.memory.data(), uv.memory.size());
	}
}


namespace rocksdb {

	using terark::BadCrc32cException;
	using terark::byte_swap;
	using terark::BlobStore;

	class TerarkZipTableIterator : public Iterator, boost::noncopyable {
	protected:
		const TerarkZipSubReader* subReader_;
		std::unique_ptr<TerarkIndex::Iterator> iter_;
		//std::string             interKeyBuf_;
		valvec<byte_t>          interKeyBuf_xx_;
		valvec<byte_t>          valueBuf_;
		Slice                   userValue_;
		//ZipValueType            zValtype_;
		Status                  status_;

	public:
		TerarkZipTableIterator(const TerarkTableReaderOptions& tro
			, const TerarkZipSubReader *subReader)
			: subReader_(subReader) {
			if (subReader_ != nullptr) {
				iter_.reset(subReader_->index_->NewIterator());
				iter_->SetInvalid();
			}
		}

		bool Valid() const override {
			return iter_->Valid();
		}

		void SeekToFirst() override {
			if (UnzipIterRecord(iter_->SeekToFirst())) {
				DecodeCurrKeyValue();
			}
		}

		void SeekToLast() override {
			if (UnzipIterRecord(iter_->SeekToLast())) {
				DecodeCurrKeyValue();
			}
		}

		void SeekForPrev(const Slice& target) override {
			Seek(target);
			if (!Valid()) {
				SeekToLast();
			}
			while (Valid() && target.compare(key()) < 0) {
				Prev();
			}
			//SeekForPrevImpl(target, &table_reader_options_->internal_comparator);
		}

		void Seek(const Slice& target) override {
			//TryPinBuffer(interKeyBuf_xx_);
			if (UnzipIterRecord(iter_->Seek(fstringOf(target)))) {
				DecodeCurrKeyValue();
			}
		}

		void Next() override {
			assert(iter_->Valid());
			if (UnzipIterRecord(iter_->Next())) {
				DecodeCurrKeyValue();
			}
		}

		void Prev() override {
			assert(iter_->Valid());
			if (UnzipIterRecord(iter_->Prev())) {
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
		}

		virtual void DecodeCurrKeyValue() {
			assert(status_.ok());
			assert(iter_->id() < subReader_->index_->NumKeys());
			userValue_ = SliceOf(fstring(valueBuf_));
		}

		bool UnzipIterRecord(bool hasRecord) {
			if (hasRecord) {
				size_t recId = iter_->id();
				//zValtype_ = ZipValueType(subReader_->type_[recId]);
				try {
					valueBuf_.erase_all();
					subReader_->store_->get_record_append(recId, &valueBuf_);
				} catch (const BadCrc32cException& ex) { // crc checksum error
					SetIterInvalid();
					status_ = Status::Corruption("TerarkZipTableIterator::UnzipIterRecord()", ex.what());
					return false;
				}
				interKeyBuf_xx_.assign((byte_t*)iter_->key().data(), iter_->key().size());
				return true;
			} else {
				SetIterInvalid();
				return false;
			}
		}
	};

	Status
	TerarkZipTableBuilder::NewIterator(Iterator** iter) {
		// check cuurent state
		// if CreateDone state:
		//   read value, index blocks
		//   load index
		// else if SecondPass state:
		//   only Add is allowed
		// add iter into dict
		if (chunk_state_ == kCreateDone) {
		} else if (chunk_state_ == kSecondPass) {
		} else {
			// invalid state
		}
		
		return Status();
	}

	Status
	TerarkZipTableBuilder::OpenForRead() {
		if (chunk_state_ == kOpenForRead) {
		} else if (chunk_state_ != kCreateDone) {
			// invalid state
		}
		// prepare chunk file
		rocksdb::EnvOptions env_options;
		rocksdb::Options options;
		std::unique_ptr<rocksdb::RandomAccessFile> file;
		rocksdb::Status s = options.env->NewRandomAccessFile(chunk_name_, &file, env_options);
		assert(s.ok());
		std::unique_ptr<rocksdb::RandomAccessFileReader> 
			file_reader(new rocksdb::RandomAccessFileReader(std::move(file), options.env));
		uint64_t file_size = 0;
		s = options.env->GetFileSize(chunk_name_, &file_size);
		assert(s.ok());
		// read meta data -- properties, index meta, value meta
		TerarkTableProperties* table_props = nullptr;
		s = TerarkReadTableProperties(file_reader.get(), file_size,
			kTerarkZipTableMagicNumber, options, &table_props);
		if (!s.ok()) {
			return s;
		}
		assert(nullptr != table_props);
		Slice file_data;
		s = file_reader->Read(0, file_size, &file_data, nullptr);
		if (!s.ok()) {
			return s;
		}
		TerarkBlockContents valueDictBlock, indexBlock;
		s = TerarkReadMetaBlock(file_reader.get(), file_size, kTerarkZipTableMagicNumber, options,
			kTerarkZipTableValueDictBlock, &valueDictBlock);
		if (!s.ok()) {
			return s;
		}
		s = TerarkReadMetaBlock(file_reader.get(), file_size, kTerarkZipTableMagicNumber, options,
			kTerarkZipTableIndexBlock, &indexBlock);
		if (!s.ok()) {
			return s;
		}
		// read contents -- index, value
		try {
			store_.reset(terark::BlobStore::load_from_user_memory(
					fstring(file_data.data(), table_props->data_size), fstringOf(valueDictBlock.data)));
			index_ = TerarkIndex::LoadMemory(fstringOf(indexBlock.data));
		} catch (const BadCrc32cException& ex) {
			return Status::Corruption("TerarkZipTableBuilder::Open()", ex.what());
		} catch (const std::exception& ex) {
			return Status::InvalidArgument("TerarkZipTableBuilder::OpenForRead()", ex.what());
		}
		// 
		long long t0 = g_pf.now();
		if (table_options_.warmUpIndexOnOpen) {
			MmapWarmUp(fstringOf(indexBlock.data));
			if (!table_options_.warmUpValueOnOpen) {
				MmapWarmUp(store_->get_dict());
				for (fstring block : store_->get_index_blocks()) {
					MmapWarmUp(block);
				}
			}
		}
		if (table_options_.warmUpValueOnOpen) {
			MmapWarmUp(store_->get_mmap());
		}
		long long t1 = g_pf.now();
		index_->BuildCache(table_options_.indexCacheRatio);
		long long t2 = g_pf.now();
		/*INFO(ioptions.info_log
		  , "TerarkZipTableReader::Open(): fsize = %zd, entries = %zd keys = %zd indexSize = %zd valueSize=%zd, warm up time = %6.3f'sec, build cache time = %6.3f'sec\n"
		  , size_t(file_size), size_t(table_properties_->num_entries)
		  , subReader_.index_->NumKeys()
		  , size_t(table_properties_->index_size)
		  , size_t(table_properties_->data_size)
		  , g_pf.sf(t0, t1)
		  , g_pf.sf(t1, t2)
		  );*/
		return Status::OK();
	}


	Status
	TerarkEmptyTableReader::Open(RandomAccessFileReader* file, uint64_t file_size) {
		file_.reset(file); // take ownership
		const auto& ioptions = table_reader_options_.ioptions;
		TerarkTableProperties* props = nullptr;
		Status s = TerarkReadTableProperties(file, file_size,
											 kTerarkZipTableMagicNumber, ioptions, &props);
		if (!s.ok()) {
			return s;
		}
		assert(nullptr != props);
		std::unique_ptr<TerarkTableProperties> uniqueProps(props);
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
		TerarkTableProperties* props = nullptr;
		Status s = TerarkReadTableProperties(file, file_size,
									   kTerarkZipTableMagicNumber, ioptions, &props);
		if (!s.ok()) {
			return s;
		}
		assert(nullptr != props);
		std::unique_ptr<TerarkTableProperties> uniqueProps(props);
		Slice file_data;
		s = file->Read(0, file_size, &file_data, nullptr);
		if (!s.ok()) {
			return s;
		}
		file_data_ = file_data;
		table_properties_.reset(uniqueProps.release());
		TerarkBlockContents valueDictBlock, indexBlock, commonPrefixBlock;
		s = TerarkReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
						  kTerarkZipTableValueDictBlock, &valueDictBlock);
		if (!s.ok()) {
			return s;
		}
		s = TerarkReadMetaBlock(file, file_size, kTerarkZipTableMagicNumber, ioptions,
						  kTerarkZipTableIndexBlock, &indexBlock);
		if (!s.ok()) {
			return s;
		}
		try {
			subReader_.store_.reset(terark::BlobStore::load_from_user_memory(
				fstring(file_data.data(), props->data_size), fstringOf(valueDictBlock.data)));
			subReader_.index_ = TerarkIndex::LoadMemory(fstringOf(indexBlock.data));
		} catch (const BadCrc32cException& ex) {
			return Status::Corruption("TerarkZipTableReader::Open()", ex.what());
		} catch (const std::exception& ex) {
			return Status::InvalidArgument("TerarkZipTableReader::Open()", ex.what());
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
		/*INFO(ioptions.info_log
			 , "TerarkZipTableReader::Open(): fsize = %zd, entries = %zd keys = %zd indexSize = %zd valueSize=%zd, warm up time = %6.3f'sec, build cache time = %6.3f'sec\n"
			 , size_t(file_size), size_t(table_properties_->num_entries)
			 , subReader_.index_->NumKeys()
			 , size_t(table_properties_->index_size)
			 , size_t(table_properties_->data_size)
			 , g_pf.sf(t0, t1)
			 , g_pf.sf(t1, t2)
			 );*/
		return Status::OK();
	}

	Iterator*
	TerarkZipTableReader::
	NewIterator() {
		return new TerarkZipTableIterator(table_reader_options_, &subReader_);
	}


	TerarkZipTableReader::~TerarkZipTableReader() {}

	TerarkZipTableReader::TerarkZipTableReader(const TerarkTableReaderOptions& tro,
		const TerarkZipTableOptions& tzto)
		: table_reader_options_(tro)
		, tzto_(tzto) {}
}
