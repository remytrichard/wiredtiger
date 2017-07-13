
// boost headers
#include <boost/scope_exit.hpp>
// terark headers
#include <terark/util/crc.hpp>
// project headers
#include "util/trk_meta_blocks.h"
#include "terark_chunk_reader.h"
#include "terark_zip_common.h"

namespace terark {

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


namespace terark {

	using terark::BadCrc32cException;
	using terark::byte_swap;
	using terark::BlobStore;

	void TerarkChunkReader::TerarkReaderIterator::SeekForPrev(const Slice& target) {
		reseted_ = false;
		Seek(target);
		if (!Valid()) {
			SeekToLast();
		}
		while (Valid() && target.compare(key()) < 0) {
			Prev();
		}
	}

	/*
	 * If the WT_CURSOR::next method is called on a cursor without 
	 * a position in the data source, it is positioned at the beginning 
	 * of the data source.
	 */
	void TerarkChunkReader::TerarkReaderIterator::Next() {
		if (reseted_) {
			SeekToFirst();
			reseted_ = false;
			return;
		}
		assert(iter_->Valid());
		UnzipIterRecord(iter_->Next());
	}

	/*
	 * If the WT_CURSOR::prev method is called on a cursor without
	 * a position in the data source, it is positioned at the end
	 * of the data source.
	 */
	void TerarkChunkReader::TerarkReaderIterator::Prev() {
		if (reseted_) {
			SeekToLast();
			reseted_ = false;
			return;
		}
		assert(iter_->Valid());
		UnzipIterRecord(iter_->Prev());
	}

	bool TerarkChunkReader::TerarkReaderIterator::UnzipIterRecord(bool hasRecord) {
		if (hasRecord) {
			assert(iter_->id() < chunk_->index_->NumKeys());
			size_t recId = iter_->id();
			try {
				valueBuf_.erase_all();
				chunk_->store_->get_record_append(recId, &valueBuf_);
			} catch (const BadCrc32cException& ex) { // crc checksum error
				iter_->SetInvalid();
				status_ = Status::Corruption("TerarkZipTableIterator::UnzipIterRecord()", ex.what());
				return false;
			}
			keyBuf_.assign((byte_t*)iter_->key().data(), iter_->key().size());
			return true;
		} else {
			iter_->SetInvalid();
			return false;
		}
	}


	TerarkChunkReader::~TerarkChunkReader() {
		index_.reset();
		store_.reset();
		file_reader_.reset();
	}
	
	TerarkChunkReader::TerarkReaderIterator*
	TerarkChunkReader::NewIterator() {
		Status s = Open();
		if (!s.ok()) {
			printf("Fail to open chunk: %s\n", s.getState());
			return nullptr;
		}
		return new TerarkReaderIterator(this);
	}

	Status
	TerarkChunkReader::Open() {
		if (file_reader_) {
			return Status::OK();
		}
		// prepare chunk file
		file_reader_.reset(new terark::MmapWholeFile(chunk_name_));
		size_t file_size = 0;
		Status s = GetFileSize(chunk_name_, &file_size);
		assert(s.ok());
		// read meta data -- properties, index meta, value meta
		Slice file_data((const char*)file_reader_->base, file_size);
		TerarkTableProperties* table_props = nullptr;
		s = TerarkReadTableProperties(file_data, file_size,
									  kTerarkZipTableMagicNumber, &table_props);
		if (!s.ok()) {
			return s;
		}
		assert(nullptr != table_props);
		TerarkBlockContents valueDictBlock, indexBlock;
		s = TerarkReadMetaBlock(file_data, file_size, kTerarkZipTableMagicNumber,
			kTerarkZipTableValueDictBlock, &valueDictBlock);
		if (!s.ok()) {
			return s;
		}
		s = TerarkReadMetaBlock(file_data, file_size, kTerarkZipTableMagicNumber,
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
		//INFO(ioptions.info_log
		printf("TerarkChunkReader::Open(): fsize = %zd, entries = %zd keys = %zd indexSize = %zd valueSize=%zd, warm up time = %6.3f'sec, build cache time = %6.3f'sec\n"
		  , size_t(file_size), size_t(table_props->num_entries)
		  , index_->NumKeys()
		  , size_t(table_props->index_size)
		  , size_t(table_props->data_size)
		  , g_pf.sf(t0, t1)
		  , g_pf.sf(t1, t2)
		  );
		return Status::OK();
	}

}
