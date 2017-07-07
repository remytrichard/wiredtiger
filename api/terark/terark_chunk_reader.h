
#pragma once

// std headers
#include <memory>
#include <random>
#include <vector>
// wiredtiger headers
#include "wiredtiger.h"
// rocksdb headers
#include "file_reader_writer.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/table.h"
#include "util/arena.h"
// terark headers
#include <terark/fstring.hpp>
#include <terark/valvec.hpp>
#include <terark/bitmap.hpp>
#include <terark/stdtypes.hpp>
#include <terark/histogram.hpp>
#include <terark/zbs/blob_store.hpp>
#include <terark/zbs/dict_zip_blob_store.hpp>
#include <terark/bitfield_array.hpp>
#include <terark/util/fstrvec.hpp>
// project headers
#include "terark_zip_config.h"
#include "terark_zip_internal.h"
#include "terark_zip_common.h"
#include "terark_zip_index.h"
//#include "trk_block.h"
#include "trk_format.h"
#include "trk_table_properties.h"

namespace rocksdb {

	using terark::fstring;
	using terark::fstrvec;
	using terark::valvec;
	using terark::byte_t;
	using terark::febitvec;
	using terark::BlobStore;
	using terark::Uint32Histogram;
	using terark::DictZipBlobStore;

	class TerarkChunkReader : boost::noncopyable {
	public:
	TerarkChunkReader(const TerarkZipTableOptions& tzto,const std::string& fname) 
		: table_options_(tzto),
			chunk_name_(fname) {}
		~TerarkChunkReader();

	public:
		class TerarkReaderIterator : public Iterator, boost::noncopyable {
	public:
	TerarkReaderIterator(TerarkChunkReader* chunk, bool isTest = false)
		: chunk_(chunk) {
			if (!isTest) {
				iter_.reset(chunk_->index_->NewIterator()); 
				iter_->SetInvalid(); 
			}
			reseted_ = false;
		}
		~TerarkReaderIterator() {}
		bool Valid() const { return iter_->Valid(); }
		void SeekToFirst() {
			reseted_ = false;
			UnzipIterRecord(iter_->SeekToFirst());
		}
		void SeekToLast() {
			reseted_ = false;
			UnzipIterRecord(iter_->SeekToLast());
		}
		void SeekForPrev(const Slice&);
		void Seek(const Slice& target) {
			reseted_ = false;
			UnzipIterRecord(iter_->Seek(fstringOf(target)));
		}
		void Next();
		void Prev();

		void SetInvalid() { reseted_ = true; }
		Slice key() const {
			assert(iter_->Valid());
			return SliceOf(keyBuf_);
		}
		Slice value() const {
			assert(iter_->Valid());
			return SliceOf(fstring(valueBuf_));
		}
		Status status() const { return status_; }
		bool UnzipIterRecord(bool);
		
	public:
		// TBD(kg): ...
		TerarkChunkReader* chunk_;

	protected:
		bool  reseted_;
		std::unique_ptr<TerarkIndex::Iterator> iter_;
		valvec<byte_t>          keyBuf_;
		valvec<byte_t>          valueBuf_;
		Status                  status_;
	};
	TerarkReaderIterator* NewIterator();

	private:
	Status Open();
	
	private:
	const std::string chunk_name_;
	const TerarkZipTableOptions& table_options_;

	std::unique_ptr<TerarkIndex> index_;
	std::unique_ptr<terark::BlobStore> store_;
	std::unique_ptr<rocksdb::RandomAccessFileReader> file_reader_;
	};


}  // namespace rocksdb

