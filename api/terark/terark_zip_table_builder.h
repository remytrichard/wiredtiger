/*
 * terark_zip_table_builder.h
 *
 *  Created on: 2017-05-02
 *      Author: zhaoming
 */


#pragma once

#ifndef TERARK_ZIP_TABLE_BUILDER_H_
#define TERARK_ZIP_TABLE_BUILDER_H_

// std headers
#include <random>
#include <vector>
// wiredtiger headers
#include "wiredtiger.h"
// rocksdb headers
//#include <table/table_builder.h>
//#include <table/block_builder.h>
//#include <table/format.h>
#include <util/arena.h>
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
#include "terark_zip_table.h"
#include "terark_zip_internal.h"
#include "terark_zip_common.h"
#include "terark_zip_index.h"
#include "trk_block_builder.h"
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

	extern std::mutex g_sumMutex;
	extern size_t g_sumKeyLen;
	extern size_t g_sumValueLen;
	extern size_t g_sumUserKeyLen;
	extern size_t g_sumUserKeyNum;
	extern size_t g_sumEntryNum;
	extern long long g_lastTime;

	class TerarkZipTableBuilder : boost::noncopyable {
	public:
		TerarkZipTableBuilder(const TerarkZipTableOptions&,
							  const TerarkTableBuilderOptions& tbo,
							  const std::string& fname,
							  size_t key_prefixLen);

		~TerarkZipTableBuilder();

		enum ChunkState {
			kJustCreated = 0,
			kFirstPass = 1,
			kSecondPass = 2,
			kCreateDone = 3,
			kOpenForRead = 4
		};
		ChunkState GetState() { return chunk_state_; }
		void SetState(ChunkState state) { chunk_state_ = state; }

		/*
		 * read phase operations
		 */
	public:
	
		class TerarChunkIterator : public Iterator, boost::noncopyable {
	public:
		TerarChunkIterator(TerarkZipTableBuilder* chunk) 
			: chunk_(chunk) {
			iter_.reset(chunk_->index_->NewIterator()); 
			iter_->SetInvalid(); 
		}
		~TerarChunkIterator() {}
		bool Valid() const { return iter_->Valid(); }
		void SeekToFirst();
		void SeekToLast();
		void SeekForPrev(const Slice&);
		void Seek(const Slice&);
		void Next();
		void Prev();
		Slice key() const;
		Slice value() const;
		Status status() const { return status_; }
		bool UnzipIterRecord(bool);

		
	public:
		// TBD(kg): ...
		TerarkZipTableBuilder* chunk_;

	protected:
		std::unique_ptr<TerarkIndex::Iterator> iter_;
		valvec<byte_t>          keyBuf_;
		valvec<byte_t>          valueBuf_;
		Status                  status_;
	};
	Iterator* NewIterator();

	private:
	Status OpenForRead();
	
	
		
	/*
	 * building phase operations
	 */
	public:
	void Add(const Slice& key, const Slice& value); // first pass
	Status Finish1stPass();

	// second pass
	void Add(const Slice& value) {
		// TBD(kg): need add State check, not sure if lock should be employed
		valvec<byte_t> record(value.data(), value.size());
		zbuilder_->addRecord(record);
	}
	Status Finish2ndPass();

	Status status() const { return status_; }
	void Abandon();
	uint64_t NumEntries() const { return properties_.num_entries; }
	uint64_t FileSize() const;
		
	private:
	struct KeyValueStatus {
		TerarkIndex::KeyStat stat;
		valvec<char> prefix;
		Uint32Histogram key;
		Uint32Histogram value;
		size_t keyFileBegin = 0;
		size_t keyFileEnd = 0;
		size_t valueFileBegin = 0;
		size_t valueFileEnd = 0;
	};
	Status EmptyTableFinish();

	Status ZipValueToFinish(std::function<void()> waitIndex);
	Status WriteStore(TerarkIndex* index, BlobStore* store,
					  KeyValueStatus& kvs, std::function<void(const void*, size_t)> write,
					  TerarkBlockHandle& dataBlock,
					  long long& t5, long long& t6, long long& t7);
	Status WriteSSTFile(long long t3, long long t4,
						terark::BlobStore* zstore,
						terark::BlobStore::Dictionary dict,
						const DictZipBlobStore::ZipStat& dzstat);
	Status WriteMetaData(std::initializer_list<std::pair<const std::string*, TerarkBlockHandle> > blocks);

	// test related
	void DebugPrepare();
	void DebugCleanup();

		
	private:

	// - build phase
	// - read only phase
	ChunkState chunk_state_;
	const std::string chunk_name_;

	const TerarkZipTableOptions& table_options_;
	const TerarkTableBuilderOptions& table_build_options_; // replace ImmutableCFOptions with TerarkTBOptions

	std::unique_ptr<DictZipBlobStore::ZipBuilder> zbuilder_;
	valvec<KeyValueStatus> histogram_; // per keyPrefix one elem
	valvec<byte_t> prevUserKey_;
	std::vector<long long> tms_;
	//valvec<byte_b> value_;
	TempFileDeleteOnClose tmpKeyFile_;
	TempFileDeleteOnClose tmpValueFile_;
	TempFileDeleteOnClose tmpSampleFile_;
	FileStream tmpDumpFile_;
	AutoDeleteFile tmpIndexFile_;
	AutoDeleteFile tmpStoreFile_;
	//AutoDeleteFile tmpZipDictFile_;
	//AutoDeleteFile tmpZipValueFile_;
	std::mt19937_64 randomGenerator_;
	uint64_t sampleUpperBound_;
	size_t sampleLenSum_ = 0;
	std::unique_ptr<rocksdb::WritableFileWriter> file_writer_;
	uint64_t offset_ = 0;
	Status status_;
	TerarkTableProperties properties_;
	terark::fstrvec valueBuf_; // collect multiple values for one key
	bool closed_ = false;  // Either Finish() or Abandon() has been called.

	long long t0 = 0;
	size_t key_prefixLen_;
		
	// for read purpose
	std::unique_ptr<TerarkIndex> index_;
	std::unique_ptr<terark::BlobStore> store_;
	std::unique_ptr<rocksdb::RandomAccessFileReader> file_reader_;
	};


}  // namespace rocksdb

#endif /* TERARK_ZIP_TABLE_BUILDER_H_ */
