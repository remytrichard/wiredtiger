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
#include "terark_zip_common.h"
#include "terark_zip_config.h"
#include "terark_zip_internal.h"
#include "terark_zip_index.h"
#include "trk_block.h"
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

	class TerarkChunkBuilder : boost::noncopyable {
	public:
		TerarkChunkBuilder(const TerarkZipTableOptions&,
						   const TerarkTableBuilderOptions& tbo,
						   const std::string& fname);

		~TerarkChunkBuilder();

	public:
		// first pass
		void Add(const Slice& key, const Slice& value);
		Status Finish1stPass();

		// second pass
		void Add(const Slice& value) {
			valvec<byte_t> record(value.data(), value.size());
			zbuilder_->addRecord(record);
		}
		Status Finish2ndPass();

		//Status status() const { return status_; }
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
						  TerarkBlockHandle& dataBlock);

		Status WriteSSTFile(terark::BlobStore* zstore,
							terark::BlobStore::Dictionary dict,
							const DictZipBlobStore::ZipStat& dzstat);
		Status WriteMetaData(std::initializer_list<std::pair<const std::string*, TerarkBlockHandle> > blocks);

		// test related
		void DebugPrepare();
		void DebugCleanup();

		
	private:
		const std::string chunk_name_;

		const TerarkZipTableOptions& table_options_;
		const TerarkTableBuilderOptions& table_build_options_; // replace ImmutableCFOptions with TerarkTBOptions

		std::unique_ptr<DictZipBlobStore::ZipBuilder> zbuilder_;
		valvec<KeyValueStatus> histogram_; // per keyPrefix one elem
		valvec<byte_t> prevUserKey_;
		//valvec<byte_b> value_;
		TempFileDeleteOnClose tmpKeyFile_;
		TempFileDeleteOnClose tmpValueFile_;
		TempFileDeleteOnClose tmpSampleFile_;
		FileStream tmpDumpFile_;
		AutoDeleteFile tmpIndexFile_;
		AutoDeleteFile tmpStoreFile_;
		std::mt19937_64 randomGenerator_;
		uint64_t sampleUpperBound_;
		size_t sampleLenSum_ = 0;
		std::unique_ptr<rocksdb::WritableFileWriter> file_writer_;
		uint64_t offset_ = 0;
		Status status_;
		TerarkTableProperties properties_;
		terark::fstrvec valueBuf_;
		bool closed_ = false;  // Either Finish() or Abandon() has been called.

	public:
		enum TimeStamp {
		    kBuildStart = 0,
			kBuildIndexStart = 1,
			kNotUsed2 = 2,
			kSampleStart = 3,
			kBlobStoreFinish = 4,
			kGetOrderMapStart = 5,
			kBZTypeBuildStart = 6,
			kReorderStart = 7,
			kBuildFinish = 8
				};
	private:
		std::vector<long long> tms_;

		long long t0 = 0; // builder start
		long long t1 = 0; // build key index start
		long long t2 = 0; // not used
		long long t3 = 0; // start sampling
		long long t4 = 0; // finish blob store
		long long t5 = 0; // start get order map(traverse)
		long long t6 = 0; // start bzType
		long long t7 = 0; // start reorder
		long long t8 = 0; // all data written, build finished
	};


}  // namespace rocksdb

#endif /* TERARK_ZIP_TABLE_BUILDER_H_ */
