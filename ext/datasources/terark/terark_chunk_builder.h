/*
 * terark_zip_table_builder.h
 *
 *  Created on: 2017-05-02
 *      Author: zhaoming
 */


#pragma once

// std headers
#include <mutex>
#include <random>
#include <vector>
// wiredtiger headers
#include "wiredtiger.h"
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
#include "util/trk_block.h"
#include "util/trk_format.h"
#include "terark_zip_common.h"
#include "terark_zip_config.h"
#include "terark_zip_index.h"


namespace terark {

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
			valAdded2ndPass_ ++;
			fstring record(value.data(), value.size());
			zbuilder_->addRecord(record);
		}
		Status Finish2ndPass();


		void Abandon();
		
	private:
		struct KeyValueStatus {
			TerarkIndex::KeyStat stat;
			valvec<char> prefix;
			Uint64Histogram key;
			Uint64Histogram value;
			size_t keyFileBegin = 0;
			size_t keyFileEnd = 0;
			size_t valueFileBegin = 0;
			size_t valueFileEnd = 0;
		};
		Status EmptyTableFinish();

		Status ZipValueToFinish(std::function<void()> waitIndex);
		void   BuildReorderMap(TerarkIndex* index, KeyValueStatus& kvs);
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
		bool useUint64Comparator_;
		std::string work_path_;
		TempFileDeleteOnClose tmpKeyFile_;
		TempFileDeleteOnClose tmpSampleFile_;
		FileWriter file_writer_;
		FileStream tmpDumpFile_;
		AutoDeleteFile tmpIndexFile_;
		AutoDeleteFile tmpReorderFile_;
		AutoDeleteFile tmpStoreFile_;
		std::mt19937_64 randomGenerator_;
		uint64_t sampleUpperBound_;
		size_t sampleLenSum_ = 0;
		size_t valAdded2ndPass_ = 0;
		uint64_t offset_ = 0;
		Status status_;
		TerarkTableProperties properties_;
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


}  // namespace

