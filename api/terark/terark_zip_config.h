/*
 * terark_zip_config.h
 *
 *  Created on: 2016-08-09
 *      Author: leipeng
 */

#pragma once

#include <string>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/options.h"

#include "trk_common.h"

namespace rocksdb {

	struct TerarkZipTableOptions {
		// copy of DictZipBlobStore::Options::EntropyAlgo
		enum EntropyAlgo {
			kNoEntropy,
			kHuffman,
			kFSE,
		};
		int indexNestLevel = 3;

		/// 0 : check sum nothing
		/// 1 : check sum meta data and index, check on file load
		/// 2 : check sum all data, not check on file load, checksum is for
		///     each record, this incurs 4 bytes overhead for each record
		/// 3 : check sum all data with one checksum value, not checksum each record,
		///     if checksum doesn't match, load will fail
		int checksumLevel = 1;

		EntropyAlgo entropyAlgo = kNoEntropy;

		///    < 0 : only last level using terarkZip
		///          this is equivalent to terarkZipMinLevel == num_levels-1
		/// others : use terarkZip when curlevel >= terarkZipMinLevel
		///          this includes the two special cases:
		///                   == 0 : all levels using terarkZip
		///          >= num_levels : all levels using fallback TableFactory
		/// it shown that set terarkZipMinLevel = 0 is the best choice
		/// if mixed with rocksdb's native SST, those SSTs may using too much
		/// memory & SSD, which degrades the performance
		int terarkZipMinLevel = 0;

		/// please always set to 0
		/// unless you know what it really does for
		/// 0 : no debug
		/// 1 : enable infomation output
		/// 2 : verify 2nd pass iter keys
		/// 3 : verify 2nd pass iter keys & values
		/// 4 : dump 1st & 2nd pass data to file
		int debugLevel = 0;

		bool useSuffixArrayLocalMatch = false;
		bool isOfflineBuild = false;
		bool warmUpIndexOnOpen = true;
		bool warmUpValueOnOpen = false;
		bool disableSecondPassIter = false;

		float estimateCompressionRatio = 0.2f;
		double sampleRatio = 0.03;
		std::string localTempDir = "/tmp";
		std::string indexType = "IL_256";
		std::string extendedConfigFile;

		size_t softZipWorkingMemLimit = 16ull << 30;
		size_t hardZipWorkingMemLimit = 32ull << 30;
		size_t smallTaskMemory = 1200 << 20; // 1.2G
		// use dictZip for value when average value length >= minDictZipValueSize
		// otherwise do not use dictZip
		size_t minDictZipValueSize = 30;
		size_t keyPrefixLen = 0; // for IndexID

		// should be a small value, typically 0.001
		// default is to disable indexCache, because the improvement
		// is about only 10% when set to 0.001
		double indexCacheRatio = 0;//0.001;
	};

	// Prints logs to stderr for faster debugging
	/*class StdoutLogger {
	public:
		StdoutLogger(){}

		// Brings overloaded Logv()s into scope so they're not hidden when we override
		// a subset of them.
		void Logv(const char* format, va_list ap) override {
			vfprintf(stdout, format, ap);
			fprintf(stdout, "\n");
		}
		};*/

	class TerarkTableBuilderOptions {
	public:
	TerarkTableBuilderOptions(const rocksdb::TComparator& comp) : internal_comparator(comp) {
			//info_log = new StdoutLogger();
		}
			
		// replace ImmutableCFOptions with Options
		rocksdb::Options ioptions;
		//
		std::string column_family_name;

		//rocksdb::Logger* info_log;

		//const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
		//  int_tbl_prop_collector_factories;
		CompressionType compression_type;

		const TComparator& internal_comparator;

		int level; // what level this table/file is on, -1 for "not set, don't know"
	};

	class TerarkTableReaderOptions {
	public:
	TerarkTableReaderOptions(const rocksdb::Comparator& comp) : internal_comparator(comp) {
		}
			
		// replace ImmutableCFOptions with Options
		rocksdb::Options ioptions;

		rocksdb::EnvOptions env_options;

		//const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
		//  int_tbl_prop_collector_factories;
		CompressionType compression_type;

		const Comparator& internal_comparator;

		// This is only used for BlockBasedTable (reader)
		bool skip_filters;

		int level; // what level this table/file is on, -1 for "not set, don't know"
	};


	/// @memBytesLimit total memory can be used for the whole process
	///   memBytesLimit == 0 indicate all physical memory can be used
	bool TerarkZipConfigFromEnv(TerarkZipTableOptions&);

}  // namespace rocksdb

