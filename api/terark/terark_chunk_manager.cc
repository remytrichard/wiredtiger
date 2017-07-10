
/*
 * terark_zip_table.cc
 *
 *  Created on: 2016-08-09
 *      Author: leipeng
 */

// std headers
#include <future>
#include <random>
#include <cstdlib>
#include <cstdint>
#include <fstream>
// for #include <sys/mman.h>
#ifdef _MSC_VER
# include <io.h>
#else
# include <sys/types.h>
# include <sys/mman.h>
#endif
// boost headers
#include <boost/predef/other/endian.h>
// 3rd-party headers
#ifdef TERARK_SUPPORT_UINT64_COMPARATOR
# if !BOOST_ENDIAN_LITTLE_BYTE && !BOOST_ENDIAN_BIG_BYTE
#   error Unsupported endian !
# endif
#endif
// rocksdb headers
#include "file_reader_writer.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
// terark headers
#include <terark/lcast.hpp>
#include <terark/util/mmap.hpp>
// project headers
#include "terark_zip_index.h"
#include "terark_zip_internal.h"
#include "terark_chunk_manager.h"

#include "trk_format.h"
#include "trk_meta_blocks.h"
#include "trk_table_properties.h"

namespace rocksdb {
	
	terark::profiling g_pf;

	const uint64_t kTerarkZipTableMagicNumber = 0x1122334455667788;

	const std::string kTerarkZipTableIndexBlock        = "TerarkZipTableIndexBlock";
	const std::string kTerarkZipTableValueTypeBlock    = "TerarkZipTableValueTypeBlock";
	const std::string kTerarkZipTableValueDictBlock    = "TerarkZipTableValueDictBlock";
	const std::string kTerarkZipTableOffsetBlock       = "TerarkZipTableOffsetBlock";
	const std::string kTerarkZipTableCommonPrefixBlock = "TerarkZipTableCommonPrefixBlock";
	const std::string kTerarkEmptyTableKey             = "ThisIsAnEmptyTable";

	namespace {

		bool TerarkZipOptionsFromEnv(TerarkZipTableOptions& tzo) {
			const char* localTempDir = getenv("TerarkZipTable_localTempDir");
			if (!localTempDir) {
				STD_INFO("TerarkZipConfigFromEnv(dbo, cfo) failed because env TerarkZipTable_localTempDir is not defined\n");
				return false;
			}
			if (!*localTempDir) {
				THROW_STD(invalid_argument,
						  "If env TerarkZipTable_localTempDir is defined, it must not be empty");
			}
			tzo.localTempDir = localTempDir;
			if (const char* algo = getenv("TerarkZipTable_entropyAlgo")) {
				if (strcasecmp(algo, "NoEntropy") == 0) {
					tzo.entropyAlgo = tzo.kNoEntropy;
				} else if (strcasecmp(algo, "FSE") == 0) {
					tzo.entropyAlgo = tzo.kFSE;
				} else if (strcasecmp(algo, "huf") == 0) {
					tzo.entropyAlgo = tzo.kHuffman;
				} else if (strcasecmp(algo, "huff") == 0) {
					tzo.entropyAlgo = tzo.kHuffman;
				} else if (strcasecmp(algo, "huffman") == 0) {
					tzo.entropyAlgo = tzo.kHuffman;
				} else {
					tzo.entropyAlgo = tzo.kNoEntropy;
					STD_WARN("bad env TerarkZipTable_entropyAlgo=%s, must be one of {NoEntropy, FSE, huf}, reset to default 'NoEntropy'\n"
							 , algo);
				}
			}
			if (const char* env = getenv("TerarkZipTable_indexType")) {
				tzo.indexType = env;
			}

#define MyGetInt(obj, name, Default)									\
			obj.name = (int)terark::getEnvLong("TerarkZipTable_" #name, Default)
#define MyGetBool(obj, name, Default)									\
			obj.name = terark::getEnvBool("TerarkZipTable_" #name, Default)
#define MyGetDouble(obj, name, Default)									\
			obj.name = terark::getEnvDouble("TerarkZipTable_" #name, Default)
#define MyGetXiB(obj, name)											\
			if (const char* env = getenv("TerarkZipTable_" #name))	\
				obj.name = terark::ParseSizeXiB(env)

			MyGetInt   (tzo, checksumLevel           , 3    );
			MyGetInt   (tzo, indexNestLevel          , 3    );
			MyGetInt   (tzo, terarkZipMinLevel       , 0    );
			MyGetInt   (tzo, debugLevel              , 0    );
			MyGetInt   (tzo, keyPrefixLen            , 0    );
			MyGetBool  (tzo, useSuffixArrayLocalMatch, false);
			MyGetBool  (tzo, warmUpIndexOnOpen       , true );
			MyGetBool  (tzo, warmUpValueOnOpen       , false);
			MyGetBool  (tzo, disableSecondPassIter   , true);

			{   // TBD(kg):...
				size_t page_num  = sysconf(_SC_PHYS_PAGES);
				size_t page_size = sysconf(_SC_PAGE_SIZE);
				size_t memBytesLimit = page_num * page_size;

				tzo.softZipWorkingMemLimit = memBytesLimit * 7 / 8;
				tzo.hardZipWorkingMemLimit = tzo.softZipWorkingMemLimit;
				tzo.smallTaskMemory = memBytesLimit / 16;
				tzo.indexNestLevel = 2;
			}

			MyGetDouble(tzo, estimateCompressionRatio, 0.20 );
			MyGetDouble(tzo, sampleRatio             , 0.03 );
			MyGetDouble(tzo, indexCacheRatio         , 0.00 );

			MyGetXiB(tzo, softZipWorkingMemLimit);
			MyGetXiB(tzo, hardZipWorkingMemLimit);
			MyGetXiB(tzo, smallTaskMemory);

			if (tzo.debugLevel) {
				STD_INFO("TerarkZipConfigFromEnv(dbo, cfo) successed\n");
			}
			return true;
		}

		bool IsBytewiseComparator(const TComparator* cmp) {
			return cmp->Name() == std::string("leveldb.BytewiseComparator");
		}

	} // namespace

	TerarkChunkManager* TerarkChunkManager::_instance = nullptr;
	TerarkChunkManager* 
	TerarkChunkManager::sharedInstance() {
		if (!_instance) {
			TerarkZipTableOptions tzo;
			if (!TerarkZipOptionsFromEnv(tzo)) {
				abort();
			}
			int err = 0;
			try {
				TempFileDeleteOnClose test;
				test.path = tzo.localTempDir + "/Terark-XXXXXX";
				test.open_temp();
				test.writer << "Terark";
				test.complete_write();
			} catch (...) {
				fprintf(stderr
						, "ERROR: bad localTempDir %s %s\n"
						, tzo.localTempDir, err ? strerror(err) : "");
				abort();
			}
			_instance = new TerarkChunkManager;
			_instance->table_options_ = tzo;
			/*if (tzo.debugLevel < 0) {
				STD_INFO("NewTerarkChunkManager(\n%s)\n",
						 _instance->GetPrintableTableOptions().c_str());
						 }*/
		}
		return _instance;
	}

	// TBD(kg): should we reuse such file_reader?
	bool TerarkChunkManager::IsChunkExist(const std::string& fname) {
		// check within reader first ?
		size_t file_size = 0;
		Status s = GetFileSize(fname, &file_size);
		if (s.ok() && file_size > 100) {
			std::unique_ptr<terark::MmapWholeFile> 
				file_reader(new terark::MmapWholeFile(fname));
			// check footer
			Slice slice((const char*)file_reader->base, file_reader->size);
			TerarkTableProperties* table_props = nullptr;
			Status s = TerarkReadTableProperties(slice, file_reader->size,
										  kTerarkZipTableMagicNumber, &table_props);
			if (s.ok()) {
				//file_reader_.reset(reader.release());
				return true;
			} else {
				abort();
			}
		}
		return false;
	}

	TerarkChunkBuilder*
	TerarkChunkManager::NewTableBuilder(const TerarkTableBuilderOptions& table_builder_options,
		const std::string& fname) {
		const rocksdb::TComparator* userCmp = &table_builder_options.internal_comparator;
		if (!IsBytewiseComparator(userCmp)) {
			THROW_STD(invalid_argument,
				"TerarkChunkManager::NewTableBuilder(): "
				"user comparator must be 'leveldb.BytewiseComparator'");
		}
		int curlevel = table_builder_options.level;
		int numlevel = table_builder_options.ioptions.num_levels;
		int minlevel = table_options_.terarkZipMinLevel;
		if (minlevel < 0) {
			minlevel = numlevel - 1;
		}
		//INFO(table_builder_options.ioptions.info_log
		printf("nth_newtable{ terark = %3zd fallback = %3zd } curlevel = %d minlevel = %d numlevel = %d\n"
			   , nth_new_terark_table_, nth_new_fallback_table_, curlevel, minlevel, numlevel);
		if (0 == nth_new_terark_table_) {
			g_lastTime = g_pf.now();
		}
		nth_new_terark_table_++;
		TerarkChunkBuilder* builder = new TerarkChunkBuilder(table_options_,
			table_builder_options,
			fname);
		return builder;
	}

	TerarkIterator*
	TerarkChunkManager::NewIterator(const std::string& fname) {
		rocksdb::TerarkChunkReader* reader = GetReader(fname);
		if (!reader) {
			reader = new TerarkChunkReader(table_options_, fname);
			AddReader(fname, reader);
		}
		return reader->NewIterator();
	}


	std::string TerarkChunkManager::GetPrintableTableOptions() const {
		std::string ret;
		ret.reserve(2000);
		const char* cvb[] = {"false", "true"};
		const int kBufferSize = 200;
		char buffer[kBufferSize];
		const auto& tzto = table_options_;
		const double gb = 1ull << 30;

		ret += "localTempDir             : ";
		ret += tzto.localTempDir;
		ret += '\n';

#ifdef M_APPEND
# error WTF ?
#endif
#define M_APPEND(fmt, value)											\
		ret.append(buffer, snprintf(buffer, kBufferSize, fmt "\n", value))

		M_APPEND("indexType                : %s", tzto.indexType.c_str());
		M_APPEND("checksumLevel            : %d", tzto.checksumLevel);
		M_APPEND("entropyAlgo              : %d", (int)tzto.entropyAlgo);
		M_APPEND("indexNestLevel           : %d", tzto.indexNestLevel);
		M_APPEND("terarkZipMinLevel        : %d", tzto.terarkZipMinLevel);
		M_APPEND("debugLevel               : %d", tzto.debugLevel);
		M_APPEND("useSuffixArrayLocalMatch : %s", cvb[!!tzto.useSuffixArrayLocalMatch]);
		M_APPEND("warmUpIndexOnOpen        : %s", cvb[!!tzto.warmUpIndexOnOpen]);
		M_APPEND("warmUpValueOnOpen        : %s", cvb[!!tzto.warmUpValueOnOpen]);
		M_APPEND("disableSecondPassIter    : %s", cvb[!!tzto.disableSecondPassIter]);
		M_APPEND("estimateCompressionRatio : %f", tzto.estimateCompressionRatio);
		M_APPEND("sampleRatio              : %f", tzto.sampleRatio);
		M_APPEND("indexCacheRatio          : %f", tzto.indexCacheRatio);
		M_APPEND("softZipWorkingMemLimit   : %.3fGB", tzto.softZipWorkingMemLimit / gb);
		M_APPEND("hardZipWorkingMemLimit   : %.3fGB", tzto.hardZipWorkingMemLimit / gb);
		M_APPEND("smallTaskMemory          : %.3fGB", tzto.smallTaskMemory / gb);

#undef M_APPEND

		return ret;
	}

}
