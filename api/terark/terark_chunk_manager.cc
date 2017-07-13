
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
#ifdef _MSC_VER
# include <io.h>
#else
# include <sys/types.h>
# include <sys/mman.h>
#endif
// boost headers
#include <boost/algorithm/string.hpp>
#include <boost/predef/other/endian.h>
// 3rd-party headers
#ifdef TERARK_SUPPORT_UINT64_COMPARATOR
# if !BOOST_ENDIAN_LITTLE_BYTE && !BOOST_ENDIAN_BIG_BYTE
#   error Unsupported endian !
# endif
#endif
// terark headers
#include <terark/lcast.hpp>
#include <terark/util/mmap.hpp>
// project headers
#include "terark_zip_index.h"
#include "terark_zip_config.h"
#include "terark_chunk_manager.h"

#include "trk_format.h"
#include "trk_meta_blocks.h"

namespace terark {
	
	terark::profiling g_pf;

	namespace {

		bool TerarkZipOptionsFromConfigString(const std::string& config, 
											  TerarkZipTableOptions& tzo) {
			using namespace boost::algorithm;
			std::vector<std::string> settings;
            split(settings, config, is_any_of(","), token_compress_on);

			std::map<std::string, std::string> dict;
			for (auto& str : settings) {
				size_t pos = str.find('=');
				if (pos != std::string::npos) {
					dict[str.substr(0, pos)] = str.substr(pos + 1);
				}
			}
            if (dict.empty()) {
				STD_INFO("TerarkZipConfigFromConfigString() failed because config is empty\n");
				return false;
            }
			const std::string localTempDir = dict["trk_localTempDir"];
			if (localTempDir.empty()) {
				STD_INFO("TerarkZipConfigFromConfigString() failed because localTempDir is not defined\n");
				return false;
			}
			tzo.localTempDir = localTempDir;
			if (dict.count("trk_entropyAlgo") > 0) {
				std::string algo = dict["trk_entropyAlgo"];
				if (algo == "NoEntropy") {
					tzo.entropyAlgo = tzo.kNoEntropy;
				} else if (algo == "FSE") {
					tzo.entropyAlgo = tzo.kFSE;
				} else if (algo == "huf") {
					tzo.entropyAlgo = tzo.kHuffman;
				} else if (algo == "huff") {
					tzo.entropyAlgo = tzo.kHuffman;
				} else if (algo == "huffman") {
					tzo.entropyAlgo = tzo.kHuffman;
				} else {
					tzo.entropyAlgo = tzo.kNoEntropy;
					STD_WARN("bad env entropyAlgo=%s, must be one of {NoEntropy, FSE, huf}, reset to default 'NoEntropy'\n"
							 , algo);
				}
			}
			if (dict.count("trk_indexType") > 0) {
				tzo.indexType = dict["trk_indexType"];
			}

			auto GetInt = [&](const std::string& name, int default_val) {
				return dict.count(name) > 0 ? atoi(dict[name].c_str()) : default_val;
			};
			auto GetDouble = [&](const std::string& name, double default_val) {
				return dict.count(name) > 0 ? atof(dict[name].c_str()) : default_val;
			};

			tzo.checksumLevel = GetInt("trk_checksumLevel", 3);
			tzo.indexNestLevel = GetInt("trk_indexNestLevel", 3);
			tzo.terarkZipMinLevel = GetInt("trk_terarkZipMinLevel", 0);
			tzo.debugLevel = GetInt("trk_debugLevel", 0);
			tzo.keyPrefixLen = GetInt("trk_keyPrefixLen", 0);
			tzo.useSuffixArrayLocalMatch = GetInt("trk_useSuffixArrayLocalMatch", 0);
			tzo.warmUpIndexOnOpen = GetInt("trk_warmUpIndexOnOpen", 1);
			tzo.warmUpValueOnOpen = GetInt("trk_warmUpValueOnOpen", 0);
			tzo.disableSecondPassIter = GetInt("trk_disableSecondPassIter", 1);
			{   // TBD(kg):...
				size_t page_num  = sysconf(_SC_PHYS_PAGES);
				size_t page_size = sysconf(_SC_PAGE_SIZE);
				size_t memBytesLimit = page_num * page_size;

				tzo.softZipWorkingMemLimit = memBytesLimit * 7 / 8;
				tzo.hardZipWorkingMemLimit = tzo.softZipWorkingMemLimit;
				tzo.smallTaskMemory = memBytesLimit / 16;
				tzo.indexNestLevel = 2;
			}
			tzo.estimateCompressionRatio = GetDouble("trk_estimateCompressionRatio", 0.20);
			tzo.sampleRatio = GetDouble("trk_sampleRatio", 0.03);
			tzo.indexCacheRatio = GetDouble("trk_indexCacheRatio", 0.00);

			std::string name = "trk_softZipWorkingMemLimit";
			if (dict.count(name) > 0) {
				tzo.softZipWorkingMemLimit = terark::ParseSizeXiB(dict[name].c_str());
			}
			name = "trk_hardZipWorkingMemLimit";
			if (dict.count(name) > 0) {
				tzo.hardZipWorkingMemLimit = terark::ParseSizeXiB(dict[name].c_str());
			}
			name = "trk_smallTaskMemory";
			if (dict.count(name) > 0) {
				tzo.smallTaskMemory = terark::ParseSizeXiB(dict[name].c_str());
			}

			if (tzo.debugLevel) {
				STD_INFO("TerarkZipConfigFromConfigString(dbo, cfo) successed\n");
			}
			return true;
		}



		bool IsBytewiseComparator(const Comparator* cmp) {
			return cmp->Name() == std::string("leveldb.BytewiseComparator");
		}

	} // namespace


	TerarkChunkManager::TerarkChunkManager(const std::string& config)
		: reader_cache_(5) {
		TerarkZipTableOptions& tzo = table_options_;
		if (!TerarkZipOptionsFromConfigString(config, tzo)) {
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
		/*if (tzo.debugLevel < 0) {
		  STD_INFO("NewTerarkChunkManager(\n%s)\n",
		  _instance->GetPrintableTableOptions().c_str());
		  }*/
	}

	// TBD(kg): should we reuse such file_reader?
	bool TerarkChunkManager::IsChunkExist(const std::string& fname, 
										  const std::string& cur_tag) {
		// check within reader
		if (GetReader(cur_tag)) {
			return true;
		}
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
		const terark::Comparator* userCmp = &table_builder_options.internal_comparator;
		if (!IsBytewiseComparator(userCmp)) {
			THROW_STD(invalid_argument,
				"TerarkChunkManager::NewTableBuilder(): "
				"user comparator must be 'leveldb.BytewiseComparator'");
		}
		int curlevel = table_builder_options.level;
		int numlevel = table_builder_options.num_levels;
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

	Iterator*
	TerarkChunkManager::NewIterator(const std::string& fname, const std::string& cur_tag) {
		terark::TerarkChunkReader* reader = GetReader(cur_tag);
		if (!reader) {
			reader = new TerarkChunkReader(table_options_, fname);
			AddReader(cur_tag, reader);
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
