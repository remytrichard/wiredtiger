/*
 * terark_zip_table.cc
 *
 *  Created on: 2016-08-09
 *      Author: leipeng
 */

// project headers
#include "terark_zip_table.h"
#include "terark_zip_index.h"
#include "terark_zip_common.h"
#include "terark_zip_internal.h"
#include "terark_zip_table_builder.h"
#include "terark_zip_table_reader.h"

// std headers
#include <future>
#include <random>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <util/arena.h> // for #include <sys/mman.h>
#ifdef _MSC_VER
# include <io.h>
#else
# include <sys/types.h>
# include <sys/mman.h>
#endif

// boost headers
#include <boost/predef/other/endian.h>

// rocksdb headers
#include <table/meta_blocks.h>

// terark headers
#include <terark/lcast.hpp>

// 3rd-party headers
#ifdef TERARK_SUPPORT_UINT64_COMPARATOR
# if !BOOST_ENDIAN_LITTLE_BYTE && !BOOST_ENDIAN_BIG_BYTE
#   error Unsupported endian !
# endif
#endif

namespace rocksdb {

	terark::profiling g_pf;

	const uint64_t kTerarkZipTableMagicNumber = 0x1122334455667788;

	const std::string kTerarkZipTableIndexBlock        = "TerarkZipTableIndexBlock";
	const std::string kTerarkZipTableValueTypeBlock    = "TerarkZipTableValueTypeBlock";
	const std::string kTerarkZipTableValueDictBlock    = "TerarkZipTableValueDictBlock";
	const std::string kTerarkZipTableOffsetBlock       = "TerarkZipTableOffsetBlock";
	const std::string kTerarkZipTableCommonPrefixBlock = "TerarkZipTableCommonPrefixBlock";
	const std::string kTerarkEmptyTableKey             = "ThisIsAnEmptyTable";


	inline static
	bool IsBytewiseComparator(const Comparator* cmp) {
#if 1
		const fstring name = cmp->Name();
		if (name.startsWith("RocksDB_SE_")) {
			return true;
		}
		if (name.startsWith("rev:RocksDB_SE_")) {
			// reverse bytewise compare, needs reverse in iterator
			return true;
		}
# if defined(TERARK_SUPPORT_UINT64_COMPARATOR)
		if (name == "rocksdb.Uint64Comparator") {
			return true;
		}
# endif
		return name == "leveldb.BytewiseComparator";
#else
		return BytewiseComparator() == cmp;
#endif
	}


	Status
	TerarkChunkManager::NewTableReader(
									   const TableReaderOptions& table_reader_options,
									   unique_ptr<RandomAccessFileReader>&& file,
									   uint64_t file_size, unique_ptr<TerarkTableReader>* table,
									   bool prefetch_index_and_filter_in_cache) const {
		const rocksdb::Comparator* userCmp = &table_reader_options.internal_comparator;
		if (!IsBytewiseComparator(userCmp)) {
			return Status::InvalidArgument("TerarkChunkManager::NewTableReader()",
										   "user comparator must be 'leveldb.BytewiseComparator'");
		}
		Footer footer;
		Status s = ReadFooterFromFile(file.get(), file_size, &footer);
		if (!s.ok()) {
			return s;
		}
		if (footer.table_magic_number() != kTerarkZipTableMagicNumber) {
			return Status::InvalidArgument(
										   "TerarkChunkManager::NewTableReader()",
										   "fallback_factory is null and magic_number is not kTerarkZipTable"
										   );
		}
#if 0
		if (!prefetch_index_and_filter_in_cache) {
			WARN(table_reader_options.ioptions.info_log
				 , "TerarkChunkManager::NewTableReader(): "
				 "prefetch_index_and_filter_in_cache = false is ignored, "
				 "all index and data will be loaded in memory\n");
		}
#endif
		BlockContents emptyTableBC;
		s = ReadMetaBlock(file.get(), file_size, kTerarkZipTableMagicNumber
						  , table_reader_options.ioptions, kTerarkEmptyTableKey, &emptyTableBC);
		if (s.ok()) {
			std::unique_ptr<TerarkEmptyTableReader>
				t(new TerarkEmptyTableReader(table_reader_options));
			s = t->Open(file.release(), file_size);
			if (!s.ok()) {
				return s;
			}
			*table = std::move(t);
			return s;
		}
		std::unique_ptr<TerarkZipTableReader>
			t(new TerarkZipTableReader(table_reader_options, table_options_));
		s = t->Open(file.release(), file_size);
		if (s.ok()) {
			*table = std::move(t);
		}
		return s;
	}

	TerarkZipTableBuilder*
	TerarkChunkManager::NewTableBuilder(const TerarkTableBuilderOptions& table_builder_options,
										uint32_t column_family_id,
										WritableFileWriter* file)
		const {
		const rocksdb::Comparator* userCmp = &table_builder_options.internal_comparator;
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
		size_t keyPrefixLen = 0;
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
		if (fstring(userCmp->Name()) == "rocksdb.Uint64Comparator") {
			keyPrefixLen = 0;
		}
#endif
#if 1
		INFO(table_builder_options.ioptions.info_log
			 , "nth_newtable{ terark = %3zd fallback = %3zd } curlevel = %d minlevel = %d numlevel = %d fallback = %p\n"
			 , nth_new_terark_table_, nth_new_fallback_table_, curlevel, minlevel, numlevel, fallback_factory_
			 );
#endif
		if (0 == nth_new_terark_table_) {
			g_lastTime = g_pf.now();
		}
		/*if (fallback_factory_) {
			if (curlevel >= 0 && curlevel < minlevel) {
				nth_new_fallback_table_++;
				TerarkZipTableBuilder* tb = fallback_factory_->NewTableBuilder(table_builder_options,
																			   column_family_id, file);
				INFO(table_builder_options.ioptions.info_log
					 , "TerarkChunkManager::NewTableBuilder() returns class: %s\n"
					 , ClassName(*tb).c_str());
				return tb;
			}
			}*/
		nth_new_terark_table_++;
		return new TerarkZipTableBuilder(
										 table_options_,
										 table_builder_options,
										 column_family_id,
										 file,
										 keyPrefixLen);
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

	Status
	TerarkChunkManager::SanitizeOptions(const DBOptions& db_opts,
										const ColumnFamilyOptions& cf_opts)
		const {
		if (!IsBytewiseComparator(cf_opts.comparator)) {
			return Status::InvalidArgument("TerarkChunkManager::SanitizeOptions()",
										   "user comparator must be 'leveldb.BytewiseComparator'");
		}
		auto indexFactory = TerarkIndex::GetFactory(table_options_.indexType);
		if (!indexFactory) {
			std::string msg = "invalid indexType: " + table_options_.indexType;
			return Status::InvalidArgument(msg);
		}
		return Status::OK();
	}

} /* namespace rocksdb */
