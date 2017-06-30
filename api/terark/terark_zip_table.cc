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
#include "terark_chunk_manager.h"
#include "terark_zip_table_builder.h"

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
#include "trk_format.h"
#include "trk_meta_blocks.h"

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




	Status
	TerarkChunkManager::NewTableReader(const TerarkTableReaderOptions& table_reader_options,
		unique_ptr<RandomAccessFileReader>&& file,
		uint64_t file_size, 
		unique_ptr<TerarkTableReader>* table,
		bool prefetch_index_and_filter_in_cache) const {
		const rocksdb::Comparator* userCmp = &table_reader_options.internal_comparator;
		if (!IsBytewiseComparator(userCmp)) {
			return Status::InvalidArgument("TerarkChunkManager::NewTableReader()",
				"user comparator must be 'leveldb.BytewiseComparator'");
		}
		TerarkFooter footer;
		Status s = TerarkReadFooterFromFile(file.get(), file_size, &footer);
		if (!s.ok()) {
			return s;
		}
		if (footer.table_magic_number() != kTerarkZipTableMagicNumber) {
			return Status::InvalidArgument("TerarkChunkManager::NewTableReader()",
				"fallback_factory is null and magic_number is not kTerarkZipTable");
		}
#if 0
		if (!prefetch_index_and_filter_in_cache) {
			WARN(table_reader_options.ioptions.info_log
				, "TerarkChunkManager::NewTableReader(): "
				"prefetch_index_and_filter_in_cache = false is ignored, "
				"all index and data will be loaded in memory\n");
		}
#endif
		TerarkBlockContents emptyTableBC;
		s = TerarkReadMetaBlock(file.get(), file_size, kTerarkZipTableMagicNumber
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
