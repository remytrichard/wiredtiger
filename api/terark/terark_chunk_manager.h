
#pragma once

// project headers
#include "terark_zip_common.h"
#include "terark_zip_table.h"
#include "terark_zip_table_builder.h"
#include "terark_zip_table_reader.h"
// std headers
#include <memory>
#include <mutex>
// rocksdb headers
#include <rocksdb/slice.h>
#include <rocksdb/env.h>
#include <rocksdb/table.h>
// terark headers
#include <terark/fstring.hpp>
#include <terark/valvec.hpp>
#include <terark/stdtypes.hpp>
#include <terark/util/profiling.hpp>


namespace rocksdb {

	using terark::fstring;
	using terark::valvec;
	using terark::byte_t;


	extern terark::profiling g_pf;

	extern const uint64_t kTerarkZipTableMagicNumber;

	extern const std::string kTerarkZipTableIndexBlock;
	extern const std::string kTerarkZipTableValueTypeBlock;
	extern const std::string kTerarkZipTableValueDictBlock;
	extern const std::string kTerarkZipTableOffsetBlock;
	extern const std::string kTerarkZipTableCommonPrefixBlock;
	extern const std::string kTerarkEmptyTableKey;

	class TerarkZipTableBuilder;
	class TerarkTableReader;
	using TerarkChunk = TerarkZipTableBuilder;
	class TerarkChunkManager : boost::noncopyable {
	private:
		static TerarkChunkManager* _instance;
	 
		TerarkChunkManager() {}
		~TerarkChunkManager() {}

	public:
		static TerarkChunkManager* sharedInstance();

		const char* Name() const { return "TerarkChunkManager"; }

	public:
		void AddChunk(const std::string&, TerarkChunk*) {}
		TerarkChunk* GetChunk(const std::string&) {}

	public:
		Status
			NewTableReader(const TerarkTableReaderOptions& table_reader_options,
				std::unique_ptr<RandomAccessFileReader>&& file,
				uint64_t file_size,
				std::unique_ptr<TerarkTableReader>* table,
				bool prefetch_index_and_filter_in_cache) const;

		TerarkZipTableBuilder*
			NewTableBuilder(const TerarkTableBuilderOptions& table_builder_options,
				uint32_t column_family_id,
				WritableFileWriter* file) const;

		std::string GetPrintableTableOptions() const;

		// Sanitizes the specified DB Options.
		Status SanitizeOptions(const DBOptions& db_opts,
			const ColumnFamilyOptions& cf_opts) const;

		void* GetOptions() { return &table_options_; }

	private:
		TerarkZipTableOptions table_options_;
		TableFactory* fallback_factory_;
		TableFactory* adaptive_factory_; // just for open table
		mutable size_t nth_new_terark_table_ = 0;
		mutable size_t nth_new_fallback_table_ = 0;
 
	private:
		// should replace name of TerarkZipTableBuilder with TerarkChunk
		std::map<std::string, TerarkChunk*> _chunk_dict;
	};


}  // namespace rocksdb
