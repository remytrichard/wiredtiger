
#pragma once

// project headers
#include "terark_zip_common.h"
#include "terark_zip_table.h"
#include "terark_zip_table_builder.h"
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
	typedef TerarkZipTableBuilder TerarkChunk;
	class TerarkChunkManager : boost::noncopyable {
	private:
		static TerarkChunkManager* _instance;
	 
		TerarkChunkManager() {}
		~TerarkChunkManager() {}

	public:
		static TerarkChunkManager* sharedInstance();

		const char* Name() const { return "TerarkChunkManager"; }

	public:
		void AddChunk(const std::string& fname, TerarkChunk* chunk) {
			chunk_dict_[fname] = chunk;
		}
		TerarkChunk* GetChunk(const std::string& fname) {
			return chunk_dict_[fname];
		}

	public:
		TerarkZipTableBuilder*
			NewTableBuilder(const TerarkTableBuilderOptions& table_builder_options,
							const std::string& fname,
							WritableFileWriter* file);
		
		Status NewIterator(const std::string& fname, Iterator** iter);

		std::string GetPrintableTableOptions() const;

		// Sanitizes the specified DB Options.
		//Status SanitizeOptions(const DBOptions& db_opts,
		//	const ColumnFamilyOptions& cf_opts) const;

		void* GetOptions() { return &table_options_; }

	private:
		TerarkZipTableOptions table_options_;
		//TableFactory* fallback_factory_;
		//TableFactory* adaptive_factory_; // just for open table
		mutable size_t nth_new_terark_table_ = 0;
		mutable size_t nth_new_fallback_table_ = 0;
 
	private:
		// should replace name of TerarkZipTableBuilder with TerarkChunk
		std::map<std::string, TerarkZipTableBuilder*> chunk_dict_;
		std::map<WT_CURSOR*, Iterator*>               cursor_dict_;
	};


}  // namespace rocksdb
