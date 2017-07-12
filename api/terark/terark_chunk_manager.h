
#pragma once

// std headers
#include <memory>
#include <mutex>
// rocksdb headers
#include "slice.h"
// wiredtiger headers
#include "wiredtiger.h"
// terark headers
#include <terark/fstring.hpp>
#include <terark/valvec.hpp>
#include <terark/stdtypes.hpp>
#include <terark/util/profiling.hpp>
// project headers
#include "lru_cache.h"
#include "terark_zip_common.h"
#include "terark_zip_config.h"
#include "terark_chunk_builder.h"
#include "terark_chunk_reader.h"


namespace terark {

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

	class TerarkChunkManager : boost::noncopyable {
	private:
		static TerarkChunkManager* _instance;
	 
	TerarkChunkManager() 
		: reader_cache_(5) {}
		~TerarkChunkManager() {}

	public:
		static TerarkChunkManager* sharedInstance();
		const char* Name() const { return "TerarkChunkManager"; }

	public:
		void AddBuilder(const std::string& fname, TerarkChunkBuilder* builder) {
			builder_dict_[fname] = builder;
		}
		void RemoveBuilder(const std::string& fname) {
			builder_dict_.erase(fname);
		}
		TerarkChunkBuilder* GetBuilder(const std::string& fname) {
			return builder_dict_[fname];
		}

		void AddIterator(WT_CURSOR* cursor, Iterator* iter) {
			assert(cursor != nullptr);
			cursor_dict_[cursor] = iter;
			assert(reader_cache_.exists(cursor->uri));
			auto reader = reader_cache_.get(cursor->uri);
			reader->AddRef();
		}
		void RemoveIterator(WT_CURSOR* cursor) {
			assert(cursor != nullptr);
			cursor_dict_.erase(cursor);
			assert(reader_cache_.exists(cursor->uri) == true);
			auto reader = reader_cache_.get(cursor->uri);
			reader->RelRef();
		}
		Iterator* GetIterator(WT_CURSOR* cursor) {
			assert(cursor != nullptr);
			if (reader_cache_.exists(cursor->uri)) {
				reader_cache_.get(cursor->uri); // lru update
			}
			return cursor_dict_[cursor];
		}


	public:
		bool IsChunkExist(const std::string&);

		TerarkChunkBuilder*
			NewTableBuilder(const TerarkTableBuilderOptions& table_builder_options,
							const std::string& fname);
		
		Iterator* 
			NewIterator(const std::string& fname, const std::string& cur_tag);

		std::string GetPrintableTableOptions() const;

		// Sanitizes the specified DB Options.
		//Status SanitizeOptions(const DBOptions& db_opts,
		//	const ColumnFamilyOptions& cf_opts) const;

		void* GetOptions() { return &table_options_; }

	private:

		void AddReader(const std::string& fname, TerarkChunkReader* reader) {
			reader->AddRef();
			reader_cache_.put(fname, reader);
		}
		TerarkChunkReader* GetReader(const std::string& fname) {
			if (!reader_cache_.exists(fname)) {
				return nullptr;
			}
			return reader_cache_.get(fname);
		}

		TerarkZipTableOptions table_options_;
		mutable size_t nth_new_terark_table_ = 0;
		mutable size_t nth_new_fallback_table_ = 0;
 
		// builder, reader key: uri, not path
		std::map<std::string, TerarkChunkBuilder*> builder_dict_;
		lru_cache<std::string, TerarkChunkReader*> reader_cache_; // cache size set to 5 temp now
		std::map<WT_CURSOR*, Iterator*>      cursor_dict_;
	};


}  // namespace
