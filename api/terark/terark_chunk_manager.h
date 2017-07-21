
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
#include "util/lru_cache.h"
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
	public:
		//static TerarkChunkManager* _instance;
	 
		TerarkChunkManager(const std::string& config);
		//: reader_cache_(5) {}
	private:
		~TerarkChunkManager() {}

	public:
		//static TerarkChunkManager* sharedInstance();
		const char* Name() const { return "TerarkChunkManager"; }

	public:
		void AddBuilder(const std::string& fname, TerarkChunkBuilder* builder) {
			//std::unique_lock<std::mutex> lock(dict_m_);
			builder_dict_[fname] = builder;
		}
		void RemoveBuilder(const std::string& fname) {
			//std::unique_lock<std::mutex> lock(dict_m_);
			builder_dict_.erase(fname);
		}
		TerarkChunkBuilder* GetBuilder(const std::string& fname) {
			//std::unique_lock<std::mutex> lock(dict_m_);
			return builder_dict_[fname];
		}

		void RemoveReader(const std::string& fname) {
			std::unique_lock<std::recursive_mutex> lock(dict_m_);
			for (auto& item : cursor_dict_) {
				if (item.first->uri == fname) {
					RemoveIterator(item.first);
				}
			}
		}

		void AddIterator(WT_CURSOR* cursor, Iterator* iter) {
			std::unique_lock<std::recursive_mutex> lock(dict_m_);
			assert(cursor != nullptr);
			cursor_dict_[cursor] = iter;
			assert(reader_cache_.exists(std::string(cursor->uri)));
			auto reader = reader_cache_.get(std::string(cursor->uri));
			reader->AddRef();
		}
		void RemoveIterator(WT_CURSOR* cursor) {
			std::unique_lock<std::recursive_mutex> lock(dict_m_);
			assert(cursor != nullptr);
			cursor_dict_.erase(cursor);
			if (reader_cache_.exists(std::string(cursor->uri))) {
				auto reader = reader_cache_.get(std::string(cursor->uri));
				reader->RelRef();
			}
		}
		Iterator* GetIterator(WT_CURSOR* cursor) {
			std::unique_lock<std::recursive_mutex> lock(dict_m_);
			assert(cursor != nullptr);
			if (reader_cache_.exists(std::string(cursor->uri))) {
				reader_cache_.get(std::string(cursor->uri)); // lru update
			}
			return cursor_dict_[cursor];
		}


	public:
		bool IsChunkExist(const std::string&, const std::string&);

		TerarkChunkBuilder*
			NewTableBuilder(const TerarkTableBuilderOptions& table_builder_options,
							const std::string& fname);
		
		Iterator* 
			NewIterator(const std::string& fname, const std::string& cur_tag);

		std::string GetPrintableTableOptions() const;

		void* GetOptions() { return &table_options_; }

	private:
		void AddReader(const std::string& fname, TerarkChunkReader* reader) {
			std::unique_lock<std::recursive_mutex> lock(dict_m_);
			reader->AddRef();
			reader_cache_.put(fname, reader);
		}
		TerarkChunkReader* GetReader(const std::string& fname) {
			std::unique_lock<std::recursive_mutex> lock(dict_m_);
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
		std::recursive_mutex dict_m_;
	};


}  // namespace
