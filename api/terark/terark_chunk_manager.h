
#pragma once

// std headers
#include <memory>
#include <mutex>
// boost
#include "boost/smart_ptr/detail/spinlock.hpp"
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
		TerarkChunkManager(const std::string& config);

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
			std::lock_guard<boost::detail::spinlock> lock(dict_s_);
			// remove related cursor
			auto siter = cursor_set_.begin();
			while (siter != cursor_set_.end()) {
				WT_CURSOR* cursor = *siter;
				if (cursor->uri == fname) {
					siter = cursor_set_.erase(siter);
					Iterator* iter = ((wt_terark_cursor*)cursor)->iter;
					delete iter;
				} else {
					siter ++;
				}
			}
			// remove reader
			auto reader = reader_dict_[fname];
			reader_dict_.erase(fname);
			if (reader) {
				delete reader;
			}
		}

		void AddIterator(WT_CURSOR* cursor) {
			std::lock_guard<boost::detail::spinlock> lock(dict_s_);
			assert(cursor != nullptr);
			cursor_set_.insert(cursor);
		}
		void RemoveIterator(WT_CURSOR* cursor) {
			assert(cursor != nullptr);
			std::lock_guard<boost::detail::spinlock> lock(dict_s_);
			cursor_set_.erase(cursor);
			Iterator* iter = ((wt_terark_cursor*)cursor)->iter;
			delete iter;
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
			std::lock_guard<boost::detail::spinlock> lock(dict_s_);
			reader_dict_[fname] = reader;
		}
		TerarkChunkReader* GetReader(const std::string& fname) {
			std::lock_guard<boost::detail::spinlock> lock(dict_s_);
			return reader_dict_[fname];
		}

		TerarkZipTableOptions table_options_;
		mutable size_t nth_new_terark_table_ = 0;
		mutable size_t nth_new_fallback_table_ = 0;
 
		// builder, reader key: uri, not path
		std::map<std::string, TerarkChunkBuilder*> builder_dict_;
		std::map<std::string, TerarkChunkReader*> reader_dict_;
		std::set<WT_CURSOR*>      cursor_set_;
		boost::detail::spinlock dict_s_;
	};


}  // namespace
