//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/slice.h"

#include "trk_common.h"
#include "trk_block.h"
#include "trk_format.h"

namespace rocksdb {

	class Env;
	class RandomAccessFile;
	struct TerarkTableProperties;
	class TerarkMetaIndexBuilder {
	public:
		TerarkMetaIndexBuilder(const TerarkMetaIndexBuilder&) = delete;
		TerarkMetaIndexBuilder& operator=(const TerarkMetaIndexBuilder&) = delete;

		TerarkMetaIndexBuilder();
		void Add(const std::string& key, const TerarkBlockHandle& handle);

		// Write all the added key/value pairs to the block and return the contents
		// of the block.
		Slice Finish();

	private:
		// store the sorted key/handle of the metablocks.
		std::map<std::string, std::string> meta_block_handles_;
		std::unique_ptr<TerarkBlockBuilder> meta_index_block_;
	};

	class TerarkPropertyBlockBuilder {
	public:
		TerarkPropertyBlockBuilder(const TerarkPropertyBlockBuilder&) = delete;
		TerarkPropertyBlockBuilder& operator=(const TerarkPropertyBlockBuilder&) = delete;

		TerarkPropertyBlockBuilder();

		void AddTableProperty(const TerarkTableProperties& props);
		void Add(const std::string& key, uint64_t value);
		void Add(const std::string& key, const std::string& value);

		// Write all the added entries to the block and return the block contents
		Slice Finish();

	private:
		std::unique_ptr<TerarkBlockBuilder> properties_block_;
		std::map<std::string, std::string> props_;
	};


	// Directly read the properties from the properties block of a plain table.
	// @returns a status to indicate if the operation succeeded. On success,
	//          *table_properties will point to a heap-allocated TableProperties
	//          object, otherwise value of `table_properties` will not be modified.
	Status TerarkReadTableProperties(RandomAccessFileReader* file, uint64_t file_size,
									 uint64_t table_magic_number,
									 TerarkTableProperties** properties);

	// Read the specified meta block with name meta_block_name
	// from `file` and initialize `contents` with contents of this block.
	// Return Status::OK in case of success.
	Status TerarkReadMetaBlock(RandomAccessFileReader* file, uint64_t file_size,
						 uint64_t table_magic_number,
						 const std::string& meta_block_name,
						 TerarkBlockContents* contents);

}  // namespace rocksdb
