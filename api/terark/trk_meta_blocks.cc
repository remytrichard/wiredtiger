//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#include <map>
#include <string>

#include "trk_block.h"
#include "trk_common.h"
#include "trk_format.h"
#include "trk_meta_blocks.h"
#include "trk_table_properties.h"

namespace rocksdb {

	TerarkMetaIndexBuilder::TerarkMetaIndexBuilder()
		: meta_index_block_(new TerarkBlockBuilder()) {}

	void TerarkMetaIndexBuilder::Add(const std::string& key,
									 const TerarkBlockHandle& handle) {
		std::string handle_encoding;
		handle.EncodeTo(&handle_encoding);
		meta_block_handles_[key] = handle_encoding;
	}

	Slice TerarkMetaIndexBuilder::Finish() {
		for (const auto& metablock : meta_block_handles_) {
			meta_index_block_->Add(metablock.first, metablock.second);
		}
		return meta_index_block_->Finish();
	}

	TerarkPropertyBlockBuilder::TerarkPropertyBlockBuilder()
		: properties_block_(new TerarkBlockBuilder()) {}

	void TerarkPropertyBlockBuilder::Add(const std::string& name,
								   const std::string& val) {
		props_[name] = val;
	}

	void TerarkPropertyBlockBuilder::Add(const std::string& name, uint64_t val) {
		assert(props_.find(name) == props_.end());
		std::string dst;
		TerarkPutFixed64(&dst, val);
		Add(name, dst);
	}

	void TerarkPropertyBlockBuilder::AddTableProperty(const TerarkTableProperties& props) {
		Add(TerarkTablePropertiesNames::kDataSize, props.data_size);
		Add(TerarkTablePropertiesNames::kIndexSize, props.index_size);
		Add(TerarkTablePropertiesNames::kNumEntries, props.num_entries);
	}

	Slice TerarkPropertyBlockBuilder::Finish() {
		for (const auto& prop : props_) {
			properties_block_->Add(prop.first, prop.second);
		}
		return properties_block_->Finish();
	}


	namespace {
		// Read the properties from the table.
		// @returns a status to indicate if the operation succeeded. On success,
		//          *table_properties will point to a heap-allocated TableProperties
		//          object, otherwise value of `table_properties` will not be modified.
		Status TerarkReadProperties(const Slice& handle_value, const Slice& file,
									TerarkTableProperties** table_properties) {
			assert(table_properties);

			Slice v = handle_value;
			TerarkBlockHandle handle;
			if (!handle.DecodeFrom(&v).ok()) {
				return Status::InvalidArgument("Failed to decode properties block handle");
			}

			TerarkBlockContents block_contents;
			Status s = TerarkReadBlockContents(file, handle, &block_contents);
			if (!s.ok()) {
				return s;
			}

			TerarkBlock properties_block(std::move(block_contents));
			std::unique_ptr<TIterator> iter(properties_block.NewIterator(GetBytewiseComparator()));
			//TerarkBlockIter iter;
			//properties_block.NewIterator(BytewiseComparator(), &iter);

			auto new_table_properties = new TerarkTableProperties();
			// All pre-defined properties of type uint64_t
			std::unordered_map<std::string, uint64_t*> predefined_uint64_properties = {
				{TerarkTablePropertiesNames::kDataSize, &new_table_properties->data_size},
				{TerarkTablePropertiesNames::kIndexSize, &new_table_properties->index_size},
				{TerarkTablePropertiesNames::kRawKeySize, &new_table_properties->raw_key_size},
				{TerarkTablePropertiesNames::kRawValueSize, &new_table_properties->raw_value_size},
				{TerarkTablePropertiesNames::kNumEntries, &new_table_properties->num_entries},
			};

			std::string last_key;
			for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
				s = iter->status();
				if (!s.ok()) {
					break;
				}
				auto key = iter->key().ToString();
				// properties block is strictly sorted with no duplicate key.
				assert(last_key.empty() ||
					   GetBytewiseComparator()->Compare(key, last_key) > 0);
				last_key = key;

				auto raw_val = iter->value();
				auto pos = predefined_uint64_properties.find(key);
				if (pos != predefined_uint64_properties.end()) {
					uint64_t val;
					if (!TerarkGetFixed64(&raw_val, &val)) {
						printf("Detect malformed value in properties meta-block");
						continue;
					}
					*(pos->second) = val;
				} else {
					// handle user-collected properties
					new_table_properties->user_collected_properties.insert({key, raw_val.ToString()});
				}
			}
			if (s.ok()) {
				*table_properties = new_table_properties;
			} else {
				delete new_table_properties;
			}

			return s;
		}

		Status TerarkFindMetaBlock(TIterator* meta_index_iter,
								   const std::string& meta_block_name,
								   TerarkBlockHandle* block_handle) {
			meta_index_iter->Seek(meta_block_name);
			if (meta_index_iter->status().ok() && meta_index_iter->Valid() &&
				meta_index_iter->key() == meta_block_name) {
				Slice v = meta_index_iter->value();
				return block_handle->DecodeFrom(&v);
			} else {
				return Status::Corruption("Cannot find the meta block", meta_block_name);
			}
		}

	}


	Status TerarkReadTableProperties(const Slice& file_data, uint64_t file_size,
							   uint64_t table_magic_number,
							   TerarkTableProperties** properties) {
		// -- Read metaindex block
		TerarkFooter footer;
		auto s = TerarkReadFooterFromFile(file_data, file_size, &footer, table_magic_number);
		if (!s.ok()) {
			return s;
		}
		auto metaindex_handle = footer.metaindex_handle();
		TerarkBlockContents metaindex_contents;
		s = TerarkReadBlockContents(file_data, metaindex_handle,
									&metaindex_contents);
		if (!s.ok()) {
			return s;
		}
		TerarkBlock metaindex_block(std::move(metaindex_contents));
		std::unique_ptr<TIterator> meta_iter(metaindex_block.NewIterator(GetBytewiseComparator()));
		if (meta_iter.get() == nullptr) {
			return Status::Corruption("bad block handle");
		}

		// -- Read property block
		TerarkBlockHandle block_handle;
		s = TerarkFindMetaBlock(meta_iter.get(), kPropertiesBlock, &block_handle);
		if (!s.ok()) {
			return s;
		}

		TerarkTableProperties table_properties;
		s = TerarkReadProperties(meta_iter->value(), file_data, properties);
		return s;
	}

	Status TerarkReadMetaBlock(const Slice& file_data, uint64_t file_size,
		uint64_t table_magic_number,
		const std::string& meta_block_name,
		TerarkBlockContents* contents) {
		Status status;
		TerarkFooter footer;
		status = TerarkReadFooterFromFile(file_data, file_size, &footer, table_magic_number);
		if (!status.ok()) {
			return status;
		}

		// Reading metaindex block
		auto metaindex_handle = footer.metaindex_handle();
		TerarkBlockContents metaindex_contents;
		status = TerarkReadBlockContents(file_data, metaindex_handle,
										 &metaindex_contents);
		if (!status.ok()) {
			return status;
		}

		// Finding metablock
		TerarkBlock metaindex_block(std::move(metaindex_contents));
		std::unique_ptr<TIterator> meta_iter;
		meta_iter.reset(metaindex_block.NewIterator(GetBytewiseComparator()));

		TerarkBlockHandle block_handle;
		status = TerarkFindMetaBlock(meta_iter.get(), meta_block_name, &block_handle);

		if (!status.ok()) {
			return status;
		}

		// Reading metablock
		return TerarkReadBlockContents(file_data, 
			block_handle, contents);
	}

}
