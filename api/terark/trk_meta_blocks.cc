//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#include <map>
#include <string>

//#include "rocksdb/table.h"
#include "util/coding.h"

#include "trk_block.h"
#include "trk_format.h"
#include "trk_iter_key.h"
#include "trk_meta_blocks.h"
#include "trk_table_properties.h"

namespace rocksdb {

	TerarkMetaIndexBuilder::TerarkMetaIndexBuilder()
		: meta_index_block_(new TerarkBlockBuilder(1 /* restart interval */)) {}

	void TerarkMetaIndexBuilder::Add(const std::string& key,
							   const TerarkBlockHandle& handle) {
		std::string handle_encoding;
		handle.EncodeTo(&handle_encoding);
		meta_block_handles_.insert({key, handle_encoding});
	}

	Slice TerarkMetaIndexBuilder::Finish() {
		for (const auto& metablock : meta_block_handles_) {
			meta_index_block_->Add(metablock.first, metablock.second);
		}
		return meta_index_block_->Finish();
	}

	TerarkPropertyBlockBuilder::TerarkPropertyBlockBuilder()
		: properties_block_(new TerarkBlockBuilder(1 /* restart interval */)) {}

	void TerarkPropertyBlockBuilder::Add(const std::string& name,
								   const std::string& val) {
		props_.insert({name, val});
	}

	void TerarkPropertyBlockBuilder::Add(const std::string& name, uint64_t val) {
		assert(props_.find(name) == props_.end());

		std::string dst;
		PutVarint64(&dst, val);

		Add(name, dst);
	}

	void TerarkPropertyBlockBuilder::Add(
								   const UserCollectedProperties& user_collected_properties) {
		for (const auto& prop : user_collected_properties) {
			Add(prop.first, prop.second);
		}
	}

	void TerarkPropertyBlockBuilder::AddTableProperty(const TerarkTableProperties& props) {
		Add(TerarkTablePropertiesNames::kRawKeySize, props.raw_key_size);
		Add(TerarkTablePropertiesNames::kRawValueSize, props.raw_value_size);
		Add(TerarkTablePropertiesNames::kDataSize, props.data_size);
		Add(TerarkTablePropertiesNames::kIndexSize, props.index_size);
		Add(TerarkTablePropertiesNames::kNumEntries, props.num_entries);
		Add(TerarkTablePropertiesNames::kNumDataBlocks, props.num_data_blocks);
		Add(TerarkTablePropertiesNames::kFilterSize, props.filter_size);
		Add(TerarkTablePropertiesNames::kFormatVersion, props.format_version);
		Add(TerarkTablePropertiesNames::kFixedKeyLen, props.fixed_key_len);
		Add(TerarkTablePropertiesNames::kColumnFamilyId, props.column_family_id);

		if (!props.filter_policy_name.empty()) {
			Add(TerarkTablePropertiesNames::kFilterPolicy, props.filter_policy_name);
		}
		if (!props.comparator_name.empty()) {
			Add(TerarkTablePropertiesNames::kComparator, props.comparator_name);
		}

		if (!props.merge_operator_name.empty()) {
			Add(TerarkTablePropertiesNames::kMergeOperator, props.merge_operator_name);
		}
		if (!props.prefix_extractor_name.empty()) {
			Add(TerarkTablePropertiesNames::kPrefixExtractorName,
				props.prefix_extractor_name);
		}
		if (!props.property_collectors_names.empty()) {
			Add(TerarkTablePropertiesNames::kPropertyCollectors,
				props.property_collectors_names);
		}
		if (!props.column_family_name.empty()) {
			Add(TerarkTablePropertiesNames::kColumnFamilyName, props.column_family_name);
		}

		if (!props.compression_name.empty()) {
			Add(TerarkTablePropertiesNames::kCompression, props.compression_name);
		}
	}

	Slice TerarkPropertyBlockBuilder::Finish() {
		for (const auto& prop : props_) {
			properties_block_->Add(prop.first, prop.second);
		}

		return properties_block_->Finish();
	}

	Status TerarkReadProperties(const Slice& handle_value, RandomAccessFileReader* file,
								const TerarkFooter& footer, const Options& ioptions,
								TerarkTableProperties** table_properties) {
		assert(table_properties);

		Slice v = handle_value;
		TerarkBlockHandle handle;
		if (!handle.DecodeFrom(&v).ok()) {
			return Status::InvalidArgument("Failed to decode properties block handle");
		}

		TerarkBlockContents block_contents;
		TerarkReadOptions read_options;
		read_options.verify_checksums = false;
		Status s = TerarkReadBlockContents(file, footer, read_options, handle, &block_contents, ioptions);
		if (!s.ok()) {
			return s;
		}

		TerarkBlock properties_block(std::move(block_contents));
		TerarkBlockIter iter;
		properties_block.NewIterator(BytewiseComparator(), &iter);

		auto new_table_properties = new TerarkTableProperties();
		// All pre-defined properties of type uint64_t
		std::unordered_map<std::string, uint64_t*> predefined_uint64_properties = {
			{TerarkTablePropertiesNames::kDataSize, &new_table_properties->data_size},
			{TerarkTablePropertiesNames::kIndexSize, &new_table_properties->index_size},
			{TerarkTablePropertiesNames::kFilterSize, &new_table_properties->filter_size},
			{TerarkTablePropertiesNames::kRawKeySize, &new_table_properties->raw_key_size},
			{TerarkTablePropertiesNames::kRawValueSize,
			 &new_table_properties->raw_value_size},
			{TerarkTablePropertiesNames::kNumDataBlocks,
			 &new_table_properties->num_data_blocks},
			{TerarkTablePropertiesNames::kNumEntries, &new_table_properties->num_entries},
			{TerarkTablePropertiesNames::kFormatVersion,
			 &new_table_properties->format_version},
			{TerarkTablePropertiesNames::kFixedKeyLen,
			 &new_table_properties->fixed_key_len},
			{TerarkTablePropertiesNames::kColumnFamilyId,
			 &new_table_properties->column_family_id},
		};

		std::string last_key;
		for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
			s = iter.status();
			if (!s.ok()) {
				break;
			}

			auto key = iter.key().ToString();
			// properties block is strictly sorted with no duplicate key.
			assert(last_key.empty() ||
				   BytewiseComparator()->Compare(key, last_key) > 0);
			last_key = key;

			auto raw_val = iter.value();
			auto pos = predefined_uint64_properties.find(key);

			new_table_properties->properties_offsets.insert(
															{key, handle.offset() + iter.ValueOffset()});

			if (pos != predefined_uint64_properties.end()) {
				// handle predefined rocksdb properties
				uint64_t val;
				if (!GetVarint64(&raw_val, &val)) {
					// skip malformed value
					auto error_msg =
						"Detect malformed value in properties meta-block:"
						"\tkey: " + key + "\tval: " + raw_val.ToString();
					Log(InfoLogLevel::ERROR_LEVEL, ioptions.info_log, "%s",
						error_msg.c_str());
					continue;
				}
				*(pos->second) = val;
			} else if (key == TerarkTablePropertiesNames::kFilterPolicy) {
				new_table_properties->filter_policy_name = raw_val.ToString();
			} else if (key == TerarkTablePropertiesNames::kColumnFamilyName) {
				new_table_properties->column_family_name = raw_val.ToString();
			} else if (key == TerarkTablePropertiesNames::kComparator) {
				new_table_properties->comparator_name = raw_val.ToString();
			} else if (key == TerarkTablePropertiesNames::kMergeOperator) {
				new_table_properties->merge_operator_name = raw_val.ToString();
			} else if (key == TerarkTablePropertiesNames::kPrefixExtractorName) {
				new_table_properties->prefix_extractor_name = raw_val.ToString();
			} else if (key == TerarkTablePropertiesNames::kPropertyCollectors) {
				new_table_properties->property_collectors_names = raw_val.ToString();
			} else if (key == TerarkTablePropertiesNames::kCompression) {
				new_table_properties->compression_name = raw_val.ToString();
			} else {
				// handle user-collected properties
				new_table_properties->user_collected_properties.insert(
																	   {key, raw_val.ToString()});
			}
		}
		if (s.ok()) {
			*table_properties = new_table_properties;
		} else {
			delete new_table_properties;
		}

		return s;
	}

	Status TerarkReadTableProperties(RandomAccessFileReader* file, uint64_t file_size,
							   uint64_t table_magic_number,
							   const Options &ioptions,
							   TerarkTableProperties** properties) {
		// -- Read metaindex block
		TerarkFooter footer;
		auto s = TerarkReadFooterFromFile(file, file_size, &footer, table_magic_number);
		if (!s.ok()) {
			return s;
		}

		auto metaindex_handle = footer.metaindex_handle();
		TerarkBlockContents metaindex_contents;
		TerarkReadOptions read_options;
		read_options.verify_checksums = false;
		s = TerarkReadBlockContents(file, footer, read_options, metaindex_handle,
							  &metaindex_contents, ioptions); //, false /* decompress */);
		if (!s.ok()) {
			return s;
		}
		TerarkBlock metaindex_block(std::move(metaindex_contents));
		std::unique_ptr<Iterator> meta_iter(metaindex_block.NewIterator(BytewiseComparator()));

		// -- Read property block
		bool found_properties_block = true;
		s = SeekToPropertiesBlock(meta_iter.get(), &found_properties_block);
		if (!s.ok()) {
			return s;
		}

		TerarkTableProperties table_properties;
		if (found_properties_block == true) {
			s = TerarkReadProperties(meta_iter->value(), file, footer, ioptions, properties);
		} else {
			s = Status::NotFound();
		}

		return s;
	}

	Status TerarkFindMetaBlock(Iterator* meta_index_iter,
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

	Status TerarkReadMetaBlock(RandomAccessFileReader* file, uint64_t file_size,
		uint64_t table_magic_number,
		const Options &ioptions,
		const std::string& meta_block_name,
		TerarkBlockContents* contents) {
		Status status;
		TerarkFooter footer;
		status = TerarkReadFooterFromFile(file, file_size, &footer, table_magic_number);
		if (!status.ok()) {
			return status;
		}

		// Reading metaindex block
		auto metaindex_handle = footer.metaindex_handle();
		TerarkBlockContents metaindex_contents;
		TerarkReadOptions read_options;
		read_options.verify_checksums = false;
		status = TerarkReadBlockContents(file, footer, read_options, metaindex_handle,
			&metaindex_contents, ioptions);
		if (!status.ok()) {
			return status;
		}

		// Finding metablock
		TerarkBlock metaindex_block(std::move(metaindex_contents));

		std::unique_ptr<Iterator> meta_iter;
		meta_iter.reset(metaindex_block.NewIterator(BytewiseComparator()));

		TerarkBlockHandle block_handle;
		status = TerarkFindMetaBlock(meta_iter.get(), meta_block_name, &block_handle);

		if (!status.ok()) {
			return status;
		}

		// Reading metablock
		return TerarkReadBlockContents(file, footer, 
			read_options, block_handle, 
			contents, ioptions);
	}

}  // namespace rocksdb
