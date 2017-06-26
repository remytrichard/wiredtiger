//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#include "table/meta_blocks.h"

#include <map>
#include <string>

//#include "db/table_properties_collector.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
//#include "table/block.h"
//#include "table/internal_iterator.h"
//#include "table/persistent_cache_helper.h"
//#include "table/table_properties_internal.h"
#include "util/coding.h"

#include "trk_meta_blocks.h"

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

	PropertyTerarkBlockBuilder::PropertyTerarkBlockBuilder()
		: properties_block_(new TerarkBlockBuilder(1 /* restart interval */)) {}

	void PropertyTerarkBlockBuilder::Add(const std::string& name,
								   const std::string& val) {
		props_.insert({name, val});
	}

	void PropertyTerarkBlockBuilder::Add(const std::string& name, uint64_t val) {
		assert(props_.find(name) == props_.end());

		std::string dst;
		PutVarint64(&dst, val);

		Add(name, dst);
	}

	void PropertyTerarkBlockBuilder::Add(
								   const UserCollectedProperties& user_collected_properties) {
		for (const auto& prop : user_collected_properties) {
			Add(prop.first, prop.second);
		}
	}

	void PropertyTerarkBlockBuilder::AddTableProperty(const TableProperties& props) {
		Add(TablePropertiesNames::kRawKeySize, props.raw_key_size);
		Add(TablePropertiesNames::kRawValueSize, props.raw_value_size);
		Add(TablePropertiesNames::kDataSize, props.data_size);
		Add(TablePropertiesNames::kIndexSize, props.index_size);
		Add(TablePropertiesNames::kNumEntries, props.num_entries);
		Add(TablePropertiesNames::kNumDataBlocks, props.num_data_blocks);
		Add(TablePropertiesNames::kFilterSize, props.filter_size);
		Add(TablePropertiesNames::kFormatVersion, props.format_version);
		Add(TablePropertiesNames::kFixedKeyLen, props.fixed_key_len);
		Add(TablePropertiesNames::kColumnFamilyId, props.column_family_id);

		if (!props.filter_policy_name.empty()) {
			Add(TablePropertiesNames::kFilterPolicy, props.filter_policy_name);
		}
		if (!props.comparator_name.empty()) {
			Add(TablePropertiesNames::kComparator, props.comparator_name);
		}

		if (!props.merge_operator_name.empty()) {
			Add(TablePropertiesNames::kMergeOperator, props.merge_operator_name);
		}
		if (!props.prefix_extractor_name.empty()) {
			Add(TablePropertiesNames::kPrefixExtractorName,
				props.prefix_extractor_name);
		}
		if (!props.property_collectors_names.empty()) {
			Add(TablePropertiesNames::kPropertyCollectors,
				props.property_collectors_names);
		}
		if (!props.column_family_name.empty()) {
			Add(TablePropertiesNames::kColumnFamilyName, props.column_family_name);
		}

		if (!props.compression_name.empty()) {
			Add(TablePropertiesNames::kCompression, props.compression_name);
		}
	}

	Slice PropertyTerarkBlockBuilder::Finish() {
		for (const auto& prop : props_) {
			properties_block_->Add(prop.first, prop.second);
		}

		return properties_block_->Finish();
	}

	Status ReadProperties(const Slice& handle_value, RandomAccessFileReader* file,
						  const TerarkFooter& footer, const Options& ioptions,
						  TableProperties** table_properties) {
		assert(table_properties);

		Slice v = handle_value;
		TerarkBlockHandle handle;
		if (!handle.DecodeFrom(&v).ok()) {
			return Status::InvalidArgument("Failed to decode properties block handle");
		}

		TerarkBlockContents block_contents;
		ReadOptions read_options;
		read_options.verify_checksums = false;
		Status s;
		s = ReadBlockContents(file, footer, read_options, handle, &block_contents, ioptions);
							  //ioptions, false /* decompress */);

		if (!s.ok()) {
			return s;
		}

		TerarkBlock properties_block(std::move(block_contents),
							   kDisableGlobalSequenceNumber);
		TerarkBlockIter iter;
		properties_block.NewIterator(BytewiseComparator(), &iter);

		auto new_table_properties = new TableProperties();
		// All pre-defined properties of type uint64_t
		std::unordered_map<std::string, uint64_t*> predefined_uint64_properties = {
			{TablePropertiesNames::kDataSize, &new_table_properties->data_size},
			{TablePropertiesNames::kIndexSize, &new_table_properties->index_size},
			{TablePropertiesNames::kFilterSize, &new_table_properties->filter_size},
			{TablePropertiesNames::kRawKeySize, &new_table_properties->raw_key_size},
			{TablePropertiesNames::kRawValueSize,
			 &new_table_properties->raw_value_size},
			{TablePropertiesNames::kNumDataBlocks,
			 &new_table_properties->num_data_blocks},
			{TablePropertiesNames::kNumEntries, &new_table_properties->num_entries},
			{TablePropertiesNames::kFormatVersion,
			 &new_table_properties->format_version},
			{TablePropertiesNames::kFixedKeyLen,
			 &new_table_properties->fixed_key_len},
			{TablePropertiesNames::kColumnFamilyId,
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
			} else if (key == TablePropertiesNames::kFilterPolicy) {
				new_table_properties->filter_policy_name = raw_val.ToString();
			} else if (key == TablePropertiesNames::kColumnFamilyName) {
				new_table_properties->column_family_name = raw_val.ToString();
			} else if (key == TablePropertiesNames::kComparator) {
				new_table_properties->comparator_name = raw_val.ToString();
			} else if (key == TablePropertiesNames::kMergeOperator) {
				new_table_properties->merge_operator_name = raw_val.ToString();
			} else if (key == TablePropertiesNames::kPrefixExtractorName) {
				new_table_properties->prefix_extractor_name = raw_val.ToString();
			} else if (key == TablePropertiesNames::kPropertyCollectors) {
				new_table_properties->property_collectors_names = raw_val.ToString();
			} else if (key == TablePropertiesNames::kCompression) {
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

	Status ReadTableProperties(RandomAccessFileReader* file, uint64_t file_size,
							   uint64_t table_magic_number,
							   const Options &ioptions,
							   TableProperties** properties) {
		// -- Read metaindex block
		TerarkFooter footer;
		auto s = ReadFooterFromFile(file, file_size, &footer, table_magic_number);
		if (!s.ok()) {
			return s;
		}

		auto metaindex_handle = footer.metaindex_handle();
		TerarkBlockContents metaindex_contents;
		ReadOptions read_options;
		read_options.verify_checksums = false;
		s = ReadBlockContents(file, footer, read_options, metaindex_handle,
							  &metaindex_contents, ioptions); //, false /* decompress */);
		if (!s.ok()) {
			return s;
		}
		TerarkBlock metaindex_block(std::move(metaindex_contents),
									kDisableGlobalSequenceNumber);
		std::unique_ptr<Iterator> meta_iter(metaindex_block.NewIterator(BytewiseComparator()));

		// -- Read property block
		bool found_properties_block = true;
		s = SeekToPropertiesBlock(meta_iter.get(), &found_properties_block);
		if (!s.ok()) {
			return s;
		}

		TableProperties table_properties;
		if (found_properties_block == true) {
			s = ReadProperties(meta_iter->value(), file, footer, ioptions, properties);
		} else {
			s = Status::NotFound();
		}

		return s;
	}

	Status ReadMetaBlock(RandomAccessFileReader* file, uint64_t file_size,
						 uint64_t table_magic_number,
						 const Options &ioptions,
						 const std::string& meta_block_name,
						 TerarkBlockContents* contents) {
		Status status;
		TerarkFooter footer;
		status = ReadFooterFromFile(file, file_size, &footer, table_magic_number);
		if (!status.ok()) {
			return status;
		}

		// Reading metaindex block
		auto metaindex_handle = footer.metaindex_handle();
		TerarkBlockContents metaindex_contents;
		ReadOptions read_options;
		read_options.verify_checksums = false;
		status = ReadBlockContents(file, footer, read_options, metaindex_handle,
								   &metaindex_contents, ioptions);
		if (!status.ok()) {
			return status;
		}

		// Finding metablock
		TerarkBlock metaindex_block(std::move(metaindex_contents),
							  kDisableGlobalSequenceNumber);

		std::unique_ptr<InternalIterator> meta_iter;
		meta_iter.reset(metaindex_block.NewIterator(BytewiseComparator()));

		TerarkBlockHandle block_handle;
		status = FindMetaBlock(meta_iter.get(), meta_block_name, &block_handle);

		if (!status.ok()) {
			return status;
		}

		// Reading metablock
		return ReadBlockContents(file, footer, read_options, block_handle, contents,
								 ioptions);
	}

}  // namespace rocksdb
