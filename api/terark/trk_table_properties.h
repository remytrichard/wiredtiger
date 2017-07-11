
#pragma once

#include <string>
#include <vector>

#include "rocksdb/options.h"

namespace rocksdb {

	// table properties' human-readable names in the property block.
	struct TerarkTablePropertiesNames {
		static const std::string kDataSize;
		static const std::string kIndexSize;
		static const std::string kFilterSize;
		static const std::string kRawKeySize;
		static const std::string kRawValueSize;
		static const std::string kNumDataBlocks;
		static const std::string kNumEntries;
		static const std::string kFormatVersion;
		static const std::string kFixedKeyLen;
		static const std::string kFilterPolicy;
		static const std::string kColumnFamilyName;
		static const std::string kColumnFamilyId;
		static const std::string kComparator;
		static const std::string kMergeOperator;
		static const std::string kPrefixExtractorName;
		static const std::string kPropertyCollectors;
		static const std::string kCompression;
	};

	// TableProperties contains a bunch of read-only properties of its associated
	// table.
	struct TerarkTableProperties {
	public:
		// the total size of all data blocks.
		uint64_t data_size = 0;
		// the size of index block.
		uint64_t index_size = 0;
		// the size of filter block.
		uint64_t filter_size = 0;
		// total raw key size
		uint64_t raw_key_size = 0;
		// total raw value size
		uint64_t raw_value_size = 0;
		// the number of blocks in this table
		uint64_t num_data_blocks = 0;
		// the number of entries in this table
		uint64_t num_entries = 0;
		// format version, reserved for backward compatibility
		uint64_t format_version = 0;
		// If 0, key is variable length. Otherwise number of bytes for each key.
		uint64_t fixed_key_len = 0;
		// ID of column family for this SST file, corresponding to the CF identified
		// by column_family_name.
		uint64_t column_family_id = 0;

		// Name of the column family with which this SST file is associated.
		// If column family is unknown, `column_family_name` will be an empty string.
		std::string column_family_name;

		// The name of the filter policy used in this table.
		// If no filter policy is used, `filter_policy_name` will be an empty string.
		std::string filter_policy_name;

		// The name of the comparator used in this table.
		std::string comparator_name;

		// The name of the merge operator used in this table.
		// If no merge operator is used, `merge_operator_name` will be "nullptr".
		std::string merge_operator_name;

		// The name of the prefix extractor used in this table
		// If no prefix extractor is used, `prefix_extractor_name` will be "nullptr".
		std::string prefix_extractor_name;

		// The names of the property collectors factories used in this table
		// separated by commas
		// {collector_name[1]},{collector_name[2]},{collector_name[3]} ..
		std::string property_collectors_names;

		// The compression algo used to compress the SST files.
		std::string compression_name;

		// user collected properties
		UserCollectedProperties user_collected_properties;
		UserCollectedProperties readable_properties;

		// The offset of the value of each property in the file.
		std::map<std::string, uint64_t> properties_offsets;

		// convert this object to a human readable form
		//   @prop_delim: delimiter for each property.
		std::string ToString(const std::string& prop_delim = "; ",
							 const std::string& kv_delim = "=") const {}

		// Aggregate the numerical member variables of the specified
		// TableProperties.
		void Add(const TerarkTableProperties& tp) {}
	};

}  // namespace rocksdb

