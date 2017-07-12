
#pragma once

#include <map>
#include <string>
#include <vector>

namespace terark {

	// table properties' human-readable names in the property block.
	struct TerarkTablePropertiesNames {
		static const std::string kDataSize;
		static const std::string kIndexSize;
		static const std::string kRawKeySize;
		static const std::string kRawValueSize;
		static const std::string kNumEntries;
	};

	// TableProperties contains a bunch of read-only properties of its associated
	// table.
	struct TerarkTableProperties {
	public:
		// the total size of all data blocks.
		uint64_t data_size = 0;
		// the size of index block.
		uint64_t index_size = 0;
		// total raw key size
		uint64_t raw_key_size = 0;
		// total raw value size
		uint64_t raw_value_size = 0;
		// the number of entries in this table
		uint64_t num_entries = 0;

		// convert this object to a human readable form
		//   @prop_delim: delimiter for each property.
		std::string ToString(const std::string& prop_delim = "; ",
							 const std::string& kv_delim = "=") const {}

		// Aggregate the numerical member variables of the specified
		// TableProperties.
		void Add(const TerarkTableProperties& tp) {}
	};

}  // namespace

