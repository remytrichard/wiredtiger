
#include "trk_table_properties.h"

namespace rocksdb {

	const std::string TerarkTablePropertiesNames::kDataSize  =
		"rocksdb.data.size";
	const std::string TerarkTablePropertiesNames::kIndexSize =
		"rocksdb.index.size";
	const std::string TerarkTablePropertiesNames::kFilterSize =
		"rocksdb.filter.size";
	const std::string TerarkTablePropertiesNames::kRawKeySize =
		"rocksdb.raw.key.size";
	const std::string TerarkTablePropertiesNames::kRawValueSize =
		"rocksdb.raw.value.size";
	const std::string TerarkTablePropertiesNames::kNumDataBlocks =
		"rocksdb.num.data.blocks";
	const std::string TerarkTablePropertiesNames::kNumEntries =
		"rocksdb.num.entries";
	const std::string TerarkTablePropertiesNames::kFilterPolicy =
		"rocksdb.filter.policy";
	const std::string TerarkTablePropertiesNames::kFormatVersion =
		"rocksdb.format.version";
	const std::string TerarkTablePropertiesNames::kFixedKeyLen =
		"rocksdb.fixed.key.length";
	const std::string TerarkTablePropertiesNames::kColumnFamilyId =
		"rocksdb.column.family.id";
	const std::string TerarkTablePropertiesNames::kColumnFamilyName =
		"rocksdb.column.family.name";
	const std::string TerarkTablePropertiesNames::kComparator = "rocksdb.comparator";
	const std::string TerarkTablePropertiesNames::kMergeOperator =
		"rocksdb.merge.operator";
	const std::string TerarkTablePropertiesNames::kPrefixExtractorName =
		"rocksdb.prefix.extractor.name";
	const std::string TerarkTablePropertiesNames::kPropertyCollectors =
		"rocksdb.property.collectors";
	const std::string TerarkTablePropertiesNames::kCompression = "rocksdb.compression";

}
