

#ifdef _MSC_VER
# include <Windows.h>
# define strcasecmp _stricmp
#else
# include <unistd.h>
#endif
#include <mutex>
// terark header
#include <terark/hash_strmap.hpp>
#include <terark/util/throw.hpp>
// project header
#include "terark_zip_config.h"
#include "terark_zip_common.h"

namespace terark {
	void DictZipBlobStore_setZipThreads(int zipThreads);
}

namespace terark {

	//terark::profiling g_pf;

	const uint64_t kTerarkZipTableMagicNumber = 0x1122334455667788;

	const std::string kTerarkZipTableIndexBlock        = "TerarkZipTableIndexBlock";
	const std::string kTerarkZipTableValueTypeBlock    = "TerarkZipTableValueTypeBlock";
	const std::string kTerarkZipTableValueDictBlock    = "TerarkZipTableValueDictBlock";
	const std::string kTerarkZipTableOffsetBlock       = "TerarkZipTableOffsetBlock";
	const std::string kTerarkZipTableCommonPrefixBlock = "TerarkZipTableCommonPrefixBlock";
	const std::string kTerarkEmptyTableKey             = "ThisIsAnEmptyTable";
	const std::string kTerarkPropertiesBlock           = "TerarkPropertyBlock";

	const std::string TerarkTablePropertiesNames::kDataSize  =
		"terark.data.size";
	const std::string TerarkTablePropertiesNames::kIndexSize =
		"terark.index.size";
	const std::string TerarkTablePropertiesNames::kRawKeySize =
		"terark.raw.key.size";
	const std::string TerarkTablePropertiesNames::kRawValueSize =
		"terark.raw.value.size";
	const std::string TerarkTablePropertiesNames::kNumEntries =
		"terark.num.entries";
	const std::string TerarkTablePropertiesNames::kComparator = 
		"terark.comparator";

	static
	int ComputeFileSizeMultiplier(double diskLimit, double minVal, int levels) {
		if (diskLimit > 0) {
			double maxSST = diskLimit / 6.0;
			double maxMul = maxSST / minVal;
			double oneMul = pow(maxMul, 1.0/(levels-1));
			if (oneMul > 1.0)
				return (int)oneMul;
			else
				return 1;
		}
		else {
			return 5;
		}
	}

	template<class T>
	T& auto_const_cast(const T& x) {
		return const_cast<T&>(x);
	}

}
