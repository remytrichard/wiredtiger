#include "terark_zip_table.h"
#include "terark_zip_common.h"
#include <terark/hash_strmap.hpp>
#include <terark/util/throw.hpp>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#ifdef _MSC_VER
# include <Windows.h>
# define strcasecmp _stricmp
#else
# include <unistd.h>
#endif
#include <mutex>

namespace terark {
	void DictZipBlobStore_setZipThreads(int zipThreads);
}

namespace rocksdb {

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

	bool TerarkZipOptionsFromEnv(TerarkZipTableOptions& tzo) {
		const char* localTempDir = getenv("TerarkZipTable_localTempDir");
		if (!localTempDir) {
			STD_INFO("TerarkZipConfigFromEnv(dbo, cfo) failed because env TerarkZipTable_localTempDir is not defined\n");
			return false;
		}
		if (!*localTempDir) {
			THROW_STD(invalid_argument,
					  "If env TerarkZipTable_localTempDir is defined, it must not be empty");
		}
		tzo.localTempDir = localTempDir;
		if (const char* algo = getenv("TerarkZipTable_entropyAlgo")) {
			if (strcasecmp(algo, "NoEntropy") == 0) {
				tzo.entropyAlgo = tzo.kNoEntropy;
			} else if (strcasecmp(algo, "FSE") == 0) {
				tzo.entropyAlgo = tzo.kFSE;
			} else if (strcasecmp(algo, "huf") == 0) {
				tzo.entropyAlgo = tzo.kHuffman;
			} else if (strcasecmp(algo, "huff") == 0) {
				tzo.entropyAlgo = tzo.kHuffman;
			} else if (strcasecmp(algo, "huffman") == 0) {
				tzo.entropyAlgo = tzo.kHuffman;
			} else {
				tzo.entropyAlgo = tzo.kNoEntropy;
				STD_WARN(
						 "bad env TerarkZipTable_entropyAlgo=%s, must be one of {NoEntropy, FSE, huf}, reset to default 'NoEntropy'\n"
						 , algo);
			}
		}
		if (const char* env = getenv("TerarkZipTable_indexType")) {
			tzo.indexType = env;
		}

#define MyGetInt(obj, name, Default)									\
		obj.name = (int)terark::getEnvLong("TerarkZipTable_" #name, Default)
#define MyGetBool(obj, name, Default)									\
		obj.name = terark::getEnvBool("TerarkZipTable_" #name, Default)
#define MyGetDouble(obj, name, Default)									\
		obj.name = terark::getEnvDouble("TerarkZipTable_" #name, Default)
#define MyGetXiB(obj, name)										\
		if (const char* env = getenv("TerarkZipTable_" #name))	\
			obj.name = terark::ParseSizeXiB(env)

		MyGetInt   (tzo, checksumLevel           , 3    );
		MyGetInt   (tzo, indexNestLevel          , 3    );
		MyGetInt   (tzo, terarkZipMinLevel       , 0    );
		MyGetInt   (tzo, debugLevel              , 0    );
		MyGetInt   (tzo, keyPrefixLen            , 0    );
		MyGetBool  (tzo, useSuffixArrayLocalMatch, false);
		MyGetBool  (tzo, warmUpIndexOnOpen       , true );
		MyGetBool  (tzo, warmUpValueOnOpen       , false);
		MyGetBool  (tzo, disableSecondPassIter   , true);

		{   // TBD(kg):...
			size_t page_num  = sysconf(_SC_PHYS_PAGES);
			size_t page_size = sysconf(_SC_PAGE_SIZE);
			memBytesLimit = page_num * page_size;

			tzo.softZipWorkingMemLimit = memBytesLimit * 7 / 8;
			tzo.hardZipWorkingMemLimit = tzo.softZipWorkingMemLimit;
			tzo.smallTaskMemory = memBytesLimit / 16;
			tzo.indexNestLevel = 2;
		}

		MyGetDouble(tzo, estimateCompressionRatio, 0.20 );
		MyGetDouble(tzo, sampleRatio             , 0.03 );
		MyGetDouble(tzo, indexCacheRatio         , 0.00 );

		MyGetXiB(tzo, softZipWorkingMemLimit);
		MyGetXiB(tzo, hardZipWorkingMemLimit);
		MyGetXiB(tzo, smallTaskMemory);

		if (tzo.debugLevel) {
			STD_INFO("TerarkZipConfigFromEnv(dbo, cfo) successed\n");
		}
		return true;
	}

	template<class T>
	T& auto_const_cast(const T& x) {
		return const_cast<T&>(x);
	}

}
