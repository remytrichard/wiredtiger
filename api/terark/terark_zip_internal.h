/*
 * terark_zip_internal.h
 *
 *  Created on: 2017-05-02
 *      Author: zhaoming
 */

#pragma once

#ifndef TERARK_ZIP_INTERNAL_H_
#define TERARK_ZIP_INTERNAL_H_

// project headers
#include "terark_zip_common.h"
#include "terark_zip_config.h"
#include "terark_chunk_builder.h"
// std headers
#include <memory>
#include <mutex>
// rocksdb headers
#include <rocksdb/slice.h>
#include <rocksdb/env.h>
// terark headers
#include <terark/fstring.hpp>
#include <terark/valvec.hpp>
#include <terark/stdtypes.hpp>
#include <terark/util/profiling.hpp>


//#define TERARK_SUPPORT_UINT64_COMPARATOR
//#define DEBUG_TWO_PASS_ITER



namespace rocksdb {

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


	template<class ByteArray>
		inline Slice SliceOf(const ByteArray& ba) {
		BOOST_STATIC_ASSERT(sizeof(ba[0] == 1));
		return Slice((const char*)ba.data(), ba.size());
	}

	inline static fstring fstringOf(const Slice& x) {
		return fstring(x.data(), x.size());
	}

	template<class ByteArrayView>
		inline ByteArrayView SubStr(const ByteArrayView& x, size_t pos) {
		assert(pos <= x.size());
		return ByteArrayView(x.data() + pos, x.size() - pos);
	}


	enum class ZipValueType : unsigned char {
		kZeroSeq = 0,
			kDelete = 1,
			kValue = 2,
			kMulti = 3,
			};
	//const size_t kZipValueTypeBits = 2;

}  // namespace rocksdb

#endif /* TERARK_ZIP_INTERNAL_H_ */
