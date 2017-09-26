
#pragma once

// std headers
#include <memory>
#include <random>
#include <vector>
// wiredtiger headers
#include "wiredtiger.h"
// terark headers
#include <terark/fstring.hpp>
#include <terark/valvec.hpp>
#include <terark/bitmap.hpp>
#include <terark/stdtypes.hpp>
#include <terark/histogram.hpp>
#include <terark/zbs/blob_store.hpp>
#include <terark/zbs/dict_zip_blob_store.hpp>
#include <terark/bitfield_array.hpp>
#include <terark/util/fstrvec.hpp>
#include <terark/util/mmap.hpp>
// project headers
#include "util/trk_format.h"
#include "terark_zip_config.h"
#include "terark_zip_common.h"
#include "terark_zip_index.h"

namespace terark {

	using terark::fstring;
	using terark::fstrvec;
	using terark::valvec;
	using terark::byte_t;
	using terark::febitvec;
	using terark::BlobStore;
	using terark::Uint32Histogram;
	using terark::DictZipBlobStore;

	class TerarkChunkReader : boost::noncopyable {
	public:
	TerarkChunkReader(const TerarkZipTableOptions& tzto,
					  const TerarkTableReaderOptions& reader_options,
					  const std::string& fname) 
		: table_options_(tzto),
			reader_options_(reader_options),
			chunk_name_(fname),
			useUint64Comparator_(false),
			ref_count_(0) {}
		~TerarkChunkReader();

		Iterator* NewIterator();
		friend class TerarkChunkIterator;

		int AddRef() { return ++ref_count_; }
		int RelRef() { return --ref_count_; }
		int RefCount() { return ref_count_; }

	private:
		Status Open();
	
	private:
		const std::string chunk_name_;
		const TerarkZipTableOptions& table_options_;
		TerarkTableReaderOptions reader_options_;
		bool useUint64Comparator_;
		int ref_count_;

		std::string commonPrefix_;
		std::unique_ptr<TerarkIndex> index_;
		std::unique_ptr<terark::BlobStore> store_;
		std::unique_ptr<terark::MmapWholeFile> file_reader_;
	};


}  // namespace

