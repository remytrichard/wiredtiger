/*
 * terark_zip_table_reader.h
 *
 *  Created on: 2017-05-02
 *      Author: zhaoming
 */


#pragma once

#ifndef TERARK_ZIP_TABLE_READER_H_
#define TERARK_ZIP_TABLE_READER_H_

// project headers
#include "terark_zip_table.h"
#include "terark_zip_internal.h"
#include "terark_zip_index.h"
#include "block_builder.h"
#include "format.h"

//#include <table/block.h"
// boost headers
#include <boost/noncopyable.hpp>
// rocksdb headers
#include <rocksdb/options.h>
#include "rocksdb/iterator.h"
#include <util/arena.h>
//#include <table/table_reader.h>

// terark headers
#include <terark/util/throw.hpp>
#include <terark/bitfield_array.hpp>
#include <terark/zbs/blob_store.hpp>

namespace rocksdb {

	class TerarkTableReader {};

	class TerarkEmptyTableReader : public TerarkTableReader, boost::noncopyable {
class Iter : public Iterator, boost::noncopyable {
		public:
			Iter() {}
			~Iter() {}
			bool Valid() const { return false; }
			void SeekToFirst() {}
			void SeekToLast() {}
			void SeekForPrev(const Slice&) {}
			void Seek(const Slice&) {}
			void Next() {}
			void Prev() {}
			Slice key() const { THROW_STD(invalid_argument, "Invalid call"); }
			Slice value() const { THROW_STD(invalid_argument, "Invalid call"); }
			Status status() const { return Status::OK(); }
		};
		const TerarkTableReaderOptions table_reader_options_;
		std::shared_ptr<const TableProperties> table_properties_;
		SequenceNumber global_seqno_;
		Slice  file_data_;
		unique_ptr<RandomAccessFileReader> file_;
	public:
		Iter* NewIterator(const ReadOptions&, Arena* a, bool) {
			return a ? new(a->AllocateAligned(sizeof(Iter)))Iter() : new Iter();
		}
		void Prepare(const Slice&)  {}
		Status Get(const ReadOptions&, const Slice&, GetContext*, bool) {
			return Status::OK();
		}
		std::shared_ptr<const TableProperties>
			GetTableProperties() const  { return table_properties_; }
		virtual ~TerarkEmptyTableReader() {}
	TerarkEmptyTableReader(const TerarkTableReaderOptions& o)
		: table_reader_options_(o)
			, global_seqno_(kDisableGlobalSequenceNumber) {
		}
		Status Open(RandomAccessFileReader* file, uint64_t file_size);
	private:
		SequenceNumber GetSequenceNumber() const  {
			return global_seqno_;
		}
		const TableReaderOptions& GetTableReaderOptions() const  {
			return table_reader_options_;
		}
	};

	struct TerarkZipSubReader {
		size_t subIndex_;
		std::string prefix_;
		unique_ptr<TerarkIndex> index_;
		unique_ptr<terark::BlobStore> store_;
		bitfield_array<2> type_;
		std::string commonPrefix_;

		enum {
			FlagNone = 0,
			FlagSkipFilter = 1,
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
			FlagUint64Comparator = 2,
#endif
		};

		Status Get(SequenceNumber, const ReadOptions&, const Slice& key,
				   GetContext*, int flag) const;

		~TerarkZipSubReader();
	};

	/**
	 * one user key map to a record id: the index NO. of a key in NestLoudsTrie,
	 * the record id is used to direct index a type enum(small integer) array,
	 * the record id is also used to access the value store
	 */
	class TerarkZipTableReader : public TerarkTableReader, boost::noncopyable {
	public:
		Iterator*
			NewIterator(const ReadOptions&, Arena*, bool skip_filters);

		Status Get(const ReadOptions&, const Slice& key, GetContext*,
				   bool skip_filters);

		std::shared_ptr<const TableProperties>
			GetTableProperties() const  { return table_properties_; }

		virtual ~TerarkZipTableReader();
		TerarkZipTableReader(const TableReaderOptions&, const TerarkZipTableOptions&);
		Status Open(RandomAccessFileReader* file, uint64_t file_size);

	private:
		SequenceNumber GetSequenceNumber() const  {
			return global_seqno_;
		}
		const TerarkTableReaderOptions& GetTableReaderOptions() const  {
			return table_reader_options_;
		}

		TerarkZipSubReader subReader_;
		static const size_t kNumInternalBytes = 8;
		Slice  file_data_;
		unique_ptr<RandomAccessFileReader> file_;
		const TerarkTableReaderOptions table_reader_options_;
		std::shared_ptr<const TableProperties> table_properties_;
		SequenceNumber global_seqno_;
		const TerarkZipTableOptions& tzto_;
		bool isReverseBytewiseOrder_;
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
		bool isUint64Comparator_;
#endif
		Status LoadIndex(Slice mem);
	};



}  // namespace rocksdb

#endif /* TERARK_ZIP_TABLE_READER_H_ */
