
#pragma once

#include "slice.h"
#include "status.h"

namespace rocksdb {

	class TIterator {
	public:
		TIterator() {}
		virtual ~TIterator() {}

		// An iterator is either positioned at a key/value pair, or
		// not valid.  This method returns true iff the iterator is valid.
		virtual bool Valid() const = 0;

		// Position at the first key in the source.  The iterator is Valid()
		// after this call iff the source is not empty.
		virtual void SeekToFirst() = 0;

		// Position at the last key in the source.  The iterator is
		// Valid() after this call iff the source is not empty.
		virtual void SeekToLast() = 0;

		// Position at the first key in the source that at or past target
		// The iterator is Valid() after this call iff the source contains
		// an entry that comes at or past target.
		virtual void Seek(const Slice& target) = 0;

		// Position at the last key in the source that at or before target
		// The iterator is Valid() after this call iff the source contains
		// an entry that comes at or before target.
		virtual void SeekForPrev(const Slice& target) {}

		// Moves to the next entry in the source.  After this call, Valid() is
		// true iff the iterator was not positioned at the last entry in the source.
		// REQUIRES: Valid()
		virtual void Next() = 0;

		// Moves to the previous entry in the source.  After this call, Valid() is
		// true iff the iterator was not positioned at the first entry in source.
		// REQUIRES: Valid()
		virtual void Prev() = 0;

		// Return the key for the current entry.  The underlying storage for
		// the returned slice is valid only until the next modification of
		// the iterator.
		// REQUIRES: Valid()
		virtual Slice key() const = 0;

		// Return the value for the current entry.  The underlying storage for
		// the returned slice is valid only until the next modification of
		// the iterator.
		// REQUIRES: !AtEnd() && !AtStart()
		virtual Slice value() const = 0;

		// If an error has occurred, return it.  Else return an ok status.
		// If non-blocking IO is requested and this operation cannot be
		// satisfied without doing some IO, then this returns Status::Incomplete().
		virtual Status status() const = 0;

	private:
		// No copying allowed
		TIterator(const TIterator&);
		void operator=(const TIterator&);
	};


	// A Comparator object provides a total order across slices that are
	// used as keys in an sstable or a database.  A Comparator implementation
	// must be thread-safe since rocksdb may invoke its methods concurrently
	// from multiple threads.
	class TComparator {
	public:
		TComparator() {}
		virtual ~TComparator() {}

		// Three-way comparison.  Returns value:
		//   < 0 iff "a" < "b",
		//   == 0 iff "a" == "b",
		//   > 0 iff "a" > "b"
		virtual int Compare(const Slice& a, const Slice& b) const = 0;

		// The name of the comparator.  Used to check for comparator
		// mismatches (i.e., a DB created with one comparator is
		// accessed using a different comparator.
		virtual const char* Name() const = 0;
	};

	class TBytewiseComparator : public TComparator {
	public:
		TBytewiseComparator() { }
		~TBytewiseComparator() { }

		virtual const char* Name() const override {
			return "leveldb.BytewiseComparator";
		}

		virtual int Compare(const Slice& a, const Slice& b) const override {
			return a.compare(b);
		}
	};

	extern TComparator* GetBytewiseComparator();
	extern Status GetFileSize(const std::string& fname,
							  uint64_t* size);

	// Standard Put... routines append to a string
	extern void TerarkPutFixed32(std::string* dst, uint32_t value);
	extern void TerarkPutFixed64(std::string* dst, uint64_t value);
	extern bool TerarkGetFixed64(Slice* input, uint64_t* value);
	extern void TerarkEncodeFixed32(char* dst, uint32_t value);
	extern uint32_t TerarkDecodeFixed32(const char* ptr);
	extern void TerarkEncodeFixed64(char* dst, uint64_t value);
	extern uint64_t TerarkDecodeFixed64(const char* ptr);



} // namespace
