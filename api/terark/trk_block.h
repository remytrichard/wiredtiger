//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
#ifdef OS_FREEBSD
#include <malloc_np.h>
#else
#include <malloc.h>
#endif
#endif

#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/slice.h"
#include "util/coding.h"

#include "trk_format.h"
#include "trk_iter_key.h"

namespace rocksdb {

	class TerarkBlockIter;
	class TerarkReadOptions {
	public:
		bool verify_checksums = false;
	};

	class TerarkBlock {
	public:
		// Initialize the block with the specified contents.
		explicit TerarkBlock(TerarkBlockContents&& contents);
		~TerarkBlock() = default;

		size_t size() const { return size_; }
		const char* data() const { return data_; }

		Iterator* NewIterator(const Comparator* comparator);
		
	private:
		TerarkBlockContents contents_;
		const char* data_;            // contents_.data.data()
		size_t size_;                 // contents_.data.size()

		// No copying allowed
		TerarkBlock(const TerarkBlock&);
		void operator=(const TerarkBlock&);
	};

	class TerarkBlockIter : public Iterator {
	public:
	TerarkBlockIter()
		: comparator_(nullptr),
			data_(nullptr),
			current_(0),
			status_(Status::OK()) {}
		
	TerarkBlockIter(const Comparator* comparator, const char* data, size_t size)
		: TerarkBlockIter() {
			assert(data_ == nullptr);           // Ensure it is called only once
			comparator_ = comparator;
			data_ = data;
			size_ = size;
			current_ = size_;
		}

		void SetStatus(Status s) {	status_ = s; }
		virtual bool Valid() const override { return current_ < size_; }
		virtual Status status() const override { return status_; }
		virtual Slice key() const override {
			assert(Valid());
			return key_.GetKey();
		}
		virtual Slice value() const override {
			assert(Valid());
			return value_;
		}

		virtual void Next() override;
		virtual void Prev() override;
		virtual void Seek(const Slice& target) override;
		virtual void SeekForPrev(const Slice& target) override;
		virtual void SeekToFirst() override;
		virtual void SeekToLast() override;

		~TerarkBlockIter() {}

		uint32_t ValueOffset() const {
			return static_cast<uint32_t>(value_.data() - data_);
		}

	private:
		const Comparator* comparator_;
		const char* data_;       // underlying block contents

		// current_ is offset in data_ of current entry.  >= restarts_ if !Valid
		uint32_t current_;
		size_t size_;
		TerarkIterKey key_;
		Slice value_;
		Status status_;

		inline int Compare(const Slice& a, const Slice& b) const {
			return comparator_->Compare(a, b);
		}

		// Return the offset in data_ just past the end of the current entry.
		inline uint32_t NextEntryOffset() const {
			// NOTE: We don't support blocks bigger than 2GB
			return static_cast<uint32_t>((value_.data() + value_.size()) - data_);
		}
		void CorruptionError();
		bool ParseNextKey();
	};

	class TerarkBlockBuilder {
	public:
		TerarkBlockBuilder(const TerarkBlockBuilder&) = delete;
		void operator=(const TerarkBlockBuilder&) = delete;

	    TerarkBlockBuilder() : finished_(false) {}

		// REQUIRES: Finish() has not been called since the last call to Reset().
		void Add(const Slice& key, const Slice& value) {
			assert(!finished_);
			// Add "<key_size><value_size>" to buffer_
			PutVarint32Varint32(&buffer_, static_cast<uint32_t>(key.size()),
								static_cast<uint32_t>(value.size()));
			// Add key, value to buffer_
			buffer_.append(key.data(), key.size());
			buffer_.append(value.data(), value.size());
		}

		// Finish building the block and return a slice that refers to the
		// block contents.  The returned slice will remain valid for the
		// lifetime of this builder or until Reset() is called.
		Slice Finish() {
			finished_ = true;
			return Slice(buffer_);
		}

		// Return true iff no entries have been added since the last Reset()
		bool empty() const {
			return buffer_.empty();
		}

	private:
		std::string           buffer_;    // Destination buffer
		bool                  finished_;  // Has Finish() been called?
	};


}  // namespace rocksdb
