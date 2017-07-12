//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Decodes the blocks generated by block_builder.cc.

#include <algorithm>
#include <string>
#include <unordered_map>
#include <vector>

#include "trk_block.h"

namespace terark {

	static inline const char* DecodeEntry(const char* p, const char* limit,
										  uint32_t* key_length,
										  uint32_t* value_length) {
		if (limit - p < 10) return nullptr;
		*key_length = TerarkDecodeFixed32(p);
		p += 4;
		*value_length = TerarkDecodeFixed32(p);
		p += 4;
		if (static_cast<uint32_t>(limit - p) < (*key_length + *value_length)) {
			return nullptr;
		}
		return p;
	}

	void TerarkBlockIter::Next() {
		assert(Valid());
		ParseNextKey();
	}

	void TerarkBlockIter::Prev() {
		abort(); // not supported
	}

	void TerarkBlockIter::Seek(const Slice& target) {
		if (data_ == nullptr) {  // Not init yet
			return;
		}
		value_ = Slice(data_, 0);
		while (true) {
			if (!ParseNextKey() || Compare(key_, target) >= 0) {
				return;
			}
		}
	}

	void TerarkBlockIter::SeekForPrev(const Slice& target) {
		abort(); // not supported
	}

	void TerarkBlockIter::SeekToFirst() {
		if (data_ == nullptr) {  // Not init yet
			return;
		}
		value_ = Slice(data_, 0);
		ParseNextKey();
	}

	void TerarkBlockIter::SeekToLast() {
		abort(); // not supported
	}

	void TerarkBlockIter::CorruptionError() {
		current_ = size_;
		status_ = Status::Corruption("bad entry in block");
		key_.clear();
		value_.clear();
	}

	bool TerarkBlockIter::ParseNextKey() {
		current_ = NextEntryOffset();
		const char* p = data_ + current_;
		const char* limit = data_ + size_;
		if (p >= limit) {
			// No more entries to return.  Mark as invalid.
			current_ = size_;
			return false;
		}
		// Decode next entry
		uint32_t key_length, value_length;
		p = DecodeEntry(p, limit, &key_length, &value_length);
		if (p == nullptr || key_length < 1) {
			CorruptionError();
			return false;
		} else {
			key_ = Slice(p, key_length);
			value_ = Slice(p + key_length, value_length);
			return true;
		}
	}

	TerarkBlock::TerarkBlock(TerarkBlockContents&& contents)
		: contents_(std::move(contents)),
		  data_(contents_.data.data()),
		  size_(contents_.data.size()) {
		if (size_ < sizeof(uint32_t)) {
			size_ = 0;  // Error marker
		}
	}

	Iterator* TerarkBlock::NewIterator(const Comparator* cmp) {
		if (size_ < 2 * sizeof(uint32_t)) {
			return nullptr;
		} else {
			return new TerarkBlockIter(cmp, data_, size_);
		}
	}

}  // namespace
