//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// TerarkBlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.



#include <algorithm>
#include <assert.h>

#include "rocksdb/comparator.h"
#include "util/coding.h"

#include "trk_block_builder.h"

namespace rocksdb {

	void TerarkBlockBuilder::Reset() {
		buffer_.clear();
		estimate_ = sizeof(uint32_t) + sizeof(uint32_t);
		counter_ = 0;
		finished_ = false;
	}

	/*size_t TerarkBlockBuilder::EstimateSizeAfterKV(const Slice& key, const Slice& value)
		const {
		size_t estimate = CurrentSizeEstimate();
		estimate += key.size() + value.size();
	
		estimate += sizeof(int32_t); // varint for shared prefix length.
		estimate += VarintLength(key.size()); // varint for key length.
		estimate += VarintLength(value.size()); // varint for value length.

		return estimate;
		}*/

	Slice TerarkBlockBuilder::Finish() {

		//PutFixed32(&buffer_, static_cast<uint32_t>(restarts_.size()));
		finished_ = true;
		return Slice(buffer_);
	}

	void TerarkBlockBuilder::Add(const Slice& key, const Slice& value) {
		assert(!finished_);

		// Add "<key_size><value_size>" to buffer_
		PutVarint32Varint32(&buffer_, static_cast<uint32_t>(key.size()),
							static_cast<uint32_t>(value.size()));

		// Add key, value to buffer_
		buffer_.append(key.data(), key.size());
		buffer_.append(value.data(), value.size());

		counter_++;
		//estimate_ += buffer_.size() - curr_size;
	}

}  // namespace rocksdb
