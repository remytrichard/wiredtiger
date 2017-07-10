//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <stdio.h>
#include <string>
#include <utility>
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/types.h"
#include "util/coding.h"
#include "util/logging.h"

namespace rocksdb {

	class TerarkIterKey {
	public:
	TerarkIterKey()
		: buf_(space_), buf_size_(sizeof(space_)), key_(buf_), key_size_(0) {}

		~TerarkIterKey() { ResetBuffer(); }

		Slice GetKey() const { return Slice(key_, key_size_); }

		Slice GetUserKey() const { return GetKey(); }

		size_t Size() const { return key_size_; }

		void Clear() { key_size_ = 0; }

		// Append "non_shared_data" to its back, from "shared_len"
		// This function is used in Block::Iter::ParseNextKey
		// shared_len: bytes in [0, shard_len-1] would be remained
		// non_shared_data: data to be append, its length must be >= non_shared_len
		void TrimAppend(const size_t shared_len, const char* non_shared_data,
						const size_t non_shared_len) {
			assert(shared_len <= key_size_);
			size_t total_size = shared_len + non_shared_len;

			if (IsKeyPinned() /* key is not in buf_ */) {
				// Copy the key from external memory to buf_ (copy shared_len bytes)
				EnlargeBufferIfNeeded(total_size);
				memcpy(buf_, key_, shared_len);
			} else if (total_size > buf_size_) {
				// Need to allocate space, delete previous space
				char* p = new char[total_size];
				memcpy(p, key_, shared_len);

				if (buf_ != space_) {
					delete[] buf_;
				}

				buf_ = p;
				buf_size_ = total_size;
			}

			memcpy(buf_ + shared_len, non_shared_data, non_shared_len);
			key_ = buf_;
			key_size_ = total_size;
		}

		Slice SetKey(const Slice& key, bool copy = true) {
			size_t size = key.size();
			if (copy) {
				// Copy key to buf_
				EnlargeBufferIfNeeded(size);
				memcpy(buf_, key.data(), size);
				key_ = buf_;
			} else {
				// Update key_ to point to external memory
				key_ = key.data();
			}
			key_size_ = size;
			return Slice(key_, key_size_);
		}

		// Copies the content of key, updates the reference to the user key in ikey
		// and returns a Slice referencing the new copy.
		/*Slice SetKey(const Slice& key, ParsedInternalKey* ikey) {
			size_t key_n = key.size();
			assert(key_n >= 8);
			SetKey(key);
			ikey->user_key = Slice(key_, key_n - 8);
			return Slice(key_, key_n);
			}*/

		// Copy the key into IterKey own buf_
		void OwnKey() {
			assert(IsKeyPinned() == true);

			Reserve(key_size_);
			memcpy(buf_, key_, key_size_);
			key_ = buf_;
		}

		// Update the sequence number in the internal key.  Guarantees not to
		// invalidate slices to the key (and the user key).
		/*void UpdateInternalKey(uint64_t seq, ValueType t) {
			assert(!IsKeyPinned());
			assert(key_size_ >= 8);
			uint64_t newval = (seq << 8) | t;
			EncodeFixed64(&buf_[key_size_ - 8], newval);
			}*/

		bool IsKeyPinned() const { return (key_ != buf_); }

		void Reserve(size_t size) {
			EnlargeBufferIfNeeded(size);
			key_size_ = size;
		}

		void EncodeLengthPrefixedKey(const Slice& key) {
			auto size = key.size();
			EnlargeBufferIfNeeded(size + static_cast<size_t>(VarintLength(size)));
			char* ptr = EncodeVarint32(buf_, static_cast<uint32_t>(size));
			memcpy(ptr, key.data(), size);
			key_ = buf_;
		}

	private:
		char* buf_;
		size_t buf_size_;
		const char* key_;
		size_t key_size_;
		char space_[32];  // Avoid allocation for short keys

		void ResetBuffer() {
			if (buf_ != space_) {
				delete[] buf_;
				buf_ = space_;
			}
			buf_size_ = sizeof(space_);
			key_size_ = 0;
		}

		// Enlarge the buffer size if needed based on key_size.
		// By default, static allocated buffer is used. Once there is a key
		// larger than the static allocated buffer, another buffer is dynamically
		// allocated, until a larger key buffer is requested. In that case, we
		// reallocate buffer and delete the old one.
		void EnlargeBufferIfNeeded(size_t key_size) {
			// If size is smaller than buffer size, continue using current buffer,
			// or the static allocated one, as default
			if (key_size > buf_size_) {
				// Need to enlarge the buffer.
				ResetBuffer();
				buf_ = new char[key_size];
				buf_size_ = key_size;
			}
		}

		// No copying allowed
		TerarkIterKey(const TerarkIterKey&) = delete;
		void operator=(const TerarkIterKey&) = delete;
	};
}
