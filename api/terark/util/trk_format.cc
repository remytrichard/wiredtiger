//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string>
#include <inttypes.h>

#include "trk_common.h"
#include "trk_format.h"

namespace terark {

	extern const uint64_t kBlockBasedTableMagicNumber;
	const uint32_t DefaultStackBufferSize = 5000;

	void TerarkBlockHandle::EncodeTo(std::string* dst) const {
		// Sanity check that all fields have been set
		assert(offset_ != ~static_cast<uint64_t>(0));
		assert(size_ != ~static_cast<uint64_t>(0));
		//TerarkPutVarint64Varint64(dst, offset_, size_);
		TerarkPutFixed64(dst, offset_);
		TerarkPutFixed64(dst, size_);
	}

	Status TerarkBlockHandle::DecodeFrom(Slice* input) {
		if (TerarkGetFixed64(input, &offset_) &&
			TerarkGetFixed64(input, &size_)) {
			return Status::OK();
		} else {
			// reset in case failure after partially decoding
			offset_ = 0;
			size_ = 0;
			return Status::Corruption("bad block handle");
		}
	}

	// Return a string that contains the copy of handle.
	std::string TerarkBlockHandle::ToString(bool hex) const {
		std::string handle_str;
		EncodeTo(&handle_str);
		if (hex) {
			return Slice(handle_str).ToString(true);
		} else {
			return handle_str;
		}
	}

	const TerarkBlockHandle TerarkBlockHandle::kNullBlockHandle(0, 0);

	//  footer format:
	//    metaindex handle (fixed64 offset, fixed64 size)
	//    index handle     (fixed64 offset, fixed64 size)
	//    <padding> to make the total size 2 * BlockHandle::kMaxEncodedLength + 1
	//    footer version (4 bytes)
	//    table_magic_number (8 bytes)
	void TerarkFooter::EncodeTo(std::string* dst) const {
		assert(HasInitializedTableMagicNumber());
		const size_t original_size = dst->size();
		metaindex_handle_.EncodeTo(dst);
		index_handle_.EncodeTo(dst);
		dst->resize(original_size + kNewVersionsEncodedLength - 12);  // Padding
		TerarkPutFixed32(dst, version());
		TerarkPutFixed32(dst, static_cast<uint32_t>(table_magic_number() & 0xffffffffu));
		TerarkPutFixed32(dst, static_cast<uint32_t>(table_magic_number() >> 32));
		assert(dst->size() == original_size + kNewVersionsEncodedLength);
	}

	TerarkFooter::TerarkFooter(uint64_t _table_magic_number, uint32_t _version)
		: version_(_version),
		  table_magic_number_(_table_magic_number) {
	}

	Status TerarkFooter::DecodeFrom(Slice* input) {
		assert(!HasInitializedTableMagicNumber());
		assert(input != nullptr);
		assert(input->size() >= kMinEncodedLength);

		const char *magic_ptr =
			input->data() + input->size() - kRocksdbMagicNumberLengthByte;
		const uint32_t magic_lo = TerarkDecodeFixed32(magic_ptr);
		const uint32_t magic_hi = TerarkDecodeFixed32(magic_ptr + 4);
		uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
						  (static_cast<uint64_t>(magic_lo)));

		set_table_magic_number(magic);

		version_ = TerarkDecodeFixed32(magic_ptr - 4);
		// Footer version 1 and higher will always occupy exactly this many bytes.
		// It consists of two block handles, padding,
		// a version number, and a magic number
		if (input->size() < kNewVersionsEncodedLength) {
			return Status::Corruption("input is too short to be an sstable");
		} else {
			input->remove_prefix(input->size() - kNewVersionsEncodedLength);
		}

		Status result = metaindex_handle_.DecodeFrom(input);
		if (result.ok()) {
			result = index_handle_.DecodeFrom(input);
		}
		if (result.ok()) {
			// We skip over any leftover data (just padding for now) in "input"
			const char* end = magic_ptr + kRocksdbMagicNumberLengthByte;
			*input = Slice(end, input->data() + input->size() - end);
		}
		return result;
	}

	std::string TerarkFooter::ToString() const {
		std::string result, handle_;
		result.reserve(1024);
		result.append("metaindex handle: " + metaindex_handle_.ToString() + "\n  ");
		result.append("index handle: " + index_handle_.ToString() + "\n  ");
		result.append("footer version: " + std::to_string(version_) + "\n  ");
		result.append("table_magic_number: " +
					  std::to_string(table_magic_number_) + "\n  ");
		return result;
	}

	Status TerarkReadFooterFromFile(const Slice& file_data, uint64_t file_size,
									TerarkFooter* footer, uint64_t enforce_table_magic_number) {
		if (file_size < TerarkFooter::kMinEncodedLength) {
			return Status::Corruption("file is too short to be an sstable");
		}

		char footer_space[TerarkFooter::kMaxEncodedLength];
		size_t read_offset = static_cast<size_t>(file_size - TerarkFooter::kMaxEncodedLength);
		Slice footer_input(file_data.data() + read_offset, TerarkFooter::kMaxEncodedLength);
		Status s = footer->DecodeFrom(&footer_input);
		if (!s.ok()) {
			return s;
		}
		if (enforce_table_magic_number != 0 &&
			enforce_table_magic_number != footer->table_magic_number()) {
			return Status::Corruption("Bad table magic number");
		}
		return Status::OK();
	}

	Status TerarkReadBlockContents(const Slice& file_data, 
								   const TerarkBlockHandle& handle, 
								   TerarkBlockContents* contents) {
		size_t n = static_cast<size_t>(handle.size());
		std::unique_ptr<char[]> heap_buf = std::unique_ptr<char[]>(new char[n + kRocksdbBlockTrailerSize]);
		char* used_buf = heap_buf.get();

		Slice slice(file_data.data() + handle.offset(), n + kRocksdbBlockTrailerSize);
		if (slice.size() != n + kRocksdbBlockTrailerSize) {
			return Status::Corruption("truncated block read");
		}
		if (slice.data() != used_buf) {
			// the slice content is not the buffer provided(mmap read currently)
			*contents = TerarkBlockContents(Slice(slice.data(), n));
		} else {
			*contents = TerarkBlockContents(std::move(heap_buf), n);
		}
		return Status::OK();
	}

}  // namespace rocksdb
