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

#include "rocksdb/env.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/perf_context_imp.h"
#include "util/string_util.h"
#include "util/xxhash.h"
#include "util/statistics.h"
#include "util/stop_watch.h"

#include "trk_format.h"

namespace rocksdb {

	extern const uint64_t kLegacyBlockBasedTableMagicNumber;
	extern const uint64_t kBlockBasedTableMagicNumber;

#ifndef ROCKSDB_LITE
	extern const uint64_t kLegacyPlainTableMagicNumber;
	extern const uint64_t kPlainTableMagicNumber;
#else
	// ROCKSDB_LITE doesn't have plain table
	const uint64_t kLegacyPlainTableMagicNumber = 0;
	const uint64_t kPlainTableMagicNumber = 0;
#endif
	const uint32_t DefaultStackBufferSize = 5000;

	bool ShouldReportDetailedTime(Env* env, Statistics* stats) {
		return env != nullptr && stats != nullptr &&
			stats->stats_level_ > kExceptDetailedTimers;
	}

	void TerarkBlockHandle::EncodeTo(std::string* dst) const {
		// Sanity check that all fields have been set
		assert(offset_ != ~static_cast<uint64_t>(0));
		assert(size_ != ~static_cast<uint64_t>(0));
		PutVarint64Varint64(dst, offset_, size_);
	}

	Status TerarkBlockHandle::DecodeFrom(Slice* input) {
		if (GetVarint64(input, &offset_) &&
			GetVarint64(input, &size_)) {
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

	namespace {
		inline bool IsLegacyFooterFormat(uint64_t magic_number) {
			return magic_number == kLegacyBlockBasedTableMagicNumber ||
				magic_number == kLegacyPlainTableMagicNumber;
		}
		inline uint64_t UpconvertLegacyFooterFormat(uint64_t magic_number) {
			if (magic_number == kLegacyBlockBasedTableMagicNumber) {
				return kBlockBasedTableMagicNumber;
			}
			if (magic_number == kLegacyPlainTableMagicNumber) {
				return kPlainTableMagicNumber;
			}
			assert(false);
			return 0;
		}
	}  // namespace

	// legacy footer format:
	//    metaindex handle (varint64 offset, varint64 size)
	//    index handle     (varint64 offset, varint64 size)
	//    <padding> to make the total size 2 * BlockHandle::kMaxEncodedLength
	//    table_magic_number (8 bytes)
	// new footer format:
	//    checksum (char, 1 byte)
	//    metaindex handle (varint64 offset, varint64 size)
	//    index handle     (varint64 offset, varint64 size)
	//    <padding> to make the total size 2 * BlockHandle::kMaxEncodedLength + 1
	//    footer version (4 bytes)
	//    table_magic_number (8 bytes)
	void TerarkFooter::EncodeTo(std::string* dst) const {
		assert(HasInitializedTableMagicNumber());
		if (IsLegacyFooterFormat(table_magic_number())) {
			// has to be default checksum with legacy footer
			assert(checksum_ == kCRC32c);
			const size_t original_size = dst->size();
			metaindex_handle_.EncodeTo(dst);
			index_handle_.EncodeTo(dst);
			dst->resize(original_size + 2 * TerarkBlockHandle::kMaxEncodedLength);  // Padding
			PutFixed32(dst, static_cast<uint32_t>(table_magic_number() & 0xffffffffu));
			PutFixed32(dst, static_cast<uint32_t>(table_magic_number() >> 32));
			assert(dst->size() == original_size + kVersion0EncodedLength);
		} else {
			const size_t original_size = dst->size();
			dst->push_back(static_cast<char>(checksum_));
			metaindex_handle_.EncodeTo(dst);
			index_handle_.EncodeTo(dst);
			dst->resize(original_size + kNewVersionsEncodedLength - 12);  // Padding
			PutFixed32(dst, version());
			PutFixed32(dst, static_cast<uint32_t>(table_magic_number() & 0xffffffffu));
			PutFixed32(dst, static_cast<uint32_t>(table_magic_number() >> 32));
			assert(dst->size() == original_size + kNewVersionsEncodedLength);
		}
	}

	TerarkFooter::TerarkFooter(uint64_t _table_magic_number, uint32_t _version)
		: version_(_version),
		  checksum_(kCRC32c),
		  table_magic_number_(_table_magic_number) {
		// This should be guaranteed by constructor callers
		assert(!IsLegacyFooterFormat(_table_magic_number) || version_ == 0);
	}

	Status TerarkFooter::DecodeFrom(Slice* input) {
		assert(!HasInitializedTableMagicNumber());
		assert(input != nullptr);
		assert(input->size() >= kMinEncodedLength);

		const char *magic_ptr =
			input->data() + input->size() - kRocksdbMagicNumberLengthByte;
		const uint32_t magic_lo = DecodeFixed32(magic_ptr);
		const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
		uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
						  (static_cast<uint64_t>(magic_lo)));

		bool legacy = IsLegacyFooterFormat(magic);
		if (legacy) {
			magic = UpconvertLegacyFooterFormat(magic);
		}
		set_table_magic_number(magic);

		if (legacy) {
			// The size is already asserted to be at least kMinEncodedLength
			// at the beginning of the function
			input->remove_prefix(input->size() - kVersion0EncodedLength);
			version_ = 0 /* legacy */;
			checksum_ = kCRC32c;
		} else {
			version_ = DecodeFixed32(magic_ptr - 4);
			// Footer version 1 and higher will always occupy exactly this many bytes.
			// It consists of the checksum type, two block handles, padding,
			// a version number, and a magic number
			if (input->size() < kNewVersionsEncodedLength) {
				return Status::Corruption("input is too short to be an sstable");
			} else {
				input->remove_prefix(input->size() - kNewVersionsEncodedLength);
			}
			uint32_t chksum;
			if (!GetVarint32(input, &chksum)) {
				return Status::Corruption("bad checksum type");
			}
			checksum_ = static_cast<ChecksumType>(chksum);
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

		bool legacy = IsLegacyFooterFormat(table_magic_number_);
		if (legacy) {
			result.append("metaindex handle: " + metaindex_handle_.ToString() + "\n  ");
			result.append("index handle: " + index_handle_.ToString() + "\n  ");
			result.append("table_magic_number: " +
						  rocksdb::ToString(table_magic_number_) + "\n  ");
		} else {
			result.append("checksum: " + rocksdb::ToString(checksum_) + "\n  ");
			result.append("metaindex handle: " + metaindex_handle_.ToString() + "\n  ");
			result.append("index handle: " + index_handle_.ToString() + "\n  ");
			result.append("footer version: " + rocksdb::ToString(version_) + "\n  ");
			result.append("table_magic_number: " +
						  rocksdb::ToString(table_magic_number_) + "\n  ");
		}
		return result;
	}

	Status TerarkReadFooterFromFile(RandomAccessFileReader* file, uint64_t file_size,
							  TerarkFooter* footer, uint64_t enforce_table_magic_number) {
		if (file_size < TerarkFooter::kMinEncodedLength) {
			return Status::Corruption("file is too short to be an sstable");
		}

		char footer_space[TerarkFooter::kMaxEncodedLength];
		Slice footer_input;
		size_t read_offset =
			(file_size > TerarkFooter::kMaxEncodedLength)
			? static_cast<size_t>(file_size - TerarkFooter::kMaxEncodedLength)
			: 0;
		Status s = file->Read(read_offset, TerarkFooter::kMaxEncodedLength, &footer_input,
							  footer_space);
		if (!s.ok()) return s;

		// Check that we actually read the whole footer from the file. It may be
		// that size isn't correct.
		if (footer_input.size() < TerarkFooter::kMinEncodedLength) {
			return Status::Corruption("file is too short to be an sstable");
		}

		s = footer->DecodeFrom(&footer_input);
		if (!s.ok()) {
			return s;
		}
		if (enforce_table_magic_number != 0 &&
			enforce_table_magic_number != footer->table_magic_number()) {
			return Status::Corruption("Bad table magic number");
		}
		return Status::OK();
	}

	// Without anonymous namespace here, we fail the warning -Wmissing-prototypes
	namespace {

		// Read a block and check its CRC
		// contents is the result of reading.
		// According to the implementation of file->Read, contents may not point to buf
		Status TerarkReadBlock(RandomAccessFileReader* file, const TerarkFooter& footer,
							   const TerarkReadOptions& options, const TerarkBlockHandle& handle,
							   Slice* contents, /* result of reading */ char* buf) {
			size_t n = static_cast<size_t>(handle.size());
			Status s;

			{
				PERF_TIMER_GUARD(block_read_time);
				s = file->Read(handle.offset(), n + kRocksdbBlockTrailerSize, contents, buf);
			}

			PERF_COUNTER_ADD(block_read_count, 1);
			PERF_COUNTER_ADD(block_read_byte, n + kRocksdbBlockTrailerSize);

			if (!s.ok()) {
				return s;
			}
			if (contents->size() != n + kRocksdbBlockTrailerSize) {
				return Status::Corruption("truncated block read");
			}

			// Check the crc of the type and the block contents
			const char* data = contents->data();  // Pointer to where Read put the data
			if (options.verify_checksums) {
				PERF_TIMER_GUARD(block_checksum_time);
				uint32_t value = DecodeFixed32(data + n + 1);
				uint32_t actual = 0;
				switch (footer.checksum()) {
				case kCRC32c:
					value = crc32c::Unmask(value);
					actual = crc32c::Value(data, n + 1);
					break;
				case kxxHash:
					actual = XXH32(data, static_cast<int>(n) + 1, 0);
					break;
				default:
					s = Status::Corruption("unknown checksum type");
				}
				if (s.ok() && actual != value) {
					s = Status::Corruption("block checksum mismatch");
				}
				if (!s.ok()) {
					return s;
				}
			}
			return s;
		}

	}  // namespace

	Status TerarkReadBlockContents(RandomAccessFileReader* file, 
		const TerarkFooter& footer,
		const TerarkReadOptions& read_options,
		const TerarkBlockHandle& handle, 
		TerarkBlockContents* contents,
		const Options &ioptions) {
		Status status;
		Slice slice;
		size_t n = static_cast<size_t>(handle.size());
		std::unique_ptr<char[]> heap_buf;
		char* used_buf = nullptr;
		rocksdb::CompressionType compression_type;

		heap_buf = std::unique_ptr<char[]>(new char[n + kRocksdbBlockTrailerSize]);
		used_buf = heap_buf.get();
		status = TerarkReadBlock(file, footer, read_options, handle, &slice, used_buf);
		if (!status.ok()) {
			return status;
		}

		compression_type = static_cast<rocksdb::CompressionType>(slice.data()[n]);
		if (slice.data() != used_buf) {
			// the slice content is not the buffer provided
			*contents = TerarkBlockContents(Slice(slice.data(), n), false, compression_type);
		} else {
			// page is uncompressed, the buffer either stack or heap provided
			*contents = TerarkBlockContents(std::move(heap_buf), n, true, compression_type);
		}
		return status;
	}

}  // namespace rocksdb
