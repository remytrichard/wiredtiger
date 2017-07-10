
#include <sys/stat.h>

#include <boost/predef/other/endian.h>

#include "trk_common.h"

namespace rocksdb {

	TComparator* GetBytewiseComparator() {
		static TBytewiseComparator* comparator_;
		if (!comparator_) {
			comparator_ = new TBytewiseComparator();
		}
		return comparator_;
	}

	Status GetFileSize(const std::string& fname,
					   uint64_t* size) {
		Status s;
		struct stat sbuf;
		if (stat(fname.c_str(), &sbuf) != 0) {
			*size = 0;
			s = Status::IOError(fname, "Fail to stat file");
		} else {
			*size = sbuf.st_size;
		}
		return s;
	}

	// Pull the last 8 bits and cast it to a character
	void TerarkPutFixed32(std::string* dst, uint32_t value) {
#if BOOST_ENDIAN_LITTLE_BYTE
		dst->append(const_cast<const char*>(reinterpret_cast<char*>(&value)),
					sizeof(value));
#else
		char buf[sizeof(value)];
		TerarkEncodeFixed32(buf, value);
		dst->append(buf, sizeof(buf));
#endif
	}

	void TerarkPutFixed64(std::string* dst, uint64_t value) {
#if BOOST_ENDIAN_LITTLE_BYTE
		dst->append(const_cast<const char*>(reinterpret_cast<char*>(&value)),
					sizeof(value));
#else
		char buf[sizeof(value)];
		TerarkEncodeFixed64(buf, value);
		dst->append(buf, sizeof(buf));
#endif
	}

	bool TerarkGetFixed64(Slice* input, uint64_t* value) {
		if (input->size() < sizeof(uint64_t)) {
			return false;
		}
		*value = TerarkDecodeFixed64(input->data());
		input->remove_prefix(sizeof(uint64_t));
		return true;
	}

	void TerarkEncodeFixed32(char* buf, uint32_t value) {
#if BOOST_ENDIAN_LITTLE_BYTE
		memcpy(buf, &value, sizeof(value));
#else
		buf[0] = value & 0xff;
		buf[1] = (value >> 8) & 0xff;
		buf[2] = (value >> 16) & 0xff;
		buf[3] = (value >> 24) & 0xff;
#endif
	}

	uint32_t TerarkDecodeFixed32(const char* ptr) {
#if BOOST_ENDIAN_LITTLE_BYTE
			// Load the raw bytes
			uint32_t result;
			memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
			return result;
#else
			return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
					| (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
					| (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
					| (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
#endif
	}

	void TerarkEncodeFixed64(char* buf, uint64_t value) {
#if BOOST_ENDIAN_LITTLE_BYTE
		memcpy(buf, &value, sizeof(value));
#else
		buf[0] = value & 0xff;
		buf[1] = (value >> 8) & 0xff;
		buf[2] = (value >> 16) & 0xff;
		buf[3] = (value >> 24) & 0xff;
		buf[4] = (value >> 32) & 0xff;
		buf[5] = (value >> 40) & 0xff;
		buf[6] = (value >> 48) & 0xff;
		buf[7] = (value >> 56) & 0xff;
#endif
	}

	uint64_t TerarkDecodeFixed64(const char* ptr) {
#if BOOST_ENDIAN_LITTLE_BYTE
			// Load the raw bytes
			uint64_t result;
			memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
			return result;
#else
			uint64_t lo = TerarkDecodeFixed32(ptr);
			uint64_t hi = TerarkDecodeFixed32(ptr + 4);
			return (hi << 32) | lo;
#endif
	}

}

