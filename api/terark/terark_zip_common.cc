

#include <stdlib.h>
#include <ctime>
#ifdef _MSC_VER
# include <io.h>
#else
# include <sys/types.h>
# include <sys/stat.h>
# include <fcntl.h>
# include <cxxabi.h>
#endif
#include <utility>
// boost header
#include <boost/predef/other/endian.h>
// terark header
#include <terark/io/byte_swap.hpp>
#include <terark/util/throw.hpp>
// project header
#include "terark_zip_common.h"

namespace terark {

	uint64_t ReadUint64(const byte_t* beg, const byte_t* end) {
		assert(end - beg <= 8);
		union {
			byte_t bytes[8];
			uint64_t value = 0;
		} c;
		size_t l = end - beg;
		memcpy(c.bytes + (8 - l), beg, l);
#if BOOST_ENDIAN_LITTLE_BYTE
		return terark::byte_swap(c.value);
#else
		return c.value;
#endif
	}

	uint64_t ReadUint64Aligned(const byte_t* beg, const byte_t* end) {
		assert(end - beg == 8);
		(void)end;
#if BOOST_ENDIAN_LITTLE_BYTE
		return terark::byte_swap(*(const uint64_t*)beg);
#else
		return *(const uint64_t*)beg;
#endif
	}

	void AssignUint64(byte_t* beg, byte_t* end, uint64_t value) {
		assert(end - beg <= 8);
		union {
			byte_t bytes[8];
			uint64_t value;
		} c;
#if BOOST_ENDIAN_LITTLE_BYTE
		c.value = terark::byte_swap(value);
#else
		c.value = value;
#endif
		size_t l = end - beg;
		memcpy(beg, c.bytes + (8 - l), l);
	}

	const char* StrDateTimeNow() {
		thread_local char buf[64];
		time_t rawtime;
		time(&rawtime);
		struct tm* timeinfo = localtime(&rawtime);
		strftime(buf, sizeof(buf), "%F %T",timeinfo);
		return buf;
	}

	std::string demangle(const char* name) {
#ifdef _MSC_VER
		return name;
#else
		int status = -4; // some arbitrary value to eliminate the compiler warning
		terark::AutoFree<char> res(abi::__cxa_demangle(name, NULL, NULL, &status));
		return (status == 0) ? res.p : name;
#endif
	}

	int wt_strndup(const void *str, size_t len, void *retp) {
		void *p;
		if (str == NULL) {
			*(void **)retp = NULL;
			return (0);
		}

		p = malloc(len + 1);
		/*
		 * Don't change this to strncpy, we rely on this function to duplicate
		 * "strings" that contain nul bytes.
		 */
		memcpy(p, str, len);
		((uint8_t *)p)[len] = '\0';

		*(void **)retp = p;
		return (0);
	}

	void AutoDeleteFile::Delete() {
		::remove(fpath.c_str());
		fpath.clear();
	}
	AutoDeleteFile::~AutoDeleteFile() {
		if (!fpath.empty()) {
			::remove(fpath.c_str());
		}
	}

	TempFileDeleteOnClose::~TempFileDeleteOnClose() {
		if (fp)
			this->close();
	}

	/// this->path is temporary filename template such as: /some/dir/tmpXXXXXX
	void TempFileDeleteOnClose::open_temp() {
		if (!terark::fstring(path).endsWith("XXXXXX")) {
			THROW_STD(invalid_argument,
					  "ERROR: path = \"%s\", must ends with \"XXXXXX\"", path.c_str());
		}
#if _MSC_VER
		if (int err = _mktemp_s(&path[0], path.size() + 1)) {
			THROW_STD(invalid_argument, "ERROR: _mktemp_s(%s) = %s"
					  , path.c_str(), strerror(err));
		}
		this->open();
#else
		int fd = mkstemp(&path[0]);
		if (fd < 0) {
			int err = errno;
			THROW_STD(invalid_argument, "ERROR: mkstemp(%s) = %s"
					  , path.c_str(), strerror(err));
		}
		this->dopen(fd);
#endif
	}
	void TempFileDeleteOnClose::open() {
		fp.open(path.c_str(), "wb+");
		fp.disbuf();
		writer.attach(&fp);
	}
	void TempFileDeleteOnClose::dopen(int fd) {
		fp.dopen(fd, "wb+");
		fp.disbuf();
		writer.attach(&fp);
	}
	void TempFileDeleteOnClose::close() {
		assert(nullptr != fp);
		fp.close();
		::remove(path.c_str());
	}
	void TempFileDeleteOnClose::complete_write() {
		writer.flush_buffer();
		fp.rewind();
	}

	FileWriter::~FileWriter() {
		//if (fp) {
		//	this->close();
		//}
	}
	void FileWriter::open() {
		fp.open(path.c_str(), "wb+");
		fp.disbuf();
		writer.attach(&fp);
	}
	void FileWriter::close() {
		//assert(nullptr != fp);
		writer.flush_buffer();
		fp.close();		
		//fp = nullptr;
	}


} // namespace rocksdb

