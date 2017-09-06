#pragma once

#include "wiredtiger.h"

#include <terark/io/DataIO.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include "util/slice.h"

namespace terark {

#if defined(IOS_CROSS_COMPILE) || defined(__DARWIN_C_LEVEL)
#define MY_THREAD_LOCAL(Type, Var)  Type Var
	//#elif defined(_WIN32)
	//  #define MY_THREAD_LOCAL(Type, Var)  static __declspec(thread) Type Var
#else
#define MY_THREAD_LOCAL(Type, Var)  static thread_local Type Var
#endif

#define STD_INFO(format, ...) fprintf(stderr, "%s INFO: " format, StrDateTimeNow(), ##__VA_ARGS__)
#define STD_WARN(format, ...) fprintf(stderr, "%s WARN: " format, StrDateTimeNow(), ##__VA_ARGS__)

#undef INFO
#undef WARN
#if defined(NDEBUG) || 1
# define INFO(logger, format, ...) Info(logger, format, ##__VA_ARGS__)
# define WARN(logger, format, ...) Warn(logger, format, ##__VA_ARGS__)
#else
# define INFO(logger, format, ...) STD_INFO(format, ##__VA_ARGS__)
# define WARN(logger, format, ...) STD_WARN(format, ##__VA_ARGS__)
#endif

	using std::string;
	using std::unique_ptr;

	using terark::byte_t;
	using terark::fstring;
	using terark::valvec;
	using terark::valvec_no_init;
	using terark::valvec_reserve;

	using terark::FileStream;
	using terark::InputBuffer;
	using terark::OutputBuffer;
	using terark::LittleEndianDataInput;
	using terark::LittleEndianDataOutput;

	class Iterator;
	struct wt_terark_cursor {
		WT_CURSOR iface;
		Iterator* iter;
	};

	template<class T>
		inline unique_ptr<T> UniquePtrOf(T* p) { return unique_ptr<T>(p); }

	uint64_t ReadUint64(const byte_t* beg, const byte_t* end);
	uint64_t ReadUint64Aligned(const byte_t* beg, const byte_t* end);
	void AssignUint64(byte_t* beg, byte_t* end, uint64_t value);

	const char* StrDateTimeNow();
	std::string demangle(const char* name);

	template<class T>
		inline std::string ClassName() {
		return demangle(typeid(T).name());
	}
	template<class T>
		inline std::string ClassName(const T& x) {
		return demangle(typeid(x).name());
	}

	template<class ByteArray>
		inline Slice SliceOf(const ByteArray& ba) {
		BOOST_STATIC_ASSERT(sizeof(ba[0] == 1));
		return Slice((const char*)ba.data(), ba.size());
	}

	inline static fstring fstringOf(const Slice& x) {
		return fstring(x.data(), x.size());
	}

	template<class ByteArrayView>
		inline ByteArrayView SubStr(const ByteArrayView& x, size_t pos) {
		assert(pos <= x.size());
		return ByteArrayView(x.data() + pos, x.size() - pos);
	}

	/*
	 * wt_strndup -- (from src/os_common/os_alloc.c)
	 *  Duplicate a byte string of a given length (and NUL-terminate).
	 */
	int wt_strndup(const void *str, size_t len, void *retp);

	class AutoDeleteFile {
	public:
		std::string fpath;
		operator fstring() const { return fpath; }
		void Delete();
		~AutoDeleteFile();
	};

	class TempFileDeleteOnClose {
	public:
		std::string path;
		FileStream  fp;
		NativeDataOutput<OutputBuffer> writer;
		~TempFileDeleteOnClose();
		void open_temp();
		void open();
		void dopen(int fd);
		void close();
		void complete_write();
	};

	class FileWriter {
	public:
		std::string path;
		FileStream  fp;
		NativeDataOutput<OutputBuffer> writer;
		~FileWriter();
		void open();
		void close();
	};

} // namespace
