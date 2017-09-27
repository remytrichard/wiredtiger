#pragma once

#include <memory>
// boost
#include <boost/intrusive_ptr.hpp>
// wt
#include "wiredtiger.h"
#include "wiredtiger_ext.h"
// terark header
#include <terark/fstring.hpp>
#include <terark/valvec.hpp>
#include <terark/util/refcount.hpp>
#include <terark/int_vector.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/StreamBuffer.hpp>
// adaptor
#include "terark_zip_config.h"

namespace terark {

	using terark::fstring;
	using terark::valvec;
	using terark::byte_t;
	using terark::NativeDataInput;
	using terark::InputBuffer;
	using std::unique_ptr;

	struct TerarkZipTableOptions;
	//struct TerarkTableBuilderOptions;
	class TempFileDeleteOnClose;

	class TerarkIndex : boost::noncopyable {
	public:
		class Iterator : boost::noncopyable {
	protected:
		size_t m_id = size_t(-1);
	public:
		virtual ~Iterator();
		virtual bool SeekToFirst() = 0;
		virtual bool SeekToLast() = 0;
		virtual bool Seek(fstring target, size_t commonPrefixLen) = 0;
		virtual bool Next() = 0;
		virtual bool Prev() = 0;
		inline bool Valid() const { return size_t(-1) != m_id; }
		inline size_t id() const { return m_id; }
		/*
		 * During Search(), we'll pass in complete key & commonPrefixLen, let
		 * the object to decide how to handle them.
		 * When we have key() called, we just return key after stripped of the
		 * common prefix. it really sucks here...
		 */
		virtual fstring key() const = 0;
		inline void SetInvalid() { m_id = size_t(-1); }
	};
	struct KeyStat {
		size_t commonPrefixLen = size_t(-1); // prefix len of all keys within one merge round
		size_t minKeyLen = 0;
		size_t maxKeyLen = size_t(-1);
		size_t sumKeyLen = 0;
		size_t numKeys   = 0;
		valvec<byte_t> minKey;
		valvec<byte_t> maxKey;
	};
	class Factory : public terark::RefCounter {
	public:
		virtual ~Factory();
		virtual void Build(NativeDataInput<InputBuffer>& tmpKeyFileReader,
						   const TerarkZipTableOptions& tzopt,
						   const TerarkTableBuilderOptions& tbo,
						   std::function<void(const void *, size_t)> write,
						   KeyStat&) const = 0;
		virtual unique_ptr<TerarkIndex> LoadMemory(fstring mem, const TerarkTableReaderOptions&) const = 0;
		virtual unique_ptr<TerarkIndex> LoadFile(fstring fpath, const TerarkTableReaderOptions&) const = 0;
		virtual size_t MemSizeForBuild(const TerarkTableBuilderOptions& tbo, const KeyStat&) const = 0;
	};
	typedef boost::intrusive_ptr<Factory> FactoryPtr;
	struct AutoRegisterFactory {
		AutoRegisterFactory(std::initializer_list<const char*> names,
							const char* rtti_name, Factory* factory);
	};
	static const Factory* GetFactory(fstring name);
	static const Factory* SelectFactory(const KeyStat&, 
		fstring key_f, WT_SESSION* session, fstring name);
	static unique_ptr<TerarkIndex> LoadFile(fstring fpath, const TerarkTableReaderOptions&);
	static unique_ptr<TerarkIndex> LoadMemory(fstring mem, const TerarkTableReaderOptions&);
	virtual ~TerarkIndex();
	virtual const char* Name() const = 0;
	virtual size_t Find(fstring key, size_t commonPrefixLen) const = 0;
	virtual size_t NumKeys() const = 0;
	virtual size_t TotalKeySize() const = 0;
	virtual fstring Memory() const = 0;
	virtual void SaveMmap(std::function<void(const void *, size_t)> write) const = 0;
	virtual Iterator* NewIterator() const = 0;
	virtual bool NeedsReorder() const = 0;
	virtual void GetOrderMap(terark::UintVecMin0& newToOld) const = 0;
	virtual void BuildCache(double cacheRatio) = 0;

	public:
	TerarkIndex(const TerarkTableReaderOptions& reader_options) : reader_options_(reader_options) {}
	protected:
	TerarkTableReaderOptions reader_options_;
	};

#define TerarkIndexRegister(clazz, ...)									\
	BOOST_STATIC_ASSERT(sizeof(BOOST_STRINGIZE(clazz)) <= 60);			\
    TerarkIndex::AutoRegisterFactory									\
		g_AutoRegister_##clazz(											\
							   {#clazz,##__VA_ARGS__},					\
							   typeid(clazz).name(),					\
							   new clazz::MyFactory()					\
																	)

}
