
// boost headers
#include <boost/scope_exit.hpp>
// terark headers
#include <terark/util/crc.hpp>
// project headers
#include "util/trk_meta_blocks.h"
#include "terark_chunk_reader.h"
#include "terark_zip_common.h"

namespace terark {

  static void MmapWarmUpBytes(const void* addr, size_t len) {
    auto base = (const byte_t*)(uintptr_t(addr) & uintptr_t(~4095));
    auto size = terark::align_up((size_t(addr) & 4095) + len, 4096);
#ifdef POSIX_MADV_WILLNEED
    posix_madvise((void*)addr, len, POSIX_MADV_WILLNEED);
#endif
    for (size_t i = 0; i < size; i += 4096) {
      volatile byte_t unused = ((const volatile byte_t*)base)[i];
      (void)unused;
    }
  }
  template<class T>
  static void MmapWarmUp(const T* addr, size_t len) {
    MmapWarmUpBytes(addr, sizeof(T)*len);
  }
  static void MmapWarmUp(fstring mem) {
    MmapWarmUpBytes(mem.data(), mem.size());
  }
  template<class Vec>
  static void MmapWarmUp(const Vec& uv) {
    MmapWarmUpBytes(uv.memory.data(), uv.memory.size());
  }
}


namespace terark {

  using terark::BadCrc32cException;
  using terark::byte_swap;
  using terark::BlobStore;

  class TerarkChunkIterator : public Iterator, boost::noncopyable {
  public:
    enum IterState {
      kInvalid = 0,
      kExactPositioned,     // after seek exact
      kPositioned, // next/prev was called
    };
  public:
    TerarkChunkIterator(TerarkChunkReader* chunk, const std::string& commonPrefix)
      : chunk_(chunk), commonPrefix_(commonPrefix) {
      iter_.reset(chunk_->index_->NewIterator()); 
      iter_->SetInvalid();
      status_ = Status::NotFound();
      iter_state_ = kInvalid;
    }
    ~TerarkChunkIterator() {}
    bool Valid() const { return status_.ok(); }
    void SeekToFirst() {
      iter_state_ = kPositioned;
      UnzipIterRecord(iter_->SeekToFirst());
    }
    void SeekToLast() {
      iter_state_ = kPositioned;
      UnzipIterRecord(iter_->SeekToLast());
    }
    void Seek(const Slice& target) {
      iter_state_ = kPositioned;
      size_t clen = fstringOf(target).commonPrefixLen(commonPrefix_);
      UnzipIterRecord(iter_->Seek(fstringOf(target), clen));
    }

    void SeekExact(const Slice&);
    void Next();
    void Prev();

    void SetInvalid(const Status& status) {
      status_ = status;
      SetInvalid();
    }
    void SetInvalid() { 
      iter_state_ = kInvalid;
      iter_->SetInvalid();
      keyBuf_.clear();
      valueBuf_.clear();
    }
    Slice key() const {
      assert(status_.ok());
      return SliceOf(keyBuf_);
    }
    Slice value() const {
      assert(status_.ok());
      return SliceOf(fstring(valueBuf_));
    }
    Status status() const { return status_; }
    virtual bool UnzipIterRecord(bool);
		
  protected:
    TerarkChunkReader* chunk_;
    std::unique_ptr<TerarkIndex::Iterator> iter_;
    std::string commonPrefix_;
    valvec<byte_t>          keyBuf_;
    valvec<byte_t>          valueBuf_;
    Status                  status_;
    IterState iter_state_;
  };

  void TerarkChunkIterator::SeekExact(const Slice& target) {
    size_t clen = fstringOf(target).commonPrefixLen(commonPrefix_);
    if (clen != commonPrefix_.length()) {
      SetInvalid(Status::NotFound());
      return;
    }
    size_t recId = chunk_->index_->Find(fstringOf(target), clen);
    if (recId == std::string::npos) {
      SetInvalid(Status::NotFound());
      return;
    }
    try {
      valueBuf_.erase_all();
      chunk_->store_->get_record_append(recId, &valueBuf_);
    } catch (const BadCrc32cException& ex) { // crc checksum error
      SetInvalid(Status::Corruption("TerarkZipTableIterator::Seek()", ex.what()));
      return;
    }
    keyBuf_.assign(target.data(), target.size());
    status_ = Status::OK();
    iter_state_ = kExactPositioned;
  }

  /*
   * If the WT_CURSOR::next method is called on a cursor without 
   * a position in the data source, it is positioned at the beginning 
   * of the data source.
   * SeekExact() -> Next()
   * Seek() -> SeekExact() -> Next()
   */
  void TerarkChunkIterator::Next() {
    if (iter_state_ == kInvalid) {
      SeekToFirst();
      return;
    } else if (iter_state_ = kExactPositioned) {
      Seek(SliceOf(keyBuf_));
    }
    assert(iter_->Valid());
    iter_state_ = kPositioned;
    UnzipIterRecord(iter_->Next());
  }

  /*
   * If the WT_CURSOR::prev method is called on a cursor without
   * a position in the data source, it is positioned at the end
   * of the data source.
   */
  void TerarkChunkIterator::Prev() {
    if (iter_state_ == kInvalid) {
      SeekToLast();
      return;
    } else if (iter_state_ == kExactPositioned) {
      Seek(SliceOf(keyBuf_));
    }
    assert(iter_->Valid());
    iter_state_ = kPositioned;
    UnzipIterRecord(iter_->Prev());
  }

  // should be called from Next/Prev/SearchNear
  bool TerarkChunkIterator::UnzipIterRecord(bool hasRecord) {
    if (hasRecord) {
      assert(iter_->id() < chunk_->index_->NumKeys());
      size_t recId = iter_->id();
      try {
	valueBuf_.erase_all();
	chunk_->store_->get_record_append(recId, &valueBuf_);
      } catch (const BadCrc32cException& ex) { // crc checksum error
	SetInvalid(Status::Corruption("TerarkZipTableIterator::UnzipIterRecord()", ex.what()));
	return false;
      }
      keyBuf_.assign(commonPrefix_);
      keyBuf_.append((byte_t*)iter_->key().data(), iter_->key().size());
      status_ = Status::OK();
      return true;
    } else {
      SetInvalid(Status::NotFound());
      return false;
    }
  }

  TerarkChunkReader::~TerarkChunkReader() {
    index_.reset();
    store_.reset();
    file_reader_.reset();
  }
	
  Iterator*
  TerarkChunkReader::NewIterator() {
    Status s = Open();
    if (!s.ok()) {
      printf("Fail to open chunk: %s\n", s.getState());
      return nullptr;
    }
    return new TerarkChunkIterator(this, commonPrefix_);
  }

  Status
  TerarkChunkReader::Open() {
    if (file_reader_) {
      return Status::OK();
    }
    // prepare chunk file
    file_reader_.reset(new terark::MmapWholeFile(chunk_name_));
    size_t file_size = 0;
    Status s = GetFileSize(chunk_name_, &file_size);
    assert(s.ok());
    // read meta data -- properties, index meta, value meta
    Slice file_data((const char*)file_reader_->base, file_size);
    TerarkTableProperties* table_props = nullptr;
    s = TerarkReadTableProperties(file_data, file_size,
				  kTerarkZipTableMagicNumber, &table_props);
    if (!s.ok()) {
      return s;
    }
    assert(nullptr != table_props);
    TerarkBlockContents valueDictBlock, indexBlock, commonPrefixBlock;
    s = TerarkReadMetaBlock(file_data, file_size, kTerarkZipTableMagicNumber,
			    kTerarkZipTableValueDictBlock, &valueDictBlock);
    if (!s.ok()) {
      return s;
    }
    s = TerarkReadMetaBlock(file_data, file_size, kTerarkZipTableMagicNumber,
			    kTerarkZipTableIndexBlock, &indexBlock);
    if (!s.ok()) {
      return s;
    }
    s = TerarkReadMetaBlock(file_data, file_size, kTerarkZipTableMagicNumber,
			    kTerarkZipTableCommonPrefixBlock, &commonPrefixBlock);
    if (s.ok()) {
      commonPrefix_.assign(commonPrefixBlock.data.data(),
			   commonPrefixBlock.data.size());
      reader_options_.common_prefix = commonPrefix_;
    } else {
      return s;
    }
    // read contents -- index, value
    try {
      store_.reset(terark::BlobStore::load_from_user_memory(
							    fstring(file_data.data(), table_props->data_size), fstringOf(valueDictBlock.data)));
      index_ = TerarkIndex::LoadMemory(fstringOf(indexBlock.data), reader_options_);
    } catch (const BadCrc32cException& ex) {
      return Status::Corruption("TerarkZipTableBuilder::Open()", ex.what());
    } catch (const std::exception& ex) {
      return Status::InvalidArgument("TerarkZipTableBuilder::OpenForRead()", ex.what());
    }
    // 
    long long t0 = g_pf.now();
    if (table_options_.warmUpIndexOnOpen) {
      MmapWarmUp(fstringOf(indexBlock.data));
      if (!table_options_.warmUpValueOnOpen) {
	MmapWarmUp(store_->get_dict());
	for (fstring block : store_->get_index_blocks()) {
	  MmapWarmUp(block);
	}
      }
    }
    if (table_options_.warmUpValueOnOpen) {
      MmapWarmUp(store_->get_mmap());
    }
    long long t1 = g_pf.now();
    index_->BuildCache(table_options_.indexCacheRatio);
    long long t2 = g_pf.now();
    //INFO(ioptions.info_log
    printf("TerarkChunkReader::Open(): fsize = %zd, entries = %zd keys = %zd indexSize = %zd valueSize=%zd, warm up time = %6.3f'sec, build cache time = %6.3f'sec\n"
	   , size_t(file_size), size_t(table_props->num_entries)
	   , index_->NumKeys()
	   , size_t(table_props->index_size)
	   , size_t(table_props->data_size)
	   , g_pf.sf(t0, t1)
	   , g_pf.sf(t1, t2)
	   );
    return Status::OK();
  }

}
