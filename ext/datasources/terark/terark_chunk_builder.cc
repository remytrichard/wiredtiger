
// std headers
#include <future>
#include <cfloat>
// boost headers
#include <boost/scope_exit.hpp>
// wiredtiger headers
#include "wiredtiger.h"
// terark headers
#include <terark/util/sortable_strvec.hpp>
#include <terark/util/mmap.hpp>
#include <terark/int_vector.hpp>
#include <terark/zbs/blob_store.hpp>
#include <terark/zbs/zip_reorder_map.hpp>
// project headers
#include "util/trk_meta_blocks.h"
#include "terark_chunk_builder.h"
#include "terark_zip_config.h"

namespace terark {

#ifdef TERARK_ZIP_TRIAL_VERSION
	const char g_trail_rand_delete[] = "TERARK_ZIP_TRIAL_VERSION random deleted this row";
#endif

	using terark::SortableStrVec;
	using terark::byte_swap;

	std::mutex g_sumMutex;
	size_t g_sumKeyLen = 0;
	size_t g_sumValueLen = 0;
	size_t g_sumUserKeyLen = 0;
	size_t g_sumUserKeyNum = 0;
	size_t g_sumEntryNum = 0;
	long long g_lastTime = g_pf.now();

	void DEBUG_PRINT_KEY(...) {}

#define DEBUG_PRINT_1ST_PASS_KEY(...) DEBUG_PRINT_KEY(__VA_ARGS__);
#define DEBUG_PRINT_2ND_PASS_KEY(...) DEBUG_PRINT_KEY(__VA_ARGS__);

	template<class ByteArray>
	static
	Status WriteBlock(const ByteArray& blockData, FileWriter& file, //WritableFileWriter* file,
					  uint64_t* offset, TerarkBlockHandle* block_handle) {
		block_handle->set_offset(*offset);
		block_handle->set_size(blockData.size());
		file.writer.ensureWrite(blockData.data(), blockData.size());
		*offset += blockData.size();
		return Status::OK();
	}

	namespace {
		struct PendingTask {
			const TerarkChunkBuilder* tztb;
			long long startTime;
		};
		
		const WT_ITEM kTombstone = { "\x14\x14", 2, 0, NULL, 0 };
		bool IsDeleted(WT_ITEM* item) {
			return (item->size == kTombstone.size &&
				memcmp(item->data, kTombstone.data, kTombstone.size) == 0);
		}
		bool IsDeleted(const Slice& slice) {
			return (slice.size() == kTombstone.size &&
					memcmp(slice.data(), kTombstone.data, kTombstone.size) == 0);
		}
	}

	TerarkChunkBuilder::TerarkChunkBuilder(const TerarkZipTableOptions& tzto,
		const TerarkTableBuilderOptions& tbo,
		const std::string& fname)
		: table_options_(tzto)
		, table_build_options_(tbo)
		, chunk_name_(fname)
		, useUint64Comparator_(false) {
		// temp mmap files
		sampleUpperBound_ = randomGenerator_.max() * table_options_.sampleRatio;
		size_t pos = fname.find_last_of('/');
		std::string ext = (pos == std::string::npos) ? fname : fname.substr(pos + 1);
		work_path_ = tzto.localTempDir + "/Terark-" + ext;
		tmpKeyFile_.path = work_path_ + ".keydata";
		tmpKeyFile_.open();
		tmpSampleFile_.path = work_path_ + ".sample";
		tmpSampleFile_.open();
		if (table_options_.debugLevel == 4) {
			tmpDumpFile_.open(work_path_ + ".dump", "wb+");
		}
		file_writer_.path = fname;
		file_writer_.open();
		// blob store builder
		DictZipBlobStore::Options dzopt;
		dzopt.entropyAlgo = DictZipBlobStore::Options::EntropyAlgo(table_options_.entropyAlgo);
		dzopt.checksumLevel = table_options_.checksumLevel;
		dzopt.useSuffixArrayLocalMatch = table_options_.useSuffixArrayLocalMatch;
		zbuilder_.reset(DictZipBlobStore::createZipBuilder(dzopt));
		// init stats
		histogram_.emplace_back();
		auto& currentHistogram = histogram_.back();
		currentHistogram.prefix.emplace_back();
		currentHistogram.stat.minKeyLen = std::numeric_limits<int>::max();
		currentHistogram.stat.maxKeyLen = 0;
		currentHistogram.stat.sumKeyLen = 0;
		currentHistogram.stat.numKeys = 0;
		tms_.resize(10);
#if BOOST_ENDIAN_LITTLE_BYTE
		useUint64Comparator_ = tzto.useUint64Comparator;
#endif
		if (useUint64Comparator_) {
			properties_.comparator_name = "uint64comparator";
		}
	}

	TerarkChunkBuilder::~TerarkChunkBuilder() {}

	Status TerarkChunkBuilder::EmptyTableFinish() {
		//INFO(table_build_options_.info_log
		printf("TerarkChunkBuilder::EmptyFinish():this=%p\n", this);
		offset_ = 0;
		TerarkBlockHandle emptyTableBH;
		Status s = WriteBlock(Slice("Empty"), file_writer_, &offset_, &emptyTableBH);
		if (!s.ok()) {
			return s;
		}
		return WriteMetaData({
				{ &kTerarkEmptyTableKey                         , emptyTableBH  },
			   });
	}

	/*
	 *  First Pass: After all key-value Added, create 2 task: one for index build, one for sample build.
	 *      Put them into task-pool, picked to start based on their Memory Rating.
	 *      Exit once sample build task done.
	 */
	void TerarkChunkBuilder::Add(const Slice& key, const Slice& value) {
		static int cnt = 1;
		if (cnt == 1 || cnt == 10 || cnt == 100 || cnt == 1000 || cnt == 10000) {
			printf("new income %d\n", cnt);
		}
		cnt++;
		if (table_options_.debugLevel == 4) {
			fprintf(tmpDumpFile_.fp(), "DEBUG: 1st pass => %s / %s \n", key.data(), value.data());
		}
		DEBUG_PRINT_1ST_PASS_KEY(key);
		uint64_t offset = uint64_t((properties_.raw_key_size + properties_.raw_value_size)
								   * table_options_.estimateCompressionRatio);
		fstring userKey(key.data(), key.size());
		{   // special impl for uint64 comp
			uint64_t u64_key;
			if (useUint64Comparator_) {
				assert(userKey.size() == 8);
				u64_key = byte_swap(*reinterpret_cast<const uint64_t*>(userKey.data()));
				userKey = fstring(reinterpret_cast<const char*>(&u64_key), 8);
			}
		}
		{	
			// update stat
			auto& currentHistogram = histogram_.back();
			auto& keyStat = histogram_.back().stat;
			if (terark_likely(keyStat.numKeys > 0)) {
				assert(prevUserKey_ < userKey);
				keyStat.commonPrefixLen = fstring(prevUserKey_.data(), keyStat.commonPrefixLen)
					.commonPrefixLen(userKey);
			} else {
				//t0 = g_pf.now();
				tms_[kBuildStart] = (g_pf.now());
				keyStat.commonPrefixLen = userKey.size();
				keyStat.minKey.assign(userKey);
			}
			keyStat.minKeyLen = std::min(userKey.size(), keyStat.minKeyLen);
			keyStat.maxKeyLen = std::max(userKey.size(), keyStat.maxKeyLen);
			keyStat.sumKeyLen += userKey.size();
			keyStat.numKeys++;
			keyStat.maxKey.assign(userKey);
			currentHistogram.key[userKey.size()]++;
			currentHistogram.value[value.size()]++;
		}

		tmpKeyFile_.writer << userKey;
		if (!value.empty() && randomGenerator_() < sampleUpperBound_) {
			tmpSampleFile_.writer << fstringOf(value);
			sampleLenSum_ += value.size();
		}
		prevUserKey_.assign(userKey);

		properties_.num_entries++;
		properties_.raw_key_size += key.size();
		properties_.raw_value_size += value.size();
	}

	Status TerarkChunkBuilder::Finish1stPass() {
		assert(!closed_);
		closed_ = true;

		if (histogram_.back().stat.numKeys == 0) {
			// empty chunk is not allowed
			std::string tmp = "TerarkSpecialTreatmentForEmptyKey";
			Slice placeholderKey(tmp), placeholderValue("\x14\x14", 2);
			Add(placeholderKey, placeholderValue);
		}

		for (auto& item : histogram_) {
			item.key.finish();
			item.value.finish();
			if (item.stat.numKeys == 1) {
				assert(item.stat.commonPrefixLen == item.stat.sumKeyLen);
				item.stat.commonPrefixLen = 0; // to avoid empty nlt trie
			}
		}
		tmpKeyFile_.complete_write();
		tmpSampleFile_.complete_write();
		{
			long long rawBytes = properties_.raw_key_size + properties_.raw_value_size;
			long long tt = g_pf.now();
			//INFO(table_build_options_.info_log
			printf("TerarkChunkBuilder::Finish():this=%p:  first pass time =%7.2f's, %8.3f'MB/sec\n"
				 , this, g_pf.sf(tms_[0], tt), rawBytes*1.0 / g_pf.uf(tms_[0], tt)
				 );
		}
		static std::mutex zipMutex;
		static std::condition_variable zipCond;
		static valvec<PendingTask> waitQueue;
		static size_t sumWaitingMem = 0;
		static size_t sumWorkingMem = 0;
		const  size_t softMemLimit = table_options_.softZipWorkingMemLimit;
		const  size_t hardMemLimit = std::max(table_options_.hardZipWorkingMemLimit, softMemLimit);
		const  size_t smallmem = table_options_.smallTaskMemory;
		const  long long myStartTime = g_pf.now();
		{
			std::unique_lock<std::mutex> zipLock(zipMutex);
			waitQueue.push_back({this, myStartTime});
		}
		auto waitForMemory = [&](size_t myWorkMem, const char* who) {
			const std::chrono::seconds waitForTime(10);
			long long now = myStartTime;
			auto shouldWait = [&]() {
				bool w;
				if (myWorkMem < softMemLimit) {
					w = (sumWorkingMem + myWorkMem >= hardMemLimit) ||
						(sumWorkingMem + myWorkMem >= softMemLimit && myWorkMem >= smallmem);
				} else {
					w = sumWorkingMem > softMemLimit / 4;
				}
				if (!w) {
					assert(!waitQueue.empty());
					now = g_pf.now();
					if (myWorkMem < smallmem) {
						return false; // do not wait
					}
					if (sumWaitingMem + sumWorkingMem < softMemLimit) {
						return false; // do not wait
					}
					if (waitQueue.size() == 1) {
						assert(this == waitQueue[0].tztb);
						return false; // do not wait
					}
					size_t minRateIdx = size_t(-1);
					double minRateVal = DBL_MAX;
					auto wq = waitQueue.data();
					// pick smallest rate first
					for (size_t i = 0, n = waitQueue.size(); i < n; ++i) {
						double rate = myWorkMem / (0.1 + now - wq[i].startTime);
						if (rate < minRateVal) {
							minRateVal = rate;
							minRateIdx = i;
						}
					}
					if (this == wq[minRateIdx].tztb) {
						return false; // do not wait
					}
				}
				return true; // wait
			};
			std::unique_lock<std::mutex> zipLock(zipMutex);
			if (myWorkMem > smallmem / 2) { // never wait for very smallmem(SST flush)
				sumWaitingMem += myWorkMem;
				while (shouldWait()) {
					//INFO(table_build_options_.info_log
					printf("TerarkChunkBuilder::Finish():this=%p: sumWaitingMem = %f GB, sumWorkingMem = %f GB, %s WorkingMem = %f GB, wait...\n"
						 , this, sumWaitingMem / 1e9, sumWorkingMem / 1e9, who, myWorkMem / 1e9
						 );
					zipCond.wait_for(zipLock, waitForTime);
				}
				//INFO(table_build_options_.info_log
				printf("TerarkChunkBuilder::Finish():this=%p: sumWaitingMem = %f GB, sumWorkingMem = %f GB, %s WorkingMem = %f GB, waited %8.3f sec, Key+Value bytes = %f GB\n"
					 , this, sumWaitingMem / 1e9, sumWorkingMem / 1e9, who, myWorkMem / 1e9
					 , g_pf.sf(myStartTime, now)
					 , (properties_.raw_key_size + properties_.raw_value_size) / 1e9
					 );
				sumWaitingMem -= myWorkMem;
			}
			sumWorkingMem += myWorkMem;
		};
		// indexing is also slow, run it in parallel
		tmpIndexFile_.fpath = work_path_ + ".index";
		async_build_index_ = 
			std::async(std::launch::async, [&]() {
					size_t fileOffset = 0;
					FileStream writer(tmpIndexFile_, "wb+");
					NativeDataInput<InputBuffer> tempKeyFileReader(&tmpKeyFile_.fp);
					for (size_t i = 0; i < histogram_.size(); ++i) {
						auto& keyStat = histogram_[i].stat;
						auto factory = TerarkIndex::SelectFactory(keyStat, table_build_options_.key_format,
																  table_build_options_.wt_session,
																  table_options_.indexType);
						if (!factory) {
							THROW_STD(invalid_argument,
									  "invalid indexType: %s", table_options_.indexType.c_str());
						}
						const size_t myWorkMem = factory->MemSizeForBuild(table_build_options_, keyStat);
						waitForMemory(myWorkMem, "nltTrie");
						BOOST_SCOPE_EXIT(myWorkMem) {
							std::unique_lock<std::mutex> zipLock(zipMutex);
							assert(sumWorkingMem >= myWorkMem);
							sumWorkingMem -= myWorkMem;
							zipCond.notify_all();
						} BOOST_SCOPE_EXIT_END;

						tms_[kBuildIndexStart] = g_pf.now();
						histogram_[i].keyFileBegin = fileOffset;
						factory->Build(tempKeyFileReader, table_options_, table_build_options_,
									   [&fileOffset, &writer](const void* data, size_t size) {
										   fileOffset += size;
										   writer.ensureWrite(data, size);
									   }, keyStat);
						histogram_[i].keyFileEnd = fileOffset;
						assert((fileOffset - histogram_[i].keyFileBegin) % 8 == 0);
						long long tt = g_pf.now();
						//INFO(table_build_options_.info_log
						printf("TerarkChunkBuilder::Finish():this=%p:  index pass time =%7.2f's, %8.3f'MB/sec\n"
							 , this, g_pf.sf(tms_[kBuildIndexStart], tt), 
							   properties_.raw_key_size*1.0 / g_pf.uf(tms_[kBuildIndexStart], tt)
							 );
					}
					tmpKeyFile_.close();
				});
		// dict memory usage
		size_t myDictMem = std::min<size_t>(sampleLenSum_, INT32_MAX) * 6;
		waitForMemory(myDictMem, "dictZip");
		BOOST_SCOPE_EXIT(&myDictMem) {
			std::unique_lock<std::mutex> zipLock(zipMutex);
			assert(sumWorkingMem >= myDictMem);
			// if success, myDictMem is 0, else sumWorkingMem should be restored
			sumWorkingMem -= myDictMem;
			myDictMem = 0;
			zipCond.notify_all();
		} BOOST_SCOPE_EXIT_END;

		wait_index_done_ = [&]() {
			//{
			//	std::unique_lock<std::mutex> zipLock(zipMutex);
			//	sumWorkingMem -= myDictMem;
			//}
			//myDictMem = 0; // success, set to 0
			async_build_index_.get();
			std::unique_lock<std::mutex> zipLock(zipMutex);
			waitQueue.trim(std::remove_if(waitQueue.begin(), waitQueue.end(),
										  [this](PendingTask x) {return this == x.tztb; }));
		};
		return GenerateSample();
	}


	Status TerarkChunkBuilder::GenerateSample() {
		DebugPrepare();
		assert(histogram_.size() == 1);
		tmpStoreFile_.fpath = work_path_ + ".zbs";
		auto& kvs = histogram_.front();
		{
			tms_[kSampleStart] = g_pf.now();
			{
				valvec<byte_t> sample;
				NativeDataInput<InputBuffer> input(&tmpSampleFile_.fp);
				size_t realsampleLenSum = 0;
				if (sampleLenSum_ < INT32_MAX) {
					for (size_t len = 0; len < sampleLenSum_; ) {
						input >> sample;
						zbuilder_->addSample(sample);
						len += sample.size();
					}
					realsampleLenSum = sampleLenSum_;
				} else {
					uint64_t upperBound2 = uint64_t(randomGenerator_.max() * double(INT32_MAX) / sampleLenSum_);
					for (size_t len = 0; len < sampleLenSum_; ) {
						input >> sample;
						if (randomGenerator_() < upperBound2) {
							zbuilder_->addSample(sample);
							realsampleLenSum += sample.size();
						}
						len += sample.size();
					}
				}
				tmpSampleFile_.close();
				if (0 == realsampleLenSum) { // prevent from empty
					zbuilder_->addSample("Hello World!");
				}
				zbuilder_->finishSample();
				zbuilder_->prepare(kvs.stat.numKeys, tmpStoreFile_);
			}
		}
		DebugCleanup();
		return Status::OK();
	}


	/*
	 *  Second Pass: Add all values again. Wait until index build task done & all values added.
	 *      Then write chunk file.
	 */
	Status TerarkChunkBuilder::Finish2ndPass() {
		if (valAdded2ndPass_ != histogram_.front().stat.numKeys) {
			printf("Finish2ndPass error: 2ndPass added keycnt:%d, 1stPass keycnt: %d", 
				   valAdded2ndPass_, histogram_.front().stat.numKeys);
			Abandon();
			return Status::OK();
		}
		terark::BlobStore::Dictionary dict;
		std::unique_ptr<terark::BlobStore> store;
		DictZipBlobStore::ZipStat dzstat;
		zbuilder_->finish(DictZipBlobStore::ZipBuilder::FinishFreeDict);
		dict = zbuilder_->getDictionary();
		terark::MmapWholeFile mfile(tmpStoreFile_.fpath);
		terark::BlobStore* bstore = terark::BlobStore::load_from_user_memory(mfile.memory(), dict);
		store.reset(bstore);
		dzstat = zbuilder_->getZipStat();
		tms_[kBlobStoreFinish] = g_pf.now();
		return WriteSSTFile(store.get(), dict, dzstat);
	}

	void TerarkChunkBuilder::BuildReorderMap(TerarkIndex* index, KeyValueStatus& kvs) {
		size_t keyCount = kvs.stat.numKeys;
        if (index->NeedsReorder()) {
			terark::UintVecMin0 newToOld(keyCount, keyCount - 1);
            index->GetOrderMap(newToOld);
			terark::ZReorderMap::Builder builder(keyCount, 1, tmpReorderFile_.fpath, "wb");
            for (size_t n = 0; n < keyCount; ++n) {
                builder.push_back(newToOld[n]);
            }
			builder.finish();
		}
	}

	Status TerarkChunkBuilder::WriteStore(TerarkIndex* index, terark::BlobStore* store,
		KeyValueStatus& kvs, std::function<void(const void*, size_t)> writeAppend,
		TerarkBlockHandle& dataBlock) {
		auto& keyStat = kvs.stat;
		tmpReorderFile_.fpath = work_path_ + ".reorder";
		BuildReorderMap(index, kvs);
		if (index->NeedsReorder()) {
			tms_[kReorderStart] = tms_[kBZTypeBuildStart] = g_pf.now();
			terark::ZReorderMap reorder(tmpReorderFile_.fpath);
			try {
				dataBlock.set_offset(offset_);
				store->reorder_zip_data(reorder, std::ref(writeAppend));
				dataBlock.set_size(offset_ - dataBlock.offset());
			} catch (const Status& s) {
				return s;
			}
		} else {
			tms_[kReorderStart] = tms_[kBZTypeBuildStart] = tms_[kGetOrderMapStart];
			try {
				dataBlock.set_offset(offset_);
				store->save_mmap(std::ref(writeAppend));
				dataBlock.set_size(offset_ - dataBlock.offset());
			} catch (const Status& s) {
				return s;
			}
		}
		return Status::OK();
	}

	Status TerarkChunkBuilder::WriteSSTFile(terark::BlobStore* zstore,
		terark::BlobStore::Dictionary dict,
		const DictZipBlobStore::ZipStat& dzstat) {
		assert(histogram_.size() == 1);
		auto& kvs = histogram_.front();
		auto& keyStat = kvs.stat;
		const size_t realsampleLenSum = dict.memory.size();
		long long rawBytes = properties_.raw_key_size + properties_.raw_value_size;
		{
			wait_index_done_();
		}
		tms_[kGetOrderMapStart] = g_pf.now();
		TerarkTableReaderOptions ropt(table_build_options_.internal_comparator);
		std::unique_ptr<TerarkIndex> index(TerarkIndex::LoadFile(tmpIndexFile_, ropt));
		printf("index->keys: %d, keyStat->keys: %d\n", index->NumKeys(), keyStat.numKeys);
		assert(index->NumKeys() == keyStat.numKeys);
		Status s;
		TerarkBlockHandle dataBlock, dictBlock, indexBlock, zvTypeBlock(0, 0);
		TerarkBlockHandle commonPrefixBlock;
		offset_ = 0;
		auto writeAppend = [&](const void* data, size_t size) {
			file_writer_.writer.ensureWrite(data, size);
			offset_ += size;
		};
		s = WriteStore(index.get(), zstore, kvs, writeAppend, dataBlock);
		if (!s.ok()) {
			return s;
		}

		std::string commonPrefix;
		commonPrefix.reserve(keyStat.commonPrefixLen);
		//commonPrefix.append(kvs.prefix.data(), kvs.prefix.size());
		commonPrefix.append((const char*)prevUserKey_.data(), keyStat.commonPrefixLen);
		WriteBlock(commonPrefix, file_writer_, &offset_, &commonPrefixBlock);
		properties_.data_size = dataBlock.size();
		printf("commonPrefix: %s\n", commonPrefix.c_str());

		s = WriteBlock(dict.memory, file_writer_, &offset_, &dictBlock);
		if (!s.ok()) {
			return s;
		}

		s = WriteBlock(index->Memory(), file_writer_, &offset_, &indexBlock);
		if (!s.ok()) {
			return s;
		}

		index.reset();
		properties_.index_size = indexBlock.size();

		WriteMetaData({
				{ dict.memory.size() ? &kTerarkZipTableValueDictBlock : NULL   , dictBlock         },
				{ &kTerarkZipTableIndexBlock                                   , indexBlock        },
				{ &kTerarkZipTableCommonPrefixBlock                            , commonPrefixBlock },
			});

		tms_[kBuildFinish] = g_pf.now();
		{
			std::unique_lock<std::mutex> lock(g_sumMutex);
			g_sumKeyLen += properties_.raw_key_size;
			g_sumValueLen += properties_.raw_value_size;
			g_sumUserKeyLen += keyStat.sumKeyLen;
			g_sumUserKeyNum += keyStat.numKeys;
			g_sumEntryNum += properties_.num_entries;
		}
		//INFO(table_build_options_.info_log,
		printf(
			 "TerarkChunkBuilder::Finish():this=%p: second pass time =%7.2f's, %8.3f'MB/sec, value only(%4.1f%% of KV)\n"
			 "   wait indexing time = %7.2f's,\n"
			 "  remap KeyValue time = %7.2f's, %8.3f'MB/sec (all stages of remap)\n"
			 "    Get OrderMap time = %7.2f's, %8.3f'MB/sec (index lex order gen)\n"
			 "  write SST data time = %7.2f's, %8.3f'MB/sec\n"
			 "    z-dict build time = %7.2f's, sample length = %7.3f'MB, throughput = %6.3f'MB/sec\n"
			 "    zip my value time = %7.2f's, unzip  length = %7.3f'GB\n"
			 "    zip my value throughput = %7.3f'MB/sec\n"
			 "    zip pipeline throughput = %7.3f'MB/sec\n"
			 "    entries = %zd  keys = %zd  avg-key = %.2f  avg-zkey = %.2f  avg-val = %.2f  avg-zval = %.2f\n"
			 "    UnZipSize{ index =%9.4f GB  value =%9.4f GB   all =%9.4f GB }\n"
			 "    __ZipSize{ index =%9.4f GB  value =%9.4f GB   all =%9.4f GB }\n"
			 "    UnZip/Zip{ index =%9.4f     value =%9.4f      all =%9.4f    }\n"
			 "    Zip/UnZip{ index =%9.4f     value =%9.4f      all =%9.4f    }\n"
			 "----------------------------\n"
			 "    total value len =%12.6f GB     avg =%8.3f KB (by entry num)\n"
			 "    total  key  len =%12.6f GB     avg =%8.3f KB\n"
			 "    total ukey  len =%12.6f GB     avg =%8.3f KB\n"
			 "    total ukey  num =%15.9f Billion\n"
			 "    total entry num =%15.9f Billion\n"
			 "    write speed all =%15.9f MB/sec (with    version num)\n"
			 "    write speed all =%15.9f MB/sec (without version num)"
			 , this, g_pf.sf(tms_[kSampleStart], tms_[kBlobStoreFinish])
			 , properties_.raw_value_size*1.0 / g_pf.uf(tms_[kSampleStart], tms_[kBlobStoreFinish])
			 , properties_.raw_value_size*100.0 / rawBytes

			 , g_pf.sf(tms_[kBlobStoreFinish], tms_[kGetOrderMapStart]) // wait indexing time
			 , g_pf.sf(tms_[kGetOrderMapStart], tms_[kBuildFinish]), double(offset_) / g_pf.uf(tms_[kGetOrderMapStart], tms_[kBuildFinish])

			 , g_pf.sf(tms_[kGetOrderMapStart], tms_[kBZTypeBuildStart]), properties_.index_size / g_pf.uf(tms_[kGetOrderMapStart], tms_[kBZTypeBuildStart]) // index lex walk

			 , g_pf.sf(tms_[kReorderStart], tms_[kBuildFinish]), double(offset_) / g_pf.uf(tms_[kReorderStart], tms_[kBuildFinish]) // write SST data

			 , dzstat.dictBuildTime, realsampleLenSum / 1e6
			 , realsampleLenSum / dzstat.dictBuildTime / 1e6

			 , dzstat.dictZipTime, properties_.raw_value_size / 1e9
			 , properties_.raw_value_size / dzstat.dictZipTime / 1e6
			 , dzstat.pipelineThroughBytes / dzstat.dictZipTime / 1e6

			 , size_t(properties_.num_entries), keyStat.numKeys
			 , double(keyStat.sumKeyLen) / keyStat.numKeys
			 , double(properties_.index_size) / keyStat.numKeys
			 , double(properties_.raw_value_size) / keyStat.numKeys
			 , double(properties_.data_size) / keyStat.numKeys

			 , keyStat.sumKeyLen / 1e9, properties_.raw_value_size / 1e9, rawBytes / 1e9

			 , properties_.index_size / 1e9, properties_.data_size / 1e9, offset_ / 1e9

			 , double(keyStat.sumKeyLen) / properties_.index_size
			 , double(properties_.raw_value_size) / properties_.data_size
			 , double(rawBytes) / offset_

			 , properties_.index_size / double(keyStat.sumKeyLen)
			 , properties_.data_size / double(properties_.raw_value_size)
			 , offset_ / double(rawBytes)

			 , g_sumValueLen / 1e9, g_sumValueLen / 1e3 / g_sumEntryNum
			 , g_sumKeyLen / 1e9, g_sumKeyLen / 1e3 / g_sumEntryNum
			 , g_sumUserKeyLen / 1e9, g_sumUserKeyLen / 1e3 / g_sumUserKeyNum
			 , g_sumUserKeyNum / 1e9
			 , g_sumEntryNum / 1e9
			 , (g_sumKeyLen + g_sumValueLen) / g_pf.uf(g_lastTime, tms_[kBuildFinish])
			 , (g_sumKeyLen + g_sumValueLen - g_sumEntryNum * 8) / g_pf.uf(g_lastTime, tms_[kBuildFinish])
			 );

		tmpIndexFile_.Delete();
		tmpReorderFile_.Delete();
		tmpStoreFile_.Delete();
		file_writer_.close();
		return s;
	}

	Status TerarkChunkBuilder::WriteMetaData(std::initializer_list<std::pair<const std::string*, TerarkBlockHandle> > blocks) {
		TerarkMetaIndexBuilder metaindexBuiler;
		for (const auto& block : blocks) {
			if (block.first) {
				metaindexBuiler.Add(*block.first, block.second);
			}
		}
		{
			TerarkPropertyBlockBuilder propBlockBuilder;
			propBlockBuilder.AddTableProperty(properties_);
			TerarkBlockHandle propBlock;
			Status s = WriteBlock(propBlockBuilder.Finish(), file_writer_, &offset_, &propBlock);
			if (!s.ok()) {
				return s;
			}
			metaindexBuiler.Add(kTerarkPropertiesBlock, propBlock);
		}
		TerarkBlockHandle metaindexBlock;
		Status s = WriteBlock(metaindexBuiler.Finish(), file_writer_, &offset_, &metaindexBlock);
		if (!s.ok()) {
			return s;
		}
		TerarkFooter footer(kTerarkZipTableMagicNumber, 0);
		footer.set_metaindex_handle(metaindexBlock);
		footer.set_index_handle(TerarkBlockHandle::NullBlockHandle());
		std::string footer_encoding;
		footer.EncodeTo(&footer_encoding);
		file_writer_.writer.ensureWrite(footer_encoding.c_str(), footer_encoding.size());
		offset_ += footer_encoding.size();
		return s;
	}


	void TerarkChunkBuilder::Abandon() {
		closed_ = true;
		wait_index_done_();
		zbuilder_->finish(DictZipBlobStore::ZipBuilder::FinishFreeDict);
		zbuilder_.reset();
		tmpIndexFile_.Delete();
		tmpStoreFile_.Delete();
		file_writer_.close();
	}

	void TerarkChunkBuilder::DebugPrepare() {}

	void TerarkChunkBuilder::DebugCleanup() {
		if (tmpDumpFile_.isOpen()) {
			tmpDumpFile_.close();
		}
	}
}
