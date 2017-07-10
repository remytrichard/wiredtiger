
#include <stdio.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <map>
#include <random>
#include <string>
#include "bridge.h"

#include "wiredtiger.h"
#include "wiredtiger_ext.h"

//#include "file_reader_writer.h"
// rocksdb headers
#include "rocksdb/slice.h"
//#include "rocksdb/table.h"
#include "rocksdb/env.h"
// project headers
#include "terark_zip_internal.h"
#include "terark_zip_config.h"
#include "terark_chunk_manager.h"
#include "terark_chunk_builder.h"
#include "terark_chunk_reader.h"

static const char *home;
static const char* sst_path = "./data/0001.sst";
static const char* sample_path = "./samples.txt";
std::map<std::string, std::string> dict;

void InitDict() {
	std::ifstream fi(sample_path);
	while (true) {
		std::string key, val;
		if (!std::getline(fi, key)) break;
		if (!std::getline(fi, val)) break;
		key = key.substr(5);
		val = val.substr(5);
		dict[key] = val;
	}
}

int main() {
	InitDict();

	std::string test = "123";
	std::string fname(sst_path);

	rocksdb::TerarkChunkManager* manager = rocksdb::TerarkChunkManager::sharedInstance();
	rocksdb::Options options;
	rocksdb::EnvOptions env_options;
	env_options.use_mmap_reads = env_options.use_mmap_writes = true;
	{
		const rocksdb::Comparator* comparator = rocksdb::BytewiseComparator();
		rocksdb::TerarkTableBuilderOptions builder_options(*comparator);
		
		rocksdb::TerarkChunkBuilder* builder = 
			manager->NewTableBuilder(builder_options, fname);
		manager->AddBuilder(fname, builder);
		
		// 1st pass
		for (auto& iter : dict) {
			builder->Add(iter.first, iter.second);
		}
		rocksdb::Status s = builder->Finish1stPass();
		assert(s.ok());
		// 2nd pass
		for (auto& iter : dict) {
			builder->Add(iter.second);
		}
		s = builder->Finish2ndPass();
		assert(s.ok());
	}
	{		
		rocksdb::Iterator* iter = manager->NewIterator(fname);
		assert(iter != nullptr);

		for (auto& di : dict) {
			iter->Seek(di.first);
			if (!iter->Valid()) {
				printf("Seek failed on key %s\n", di.first.c_str());
				return 1;
			}
			std::string key(iter->key().data(), iter->key().size());
			std::string val(iter->value().data(), iter->value().size());
			if (di.first != key) {
				printf("key expected:%s actual:%s\n", di.first.c_str(), key.c_str());
				return 1;
			}
			if (di.second != val) {
				printf("val expected %s actual: %s\n", di.second.c_str(), val.c_str());
				return 1;
			}
		}
		std::cout << "\n\nTest Case Passed!\n\n";
	}

	return 0;
}
