
#include <stdio.h>
#include <iostream>
#include <memory>
#include "bridge.h"

#include "wiredtiger.h"
#include "wiredtiger_ext.h"

#include "file_reader_writer.h"
// rocksdb headers
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/env.h"
// project headers
#include "terark_zip_internal.h"
#include "terark_chunk_manager.h"
#include "terark_zip_table.h"
#include "terark_zip_table_builder.h"


static const char *home;
static const char* sst_path = "./data/0001.sst";
int main() {
	rocksdb::TerarkChunkManager* manager = rocksdb::TerarkChunkManager::sharedInstance();
	rocksdb::Options options;
	const rocksdb::Comparator* comparator = rocksdb::BytewiseComparator();
	rocksdb::TerarkTableBuilderOptions builder_options(*comparator);

	std::unique_ptr<rocksdb::WritableFile> file;
	rocksdb::EnvOptions env_options;
	env_options.use_mmap_reads = env_options.use_mmap_writes = true;
	std::string fname(sst_path);
	rocksdb::Status s = options.env->NewWritableFile(fname, &file, env_options);
	assert(s.ok());

	std::unique_ptr<rocksdb::WritableFileWriter> 
		file_writer(new rocksdb::WritableFileWriter(std::move(file), env_options));
	rocksdb::TerarkZipTableBuilder* chunk = manager->NewTableBuilder(builder_options, 0, file_writer.get());

	for (int i = 0; i < 100; i++) {
		char key[50] = { 0 };
		char val[50] = { 0 };
		sprintf(key, "key%04d", i);
		sprintf(val, "value%04d", i);
		chunk->Add(key, val);
	}
	s = chunk->Finish();
	assert(s.ok());

	return 0;
}
