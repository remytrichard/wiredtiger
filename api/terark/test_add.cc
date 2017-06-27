
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
int main() {
	rocksdb::TerarkChunkManager* manager = rocksdb::TerarkChunkManager::sharedInstance();
	
	return 0;
}
