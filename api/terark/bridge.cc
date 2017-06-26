
#include <stdio.h>
#include <iostream>
#include <memory>
#include "bridge.h"

#include "wiredtiger.h"
#include "wiredtiger_ext.h"

#include "file_reader_writer.h"
#include "rocksdb/env.h"

#include "terark_zip_internal.h"
#include "terark_zip_table.h"
#include "terark_zip_table_builder.h"


// TBD(kg): parse config as well
int trk_create(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
			   const char *uri, WT_CONFIG_ARG *config) {
	(void)dsrc;
	(void)session;
	(void)uri;
	(void)config;

	rocksdb::TerarkChunkManager* manager = rocksdb::TerarkChunkManager::sharedInstance();
	rocksdb::Options options;
	const rocksdb::Comparator* comparator = rocksdb::BytewiseComparator();
	rocksdb::TerarkTableBuilderOptions builder_options(*comparator);

	std::unique_ptr<rocksdb::WritableFile> file;
	// TBD(kg): need more settings on env
	rocksdb::EnvOptions env_options;
	env_options.use_mmap_reads = env_options.use_mmap_writes = true;
	std::string fname(uri);
	rocksdb::Status s = options.env->NewWritableFile(fname, &file, env_options);
	assert(s.ok());
	//file->SetPreallocationBlockSize(immutable_db_options_.manifest_preallocation_size);
	std::unique_ptr<rocksdb::WritableFileWriter> 
		file_writer(new rocksdb::WritableFileWriter(std::move(file), env_options));
	// TBD(kg): file_writer.get()...	
	rocksdb::TerarkChunk* chunk = manager->NewTableBuilder(builder_options, 0, file_writer.get());
	manager->AddChunk(uri, chunk);

	return (0);
}

// can we open multi times on one data-source ? how many diff cursors can we get ?
int trk_open_cursor(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
					const char *uri, WT_CONFIG_ARG *config, WT_CURSOR **new_cursor) {
	(void)dsrc;
	(void)session;
	(void)uri;
	(void)config;
	(void)new_cursor;

	// 

	return (0);
}

int trk_pre_merge(WT_DATA_SOURCE *dsrc, WT_CURSOR *src_cursor, WT_CURSOR *dest) {
	(void)dsrc;
	(void)src_cursor;
	(void)dest;

	std::string fname(dest->uri);
	rocksdb::TerarkChunkManager* manager = rocksdb::TerarkChunkManager::sharedInstance();
	rocksdb::TerarkChunk* chunk = manager->GetChunk(fname);
	if (chunk->GetState() != rocksdb::TerarkChunk::kJustCreated) {
		printf("trk_pre_merge: Invalid State\n");
		return -1;
	}
	
	chunk->SetState(rocksdb::TerarkChunk::kFirstPass);
	int ret = 0;
	const char *key, *value;
	while ((ret = src_cursor->next(src_cursor)) == 0) {
		ret = src_cursor->get_key(src_cursor, &key);
		ret = src_cursor->get_value(src_cursor, &value);
		chunk->Add(key, value, &src_cursor->value);
	}
	chunk->SetState(rocksdb::TerarkChunk::kSecondPass);

	return (0);
}






int trk_cursor_next(WT_CURSOR *cursor) {
	(void)cursor;
	return (0);
}

int trk_cursor_prev(WT_CURSOR *cursor) {
	(void)cursor;
	return (0);
}

int trk_cursor_reset(WT_CURSOR *cursor) {
	(void)cursor;
	return (0);
}

int trk_cursor_search(WT_CURSOR *cursor) {
	(void)cursor;
	return (0);
}

int tr_cursor_search_near(WT_CURSOR *cursor, int *exactp) {
	(void)cursor;
	(void)exactp;

	return (0);
}

int trk_cursor_insert(WT_CURSOR *cursor) {
	(void)cursor;
	return (0);
}



static const char *home;
int main() {
	WT_CONNECTION *conn;
	WT_SESSION *session;
	int ret;

    if (getenv("WIREDTIGER_HOME") == NULL) {
		home = "WT_HOME";
		ret = system("rm -rf WT_HOME && mkdir WT_HOME");
    } else
		home = NULL;

	ret = wiredtiger_open(home, NULL, "create", &conn);
	ret = conn->open_session(conn, NULL, NULL, &session);

	//my_data_source_init(conn);

	/*! [WT_DATA_SOURCE register] */
	//WT_DATA_SOURCE trk_dsrc;
	
	//trk_dsrc.open_cursor = trk_open_cursor;
	static WT_DATA_SOURCE trk_dsrc = {
		NULL, //__wt_lsm_tree_alter, //my_alter,
		trk_create,
		NULL, //__wt_lsm_compact, //NULL, //my_compact,
		NULL, //__wt_lsm_tree_drop, //NULL, //my_drop,
		trk_open_cursor,
		NULL, //__wt_lsm_tree_rename, //NULL, //my_rename,
		NULL, //my_salvage,
		NULL, //__wt_lsm_tree_truncate, //NULL, //my_truncate,
		NULL, //my_range_truncate,
		NULL, //my_verify,
		NULL, //my_checkpoint,
		NULL,  //my_terminate
		trk_pre_merge
	};
	
	ret = conn->add_data_source(conn, "trk_sst:", &trk_dsrc, NULL);
	// TBD(kg): make sure start_generation is set as 1
	// just set it in config_def right now.
	//ret = conn->configure_method(conn,
	//  "WT_SESSION.create", NULL, "start_generation=1", "int", NULL);
	
	/*! [WT_DATA_SOURCE register] */

	{
		WT_CURSOR *c;
		session->create(session, "table:bucket", "type=lsm,key_format=S,value_format=S");
		session->open_cursor(session, "table:bucket", NULL, NULL, &c);
		for (int i = 0; i < 300000; i++) {
			char key[20] = { 0 };
			char value[40] = { 0 };
			snprintf(key, 20, "key%05d", i);
			snprintf(value, 40, "value%010d", i);
			c->set_key(c, key);
			c->set_value(c, value);
			c->insert(c);
		}
		{
			// cursor read ...
		}

		c->close(c);
	}

	ret = conn->close(conn, NULL);

	return (ret == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
}
