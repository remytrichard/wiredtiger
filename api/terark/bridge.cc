
#include <stdio.h>
#include <iostream>
#include "bridge.h"

#include "wiredtiger.h"
#include "wiredtiger_ext.h"

#include "terark_zip_internal.h"
#include "terark_zip_table.h"

/*#if defined(__cplusplus)
extern "C" {
	#include "wt_internal.h"
	#include "extern.h"
}
#endif
*/


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

	unique_ptr<WritableFile> file;
	// TBD(kg): need more settings on env
	rocksdb::EnvOptions env_options;
	env_options.use_mmap_reads = env_options.use_mmap_writes = true;
	rocksdb::Status s = options.env->NewWritableFile(*uri, &file, env_options);
	assert(s.ok());
	//file->SetPreallocationBlockSize(immutable_db_options_.manifest_preallocation_size);
	unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(std::move(file), env_options));
		
	manager->NewTableBuilder(builder_options, 0, file_writer.get());
	return (0);
}

int trk_open_cursor(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
					const char *uri, WT_CONFIG_ARG *config, WT_CURSOR **new_cursor) {
	(void)dsrc;
	(void)session;
	(void)uri;
	(void)config;
	(void)new_cursor;

	return (0);
}

int trk_pre_merge(WT_DATA_SOURCE *dsrc, WT_CURSOR *source, WT_CURSOR *dest) {
	(void)dsrc;
	(void)source;
	(void)dest;

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
