
#include <stdio.h>
#include <iostream>
#include <memory>
#include "bridge.h"

#include "wiredtiger.h"
#include "wiredtiger_ext.h"

//#include "file_reader_writer.h"
#include "rocksdb/env.h"

#include "terark_zip_internal.h"
#include "terark_chunk_manager.h"
#include "terark_zip_table.h"
#include "terark_chunk_builder.h"
#include "terark_chunk_reader.h"


static const char *home;
static WT_CONNECTION *conn;
static rocksdb::TerarkChunkManager* chunk_manager;

// TBD(kg): parse config as well
int trk_create(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
			   const char *uri, WT_CONFIG_ARG *config) {
	(void)dsrc;
	(void)session;
	(void)config;

	// TBD(kg): make sure such file is not exist, remove it anyway
	rocksdb::Options options;
	const rocksdb::Comparator* comparator = rocksdb::BytewiseComparator();
	rocksdb::TerarkTableBuilderOptions builder_options(*comparator);

	std::unique_ptr<rocksdb::WritableFile> file;
	// TBD(kg): need more settings on env
	std::string path(std::string(home) + "/" + uri);
	rocksdb::TerarkChunkBuilder* builder = chunk_manager->NewTableBuilder(builder_options, path);
	chunk_manager->AddBuilder(uri, builder);

	WT_EXTENSION_API *wt_api = conn->get_extension_api(conn);
	const char* value = "key_format=S,value_format=S,app_metadata=";
	int ret = wt_api->metadata_insert(wt_api, session, uri, value);
	assert(ret == 0);

	return (0);
}

// can we open multi times on one data-source ? how many diff cursors can we get ?
int trk_open_cursor(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
					const char *uri, WT_CONFIG_ARG *config, WT_CURSOR **new_cursor) {
	(void)dsrc;
	(void)session;
	(void)config;

	// Allocate and initialize a WiredTiger cursor.
	WT_CURSOR *cursor;
	if ((cursor = (WT_CURSOR*)calloc(1, sizeof(*cursor))) == NULL)
		return (errno);

	/*
	 * Configure local cursor information.
	 */
	cursor->key_format = "S";
	cursor->value_format = "S";
	// TBD(kg): make sure it's safe
	cursor->uri = uri;

	// set cursor-ops based on builder/reader
	if (chunk_manager->IsChunkExist(uri)) {
		cursor->next = trk_cursor_next;
		cursor->prev = trk_cursor_prev;
		cursor->reset = trk_cursor_reset;
		cursor->search = trk_cursor_search;
		cursor->search_near = trk_cursor_search_near;
		cursor->get_key = trk_get_key;
		cursor->get_value = trk_get_value;
		cursor->close = trk_reader_cursor_close;
		// read iterator
		rocksdb::Iterator* iter = chunk_manager->NewIterator(uri);
		chunk_manager->AddIterator(cursor, iter);
	} else {
		cursor->get_key = trk_get_key;
		cursor->get_value = trk_get_value;
		cursor->set_key = trk_set_key;
		cursor->set_value = trk_set_value;
		cursor->insert = trk_cursor_insert;
		cursor->close = trk_builder_cursor_close;
	}

	/* Return combined cursor to WiredTiger. */
	*new_cursor = (WT_CURSOR *)cursor;

	return (0);
}


int trk_pre_merge(WT_DATA_SOURCE *dsrc, WT_CURSOR *cursor, WT_CURSOR *dest) {
	(void)dsrc;
	(void)cursor;
	(void)dest;
	
	printf("trk_pre_merge: %s\n", dest->uri);

	std::string fname(dest->uri);
	rocksdb::TerarkChunkBuilder* builder = chunk_manager->GetBuilder(fname);

	int ret = 0;
	WT_ITEM key, value;
	while ((ret = cursor->next(cursor)) == 0) {
		ret = cursor->get_key(cursor, &key);
		ret = cursor->get_value(cursor, &value);
		builder->Add(rocksdb::Slice((const char*)key.data, key.size), 
				   rocksdb::Slice((const char*)value.data, value.size));
	}
	builder->Finish1stPass();

	return (0);
}


/*!
 * TBD(kg): key/value related ops use WT_ITEM currently
 */
int trk_get_key(WT_CURSOR *cursor, ...) {
	va_list ap;
	va_start(ap, cursor);
	WT_ITEM* key = va_arg(ap, WT_ITEM *);
	key->data = cursor->key.data;
	key->size = cursor->key.size;
	va_end(ap);

	return (0);
}

int trk_get_value(WT_CURSOR *cursor, ...) {
	va_list ap;
	va_start(ap, cursor);
	WT_ITEM* value = va_arg(ap, WT_ITEM *);
	value->data = cursor->value.data;
	value->size = cursor->value.size;
	va_end(ap);

	return (0);
}

void trk_set_key(WT_CURSOR *cursor, ...) {
	WT_ITEM* buf = &cursor->key;
	va_list ap;
	va_start(ap, cursor);
	WT_ITEM* item = va_arg(ap, WT_ITEM *);
	buf->size = item->size;
	buf->data = item->data;
	va_end(ap);
}

void trk_set_value(WT_CURSOR *cursor, ...) {
	WT_ITEM* buf = &cursor->key;
	va_list ap;
	va_start(ap, cursor);
	WT_ITEM* item = va_arg(ap, WT_ITEM *);
	buf->size = item->size;
	buf->data = item->data;
	va_end(ap);
}

int trk_cursor_insert(WT_CURSOR *cursor) {
	rocksdb::TerarkChunkBuilder* builder = chunk_manager->GetBuilder(cursor->uri);
	WT_ITEM value;
	int ret = cursor->get_value(cursor, &value);
	assert(ret == 0);
	builder->Add(rocksdb::Slice((const char*)value.data, value.size));

	return (0);
}


int trk_builder_cursor_close(WT_CURSOR *cursor) {
	(void)cursor;
	
	printf("trk_cursor_close\n");
	rocksdb::TerarkChunkBuilder* builder = chunk_manager->GetBuilder(cursor->uri);
	builder->Finish2ndPass();

	// TBD(kg): remove/delete builder from manager
	return (0);
}

int trk_reader_cursor_close(WT_CURSOR *cursor) {
	(void)cursor;
	// TBD(kg): remove/delete builder from manager

	return (0);
}

static inline void set_kv(rocksdb::Iterator* iter, WT_CURSOR* cursor) {
	{
		WT_ITEM* buf = &cursor->key;
		rocksdb::Slice key = iter->key();
		buf->size = key.size();
		buf->data = key.data();
	}
	{
		WT_ITEM* buf = &cursor->value;
		rocksdb::Slice value = iter->value();
		buf->size = value.size();
		buf->data = value.data();
	}
}

// only reader will use the following cursor-ops
int trk_cursor_next(WT_CURSOR *cursor) {
	rocksdb::Iterator* iter = chunk_manager->GetIterator(cursor);
	iter->Next();
	if (!iter->Valid()) {
		return -1;
	}
	set_kv(iter, cursor);
	return (0);
}

int trk_cursor_prev(WT_CURSOR *cursor) {
	rocksdb::Iterator* iter = chunk_manager->GetIterator(cursor);
	iter->Prev();
	if (!iter->Valid()) {
		return -1;
	}
	set_kv(iter, cursor);
	return (0);
}

// TBD(kg): any resources held by the cursor are released
int trk_cursor_reset(WT_CURSOR *cursor) {
	(void)cursor;
	return (0);
}

int trk_cursor_search(WT_CURSOR *cursor) {
	rocksdb::Iterator* iter = chunk_manager->GetIterator(cursor);
	iter->Seek(rocksdb::Slice((const char*)cursor->key.data, cursor->key.size));
	if (!iter->Valid()) {
		return WT_NOTFOUND;
	}
	rocksdb::Slice key = iter->key();
	WT_ITEM* buf = &cursor->key;
	if (key.size() != buf->size ||
		memcmp(key.data(), buf->data, key.size())) {
		return WT_NOTFOUND;
	}
	return (0);
}

int trk_cursor_search_near(WT_CURSOR *cursor, int *exactp) {
	(void)cursor;
	(void)exactp;

	return (0);
}


int main() {
	WT_SESSION *session;
	int ret;

    if (getenv("WIREDTIGER_HOME") == NULL) {
		home = "WT_HOME";
		ret = system("rm -rf WT_HOME && mkdir WT_HOME");
		ret = system("rm -rf temp && mkdir temp");
    } else
		home = NULL;

	chunk_manager = rocksdb::TerarkChunkManager::sharedInstance();
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
	
	ret = conn->add_data_source(conn, "terark", &trk_dsrc, NULL);

	ret = conn->configure_method(conn,
								 "WT_SESSION.open_cursor", NULL, "collator=", "string", NULL);

	// TBD(kg): make sure start_generation is set as 1
	// just set it in config_def right now.
	//ret = conn->configure_method(conn,
	//  "WT_SESSION.create", NULL, "start_generation=1", "int", NULL);
	
	/*! [WT_DATA_SOURCE register] */

	{
		WT_CURSOR *c;
		//session->create(session, "table:bucket", "type=lsm,key_format=S,value_format=S,merge_custom=(prefix=terark,start_generation=2)");
		// TBD(kg): should we set raw == true ?
		session->create(session, "table:bucket", 
						"type=lsm,lsm=(merge_min=2,merge_custom=(prefix=terark,start_generation=2),chunk_size=10MB),"
						"key_format=S,value_format=S");

		/*session->create(session, "table:bucket", 
						"type=lsm,lsm=(merge_min=2,merge_custom=(prefix=file,suffix=.terark,start_generation=2)),"
						"key_format=S,value_format=S");
		*/
		session->open_cursor(session, "table:bucket", NULL, NULL, &c);
		for (int i = 0; i < 10 * 1000 * 1000; i++) {
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
			c->reset(c);
			const char *key, *value;
			for (int i = 0; i < 10 * 1000 * 1000; i++) {
				ret = c->next(c);
				assert(ret == 0);
				c->get_key(c, &key);
				c->get_value(c, &value);
				// prepare
				char ckey[20] = { 0 };
				char cvalue[20] = { 0 };
				snprintf(ckey, 20, "key%05d", i);
				snprintf(cvalue, 40, "value%010d", i);
				// cmp
				if (i >= 9999) {
					assert(memcmp(key, ckey, strlen(key)) == 0);
					assert(memcmp(value, cvalue, strlen(value)) == 0);
					printf("cmp key %s ok\n", key);
				}
			}
		}

		c->close(c);
	}

	ret = conn->close(conn, NULL);

	return (ret == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
}
