
#ifdef _MSC_VER
# include <io.h>
#else
# include <sys/types.h>
# include <sys/stat.h>
# include <fcntl.h>
# include <cxxabi.h>
#endif
#include <stdio.h>
#include <fstream>
#include <iostream>
#include <memory>

#include "wiredtiger.h"
#include "wiredtiger_ext.h"

// project header
#include "bridge.h"
#include "terark_zip_internal.h"
#include "terark_chunk_manager.h"
#include "terark_zip_config.h"
#include "terark_chunk_builder.h"
#include "terark_chunk_reader.h"


static const char *home;
static WT_CONNECTION *conn;
static terark::TerarkChunkManager* chunk_manager;

// TBD(kg): parse config as well
int trk_create(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
			   const char *uri, WT_CONFIG_ARG *config) {
	(void)dsrc;
	(void)session;
	(void)config;

	// TBD(kg): make sure such file is not exist, remove it anyway
	const terark::TComparator* comparator = terark::GetBytewiseComparator();
	terark::TerarkTableBuilderOptions builder_options(*comparator);

	// TBD(kg): need more settings on env
	std::string path(std::string(home) + "/" + uri);
	std::replace(path.begin(), path.end(), ':', '-');
	terark::TerarkChunkBuilder* builder = chunk_manager->NewTableBuilder(builder_options, path);
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
	std::string path(std::string(home) + "/" + uri);
	std::replace(path.begin(), path.end(), ':', '-');
	if (chunk_manager->IsChunkExist(path)) {
		printf("open cursor for read: %s\n", uri);
		cursor->next = trk_cursor_next;
		cursor->prev = trk_cursor_prev;
		cursor->reset = trk_reader_cursor_reset;
		cursor->search = trk_cursor_search;
		cursor->search_near = trk_cursor_search_near;
		cursor->close = trk_reader_cursor_close;
		// read iterator
		terark::TerarkIterator* iter = chunk_manager->NewIterator(path);
		chunk_manager->AddIterator(cursor, iter);
	} else {
		printf("open cursor for build: %s\n", uri);
		cursor->reset = trk_builder_cursor_reset;
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
	if (cursor->uri) {
		printf("src is %s\n", cursor->uri);
	}
	terark::TerarkChunkBuilder* builder = chunk_manager->GetBuilder(dest->uri);

	int ret = 0;
	WT_ITEM key, value;
	while ((ret = cursor->next(cursor)) == 0) {
		ret = cursor->get_key(cursor, &key);
		ret = cursor->get_value(cursor, &value);
		builder->Add(terark::Slice((const char*)key.data, key.size), 
				   terark::Slice((const char*)value.data, value.size));
	}
	builder->Finish1stPass();
	printf("trk_pre_merge done\n");

	return (0);
}

int trk_drop(WT_DATA_SOURCE *dsrc, WT_SESSION *session, const char *uri, WT_CONFIG_ARG *config) {
	printf("enter drop: %s\n", uri);
	
	std::string path(std::string(home) + "/" + uri);
	std::replace(path.begin(), path.end(), ':', '-');
	::remove(path.c_str());
	return (0);
}


int trk_cursor_insert(WT_CURSOR *cursor) {
	terark::TerarkChunkBuilder* builder = chunk_manager->GetBuilder(cursor->uri);
	builder->Add(terark::Slice((const char*)cursor->value.data, cursor->value.size));

	return (0);
}


int trk_builder_cursor_close(WT_CURSOR *cursor) {
	printf("builder close entered: %s\n", cursor->uri);
	terark::TerarkChunkBuilder* builder = chunk_manager->GetBuilder(cursor->uri);
	builder->Finish2ndPass();

	// TBD(kg): remove/delete builder from manager
	chunk_manager->RemoveBuilder(cursor->uri);
	delete builder;
	return (0);
}

int trk_reader_cursor_close(WT_CURSOR *cursor) {
	// TBD(kg): remove/delete builder from manager
	printf("reader close entered: %s\n", cursor->uri);
	terark::TerarkIterator* iter = chunk_manager->GetIterator(cursor);
	chunk_manager->RemoveIterator(cursor);
	delete iter;
	return (0);
}

static inline void set_kv(terark::TerarkIterator* iter, WT_CURSOR* cursor) {
	{
		WT_ITEM* buf = &cursor->key;
		terark::Slice key = iter->key();
		buf->size = key.size();
		buf->data = key.data();
	}
	{
		WT_ITEM* buf = &cursor->value;
		terark::Slice value = iter->value();
		buf->size = value.size();
		buf->data = value.data();
	}
}

// only reader will use the following cursor-ops
int trk_cursor_next(WT_CURSOR *cursor) {
	//printf("next entered: %s\n", cursor->uri);
	terark::TerarkIterator* iter = chunk_manager->GetIterator(cursor);
	iter->Next();
	if (!iter->Valid()) {
		printf("to the end: %s\n", cursor->uri);
		return WT_NOTFOUND;
	}
	set_kv(iter, cursor);
	return (0);
}

int trk_cursor_prev(WT_CURSOR *cursor) {
	printf("prev entered: %s\n", cursor->uri);
	terark::TerarkIterator* iter = chunk_manager->GetIterator(cursor);
	iter->Prev();
	if (!iter->Valid()) {
		return WT_NOTFOUND;
	}
	set_kv(iter, cursor);
	return (0);
}

// TBD(kg): any resources held by the cursor are released
// reset followed by next?
int trk_reader_cursor_reset(WT_CURSOR *cursor) {
	//printf("reader reset entered: %s\n", cursor->uri);
	terark::TerarkIterator* iter = chunk_manager->GetIterator(cursor);
	iter->SetInvalid();

	return (0);
}
int trk_builder_cursor_reset(WT_CURSOR *cursor) {
	printf("builder reset entered: %s\n", cursor->uri);
	return (0);
}

int trk_cursor_search(WT_CURSOR *cursor) {
	//printf("search entered: %s\n", cursor->uri);
	terark::TerarkIterator* iter = chunk_manager->GetIterator(cursor);
	iter->Seek(terark::Slice((const char*)cursor->key.data, cursor->key.size));
	if (!iter->Valid()) {
		return WT_NOTFOUND;
	}
	terark::Slice key = iter->key();
	WT_ITEM* kbuf = &cursor->key;
	if (key.size() != kbuf->size ||
		memcmp(key.data(), kbuf->data, key.size())) {
		return WT_NOTFOUND;
	}
	terark::Slice value = iter->value();
	WT_ITEM* vbuf = &cursor->value;
	vbuf->size = value.size();
	vbuf->data = value.data();

	return (0);
}

int trk_cursor_search_near(WT_CURSOR *cursor, int *exactp) {
	printf("search near entered: %s\n", cursor->uri);
	(void)cursor;
	(void)exactp;

	terark::TerarkIterator* iter = chunk_manager->GetIterator(cursor);
	terark::Slice key = iter->key();
	WT_ITEM* kbuf = &cursor->key;
	WT_ITEM* vbuf = &cursor->value;
	iter->Seek(terark::Slice((const char*)kbuf->data, kbuf->size));
	if (!iter->Valid()) { // target > last elem
		iter->SeekToLast();
		*exactp = -1;
	} else if (key.size() == kbuf->size &&
			memcmp(key.data(), kbuf->data, key.size()) == 0) {
		*exactp = 0;
	} else {
		*exactp = 1;
	}
	vbuf->size = iter->value().size();
	vbuf->data = iter->value().data();

	return (0);
}

std::map<std::string, std::string> dict;
void InitDict() {
	std::ifstream fi("./samples_large.txt");
	//std::ifstream fi("./samples.txt");
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
	WT_SESSION *session;
	int ret;

    if (getenv("WIREDTIGER_HOME") == NULL) {
		home = "WT_HOME";
		ret = system("rm -rf WT_HOME && mkdir WT_HOME");
		ret = system("rm -rf temp && mkdir temp");
    } else
		home = NULL;

	chunk_manager = terark::TerarkChunkManager::sharedInstance();
	//ret = wiredtiger_open(home, NULL, "create,verbose=[lsm,lsm_manager]", &conn);
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
		trk_drop, //__wt_lsm_tree_drop, //NULL, //my_drop,
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
		// TBD(kg): should we set raw == true ?
		session->create(session, "table:bucket", 
						"type=lsm,lsm=(merge_min=2,merge_custom=(prefix=terark,start_generation=2),chunk_size=2MB),"
						"key_format=S,value_format=S");
		
		/*session->create(session, "table:bucket", 
						"type=lsm,lsm=(merge_min=2,chunk_size=2MB),"
						"key_format=S,value_format=S");
		*/
		session->open_cursor(session, "table:bucket", NULL, NULL, &c);

		printf("start insert...\n");
		InitDict();
		for (auto& iter : dict) {
			c->set_key(c, iter.first.c_str());
			c->set_value(c, iter.second.c_str());
			c->insert(c);
		}
		printf("insert done\n");
		//sleep(30);
		printf("start search...\n");
		{
			// cursor read ...
			//c->reset(c);
			int i = 0;
			for (auto& di : dict) {
				//printf("will search %dth key %s\n", i++, di.first.c_str());
				c->set_key(c, di.first.c_str());
				int ret = c->search(c);
				assert(ret == 0);
				const char *value;
				c->get_value(c, &value);
				ret = memcmp(value, di.second.c_str(), strlen(value));
				//printf("val expected %s actual: %s\n", di.second.c_str(), value);
				assert(ret == 0);
			}
		}
		std::cout << "\n\nTest Case Passed!\n\n";

		c->close(c);
	}

	ret = conn->close(conn, NULL);

	return (ret == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
}
