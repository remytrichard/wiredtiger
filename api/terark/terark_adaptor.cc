
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
#include "terark_adaptor.h"
#include "terark_chunk_manager.h"
#include "terark_zip_config.h"
#include "terark_chunk_builder.h"
#include "terark_chunk_reader.h"

std::map<std::string, int> cur_stats;

static terark::TerarkChunkManager* chunk_manager;
// TBD(kg): Global scope? how about session scope?
int trk_init(const char* config) {
	if (!chunk_manager) {
		chunk_manager = new terark::TerarkChunkManager(config);
	}
	return 0;
}

static inline std::string ComposePath(WT_CONNECTION *conn, const std::string& uri) {
	assert(conn);
	std::string home = conn->get_home(conn);
	if (home.back() != '/') home.push_back('/');
	size_t pos = uri.find(':');
	if (pos != std::string::npos) {
		return std::string(home) + uri.substr(pos + 1);
	} else {
		return std::string(home) + uri;
	}
}

/*
 * TBD(kg): there should be an Init() within which chunk_manager is 
 * inited, not in create().  Otherwise, read-only workload will failed forever
 */
int trk_create(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
			   const char *uri, WT_CONFIG_ARG *config) {
	const char* sconfig = ((const char**)config)[0]; // session create config
	//if (!chunk_manager) {
	//	chunk_manager = new terark::TerarkChunkManager(sconfig);
	//}
	const terark::Comparator* comparator = terark::GetBytewiseComparator();
	terark::TerarkTableBuilderOptions builder_options(*comparator);

	WT_CONNECTION *conn = session->connection;
	std::string path = ComposePath(conn, uri);
   	::remove(path.c_str()); // make sure such file is not exist, remove it anyway
	terark::TerarkChunkBuilder* builder = chunk_manager->NewTableBuilder(builder_options, path);
	chunk_manager->AddBuilder(uri, builder);
	
	WT_EXTENSION_API *wt_api = conn->get_extension_api(conn);
	int ret = wt_api->metadata_insert(wt_api, session, uri, sconfig);
	assert(ret == 0);

	return (0);
}

// can we open multi times on one data-source ? how many diff cursors can we get ?
int trk_open_cursor(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
					const char *uri, WT_CONFIG_ARG *config, WT_CURSOR **new_cursor) {
	// Allocate and initialize a WiredTiger cursor.
	WT_CURSOR *cursor;
	if ((cursor = (WT_CURSOR*)calloc(1, sizeof(*cursor))) == NULL)
		return (errno);

	/*
	 * TBD(kg): Configure local cursor information.
	 */
	cursor->key_format = "S";
	cursor->value_format = "S";
	cursor->uri = uri;

	// set cursor-ops based on builder/reader
	std::string path = ComposePath(session->connection, uri);
	if (chunk_manager->IsChunkExist(path, uri)) {
		printf("\nopen cursor for read: %s, cnt %d\n", uri, ++cur_stats[uri + std::string("_opened")]);
		cursor->next = trk_cursor_next;
		cursor->prev = trk_cursor_prev;
		cursor->reset = trk_reader_cursor_reset;
		cursor->search = trk_cursor_search;
		cursor->search_near = trk_cursor_search_near;
		cursor->close = trk_reader_cursor_close;
		// read iterator
		terark::Iterator* iter = chunk_manager->NewIterator(path, uri);
		chunk_manager->AddIterator(cursor, iter);
	} else {
		printf("\nopen cursor for build: %s\n", uri);
		cursor->reset = trk_builder_cursor_reset;
		cursor->insert = trk_cursor_insert;
		cursor->close = trk_builder_cursor_close;
	}

	/* Return combined cursor to WiredTiger. */
	*new_cursor = (WT_CURSOR *)cursor;

	return (0);
}


int trk_pre_merge(WT_DATA_SOURCE *dsrc, WT_CURSOR *cursor, WT_CURSOR *dest) {
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

	return (0);
}

int trk_drop(WT_DATA_SOURCE *dsrc, WT_SESSION *session, const char *uri, WT_CONFIG_ARG *config) {
	printf("\ntrk_drop: %s\n", uri);
	printf("\t open %d, close %d\n", cur_stats[uri + std::string("_opened")], 
		   cur_stats[uri + std::string("_closed")]);
	//std::string path = ComposePath(session->connection, uri);
	//::remove(path.c_str());
	if (uri && strlen(uri) > 0) {
		chunk_manager->RemoveReader(uri);
	}
	return (0);
}


int trk_cursor_insert(WT_CURSOR *cursor) {
	terark::TerarkChunkBuilder* builder = chunk_manager->GetBuilder(cursor->uri);
	builder->Add(terark::Slice((const char*)cursor->value.data, cursor->value.size));
	return (0);
}


int trk_builder_cursor_close(WT_CURSOR *cursor) {
	printf("\nbuilder cursor close: %s\n", cursor->uri);
	terark::TerarkChunkBuilder* builder = chunk_manager->GetBuilder(cursor->uri);
	builder->Finish2ndPass();

	chunk_manager->RemoveBuilder(cursor->uri);
	delete builder;
	return (0);
}

int trk_reader_cursor_close(WT_CURSOR *cursor) {
	printf("\nreader cursor close: %s, %d\n", cursor->uri, ++cur_stats[cursor->uri + std::string("_closed")]);
	terark::Iterator* iter = chunk_manager->GetIterator(cursor);
	chunk_manager->RemoveIterator(cursor);
	delete iter;
	return (0);
}

static inline void set_kv(terark::Iterator* iter, WT_CURSOR* cursor) {
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
	terark::Iterator* iter = chunk_manager->GetIterator(cursor);
	iter->Next();
	if (!iter->Valid()) {
		printf("to the end: %s\n", cursor->uri);
		return WT_NOTFOUND;
	}
	set_kv(iter, cursor);
	return (0);
}

int trk_cursor_prev(WT_CURSOR *cursor) {
	terark::Iterator* iter = chunk_manager->GetIterator(cursor);
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
	terark::Iterator* iter = chunk_manager->GetIterator(cursor);
	iter->SetInvalid();
	return (0);
}
int trk_builder_cursor_reset(WT_CURSOR *cursor) {
	return (0);
}

int trk_cursor_search(WT_CURSOR *cursor) {
	terark::Iterator* iter = chunk_manager->GetIterator(cursor);
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
	terark::Iterator* iter = chunk_manager->GetIterator(cursor);
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

