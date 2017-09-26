
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
#include <mutex>

#include "wiredtiger.h"
#include "wiredtiger_ext.h"

// project header
#include "terark_adaptor.h"
#include "terark_chunk_manager.h"
#include "terark_zip_common.h"
#include "terark_zip_config.h"
#include "terark_chunk_builder.h"
#include "terark_chunk_reader.h"

#undef	EMSG_ERR
#define	EMSG_ERR(wt_api, session, ...) do {				\
	(void)								\
	    wt_api->err_printf(wt_api, session, "terark: " __VA_ARGS__);\
	goto err;							\
} while (0)

typedef std::pair<std::string, std::string> StrPair;
extern const char* gitversion;
std::map<std::string, int> cur_stats;
std::map<std::string, StrPair> format_dict;
static terark::TerarkChunkManager* chunk_manager;
static std::mutex g_lock;
WT_EXTENSION_API *g_wt_api;

#if defined(__cplusplus)
extern "C" {
#endif

	static int trk_reader_cursor_next(WT_CURSOR *cursor);
	static int trk_reader_cursor_prev(WT_CURSOR *cursor);
	static int trk_builder_cursor_reset(WT_CURSOR *cursor);
	static int trk_reader_cursor_reset(WT_CURSOR *cursor);
	static int trk_reader_cursor_search(WT_CURSOR *cursor);
	static int trk_reader_cursor_search_near(WT_CURSOR *cursor, int *exactp);
	static int trk_builder_cursor_insert(WT_CURSOR *cursor);
	static int trk_builder_cursor_close(WT_CURSOR *cursor);
	static int trk_reader_cursor_close(WT_CURSOR *cursor);

#if defined(__cplusplus)
}
#endif


int trk_init() {
	// TBD(kg): add support for windows
	std::unique_lock<std::mutex> lock(g_lock);
	if (!chunk_manager) {
		std::stringstream ss;
		char* lpath = nullptr;
		if ((lpath = getenv("WT_TERARK_HOME")) != NULL) {
			ss << "trk_localTempDir=" << lpath << ",";
			char op[128] = { 0 };
			snprintf(op, sizeof(op), "mkdir %s", lpath);
			system(op);
		} else {
			ss << "trk_localTempDir=" << "./terark_tmp,";
			system("mkdir terark_tmp");
		}
		ss << "trk_indexNestLevel=2,";
		ss << "trk_indexCacheRatio=0.005,";
		ss << "trk_smallTaskMemory=1G,";
		ss << "trk_softZipWorkingMemLimit=16G,";
		ss << "trk_hardZipWorkingMemLimit=32G,";
		ss << "trk_minDictZipValueSize=1024,";
		ss << "trk_offsetArrayBlockUnits=128,";
		ss << "trk_max_background_flushes=4";
		chunk_manager = new terark::TerarkChunkManager(ss.str());
	}
	return (0);
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

static inline std::string ComposeLSMName(const std::string& uri) {
	size_t start_pos = uri.find(":");
	size_t end_pos = uri.find_last_of("-");
	if (start_pos == std::string::npos ||
		end_pos == std::string::npos) {
		return "";
	}
	std::string sub = uri.substr(start_pos + 1, end_pos - start_pos - 1);
	return "lsm:" + sub;
}

static void parse_table_config(WT_SESSION *session, const char *uri) {
	WT_CONNECTION *conn = session->connection;
	char* config = nullptr;
	WT_CONFIG_ITEM key_item, value_item;
	std::string key_f = "u", value_f = "u";
	std::string lsm_uri = ComposeLSMName(uri);
	if (g_wt_api->metadata_search(g_wt_api, session, lsm_uri.c_str(), &config) == 0 &&
		g_wt_api->config_get_string(g_wt_api, session, config, "key_format", &key_item) == 0 &&
		g_wt_api->config_get_string(g_wt_api, session, config, "value_format", &value_item) == 0) {
		key_f.assign(key_item.str, key_item.len);
		value_f.assign(value_item.str, value_item.len);
	}
	StrPair str_pair(key_f, value_f);
	format_dict.insert(std::make_pair(uri, str_pair));
}

int trk_create(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
			   const char *uri, WT_CONFIG_ARG *config) {
	/*
	 * extract conf, set into builder_options
	 */ 
	const terark::Comparator* comparator = terark::GetBytewiseComparator();
	terark::TerarkTableBuilderOptions builder_options(*comparator);
	parse_table_config(session, uri);
	builder_options.key_format = format_dict[uri].first;
	builder_options.wt_session = session;

	WT_CONNECTION *conn = session->connection;
	std::string path = ComposePath(conn, uri);
   	::remove(path.c_str()); // make sure such file is not exist, remove it anyway
	terark::TerarkChunkBuilder* builder = chunk_manager->NewTableBuilder(builder_options, path);
	chunk_manager->AddBuilder(uri, builder);

	/*
	 * insert config into metatable,
	 * during cur_ds:open_cursor, such config will be used
	 */
	char* sconfig = ((char**)config)[0];
	g_wt_api->metadata_insert(g_wt_api, session, uri, sconfig);

	return (0);
}

int trk_open_cursor(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
					const char *uri, WT_CONFIG_ARG *config, WT_CURSOR **new_cursor) {
	// Allocate and initialize a WiredTiger cursor.
	terark::wt_terark_cursor *terark_cursor;
	if ((terark_cursor = ((terark::wt_terark_cursor*)calloc(1, sizeof(*terark_cursor)))) == NULL)
		return (errno);
	WT_CURSOR *cursor = (WT_CURSOR*)&terark_cursor->iface;

	// update cursor config
	//std::string key_f = format_dict[uri].first,
	//	value_f = format_dict[uri].second;
	terark::wt_strndup("u", 1, &cursor->key_format);
	terark::wt_strndup("u", 1, &cursor->value_format);
	cursor->uri = uri;
	cursor->session = session;

	// set cursor-ops based on builder/reader
	std::string path = ComposePath(session->connection, uri);
	if (chunk_manager->IsChunkExist(path, uri)) {
		printf("\nopen cursor for read: %s, cnt %d\n", uri, ++cur_stats[uri + std::string("_opened")]);
		cursor->next = trk_reader_cursor_next;
		cursor->prev = trk_reader_cursor_prev;
		cursor->reset = trk_reader_cursor_reset;
		cursor->search = trk_reader_cursor_search;
		cursor->search_near = trk_reader_cursor_search_near;
		cursor->close = trk_reader_cursor_close;
		// read iterator
		terark::TerarkTableReaderOptions reader_options(*terark::GetBytewiseComparator());
		reader_options.key_format = format_dict[uri].first;
		reader_options.wt_session = session;
		terark::Iterator* iter = chunk_manager->NewIterator(reader_options, path, uri);
		terark_cursor->iter = iter;
		chunk_manager->AddIterator(cursor);
	} else {
		printf("\nopen cursor for build: %s\n", uri);
		cursor->reset = trk_builder_cursor_reset;
		cursor->insert = trk_builder_cursor_insert;
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

// TBD(kg): ...
int trk_verify(WT_DATA_SOURCE *dsrc, WT_SESSION *session, 
			   const char *name, WT_CONFIG_ARG *config) {
	(void)session;
	(void)name;
	(void)config;

	return (0);
}


int trk_builder_cursor_insert(WT_CURSOR *cursor) {
	terark::TerarkChunkBuilder* builder = chunk_manager->GetBuilder(cursor->uri);
	terark::Slice slice((const char*)cursor->value.data, cursor->value.size);
	builder->Add(slice);
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
	chunk_manager->RemoveIterator(cursor);
	return (0);
}

/*
 * checkout src/cursor/cur_ds.c:__curds_cursor_resolve(), which requires
 * the underlying data-source never returns with the cursor/source key referencing
 * application memory, it'd be great to do a copy as necessary.
 * However, i haven't found where free() is called hence no copy here at least now
 */
static inline void set_kv(terark::Iterator* iter, WT_CURSOR* cursor) {
	std::string uri = cursor->uri;
	//if (uri == "Q" || uri == "r") {
	//} else {
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
	{
		// not sure if such unpack is appropriate here
		//WT_CONNECTION *conn = cursor->session->connection;
		//WT_EXTENSION_API *wt_api = conn->get_extension_api(conn);
		WT_ITEM* buf = &cursor->value;
		g_wt_api->struct_unpack(g_wt_api, cursor->session, buf->data,
							  buf->size, "r", &cursor->recno);
	}
}

// only reader will use the following cursor-ops
int trk_reader_cursor_next(WT_CURSOR *cursor) {
	terark::Iterator* iter = ((terark::wt_terark_cursor*)cursor)->iter;
	iter->Next();
	if (!iter->Valid()) {
		return WT_NOTFOUND;
	}
	set_kv(iter, cursor);
	return (0);
}

int trk_reader_cursor_prev(WT_CURSOR *cursor) {
	terark::Iterator* iter = ((terark::wt_terark_cursor*)cursor)->iter;
	iter->Prev();
	if (!iter->Valid()) {
		return WT_NOTFOUND;
	}
	set_kv(iter, cursor);
	return (0);
}

int trk_reader_cursor_reset(WT_CURSOR *cursor) {
	terark::Iterator* iter = ((terark::wt_terark_cursor*)cursor)->iter;
	iter->SetInvalid();
	return (0);
}
int trk_builder_cursor_reset(WT_CURSOR *cursor) {
	return (0);
}

int trk_reader_cursor_search(WT_CURSOR *cursor) {
	terark::Iterator* iter = ((terark::wt_terark_cursor*)cursor)->iter;
	iter->SeekExact(terark::Slice((const char*)cursor->key.data, cursor->key.size));
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

int trk_reader_cursor_search_near(WT_CURSOR *cursor, int *exactp) {
	//printf("search near entered: %s\n", cursor->uri);
	terark::Iterator* iter = ((terark::wt_terark_cursor*)cursor)->iter;
	WT_ITEM* kbuf = &cursor->key;
	WT_ITEM* vbuf = &cursor->value;
	iter->Seek(terark::Slice((const char*)kbuf->data, kbuf->size));
	if (!iter->Valid()) { // target > last elem
		iter->SeekToLast();
		*exactp = -1;
	} else if (iter->key().size() == kbuf->size &&
			   memcmp(iter->key().data(), kbuf->data, iter->key().size()) == 0) {
		*exactp = 0;
	} else {
		*exactp = 1;
	}
	vbuf->size = iter->value().size();
	vbuf->data = iter->value().data();

	return (0);
}

/*
 * wiredtiger_extension_init --
 *	Initialize the Helium connector code.
 */
int
wiredtiger_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config) {
	printf("gitversion is %s\n", gitversion);
	// init terark manager
	trk_init();
	// add terark data source as extension into wt
	static WT_DATA_SOURCE trk_dsrc = {
		NULL, //__wt_lsm_tree_alter
		trk_create,
		NULL, //__wt_lsm_compact
		trk_drop, //__wt_lsm_tree_drop
		trk_open_cursor,
		NULL, //__wt_lsm_tree_rename
		NULL, //__wt_lsm_tree_salvage
		NULL, //__wt_lsm_tree_truncate
		NULL, //__wt_lsm_range_truncate
		trk_verify,
		NULL, //__wt_lsm_checkpoint
		NULL,  //__wt_lsm_terminate
		trk_pre_merge
	};
	g_wt_api = connection->get_extension_api(connection);

	int ret = 0;
	if ((ret = connection->add_data_source(
		connection, "terark:", &trk_dsrc, NULL)) != 0) {
		EMSG_ERR(g_wt_api, NULL, 
				 "WT_CONNECTION.add_data_source: %s",
				 g_wt_api->strerror(g_wt_api, NULL, ret));
	}
	ret = connection->configure_method(
		connection, "WT_SESSION.open_cursor", NULL, "collator=", "string", NULL);
	return (0);

 err:
	return ret;
}
