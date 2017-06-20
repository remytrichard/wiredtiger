
#include <stdio.h>
#include <iostream>
#include "bridge.h"

#include "wiredtiger.h"
#include "wiredtiger_ext.h"

#if defined(__cplusplus)
extern "C" {
	#include "wt_internal.h"
	#include "extern.h"
}
#endif

void terark_doit(const char* tag, const char* str, int len) {
    printf("%s: %.*s\n", tag, len, str);
}


int trk_create(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
			   const char *uri, WT_CONFIG_ARG *config) {
	(void)dsrc;
	(void)session;
	(void)uri;
	(void)config;
	
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
	
	ret = conn->add_data_source(conn, "trk_dsrc:", &trk_dsrc, NULL);
	/*! [WT_DATA_SOURCE register] */

    /* Do some work... */
    {
		WT_CURSOR *c;
		session->create(session, "table:bucket", "type=trk_dsrc,key_format=S,value_format=S");
		session->open_cursor(session, "table:bucket", NULL, NULL, &c);
		{
			c->set_key(c, "key_test");
			c->set_value(c, "val1");
			c->insert(c);
		}
		{
			c->set_key(c, "key_test");
			c->set_value(c, "val2");
			c->insert(c);
		}
		{
			printf("will iter kvs:\n");
			const char *key, *value;
			c->reset(c);
			int ret;
			while ((ret = c->next(c)) == 0) {
				ret = c->get_key(c, &key);
				ret = c->get_value(c, &value);
				printf("get key: %s, value %s\n", key, value);
			}
		}

		c->close(c);
    }

    /* Note: closing the connection implicitly closes open session(s). */
    if ((ret = conn->close(conn, NULL)) != 0) {
		fprintf(stderr, "Error closing %s: %s\n",
				home == NULL ? "." : home, wiredtiger_strerror(ret));
		return (EXIT_FAILURE);
    }

	return (ret == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
}
