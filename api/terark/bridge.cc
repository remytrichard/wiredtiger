
#include <stdio.h>
#include <iostream>
#include "bridge.h"

#include "wiredtiger.h"
#include "wiredtiger_ext.h"

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

int trk_premerge(WT_DATA_SOURCE *dsrc, WT_CURSOR *source, WT_CURSOR *dest) {
	(void)dsrc;
	(void)source;
	(void)dest;

	return (0);
}

int main() {
	WT_CONNECTION *conn;
	WT_SESSION *session;
	int ret;

	ret = wiredtiger_open(NULL, NULL, "create", &conn);
	ret = conn->open_session(conn, NULL, NULL, &session);

	//my_data_source_init(conn);

	/*! [WT_DATA_SOURCE register] */
	//WT_DATA_SOURCE trk_dsrc;
	
	//trk_dsrc.open_cursor = trk_open_cursor;
	static WT_DATA_SOURCE trk_dsrc = {
		NULL, //my_alter,
		trk_create,
		NULL, //my_compact,
		NULL, //my_drop,
		trk_open_cursor,
		NULL, //my_rename,
		NULL, //my_salvage,
		NULL, //my_truncate,
		NULL, //my_range_truncate,
		NULL, //my_verify,
		NULL, //my_checkpoint,
		NULL,  //my_terminate
		trk_premerge
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
