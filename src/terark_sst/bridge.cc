
#include <stdio.h>
#include <iostream>
#include "bridge.h"

//#include "wt_internal.h"

void terark_doit(const char* tag, const char* str, int len) {
    printf("%s: %.*s\n", tag, len, str);
}

/*
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

*/
