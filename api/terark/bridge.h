
#ifndef __BRIDGE_H_
#define __BRIDGE_H_

#include "wiredtiger.h"

#if defined(__cplusplus)
extern "C" {
#endif

void terark_doit(const char* tag, const char* str, int len);

// Callback to create a new object.
int trk_create(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
			   const char *uri, WT_CONFIG_ARG *config);

// Callback to initialize a cursor.
int trk_open_cursor(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
					const char *uri, WT_CONFIG_ARG *config, WT_CURSOR **new_cursor);

//Callback performed before an LSM merge.
int trk_premerge(WT_DATA_SOURCE *dsrc, WT_CURSOR *source, WT_CURSOR *dest);

#if defined(__cplusplus)
}
#endif

#endif



