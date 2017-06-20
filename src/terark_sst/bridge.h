
#ifndef __BRIDGE_H_
#define __BRIDGE_H_

//#include "wt_internal.h"

#ifdef __cplusplus
 #define EXTERNC extern "C"
#else
 #define EXTERNC
#endif

EXTERNC void terark_doit(const char* tag, const char* str, int len);
/*
// Callback to create a new object.
EXTERNC int trk_create(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
					   const char *uri, WT_CONFIG_ARG *config);

// Callback to initialize a cursor.
EXTERNC int trk_open_cursor(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
							const char *uri, WT_CONFIG_ARG *config, WT_CURSOR **new_cursor);

//Callback performed before an LSM merge.
EXTERNC int trk_premerge(WT_DATA_SOURCE *dsrc, WT_CURSOR *source, WT_CURSOR *dest);
*/
#undef EXTERNC

#endif



