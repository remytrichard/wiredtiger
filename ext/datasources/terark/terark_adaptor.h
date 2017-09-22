
#ifndef __BRIDGE_H_
#define __BRIDGE_H_

#define DLL_PUBLIC __attribute__ ((visibility ("default")))

#include <vector>

#include "wiredtiger.h"


#if defined(__cplusplus)
extern "C" {
#endif
	//!!! BEFORE any other methods called, always call init first
	DLL_PUBLIC int trk_init();

	// Callback to create a new object.
	DLL_PUBLIC int trk_create(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
				   const char *uri, WT_CONFIG_ARG *config);

	// Callback to initialize a cursor.
	DLL_PUBLIC int trk_open_cursor(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
						const char *uri, WT_CONFIG_ARG *config, WT_CURSOR **new_cursor);

	//Callback performed before an LSM merge.
	DLL_PUBLIC int trk_pre_merge(WT_DATA_SOURCE *dsrc, WT_CURSOR *source, WT_CURSOR *dest);

	DLL_PUBLIC int trk_drop(WT_DATA_SOURCE *dsrc, WT_SESSION *session, const char *uri, WT_CONFIG_ARG *config);
	
	DLL_PUBLIC int trk_verify(WT_DATA_SOURCE *dsrc, WT_SESSION *session, const char *name, WT_CONFIG_ARG *config);

#if defined(__cplusplus)
}
#endif


#endif



