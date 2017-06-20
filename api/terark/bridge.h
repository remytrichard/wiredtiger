
#ifndef __BRIDGE_H_
#define __BRIDGE_H_

//#include
#include "wiredtiger.h"


#if defined(__cplusplus)
extern "C" {
#endif

// Callback to create a new object.
int trk_create(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
			   const char *uri, WT_CONFIG_ARG *config);

// Callback to initialize a cursor.
int trk_open_cursor(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
					const char *uri, WT_CONFIG_ARG *config, WT_CURSOR **new_cursor);

//Callback performed before an LSM merge.
int trk_pre_merge(WT_DATA_SOURCE *dsrc, WT_CURSOR *source, WT_CURSOR *dest);

#if defined(__cplusplus)
}
#endif

class TerarkChunk {
 public:
	
};



class TerarkChunkManager {
 public:
	static TerarkChunk* CreateTerarkChunk(const char*);

	void AddChunk(TerarkChunk*);
	TerarkChunk* GetChunk(const char*);


 private:
	std::vector<TerarkChunk*> _chunks;

};

#endif



