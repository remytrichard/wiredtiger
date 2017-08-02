
#ifdef _MSC_VER
# include <io.h>
#else
# include <sys/types.h>
# include <sys/stat.h>
# include <fcntl.h>
# include <cxxabi.h>
#endif
#include <assert.h>
#include <stdio.h>
#include <cstring>

#include <fstream>
#include <iostream>
#include <map>
#include <memory>

#include "wiredtiger.h"
#include "wiredtiger_ext.h"

// project header
#include "terark_adaptor.h"


void InitTerark() {
	const char* config = "trk_localTempDir=./temp,"
		"trk_indexNestLevel=2,"
		"trk_indexCacheRatio=0.005,"
		"trk_smallTaskMemory=1G,"
		"trk_softZipWorkingMemLimit=16G,"
		"trk_hardZipWorkingMemLimit=32G,"
		"trk_minDictZipValueSize=1024,"
		"trk_offsetArrayBlockUnits=128,"
		"trk_max_background_flushes=4";
	trk_init(config);
}

std::map<std::string, std::string> dict;
void InitDict() {
	std::ifstream fi("./samples_large.txt");
	//std::ifstream fi("./samples_simple.txt");
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
	const char* home;
	int ret;
    if (getenv("WIREDTIGER_HOME") == NULL) {
		home = "WT_HOME";
		ret = system("rm -rf WT_HOME && mkdir WT_HOME");
		ret = system("rm -rf temp && mkdir temp");
    } else {
		home = NULL;
	}
	
	WT_CONNECTION *conn;
	ret = wiredtiger_open(home, NULL, "create", &conn);
	ret = conn->open_session(conn, NULL, NULL, &session);

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
	
	ret = conn->add_data_source(conn, "terark:", &trk_dsrc, NULL);

	ret = conn->configure_method(conn,
								 "WT_SESSION.open_cursor", NULL, "collator=", "string", NULL);
	InitTerark();

	{
		WT_CURSOR *c;
		// TBD(kg): should we set raw == true ?
		session->create(session, "table:bucket", 
						"type=lsm,lsm=(merge_min=2,merge_custom=(prefix=terark,start_generation=2,suffix=.trk),chunk_size=2MB),"
						"key_format=S,value_format=S");
		
		session->open_cursor(session, "table:bucket", NULL, NULL, &c);

		printf("start insert...\n");
		InitDict();
		for (auto& iter : dict) {
			//long recno = atol(iter.first.c_str());
			//c->set_key(c, recno);
			c->set_key(c, iter.first.c_str());
			c->set_value(c, iter.second.c_str());
			c->insert(c);
		}
		printf("insert done\n");
		printf("start search...\n");
		{
			//c->reset(c);
			int i = 0;
			for (auto& di : dict) {
				//long recno = atol(di.first.c_str());
				//c->set_key(c, recno);
				c->set_key(c, di.first.c_str());
				int ret = c->search(c);
				assert(ret == 0);
				const char *value;
				c->get_value(c, &value);
				ret = memcmp(value, di.second.c_str(), strlen(value));
				assert(ret == 0);
			}
		}
		std::cout << "\n\nTest Case Passed!\n\n";

		c->close(c);
	}

	ret = conn->close(conn, NULL);

	return (ret == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
}
