
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
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <map>
#include <memory>

#include "wiredtiger.h"
#include "wiredtiger_ext.h"

// project header
#include "terark_adaptor.h"

bool g_search_near = false;

std::map<std::string, std::string> dict;
void InitDict() {
	//std::ifstream fi("./samples_large.txt");
	std::ifstream fi("./samples_simple.txt");
	//std::ifstream fi("./samples_uint64.txt");
	while (true) {
		std::string key, val;
		if (!std::getline(fi, key)) break;
		if (!std::getline(fi, val)) break;
		key = key.substr(5);
		val = val.substr(5);
		dict[key] = val;
	}
}

void test_search_near(WT_CURSOR* c) {
	int exact = 0;
	auto di = dict.begin();
	for (; di != dict.end(); di++) {
        auto ni = std::next(di, 1);
		if (ni == dict.end()) break;
		std::string low_key = di->first + "append";
		c->set_key(c, low_key.c_str());
		int ret = c->search_near(c, &exact);
		assert(ret == 0);
		assert(exact = 1);
		const char *value;
		c->get_value(c, &value);
		ret = memcmp(value, ni->second.c_str(), strlen(value));
		assert(ret == 0);
		printf("key %s done\n", low_key.c_str());
	}
	printf("exact == 1 done\n");

	{
		const char *key = "10000000000000999999", *value;
		c->set_key(c, key);
		int ret = c->search_near(c, &exact);
		assert(exact == -1);
		printf("exact == -1 done\n");
	}

	{
		const char *key = "0000000000000999975", *value;
        c->set_key(c, key);
        int ret = c->search_near(c, &exact);
        assert(exact == 0);
		printf("exact == 0 done\n");
	}
}

void test_search(WT_CURSOR* c) {
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
	ret = wiredtiger_open(home, NULL, "create,"
						  "extensions=[/newssd1/zzz/wiredtiger/ext/datasources/terark/libterark-adaptor.so]", &conn);
	ret = conn->open_session(conn, NULL, NULL, &session);
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
		printf("sleep 10s\n");
		sleep(10);
		printf("start search...\n");
		if (g_search_near) {
			test_search_near(c);
		} else {
			test_search(c);
		}
		std::cout << "\n\nTest Case Passed!\n\n";

		c->close(c);
	}

	ret = conn->close(conn, NULL);

	return (ret == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
}
//c->set_key(c, recno);
