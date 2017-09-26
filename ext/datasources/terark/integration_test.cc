
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

typedef std::map<std::string, std::string> S2SDict;
static void test_search_near(WT_CURSOR*, S2SDict&);
static void test_search(WT_CURSOR*, S2SDict&);

void InitDict(const std::string& fpath, S2SDict& dict) {
	//std::ifstream fi("./samples_large.txt");
	std::ifstream fi(fpath.c_str());
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

void test_without_compact(WT_CONNECTION* conn) {
	/*
	 * 1. init dict & create table
	 * 2. test_search
	 * 3. test_search_near
	 * 4. drop table
	 */
	S2SDict dict;
	//InitDict("./samples_simple.txt", dict);
	InitDict("./samples_large.txt", dict);
	WT_SESSION *session = 0;
	int ret = conn->open_session(conn, NULL, NULL, &session);
	{
		WT_CURSOR *c;
		session->create(session, "table:bucket", 
						"type=lsm,lsm=(merge_min=2,merge_custom=(prefix=terark,start_generation=2,suffix=.trk),chunk_size=2MB),"
						"key_format=S,value_format=S");

		session->open_cursor(session, "table:bucket", NULL, NULL, &c);

		printf("start insert...\n");
		for (auto& iter : dict) {
			c->set_key(c, iter.first.c_str());
			c->set_value(c, iter.second.c_str());
			c->insert(c);
		}
		
		printf("insert done, start compact...\n");
		//sleep(20);
		//session->compact(session, "table:bucket", 0);
		//printf("compact done\n");
		test_search(c, dict);
		test_search_near(c, dict);

		std::cout << "\n\nTest Case Passed!\n\n";
		c->close(c);
	}
}

void test_uint_without_compact(WT_CONNECTION* conn) {
	/*
	 * 1. init dict & create table
	 * 2. test_search
	 * 3. test_search_near
	 * 4. drop table
	 */
	S2SDict dict;
	InitDict("./samples_uint64.txt", dict);
	WT_SESSION *session = 0;
	int ret = conn->open_session(conn, NULL, NULL, &session);
	{
		WT_CURSOR *c;
		session->create(session, "table:bucket64", 
						"type=lsm,lsm=(merge_min=2,merge_custom=(prefix=terark,start_generation=2,suffix=.trk),chunk_size=2MB),"
						//"type=lsm,"
						"key_format=Q,value_format=S");

		session->open_cursor(session, "table:bucket64", NULL, NULL, &c);

		printf("start insert...\n");
		for (auto& iter : dict) {
			long recno = atol(iter.first.c_str());
			c->set_key(c, recno);
			c->set_value(c, iter.second.c_str());
			c->insert(c);
		}
		
		printf("insert done, start compact...\n");
		//sleep(20);
		session->compact(session, "table:bucket64", 0);
		//printf("compact done\n");
		test_search(c, dict);
		//test_search_near(c, dict);

		std::cout << "\n\nTest Case Passed!\n\n";
		c->close(c);
	}
}

void test_search_near(WT_CURSOR* c, S2SDict& dict) {
	printf("start search_near()...\n");
	int exact = 0;
	auto di = dict.begin();
	int cnt = 0;
	for (; di != dict.end(); di++) {
        auto ni = std::next(di, 1);
		if (ni == dict.end()) break;
		// just minor larger than key
		std::string low_key = di->first + "\x01";
		c->set_key(c, low_key.c_str());
		int ret = c->search_near(c, &exact);
		assert(ret == 0);
		assert(exact = 1);
		const char *value;
		c->get_value(c, &value);
		ret = memcmp(value, ni->second.c_str(), strlen(value));
		if (ret != 0) {
			printf("low_key %s not equal %d\n", low_key.c_str(), cnt);
			printf("expected %s, actual val %s\n", ni->second.c_str(), value);
		}
		assert(ret == 0);
		cnt ++;
	}
	printf("exact == 1 done\n");

	{
		//const char *key = "10000000000000999999", *value;
		const char *key = "~~~~~~~~~~~~", *value;
		c->set_key(c, key);
		int ret = c->search_near(c, &exact);
		assert(exact == -1);
		printf("exact == -1 done\n");
	}

	{
		//const char *key = "0000000000000999975", *value;
		const char *key = "!%5?", *value;
        c->set_key(c, key);
        int ret = c->search_near(c, &exact);
        assert(exact == 0);
		printf("exact == 0 done\n");
	}
	printf("\ntest search_near() done\n");
}

void test_search(WT_CURSOR* c, S2SDict& dict) {
	printf("start test search() ...\n");
	int i = 0;
	for (auto& di : dict) {
		long recno = atol(di.first.c_str());
		printf("will search %lld\n", recno);
		c->set_key(c, recno);
		//c->set_key(c, di.first.c_str());
		int ret = c->search(c);
		assert(ret == 0);
		const char *value;
		c->get_value(c, &value);
		ret = memcmp(value, di.second.c_str(), strlen(value));
		assert(ret == 0);
	}
	printf("\ntest search() done\n");
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
	ret = wiredtiger_open(home, NULL, "create,statistics=(all),"
						  "extensions=[/newssd1/zzz/wiredtiger/ext/datasources/terark/libterark-adaptor.so]", &conn);
	//test_without_compact(conn);
	test_uint_without_compact(conn);
	/*ret = conn->open_session(conn, NULL, NULL, &session);
	{
		WT_CURSOR *c;
		session->create(session, "table:bucket", 
						"type=lsm,lsm=(merge_min=2,merge_custom=(prefix=terark,start_generation=2,suffix=.trk),chunk_size=2MB),"
						"key_format=S,value_format=S");

		session->open_cursor(session, "table:bucket", NULL, NULL, &c);

		printf("start insert...\n");
		InitDict();
		For (auto& iter : dict) {
			//long recno = atol(iter.first.c_str());
			//c->set_key(c, recno);
			c->set_key(c, iter.first.c_str());
			c->set_value(c, iter.second.c_str());
			c->insert(c);
		}
		printf("insert done\n");
		//printf("sleep 10s\n");
		//sleep(10);
		printf("start search...\n");
		if (g_search_near) {
			test_search_near(c);
		} else {
			test_search(c);
		}
		std::cout << "\n\nTest Case Passed!\n\n";
		c->close(c);
		}*/

	/*{
		WT_CURSOR *c;
		session->open_cursor(session,
							 "statistics:table:bucket", NULL, "statistics=(all)", &c);
		const char *desc, *pvalue;
        uint64_t value;
        int ret;
        while ((ret = c->next(c)) == 0) {
			ret = c->get_value(c, &desc, &pvalue, &value);
			if (value != 0)
				printf("%s=%s\n", desc, pvalue);
        }
		c->close(c);
		}*/

	ret = conn->close(conn, NULL);

	return (ret == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
}
//c->set_key(c, recno);
