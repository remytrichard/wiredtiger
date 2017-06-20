/*-
 * Public Domain 2014-2016 MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *
 * ex_hello.c
 *	This is an example demonstrating how to create and connect to a
 *	database.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <wiredtiger.h>

static const char *home;

int
main(void)
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    int ret;

    /*
     * Create a clean test directory for this run of the test program if the
     * environment variable isn't already set (as is done by make check).
     */
    if (getenv("WIREDTIGER_HOME") == NULL) {
		home = "WT_HOME";
		ret = system("rm -rf WT_HOME && mkdir WT_HOME");
    } else
		home = NULL;

    /* Open a connection to the database, creating it if necessary. */
    if ((ret = wiredtiger_open(home, NULL, "create", &conn)) != 0) {
		fprintf(stderr, "Error connecting to %s: %s\n",
				home == NULL ? "." : home, wiredtiger_strerror(ret));
		return (EXIT_FAILURE);
    }

    /* Open a session for the current thread's work. */
    if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0) {
		fprintf(stderr, "Error opening a session on %s: %s\n",
				home == NULL ? "." : home, wiredtiger_strerror(ret));
		return (EXIT_FAILURE);
    }

    /* Do some work... */
    {
		WT_CURSOR *c;
		session->create(session, "table:bucket", "type=lsm,key_format=S,value_format=S");
		session->open_cursor(session, "table:bucket", NULL, NULL, &c);
		for (int i = 0; i < 1000000; i++) {
		  char key[20] = { 0 };
		  char value[40] = { 0 };
		  snprintf(key, 20, "key%05d", i);
		  snprintf(value, 40, "value%010d", i);
		  c->set_key(c, key);
		  c->set_value(c, value);
		  c->insert(c);
		  }
		  ret = session->compact(session, "table:bucket", NULL);
		/*{
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
			c->set_key(c, "key_test");
			c->remove(c);
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
			}*/

		c->close(c);
    }

    /* Note: closing the connection implicitly closes open session(s). */
    if ((ret = conn->close(conn, NULL)) != 0) {
		fprintf(stderr, "Error closing %s: %s\n",
				home == NULL ? "." : home, wiredtiger_strerror(ret));
		return (EXIT_FAILURE);
    }

    return (EXIT_SUCCESS);
}
