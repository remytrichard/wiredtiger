
#!/bin/bash
base_dir=/newssd1/zzz/wiredtiger
g++ -g -std=c++11 -I. -I$base_dir -I$base_dir/src/include -L$base_dir/.libs -lwiredtiger  -o bridge bridge.cc
