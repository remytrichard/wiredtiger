

TerarkDir=/newssd1/zzz/terark/src
TigerDir=/newssd1/zzz/wiredtiger
RocksDir=/newssd1/zzz/rocksdb
CXX=g++
CFLAGS=-g -std=c++11 -DROCKSDB_PLATFORM_POSIX
INC=-I. -I$(TigerDir) -I$(TerarkDir) -I$(RocksDir)/include -I$(RocksDir)/port -I$(RocksDir) -I$(RocksDir)/util
LIB=-L$base_dir/.libs -lwiredtiger 
#g++ -g -std=c++11 -I. -I$base_dir -I$base_dir/src/include -L$base_dir/.libs -lwiredtiger  -o bridge bridge.cc

OBJS = terark_zip_table.o bridge.o

all: ${OBJS}


terark_zip_table.o: terark_zip_internal.h terark_zip_table.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c terark_zip_table.cc

bridge.o: terark_zip_internal.h terark_zip_table.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c bridge.cc


clean:
	rm -f ${EXES} *.o *.exe *.a

