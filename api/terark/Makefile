

TerarkDir=/newssd1/zzz/terark/src
TerarkLib=/newssd1/zzz/terark-zip-rocksdb/pkg/terark-zip-rocksdb-Linux-x86_64-g++-5.4-bmi2-1
TigerDir=/newssd1/zzz/wiredtiger
RocksDir=/newssd1/zzz/rocksdb
CXX=g++
CFLAGS=-g -std=c++11 -DROCKSDB_PLATFORM_POSIX
INC=-I. -I./util -I$(TigerDir) -I$(TerarkDir)
LIB=-L$(TigerDir)/.libs -L$(TerarkLib)/lib -lpthread  -lwiredtiger -lterark-core-d -lterark-fsa-d -lterark-zbs-d


OBJS = trk_block.o trk_common.o trk_meta_blocks.o trk_format.o terark_chunk_reader.o terark_zip_common.o terark_zip_config.o terark_zip_index.o terark_chunk_builder.o terark_chunk_manager.o

#all: test_add
all: bridge

bridge: ${OBJS}
	${CXX} ${CFLAGS} ${INC} ${LIB} ${OBJS} -o bridge bridge.cc

test_add: ${OBJS}
	${CXX} ${CFLAGS} ${INC} ${LIB} ${OBJS} -o test_add test_add.cc

terark_zip_config.o: terark_zip_config.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c terark_zip_config.cc

terark_chunk_reader.o: terark_chunk_reader.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c terark_chunk_reader.cc

terark_zip_common.o: terark_zip_common.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c terark_zip_common.cc

terark_zip_index.o: terark_zip_index.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c terark_zip_index.cc

terark_chunk_builder.o: terark_chunk_builder.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c terark_chunk_builder.cc

terark_chunk_manager.o: terark_chunk_manager.h util/lru_cache.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c terark_chunk_manager.cc

trk_common.o: util/trk_common.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c util/trk_common.cc

trk_block.o: util/trk_block.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c util/trk_block.cc

trk_meta_blocks.o: util/trk_meta_blocks.h util/trk_format.h util/trk_block.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c util/trk_meta_blocks.cc

trk_format.o: util/trk_format.h util/trk_common.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c util/trk_format.cc


clean:
	rm -f ${EXES} *.o *.exe *.a test_add bridge

