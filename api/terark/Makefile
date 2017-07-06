

TerarkDir=/newssd1/zzz/terark/src
TerarkLib=/newssd1/zzz/terark-zip-rocksdb/pkg/terark-zip-rocksdb-Linux-x86_64-g++-5.4-bmi2-1
TigerDir=/newssd1/zzz/wiredtiger
RocksDir=/newssd1/zzz/rocksdb
CXX=g++
CFLAGS=-g -std=c++11 -DROCKSDB_PLATFORM_POSIX
INC=-I. -I$(TigerDir) -I$(TerarkDir) -I$(RocksDir)/include -I$(RocksDir)/port -I$(RocksDir) -I$(RocksDir)/util
# don't use terark-zip-rocksdb any more
LIB=-L$(TigerDir)/.libs -L$(TerarkLib)/lib -lpthread  -lwiredtiger -lrocksdb -lterark-core-d -lterark-fsa-d -lterark-zbs-d


OBJS = terark_chunk_reader.o terark_zip_common.o terark_zip_index.o trk_block.o trk_block_builder.o trk_table_properties.o trk_meta_blocks.o trk_format.o terark_chunk_builder.o terark_chunk_manager.o

#all: test_add
all: bridge

bridge: ${OBJS}
	${CXX} ${CFLAGS} ${INC} ${LIB} ${OBJS} -o bridge bridge.cc

test_add: ${OBJS}
	${CXX} ${CFLAGS} ${INC} ${LIB} ${OBJS} -o test_add test_add.cc

terark_zip_config.o: terark_zip_internal.h terark_zip_config.h trk_table_properties.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c terark_zip_config.cc

terark_chunk_reader.o: terark_zip_internal.h terark_chunk_reader.h trk_table_properties.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c terark_chunk_reader.cc

terark_zip_common.o: terark_zip_common.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c terark_zip_common.cc

terark_zip_index.o: terark_zip_index.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c terark_zip_index.cc

terark_chunk_builder.o: terark_zip_internal.h terark_chunk_builder.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c terark_chunk_builder.cc

terark_chunk_manager.o: terark_zip_internal.h 
	${CXX} ${CFLAGS} ${INC} ${LIB} -c terark_chunk_manager.cc

trk_block.o: trk_block.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c trk_block.cc

trk_block_builder.o: trk_format.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c trk_block_builder.cc

trk_table_properties.o: trk_table_properties.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c trk_table_properties.cc

trk_meta_blocks.o: trk_meta_blocks.h trk_format.h trk_block.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c trk_meta_blocks.cc

trk_format.o: trk_format.h
	${CXX} ${CFLAGS} ${INC} ${LIB} -c trk_format.cc


clean:
	rm -f ${EXES} *.o *.exe *.a test_add bridge

