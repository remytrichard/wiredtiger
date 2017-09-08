#!/bin/bash

. ../../set-env.sh

rm -rf ./data/*

gdb ./integration_test
#./bridge
#./test_add

: <<EOF
env TerarkZipTable_blackListColumnFamily=oplogCF \
    TerarkZipTable_indexNestLevel=2 \
    TerarkZipTable_indexCacheRatio=0.005 \
    TerarkZipTable_smallTaskMemory=1G \
    TerarkZipTable_softZipWorkingMemLimit=16G \
    TerarkZipTable_hardZipWorkingMemLimit=32G \
    TerarkZipTable_minDictZipValueSize=1024 \
    TerarkZipTable_offsetArrayBlockUnits=128 \
    TerarkZipTable_disableSecondPassIter=true \
    TerarkZipTable_max_background_flushes=4 \
    GLIBCXX_FORCE_NEW=1 \
	./bridge
#./test_add
EOF

	




