#!/bin/sh

set -e

./ixtest
./ixtest_ixhandle
./ixtest_ixmgr
./ixtest_ixscan_float
./ixtest_ixscan_float_rand
./ixtest_ixscan_int
./ixtest_ixscan_int_rand

