#!/usr/bin/python

from struct import *
import ctypes
import binascii
from binascii import hexlify

PF_PAGESIZE = 4096
DATA_SIZE = (PF_PAGESIZE - 20) # left,right,free offset, node type [0:del,1:index,2:leaf], number of items
MAX_ENTRIES = DATA_SIZE / 12;

# =I: native endianness, unsigned int (32bits)
PACK_SUFFIX = "=IIIII"


# takes in zipped key, value pairs, where keys are ints, and values are page,slot ids.
def construct_node(kv):
	buf = ctypes.create_string_buffer(PF_PAGESIZE)
	if len(kv)*3*4 > DATA_SIZE:
		raise Exception("too many entries for a node.")

	offset = 0
	for k, v in kv:
		pid, slot = v
		pack_into("=iII", buf, offset, k, pid, slot)
		offset += 3*4

	pack_into(PACK_SUFFIX, buf, DATA_SIZE, *(0, 0, offset, 2, len(kv)))

	return buf
		

# a nice padding buffer where the root node is meant to sit.
zero_buf = ctypes.create_string_buffer(PF_PAGESIZE)

# construct key, value pairs
entries = range(0,MAX_ENTRIES)
keys = []
values = []
for k in entries:
	keys.append(k*10)
	values.append((k*100, k*1000))

node = construct_node(zip(keys,values))

f = open('mock_leaf_int.a1', "w+")
f.write('\x00'*PF_PAGESIZE)
f.write(*unpack("=4096s", node))
f.close()
#print hexlify(node)
