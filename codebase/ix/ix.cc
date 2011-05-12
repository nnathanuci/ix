#include "ix.h"

#include "IndexNode.h"

extern int test_read_write_to_bytes();

RC IX_IndexHandle::InsertEntry(void *key, const RID &rid)
{
	unsigned char* buffer [PF_PAGE_SIZE] = {0};
	IndexNode node;

	ReadNode(0,buffer);
	/*Assuming that key is an int for now.*/
	node.read_node(buffer);
	node.insert(*((int*)key),rid.pageNum);
	node.write_node(buffer);
	WriteNode(0, buffer);
	return 0;
}

RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid)
{
	unsigned char* buffer [PF_PAGE_SIZE];
	IndexNode node;
	ReadNode(0,buffer);
	/*Assuming that key is an int for now.*/
	node.read_node(buffer);
	node.remove(*((int*)key));
	node.write_node(buffer);
	WriteNode(0, buffer);
	return 0;
}
