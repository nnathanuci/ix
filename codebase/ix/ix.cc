#include "ix.h"
#include "IndexNode.h"

RC IX_IndexHandle::InsertEntry(void *key, const RID &rid)
{
	IndexNode node(*this, 0);
	if (this->attr.type == TypeReal)
		node.insert(*((float*)key),rid.pageNum,rid.slotNum);
	else if (this->attr.type == TypeInt)
		node.insert( *((int*)key),rid.pageNum,rid.slotNum );
	else
		return -1;

	return 0;
}

RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid)
{
	/*
	 * NOT DONE.
	 */
	return -1;
}
