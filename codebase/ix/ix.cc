#include "ix.h"
#include "IndexNode.h"

RC IX_IndexHandle::InsertEntry(void *key, const RID &rid)
{
	RC ret;
	IndexNode node( (*this), 0 );
	if (this->attr.type == TypeReal)
		ret= node.insert(*((float*)key), rid.pageNum, rid.slotNum);
	else if (this->attr.type == TypeInt)
		ret= node.insert( *((int*)key), rid.pageNum, rid.slotNum );
	else
		ret= -1;

	return ret;
}

RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid)
{
	RC ret;
	IndexNode node( (*this), 0 );
	if (this->attr.type == TypeReal)
		ret= node.remove(*((float*)key));
	else if (this->attr.type == TypeInt)
		ret= node.remove( *((int*)key));
	else
		ret= -1;

	return ret;
}
