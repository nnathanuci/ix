#include "ix.h"
#include "IndexNode.h"
//class IX_IndexScan {
// public:
//  IX_IndexScan();  								// Constructor
//  ~IX_IndexScan(); 								// Destructor
//
//  // for the format of "value", please see IX_IndexHandle::InsertEntry()
//  RC OpenScan(const IX_IndexHandle &indexHandle, // Initialize index scan
//	      CompOp      compOp,
//	      void        *value);           
//
//  RC GetNextEntry(RID &rid);  // Get next matching entry
//  RC CloseScan();             // Terminate index scan
//};


IX_IndexScan::IX_IndexScan() // {{{
{
} // }}}

IX_IndexScan::~IX_IndexScan() // {{{
{
} // }}}

RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle, CompOp compOp, void *value) // {{{
{
    /* find which data node has the key. */
	RID rid;
	rid.pageNum = 0;
	rid.slotNum = 0;

        if (!indexHandle.in_use)
            return -1;

	IndexNode node( (IX_IndexHandle&) indexHandle, 0);

	if (compOp == NE_OP || compOp == NO_OP)
		node.get_leftmost_data_node(rid);
	else
	{
		if (indexHandle.attr.type == TypeReal)
			node.find(*((float*)value),rid,true);
		else if (indexHandle.attr.type == TypeInt)
			node.find(*((int*)value),rid,true);
		else
			return -1;
	}

    return OpenScan(indexHandle, compOp, value, rid.pageNum) ;
    //return 0;
} // }}}

RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle, CompOp compOp, void *value, int anchor_pid) // {{{
{
    handle = indexHandle;
    op = compOp;
    start_pid = anchor_pid;
    next_pid = anchor_pid;
    cond_attr = indexHandle.attr;
    n_matches = 0;

    /* use invalid values for now. */
    last_node_next = PF_PAGE_SIZE;
    last_node_pid = 0;

    /* debugging only. */
    //handle.DumpNode(anchor_pid, 3);

    /* don't inspect the value if it's NOOP. */
    if (compOp == NO_OP)
        return 0;

    if (cond_attr.type == TypeInt)
    {
        k_int = *((int *) value);
    }
    else if (cond_attr.type == TypeReal)
    {
        k_float = *((float *) value);
    }
    else if (cond_attr.type == TypeVarChar)
    {
        k_len = *((int *) value);

        /* zero the buffer. */
        memset(k_varchar, 0, PF_PAGE_SIZE);

        /* copy in the actual data. */
        memcpy(k_varchar, ((char *) value)+sizeof(k_len), k_len);
    }

    return 0;
} // }}}

RC IX_IndexScan::GetNextEntry(RID &rid) // {{{
{
    if (op == EQ_OP)
        return GetNextEntryEQ(rid);

    if (op == NE_OP)
        return GetNextEntryNE(rid);

    if (op == GT_OP)
        return GetNextEntryGT(rid);

    if (op == GE_OP)
        return GetNextEntryGE(rid);

    if (op == LT_OP)
        return GetNextEntryLT(rid);

    if (op == LE_OP)
        return GetNextEntryLE(rid);

    if (op == NO_OP)
        return GetNextEntryNOOP(rid);

    return 1;
} // }}}

RC IX_IndexScan::GetNextEntryEQ(RID &rid) // {{{
{
    unsigned int n_entries;
    unsigned int type;
    unsigned int left_pid;
    unsigned int right_pid;
    unsigned int free_offset;
    unsigned int free_space;

    /* if the next entry is invalid, then read in new node. */
    if(last_node_next == PF_PAGE_SIZE)
    {
        /* read in the node, it will persist in the structure, so only needs to be done once. */
        if(handle.ReadNode(start_pid, last_node))
            return 1;

        last_node_pid = start_pid;
        last_node_next = 0;
    }

    /* collect all metadata from the node. */
    {
        n_entries = DUMP_GET_NUM_ENTRIES(last_node);
        type = DUMP_GET_TYPE(last_node);
        left_pid = DUMP_GET_LEFT_PID(last_node);
        right_pid = DUMP_GET_RIGHT_PID(last_node);
        free_offset = DUMP_GET_FREE_OFFSET(last_node);
        free_space = DUMP_GET_FREE_SPACE(last_node);
    }

   
    /* scan the node until we find a match. */
    while (last_node_next < (int)n_entries)
    {
        if (cond_attr.type == TypeInt)
        {
           if (k_int == *((int *) &last_node[last_node_next*12]))
           {
               /* found match. */
               rid.pageNum = *((unsigned int *) &last_node[last_node_next*12 + 4]);
               rid.slotNum = *((unsigned int *) &last_node[last_node_next*12 + 8]);

               /* increment so we check the entry on next call. */
               last_node_next++;
               n_matches++;

               return 0;
           }
           else if (k_int < *((int *) &last_node[last_node_next*12]))
           {
               /* won't find a match now, all keys are now larger. */
               return IX_EOF;
           }
           else /* k_int < last_node_next */
           {
               last_node_next++;
           }
        }
        else if (cond_attr.type == TypeReal)
        {
           if (k_float == *((float *) &last_node[last_node_next*12]))
           {
               /* found match. */
               rid.pageNum = *((unsigned int *) &last_node[last_node_next*12 + 4]);
               rid.slotNum = *((unsigned int *) &last_node[last_node_next*12 + 8]);

               /* increment so we check the entry on next call. */
               last_node_next++;
               n_matches++;

               return 0;
           }
           else if (k_float < *((float *) &last_node[last_node_next*12]))
           {
               /* won't find a match now, all keys are now larger. */
               return IX_EOF;
           }
           else /* k_float < last_node_next */
           {
               last_node_next++;
           }
        }
        else if (cond_attr.type == TypeVarChar)
        {
            cout << "NOT IMPLEMENTED YET." << endl;
            return 1;
        }
    }
    
    /* not found among the entries. */
    return IX_EOF;
} // }}}

RC IX_IndexScan::GetNextEntryNE(RID &rid) // {{{
{
    unsigned int n_entries;
    unsigned int type;
    unsigned int left_pid;
    unsigned int right_pid;
    unsigned int free_offset;
    unsigned int free_space;

    /* if the next entry is invalid, then read in new node. */
    if(last_node_next == PF_PAGE_SIZE)
    {
        /* read in the node, it will persist in the structure, so only needs to be done once. */
        if(handle.ReadNode(next_pid, last_node))
            return 1;

        last_node_pid = next_pid;
        last_node_next = 0;
    }

    /* collect all metadata from the node. */
    {
        n_entries = DUMP_GET_NUM_ENTRIES(last_node);
        type = DUMP_GET_TYPE(last_node);
        left_pid = DUMP_GET_LEFT_PID(last_node);
        right_pid = DUMP_GET_RIGHT_PID(last_node);
        free_offset = DUMP_GET_FREE_OFFSET(last_node);
        free_space = DUMP_GET_FREE_SPACE(last_node);
    }

    /* scan the node until we find a match. */
    while (last_node_next < (int)n_entries)
    {
        if (cond_attr.type == TypeInt)
        {
           if (k_int != *((int *) &last_node[last_node_next*12]))
           {
               /* found match. */
               rid.pageNum = *((unsigned int *) &last_node[last_node_next*12 + 4]);
               rid.slotNum = *((unsigned int *) &last_node[last_node_next*12 + 8]);

               /* increment so we check the entry on next call. */
               last_node_next++;
               n_matches++;

               return 0;
           }
           else if (k_int == *((int *) &last_node[last_node_next*12]))
           {
               last_node_next++;
           }
        }
        else if (cond_attr.type == TypeReal)
        {
           if (k_float != *((float *) &last_node[last_node_next*12]))
           {
               /* found match. */
               rid.pageNum = *((unsigned int *) &last_node[last_node_next*12 + 4]);
               rid.slotNum = *((unsigned int *) &last_node[last_node_next*12 + 8]);

               /* increment so we check the entry on next call. */
               last_node_next++;
               n_matches++;

               return 0;
           }
           else if (k_float == *((float *) &last_node[last_node_next*12]))
           {
               last_node_next++;
           }
        }
        else if (cond_attr.type == TypeVarChar)
        {
            cout << "NOT IMPLEMENTED YET." << endl;
            return 1;
        }
    }

    /* right sibling exists */
    if(right_pid != 0)
    {
        last_node_next = PF_PAGE_SIZE; // reset the next offset (triggers reading in new node)
        next_pid = right_pid;
        return GetNextEntryNE(rid);
    }

    /* finished scan, no more siblings to be read. */
    return IX_EOF;
} // }}}

RC IX_IndexScan::GetNextEntryGT(RID &rid) // {{{
{
    unsigned int n_entries;
    unsigned int type;
    unsigned int left_pid;
    unsigned int right_pid;
    unsigned int free_offset;
    unsigned int free_space;

    /* if the next entry is invalid, then read in new node. */
    if(last_node_next == PF_PAGE_SIZE)
    {
        /* read in the node, it will persist in the structure, so only needs to be done once. */
        if(handle.ReadNode(next_pid, last_node))
            return 1;

        last_node_pid = next_pid;
        last_node_next = 0;
    }

    /* collect all metadata from the node. */
    {
        n_entries = DUMP_GET_NUM_ENTRIES(last_node);
        type = DUMP_GET_TYPE(last_node);
        left_pid = DUMP_GET_LEFT_PID(last_node);
        right_pid = DUMP_GET_RIGHT_PID(last_node);
        free_offset = DUMP_GET_FREE_OFFSET(last_node);
        free_space = DUMP_GET_FREE_SPACE(last_node);
    }

    /* scan the node until we find a match. */
    while (last_node_next < (int)n_entries)
    {
        if (cond_attr.type == TypeInt)
        {
           if (*((int *) &last_node[last_node_next*12]) > k_int)
           {
               /* found match. */
               rid.pageNum = *((unsigned int *) &last_node[last_node_next*12 + 4]);
               rid.slotNum = *((unsigned int *) &last_node[last_node_next*12 + 8]);

               /* increment so we check the entry on next call. */
               last_node_next++;
               n_matches++;

               return 0;
           }
           else if (*((int *) &last_node[last_node_next*12]) <= k_int)
           {
               last_node_next++;
           }
        }
        else if (cond_attr.type == TypeReal)
        {
           if (*((float *) &last_node[last_node_next*12]) > k_float)
           {
               /* found match. */
               rid.pageNum = *((unsigned int *) &last_node[last_node_next*12 + 4]);
               rid.slotNum = *((unsigned int *) &last_node[last_node_next*12 + 8]);

               /* increment so we check the entry on next call. */
               last_node_next++;
               n_matches++;

               return 0;
           }
           else if (*((float *) &last_node[last_node_next*12]) <= k_float)
           {
               last_node_next++;
           }
        }
        else if (cond_attr.type == TypeVarChar)
        {
            cout << "NOT IMPGTMENTED YET." << endl;
            return 1;
        }
    }

    /* right sibling exists */
    if(right_pid != 0)
    {
        last_node_next = PF_PAGE_SIZE; // reset the next offset (triggers reading in new node)
        next_pid = right_pid;
        return GetNextEntryGT(rid);
    }

    /* finished scan, no more siblings to be read. */
    return IX_EOF;
} // }}}

RC IX_IndexScan::GetNextEntryGE(RID &rid) // {{{
{
    unsigned int n_entries;
    unsigned int type;
    unsigned int left_pid;
    unsigned int right_pid;
    unsigned int free_offset;
    unsigned int free_space;

    /* if the next entry is invalid, then read in new node. */
    if(last_node_next == PF_PAGE_SIZE)
    {
        /* read in the node, it will persist in the structure, so only needs to be done once. */
        if(handle.ReadNode(next_pid, last_node))
            return 1;

        last_node_pid = next_pid;
        last_node_next = 0;
    }

    /* collect all metadata from the node. */
    {
        n_entries = DUMP_GET_NUM_ENTRIES(last_node);
        type = DUMP_GET_TYPE(last_node);
        left_pid = DUMP_GET_LEFT_PID(last_node);
        right_pid = DUMP_GET_RIGHT_PID(last_node);
        free_offset = DUMP_GET_FREE_OFFSET(last_node);
        free_space = DUMP_GET_FREE_SPACE(last_node);
    }

    /* scan the node until we find a match. */
    while (last_node_next < (int)n_entries)
    {
        if (cond_attr.type == TypeInt)
        {
           if (*((int *) &last_node[last_node_next*12]) >= k_int)
           {
               /* found match. */
               rid.pageNum = *((unsigned int *) &last_node[last_node_next*12 + 4]);
               rid.slotNum = *((unsigned int *) &last_node[last_node_next*12 + 8]);

               /* increment so we check the entry on next call. */
               last_node_next++;
               n_matches++;

               return 0;
           }
           else if (*((int *) &last_node[last_node_next*12]) < k_int)
           {
               last_node_next++;
           }
        }
        else if (cond_attr.type == TypeReal)
        {
           if (*((float *) &last_node[last_node_next*12]) >= k_float)
           {
               /* found match. */
               rid.pageNum = *((unsigned int *) &last_node[last_node_next*12 + 4]);
               rid.slotNum = *((unsigned int *) &last_node[last_node_next*12 + 8]);

               /* increment so we check the entry on next call. */
               last_node_next++;
               n_matches++;

               return 0;
           }
           else if (*((float *) &last_node[last_node_next*12]) < k_float)
           {
               last_node_next++;
           }
        }
        else if (cond_attr.type == TypeVarChar)
        {
            cout << "NOT IMPGEMENTED YET." << endl;
            return 1;
        }
    }

    /* right sibling exists */
    if(right_pid != 0)
    {
        last_node_next = PF_PAGE_SIZE; // reset the next offset (triggers reading in new node)
        next_pid = right_pid;
        return GetNextEntryGE(rid);
    }

    /* finished scan, no more siblings to be read. */
    return IX_EOF;
} // }}}

RC IX_IndexScan::GetNextEntryLT(RID &rid) // {{{
{
    unsigned int n_entries;
    unsigned int type;
    unsigned int left_pid;
    unsigned int right_pid;
    unsigned int free_offset;
    unsigned int free_space;

    /* if the next entry is invalid, then read in new node. */
    if(last_node_next == PF_PAGE_SIZE)
    {
        /* read in the node, it will persist in the structure, so only needs to be done once. */
        if(handle.ReadNode(next_pid, last_node))
            return 1;

        last_node_pid = next_pid;
    }

    /* collect all metadata from the node. */
    {
        n_entries = DUMP_GET_NUM_ENTRIES(last_node);
        type = DUMP_GET_TYPE(last_node);
        left_pid = DUMP_GET_LEFT_PID(last_node);
        right_pid = DUMP_GET_RIGHT_PID(last_node);
        free_offset = DUMP_GET_FREE_OFFSET(last_node);
        free_space = DUMP_GET_FREE_SPACE(last_node);

        /* since scanning right to left, we need to read in n_entries first) */
        if (last_node_next == PF_PAGE_SIZE)
            last_node_next = n_entries-1; /* scanning right to left. */
    }

    /* scan the node until we find a match. */
    while (last_node_next >= 0)
    {
        if (cond_attr.type == TypeInt)
        {
           if (*((int *) &last_node[last_node_next*12]) < k_int)
           {
               /* found match. */
               rid.pageNum = *((unsigned int *) &last_node[last_node_next*12 + 4]);
               rid.slotNum = *((unsigned int *) &last_node[last_node_next*12 + 8]);

               /* increment so we check the entry on next call. */
               last_node_next--;
               n_matches++;

               return 0;
           }
           else if (*((int *) &last_node[last_node_next*12]) >= k_int)
           {
               last_node_next--;
           }
        }
        else if (cond_attr.type == TypeReal)
        {
           if (*((float *) &last_node[last_node_next*12]) < k_float)
           {
               /* found match. */
               rid.pageNum = *((unsigned int *) &last_node[last_node_next*12 + 4]);
               rid.slotNum = *((unsigned int *) &last_node[last_node_next*12 + 8]);

               /* increment so we check the entry on next call. */
               last_node_next--;
               n_matches++;

               return 0;
           }
           else if (*((float *) &last_node[last_node_next*12]) >= k_float)
           {
               last_node_next--;
           }
        }
        else if (cond_attr.type == TypeVarChar)
        {
            cout << "NOT IMPLTMENTED YET." << endl;
            return 1;
        }
    }

    /* left sibling exists */
    if(left_pid != 0)
    {
        last_node_next = PF_PAGE_SIZE; // reset the next offset (triggers reading in new node)
        next_pid = left_pid;
        return GetNextEntryLT(rid);
    }

    /* finished scan, no more siblings to be read. */
    return IX_EOF;
} // }}}

RC IX_IndexScan::GetNextEntryLE(RID &rid) // {{{
{
    unsigned int n_entries;
    unsigned int type;
    unsigned int left_pid;
    unsigned int right_pid;
    unsigned int free_offset;
    unsigned int free_space;

    /* if the next entry is invalid, then read in new node. */
    if(last_node_next == PF_PAGE_SIZE)
    {
        /* read in the node, it will persist in the structure, so only needs to be done once. */
        if(handle.ReadNode(next_pid, last_node))
            return 1;

        last_node_pid = next_pid;
    }

    /* collect all metadata from the node. */
    {
        n_entries = DUMP_GET_NUM_ENTRIES(last_node);
        type = DUMP_GET_TYPE(last_node);
        left_pid = DUMP_GET_LEFT_PID(last_node);
        right_pid = DUMP_GET_RIGHT_PID(last_node);
        free_offset = DUMP_GET_FREE_OFFSET(last_node);
        free_space = DUMP_GET_FREE_SPACE(last_node);

        /* since scanning right to left, we need to read in n_entries first) */
        if (last_node_next == PF_PAGE_SIZE)
            last_node_next = n_entries-1; /* scanning right to left. */
    }

    /* scan the node until we find a match. */
    while (last_node_next >= 0)
    {
        if (cond_attr.type == TypeInt)
        {
           if (*((int *) &last_node[last_node_next*12]) <= k_int)
           {
               /* found match. */
               rid.pageNum = *((unsigned int *) &last_node[last_node_next*12 + 4]);
               rid.slotNum = *((unsigned int *) &last_node[last_node_next*12 + 8]);

               /* increment so we check the entry on next call. */
               last_node_next--;
               n_matches++;

               return 0;
           }
           else if (*((int *) &last_node[last_node_next*12]) > k_int)
           {
               last_node_next--;
           }
        }
        else if (cond_attr.type == TypeReal)
        {
           if (*((float *) &last_node[last_node_next*12]) <= k_float)
           {
               /* found match. */
               rid.pageNum = *((unsigned int *) &last_node[last_node_next*12 + 4]);
               rid.slotNum = *((unsigned int *) &last_node[last_node_next*12 + 8]);

               /* increment so we check the entry on next call. */
               last_node_next--;
               n_matches++;

               return 0;
           }
           else if (*((float *) &last_node[last_node_next*12]) > k_float)
           {
               last_node_next--;
           }
        }
        else if (cond_attr.type == TypeVarChar)
        {
            cout << "NOT IMPLEMENTED YET." << endl;
            return 1;
        }
    }

    /* left sibling exists */
    if(left_pid != 0)
    {
        last_node_next = PF_PAGE_SIZE; // reset the next offset (triggers reading in new node)
        next_pid = left_pid;
        return GetNextEntryLE(rid);
    }

    /* finished scan, no more siblings to be read. */
    return IX_EOF;
} // }}}

RC IX_IndexScan::GetNextEntryNOOP(RID &rid) // {{{
{
    unsigned int n_entries;
    unsigned int type;
    unsigned int left_pid;
    unsigned int right_pid;
    unsigned int free_offset;
    unsigned int free_space;

    /* if the next entry is invalid, then read in new node. */
    if(last_node_next == PF_PAGE_SIZE)
    {
        /* read in the node, it will persist in the structure, so only needs to be done once. */
        if(handle.ReadNode(next_pid, last_node))
            return 1;

        last_node_pid = next_pid;
        last_node_next = 0;
    }

    /* collect all metadata from the node. */
    {
        n_entries = DUMP_GET_NUM_ENTRIES(last_node);
        type = DUMP_GET_TYPE(last_node);
        left_pid = DUMP_GET_LEFT_PID(last_node);
        right_pid = DUMP_GET_RIGHT_PID(last_node);
        free_offset = DUMP_GET_FREE_OFFSET(last_node);
        free_space = DUMP_GET_FREE_SPACE(last_node);
    }

    /* scan the node until we find a match. */
    while (last_node_next < n_entries)
    {
        if (cond_attr.type == TypeInt || cond_attr.type == TypeReal)
        {
           rid.pageNum = *((unsigned int *) &last_node[last_node_next*12 + 4]);
           rid.slotNum = *((unsigned int *) &last_node[last_node_next*12 + 8]);
           last_node_next++;
           n_matches++;
        }
        else if (cond_attr.type == TypeVarChar)
        {
            cout << "NOT IMPLEMENTED YET." << endl;
            return 1;
        }
    }

    /* right sibling exists */
    if(right_pid != 0)
    {
        last_node_next = PF_PAGE_SIZE; // reset the next offset (triggers reading in new node)
        next_pid = right_pid;
        return GetNextEntryNOOP(rid);
    }

    /* finished scan, no more siblings to be read. */
    return IX_EOF;
} // }}}

RC IX_IndexScan::CloseScan() // {{{
{
    op = NO_OP;
    start_pid = 0;
    next_pid = 0;
    n_matches = 0;
    last_node_next = PF_PAGE_SIZE;
    last_node_pid = 0;
    k_int = k_float = k_len = 0;

    cond_attr.name = "";

    return 0;
} // }}}
