#include "ix.h"

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

    /* return OpenScan(indexHandle, compOp, value, data_page_id) */
    return -1;
} // }}}


/* a test verson, where we assume search(key) yields anchor_pid the first data node. */ // {{{
RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle, CompOp compOp, void *value, int anchor_pid)
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

    /* NO_OP makes no sense. */
    if(compOp == NO_OP)
        return -1;

    

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

    /* 1 since -1 is IX_EOF. */
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
    while (last_node_next < n_entries)
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
    while (last_node_next < n_entries)
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
    while (last_node_next < n_entries)
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

    /* scanned node and didn't find any matches, which means no matches in data. */
    if ((start_pid == next_pid) && (n_matches == 0))
        return IX_EOF;

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
    while (last_node_next < n_entries)
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

    /* scanned node and didn't find any matches, which means no matches in data. */
    if ((start_pid == next_pid) && (n_matches == 0))
        return IX_EOF;

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
