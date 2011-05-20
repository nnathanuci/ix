#include "ix.h"
#include <iomanip>

IX_IndexHandle::IX_IndexHandle()
{
    pf = PF_Manager::Instance();
    in_use = false;
}

IX_IndexHandle::~IX_IndexHandle()
{
}

RC IX_IndexHandle::OpenFile(const char *fileName, Attribute &a)
{
    if (pf->OpenFile(fileName, handle))
        return -1;

    /* set the internal attribute. */
    attr = a;

    return 0;
}

RC IX_IndexHandle::CloseFile()
{
    return(pf->CloseFile(handle));
}

unsigned IX_IndexHandle::GetNumberOfPages()
{
    return handle.GetNumberOfPages();
}

RC IX_IndexHandle::ReadNode(unsigned int pid, const void *data)
{
    return handle.ReadPage(pid, (void *) data);
}

RC IX_IndexHandle::WriteNode(unsigned int pid, const void *data)
{
    return handle.WritePage(pid, data);
}

RC IX_IndexHandle::DeleteNode(unsigned int pid, const void *data)
{
    /* create a buffer of all 0. */
    char buf[PF_PAGE_SIZE] = {0};
    *((unsigned int *) &buf[DUMP_TYPE]) = DUMP_TYPE_DELETED;

    /* better space management can be done here by deleting nodes when pid == (GetNumberOfPages()-1). */
    return WriteNode(pid, buf);
}

RC IX_IndexHandle::NewNode(const void *data, unsigned int &pid)
{
    // old method (returns newly allocated node as the appended page.)
    //if (handle.AppendPage(data))
    //    return -1;
    //
    ///* return the page id. */
    //pid = GetNumberOfPages() - 1;
    //
    //return 0;


    for (unsigned int i=0; i<GetNumberOfPages(); i++)
    {
        char buf[PF_PAGE_SIZE];
	int type;

        if (ReadNode(i, buf))
            return -1;

        /* check if the node is deleted. */
	type = *((unsigned int *) &(buf[PF_PAGE_SIZE - 8]));

        /* found deleted node, return the pid. */
        if (type == DUMP_TYPE_DELETED)
        {
            /* write out new node. */
            if (WriteNode(i, data))
                 return -1;

            /* return page id of new node. */
            pid = i;

            return 0;
        }
    }

    /* no unused nodes found, append a page with the new node, and return the last index. */
    if (handle.AppendPage(data))
        return -1;

    pid = GetNumberOfPages() - 1;

    return 0;    
}

// moved to ix.h, but stays here for handy reference.
///* used only for the dump routines. */
//#define DUMP_TYPE_DATA (0)
//#define DUMP_TYPE_INDEX (1)
//#define DUMP_TYPE_DELETED (2)
//#define DUMP_NO_PID (0)
//
//
///* header fields */
//#define DUMP_HEADER_START (PF_PAGE_SIZE - 20)
//#define DUMP_LEFT_PID (DUMP_HEADER_START+0)
//#define DUMP_RIGHT_PID (DUMP_HEADER_START+4)
//#define DUMP_FREE_OFFSET (DUMP_HEADER_START+8)
//#define DUMP_TYPE (DUMP_HEADER_START+12)
//#define DUMP_NUM_ENTRIES (DUMP_HEADER_START+16)
//
///* macros to get the values. */
//#define DUMP_GET_LEFT_PID(buf_start) (*((unsigned int *) &((buf_start)[DUMP_LEFT_PID])))
//#define DUMP_GET_RIGHT_PID(buf_start) (*((unsigned int *) &((buf_start)[DUMP_RIGHT_PID])))
//#define DUMP_GET_FREE_OFFSET(buf_start) (*((unsigned int *) &((buf_start)[DUMP_FREE_OFFSET])))
//#define DUMP_GET_TYPE(buf_start) (*((unsigned int *) &((buf_start)[DUMP_TYPE])))
//#define DUMP_GET_NUM_ENTRIES(buf_start) (*((unsigned int *) &((buf_start)[DUMP_NUM_ENTRIES])))
//#define DUMP_GET_FREE_SPACE(buf_start) (DUMP_HEADER_START - DUMP_GET_FREE_OFFSET(buf_start))


/* useful routines for outputting a node. */
RC IX_IndexHandle::DumpNode(unsigned int pid, unsigned int verbose)
{
    char buf[PF_PAGE_SIZE];

    if (handle.ReadPage(pid, buf))
        return -1;

    DumpNode(buf, pid, verbose);

    return 0;
}

void IX_IndexHandle::DumpNodeTerse(const char *node, unsigned int pid)
{
    unsigned int n_entries = DUMP_GET_NUM_ENTRIES(node);
    unsigned int type = DUMP_GET_TYPE(node);
    unsigned int left_pid = DUMP_GET_LEFT_PID(node);
    unsigned int right_pid = DUMP_GET_RIGHT_PID(node);
    unsigned int free_offset = DUMP_GET_FREE_OFFSET(node);
    unsigned int free_space = DUMP_GET_FREE_SPACE(node);

    if(type == DUMP_TYPE_DELETED)
    {
        cout << "[ pid:" << pid << " type:0 ]" << endl;
        return;
    }

    cout << "[ ";
    cout << "pid:" << pid << "  ";
    cout << "type:" << type << "  ";
    cout << "num_entries:" << n_entries << "  ";
    cout << "free_off:" << free_offset << "  ";
    cout << "space:" << free_space << "  ";
    cout << "left:" << left_pid;

    if(type == DUMP_TYPE_DATA)
        cout << "  right:" << right_pid;

    cout << " ]" << endl;
}


/* verbose=0 means off, verbose=1 means everything but entries, verbose>1 means dump all the entries. */
void IX_IndexHandle::DumpNode(const char *node, unsigned int pid, unsigned int verbose)
{
    unsigned int n_entries = DUMP_GET_NUM_ENTRIES(node);
    unsigned int type = DUMP_GET_TYPE(node);
    unsigned int left_pid = DUMP_GET_LEFT_PID(node);
    unsigned int right_pid = DUMP_GET_RIGHT_PID(node);
    unsigned int free_offset = DUMP_GET_FREE_OFFSET(node);
    unsigned int free_space = DUMP_GET_FREE_SPACE(node);
    
    if (verbose == 0)
    {
        DumpNodeTerse(node, pid);
        return;
    }

    if (verbose > 1)
    {
        if (attr.type == TypeInt)
            cout << "[ BEGIN node:int  (pid:" << pid << ") ]" << endl;
        else if (attr.type == TypeReal)
            cout << "[ BEGIN node:float  (pid:" << pid << ") ]" << endl;
        else if (attr.type == TypeVarChar)
            cout << "[ BEGIN node:char  (pid:" << pid << ") ]" << endl;
    }
    else
        cout << "[ BEGIN node  (pid:" << pid << ") ]" << endl;

    /* dump type. */ // {{{
    if (verbose > 1)
    {
        if (type == DUMP_TYPE_DELETED)
            cout << "   type: deleted" << endl;
        else if (type == DUMP_TYPE_INDEX)
            cout << "   type: index" << endl;
        else if (type == DUMP_TYPE_DATA)
            cout << "   type: data" << endl;
    }
    else
    {
        cout << "   type: " << type << endl;
    }

    // if a deleted node, we're done
    if (type == DUMP_TYPE_DELETED)
        goto end;
    // }}}

    /* number of entries, free offset, free space */ // {{{
    cout << "   num entries: " << n_entries << endl;
    cout << "   free offset: " << free_offset << endl;
    cout << "   free space: " << free_space << endl;
    // }}}

    /* left/right child. */ // {{{
    if (type == DUMP_TYPE_INDEX)
    {
        if(verbose > 1)
            if(left_pid == DUMP_NO_PID)
                cout << "   left child: " << "null" << endl;
            else
                cout << "   left child: " << left_pid << endl;
        else
            cout << "   left child: " << left_pid << endl;
    }
    else if (type == DUMP_TYPE_DATA)
    {
        if(verbose > 1)
        {
            if(left_pid == DUMP_NO_PID)
                cout << "   left child: " << "null" << endl;
            else
                cout << "   left child: " << left_pid << endl;

            if(right_pid == DUMP_NO_PID)
                cout << "   right child: " << "null" << endl;
            else
                cout << "   right child: " << right_pid << endl;
        }
        else
        {
            cout << "   left child: " << left_pid << endl;
            cout << "   right child: " << right_pid << endl;
        }
    }
    // }}}

    /* dump entries (index or data) */ // {{{
    if (verbose > 2) 
    {
        cout << endl;

        if(type == DUMP_TYPE_DATA)
            cout << "   [ BEGIN entries (key, page, slot) ]" << endl;
        else
            cout << "   [ BEGIN entries (key, page) ]" << endl;

        if (attr.type != TypeVarChar)
        {
            for(unsigned int i=0; i<n_entries; i++)
            {
                int k_int;
                float k_float;
                unsigned int page_id;
                unsigned int slot_id;

                // index entries are: key, page_id; data entries are: key, page_id, slot_id
                unsigned int entry_size = (type == DUMP_TYPE_INDEX) ? (8) : (12);

                /* determine the type when it comes time to print, assume that key could be either int/float for now. */
                k_int = *((int *) &node[entry_size*i]);
                k_float = *((float *) &node[entry_size*i]);
                page_id = *((unsigned int *) &node[entry_size*i+4]);

                if (type == DUMP_TYPE_DATA)
                    slot_id = *((unsigned int *) &node[entry_size*i+8]);

                cout << "       ";
                if(attr.type == TypeInt)
                    cout << setw(-8) << k_int;
                else if(attr.type == TypeReal)
                    cout << setiosflags(ios::fixed) << k_float;

                cout << "  ";
	        cout << setw(-8) << page_id;

                if (type == DUMP_TYPE_DATA)
                {
                    cout << "  ";
	            cout << setw(-8) << slot_id;
                }

                cout << endl;
            }
        }
        else if (attr.type == TypeVarChar)
        {
            cout << "NOT IMPLEMENTED YET" << endl;
        }

        cout << "   [ END  entries ]" << endl;
    }
    // }}}

end:
    if (verbose > 1)
    {
        if (attr.type == TypeInt)
            cout << "[ END node:int  (pid:" << pid << ") ]" << endl;
        else if (attr.type == TypeReal)
            cout << "[ END node:float  (pid:" << pid << ") ]" << endl;
        else if (attr.type == TypeVarChar)
            cout << "[ END node:char  (pid:" << pid << ") ]" << endl;
    }
    else
        cout << "[ END node  (pid:" << pid << ") ]" << endl;
}
