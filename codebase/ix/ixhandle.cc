#include "ix.h"

IX_IndexHandle::IX_IndexHandle()
{
    pf = PF_Manager::Instance();
}

IX_IndexHandle::~IX_IndexHandle()
{
    /* ignore errors, we don't care if it's not already open. */
    handle.CloseFile();
}

RC IX_IndexHandle::OpenFile(const char *fileName, Attribute &a)
{
    if(pf->OpenFile(fileName, handle))
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

RC IX_IndexHandle::NewNode(const void *data, unsigned int &pid)
{
    // old method (returns newly allocated node as the appended page.)
    //if(handle.AppendPage(data))
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

        if(ReadNode(i, buf))
            return -1;

        /* check if the node is deleted. */
	type = *((unsigned int *) &(buf[PF_PAGE_SIZE - 8]));

        /* found deleted node, [type id == 0], return the pid. */
        if(type == 0)
        {
            /* write out new node. */
            if(WriteNode(i, buf))
                 return -1;

            /* return page id of new node. */
            pid = i;

            return 0;
        }
    }

    /* no unused nodes found, append a page with the new node, and return the last index. */
    if(handle.AppendPage(data))
        return -1;

    pid = GetNumberOfPages() - 1;

    return 0;    
}
