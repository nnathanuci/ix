#include "pf.h"
#include <cstdio>

PF_Manager* PF_Manager::_pf_manager = 0;


PF_Manager* PF_Manager::Instance()
{
    if(!_pf_manager)
        _pf_manager = new PF_Manager();

    return _pf_manager;
}


PF_Manager::PF_Manager()
{
}


PF_Manager::~PF_Manager()
{
}

    
RC PF_Manager::CreateFile(const char *fileName)
{
    FILE *f = NULL;

    /* check if file exists */
    if((f = fopen(fileName, "rb")))
    {
        /* file exists, close and return. */
        fclose(f);
        return -1;
    }

    /* create file and close it immediately, if both successful, then return. */
    if((f = fopen(fileName, "wb+")) && !fclose(f))
        return 0;

    /* error */
    return -1;
}


RC PF_Manager::DestroyFile(const char *fileName)
{
    if(!remove(fileName))
        return 0;

    /* error */
    return -1;
}


RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle)
{
    return fileHandle.OpenFile(fileName);
}


RC PF_Manager::CloseFile(PF_FileHandle &fileHandle)
{
    return fileHandle.CloseFile();
}


PF_FileHandle::PF_FileHandle()
{
    handle = NULL;
}
 

PF_FileHandle::~PF_FileHandle()
{
}


RC PF_FileHandle::OpenFile(const char *fileName)
{
    /* filehandle already has a file open, return error. */
    if(handle)
        return -1;

    if((handle = fopen(fileName, "rb+")))
        return 0;

    /* error */
    return -1;
}


RC PF_FileHandle::CloseFile()
{
    /* no handle to close should result in an error. */
    if(!handle)
        return -1;

    if(!fclose(handle))
    {
        handle = NULL;
        return 0;
    }

    /* error */
    return -1;
}

RC PF_FileHandle::TruncateFile(const char *tablename)
{
    /* no handle open, therefore cannot truncate. */
    if(!handle)
        return -1;

    /* close the stream. */
    if(!fclose(handle))
        handle = NULL;
    else
        return -1;

    /* open the stream, truncate the file. */
    if(handle = fopen(tablename, "wb+"))
        return 0;

    /* error */
    return -1;
}

RC PF_FileHandle::ReadPage(PageNum pageNum, void *data)
{
    unsigned lastPage;

    /* no pages exist. */
    if(GetNumberOfPages() == 0)
        return -1;

    /* last page index. */
    lastPage = GetNumberOfPages() - 1;

    /* make sure the page is accessible in the current range of pages. */
    if(pageNum > lastPage)
        return -1;

    /* seek to page location. */
    fseek(handle, ((long) (pageNum * PF_PAGE_SIZE)), SEEK_SET);

    /* read one page object, fread returns 1. */
    if(fread(data, PF_PAGE_SIZE, 1, handle) == 1)
        return 0;

    return -1;
}


RC PF_FileHandle::WritePage(PageNum pageNum, const void *data)
{
    unsigned lastPage;

    /* no pages exist. */
    if(GetNumberOfPages() == 0)
        return -1;

    /* last page index. */
    lastPage = GetNumberOfPages() - 1;

    /* make sure the page is in the current range of pages. */
    if(pageNum > lastPage)
        return -1;

    /* seek to page location. */
    fseek(handle, ((long) (pageNum * PF_PAGE_SIZE)), SEEK_SET);

    /* write one page object, then commit to disk */
    if((fwrite(data, PF_PAGE_SIZE, 1, handle) == 1) && !fflush(handle))
        return 0;

    return -1;
}


RC PF_FileHandle::AppendPage(const void *data)
{
    /* The new page will start at the index of the number of pages.  */
    unsigned int newPage = GetNumberOfPages();

    /* seek to end. */
    fseek(handle, ((long) (newPage * PF_PAGE_SIZE)), SEEK_SET);

    /* write one page object, commit to disk. */
    if((fwrite(data, PF_PAGE_SIZE, 1, handle) == 1) && !fflush(handle))
        return 0;

    return -1;
}


unsigned PF_FileHandle::GetNumberOfPages()
{
    long fileSize;

    if(fseek(handle, 0L, SEEK_END) == -1)
        return -1;

    if((fileSize = ftell(handle)) == -1)
        return -1;

    /* ignore trailing data that's not on a page boundary. */
    return ((unsigned) (fileSize / PF_PAGE_SIZE));
}
