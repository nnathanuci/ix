#include <fstream>
#include <iostream>
#include <cassert>
#include <cstring>

#include "pf.h"

using namespace std;

#define ZERO_ASSERT(x) assert((x) == 0)
#define NONZERO_ASSERT(x) assert((x) != 0)

void pfTest_CreateDelete(PF_Manager *pf) // {{{
{
    const char *fn = "createdelete.test";

    ZERO_ASSERT(pf->CreateFile(fn));
    cout << "PASS: create file" << endl;

    NONZERO_ASSERT(pf->CreateFile(fn));
    cout << "PASS: create file (already exists) [expecting error]" << endl;

    ZERO_ASSERT(pf->DestroyFile(fn));
    cout << "PASS: delete file" << endl;

    /* delete file - FAIL (file doesnt exist) */
    NONZERO_ASSERT(pf->DestroyFile(fn));
    cout << "PASS: delete file (no longer exists) [expecting error]" << endl;

    cout << endl;
} // }}}

void pfTest_OpenClose(PF_Manager *pf) // {{{
{
    PF_FileHandle pf_handle, pf_handle2;

    const char *fn = "openclose.test";
    const char *fn2 = "openclose2.test";

    NONZERO_ASSERT(pf->OpenFile(fn, pf_handle));
    cout << "PASS: open file (doesnt exist) [expecting error]" << endl;

    NONZERO_ASSERT(pf->CloseFile(pf_handle));
    cout << "PASS: close file (no file opened) [expecting error]" << endl;

    ZERO_ASSERT(pf->CreateFile(fn));
    cout << "PASS: create file" << endl;

    ZERO_ASSERT(pf->CreateFile(fn2));
    cout << "PASS: create file2" << endl;

    ZERO_ASSERT(pf->OpenFile(fn, pf_handle));
    cout << "PASS: open file" << endl;

    NONZERO_ASSERT(pf->OpenFile(fn2, pf_handle));
    cout << "PASS: open file2 (using already open handle) [expecting error]" << endl;

    ZERO_ASSERT(pf->OpenFile(fn, pf_handle2));
    cout << "PASS: open file (using new handler)" << endl;

    ZERO_ASSERT(pf->CloseFile(pf_handle2));
    cout << "PASS: close file (using new handler)" << endl;

    ZERO_ASSERT(pf->CloseFile(pf_handle));
    cout << "PASS: close file" << endl;

    NONZERO_ASSERT(pf->CloseFile(pf_handle));
    cout << "PASS: close file (already closed) [expecting error]" << endl;

    ZERO_ASSERT(pf->DestroyFile(fn));
    cout << "PASS: delete file" << endl;

    ZERO_ASSERT(pf->DestroyFile(fn2));
    cout << "PASS: delete file2" << endl;

    cout << endl;
} // }}}

void pfTest_PageIO(PF_Manager *pf) // {{{const 
{
    PF_FileHandle pf_handle;

    const char *fn = "readwrite.test";

    unsigned char pat1 = 0xAB;
    unsigned char pat2 = 0xCD;

    unsigned char buf_write[PF_PAGE_SIZE];
    unsigned char buf_read[PF_PAGE_SIZE];

    /* clear data buffer */
    memset(buf_write, 0, PF_PAGE_SIZE);
    memset(buf_read, 0, PF_PAGE_SIZE);

    ZERO_ASSERT(pf->CreateFile(fn));
    cout << "PASS: create file" << endl;

    ZERO_ASSERT(pf->OpenFile(fn, pf_handle));
    cout << "PASS: open file" << endl;

    /* set pattern to AB. */
    memset(buf_write, pat1, PF_PAGE_SIZE);

    assert(pf_handle.GetNumberOfPages() == 0);
    cout << "PASS: num_pages = 0" << endl;

    NONZERO_ASSERT(pf_handle.WritePage(1, (void *) buf_write));
    cout << "PASS: write page 1 (pat: 0xAB) (doesnt exist) [expecting error]" << endl;

    NONZERO_ASSERT(pf_handle.ReadPage(0, (void *) buf_read));
    cout << "PASS: read page 0 (pat: 0xAB) (doesnt exist) [expecting error]" << endl;

    ZERO_ASSERT(pf_handle.AppendPage((void *) buf_write));
    cout << "PASS: append to page 0 (pat: 0xAB)" << endl;

    assert(pf_handle.GetNumberOfPages() == 1);
    cout << "PASS: num_pages = 1" << endl;

    NONZERO_ASSERT(pf_handle.WritePage(1, (void *) buf_write));
    cout << "PASS: write page 1 (pat: 0xAB) (doesnt exist) [expecting error]" << endl;

    ZERO_ASSERT(pf_handle.ReadPage(0, (void *) buf_read));
    ZERO_ASSERT(memcmp(buf_read, buf_write, PF_PAGE_SIZE));
    memset(buf_read, 0, PF_PAGE_SIZE);
    cout << "PASS: read page 0 (pat: 0xAB)" << endl;

    /* set pattern to CD. */
    memset(buf_write, pat2, PF_PAGE_SIZE);
    ZERO_ASSERT(pf_handle.AppendPage((void *) buf_write));
    cout << "PASS: append to page 0 (pat: 0xCD)" << endl;

    assert(pf_handle.GetNumberOfPages() == 2);
    cout << "PASS: num_pages = 2" << endl;

    /* set pattern to AB. */
    memset(buf_write, pat1, PF_PAGE_SIZE);
    ZERO_ASSERT(pf_handle.ReadPage(0, (void *) buf_read));
    ZERO_ASSERT(memcmp(buf_read, buf_write, PF_PAGE_SIZE));
    memset(buf_read, 0, PF_PAGE_SIZE);
    cout << "PASS: read page 0 (pat: 0xAB)" << endl;

    /* set pattern to CD. */
    memset(buf_write, pat2, PF_PAGE_SIZE);
    ZERO_ASSERT(pf_handle.ReadPage(1, (void *) buf_read));
    ZERO_ASSERT(memcmp(buf_read, buf_write, PF_PAGE_SIZE));
    memset(buf_read, 0, PF_PAGE_SIZE);
    cout << "PASS: read page 1 (pat: 0xCD)" << endl;

    ZERO_ASSERT(pf_handle.WritePage(0, (void *) buf_write));
    cout << "PASS: write page 0 (pat: 0xCD)" << endl;

    ZERO_ASSERT(pf_handle.ReadPage(0, (void *) buf_read));
    ZERO_ASSERT(memcmp(buf_read, buf_write, PF_PAGE_SIZE));
    memset(buf_read, 0, PF_PAGE_SIZE);
    cout << "PASS: read page 0 (pat: 0xCD)" << endl;

    ZERO_ASSERT(pf->CloseFile(pf_handle));
    cout << "PASS: close file" << endl;

    ZERO_ASSERT(pf->DestroyFile(fn));
    cout << "PASS: delete file" << endl;

    cout << endl;
} // }}}

void pfTest()
{
    const char *testFiles[4] = { "createdelete.test", "openclose.test", "openclose2.test", "readwrite.test" };
    PF_Manager *pf = PF_Manager::Instance();

    /* cleanup */
    for(int i = 0; i < 4; i++)
        remove(testFiles[i]);

    cout << "CreateFile and DeleteFile Tests" << endl << endl;
    pfTest_CreateDelete(pf);

    cout << "OpenFile and CloseFile Tests" << endl << endl;
    pfTest_OpenClose(pf);

    cout << "ReadPage, WritePage, AppendPage, and GetNumberOfPages Tests" << endl << endl;
    pfTest_PageIO(pf);

}


int main() 
{
    cout << "test..." << endl << endl;
    
    pfTest();
    // other tests go here
    
    cout << "OK" << endl;
}
