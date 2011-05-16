#include <fstream>
#include <iostream>
#include <cassert>
#include <string>
#include <sstream>

#include "ix.h"

#define ZERO_ASSERT(x) assert((x) == 0)
#define NONZERO_ASSERT(x) assert((x) != 0)

#define MAX_ENTRIES  ((PF_PAGE_SIZE-20)/12)

#define DUMP_KEYVAL(i,r) do { cout << "(" << i << "," << r.pageNum << "," << r.slotNum << ")" << endl; } while (0)

int debug = 0;

using namespace std;

void cleanup() // {{{
{
  const char *files[16] = { "systemcatalog", "t1.a1", "t1.a2", "t1", "data_test_eq", "data_test_eq.a1", "data_test_ne", "data_test_ne.a1", "data_test_gt", "data_test_gt.a1","data_test_ge","data_test_ge.a1","data_test_lt", "data_test_lt.a1","data_test_le", "data_test_le.a1" };


  for(int i = 0; i < 16; i++)
    remove(files[i]);

} // }}}

string output_schema(string table_name, vector<Attribute> &attrs) // {{{
{
    stringstream ss;
    ss << table_name;

    for (unsigned int i = 0; i < attrs.size(); i++)
    {
        ss << " " << attrs[i].name << ":";
        switch (attrs[i].type)
        {
            case TypeInt:
                 ss << "Int";
                 break;

            case TypeReal:
                 ss << "Real";
                 break;

            case TypeVarChar:
                 ss << "VarChar(" << attrs[i].length << ")";
                 break;
        }
    }

    return ss.str();
} // }}}

void ixTest_Insert(IX_Manager *ixmgr) // {{{
{
    RM *rm = RM::Instance();
    IX_IndexHandle handle;
    IX_IndexScan scan;

    string t1 = "t1";
    vector<Attribute> t1_attrs;
    t1_attrs.push_back((struct Attribute) { "a1", TypeInt, 0 });
    t1_attrs.push_back((struct Attribute) { "a2", TypeReal, 0 });
    t1_attrs.push_back((struct Attribute) { "a3", TypeVarChar, 500 });

    int key = 0;
    RID rid = {0,0};
    RID aux_rid;

    /* create table, create index, insert an entry, scan/retrieve entry, delete index, delete table. */ // {{{
    cout << "\n[ index creation,insertion,scan test, 1 entry ]" << endl;
    ZERO_ASSERT(rm->createTable(t1, t1_attrs));
    cout << "PASS: createTable(" << output_schema(t1, t1_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex("t1", "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex("t1", "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;

    key = 1, rid.pageNum = 200, rid.slotNum = 3000;
    ZERO_ASSERT(handle.InsertEntry(&key, rid));
    cout << "PASS: h.InsertEntry(1,{200,3000})" << endl;

    ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &key));
    cout << "PASS: scan.OpenScan(h,=,1)" << endl;

    ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(key, aux_rid);
    assert(aux_rid.pageNum == 200 && aux_rid.slotNum == 3000);
    cout << "PASS: scan.GetNextEntry(aux_rid) && aux_rid == {200,3000}" << endl;
    
    assert(scan.GetNextEntry(aux_rid) == IX_EOF);
    cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

    ZERO_ASSERT(scan.CloseScan());
    cout << "PASS: scan.CloseScan()" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex("t1", "a1"));
    cout << "PASS: DestroyIndex(t1,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(t1));
    cout << "PASS: deleteTable(" << t1 << ")" << endl;
    // }}}
} // }}}

void ixTest_data_test_eq(IX_Manager *ixmgr) // {{{
{
    RM *rm = RM::Instance();
    IX_IndexHandle handle;
    IX_IndexScan scan;

    string data_test_eq = "data_test_eq";
    vector<Attribute> data_test_eq_attrs;
    data_test_eq_attrs.push_back((struct Attribute) { "a1", TypeInt, 0 });
    data_test_eq_attrs.push_back((struct Attribute) { "a2", TypeInt, 0 });


    /* single key equality test [k1 k2 ... k* ... kn]. */ // {{{
    cout << "\n[ data_test_eq - single key equality test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_eq, data_test_eq_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_eq, data_test_eq_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_eq, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_eq, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;

    {
        /* create a data node (339 entries): [ (0,0,0) (10,100,1000) (20,200,2000) ... (3380, 33800, 338000) ] */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;

        for(int i=0; i<MAX_ENTRIES; i++, offset += 12)
        {
            (*((int *) &new_buf[i*12])) = i*10;
            (*((unsigned int *) &new_buf[i*12+4])) = i*100;
            (*((unsigned int *) &new_buf[i*12+8])) = i*1000;
        }

        *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries
    
        ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
        assert(new_pid == 1);
        cout << "PASS: handle.NewNode([data]) && new_pid == 1" << endl;
    }

    {
        int key = 90;
        RID aux_rid = {0, 0};

        /* equality test */
        ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,=," << key << ")" << endl;

        ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(key, aux_rid);
        assert((aux_rid.pageNum == (unsigned int) key*10) && (aux_rid.slotNum == (unsigned int) key*100));
        cout << "PASS: scan.GetNextEntry(aux_rid) && aux_rid == {" << key*100 << "," << key*1000 << "}" << endl;
        
        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_eq, "a1"));
    cout << "PASS: DestroyIndex(data_test_eq,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_eq));
    cout << "PASS: deleteTable(" << data_test_eq << ")" << endl;
    // }}}

    /* single key equality test (missing key) [ki ki+1 ... kn], key: kj, s.t. j>n or j<i. */ // {{{
    cout << "\n[ data_test_eq - single key equality test (missing key) ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_eq, data_test_eq_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_eq, data_test_eq_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_eq, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_eq, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;

    {
        /* create a data node (339 entries): [ (0,0,0) (10,100,1000) (20,200,2000) ... (3380, 33800, 338000) ] */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;

        for(int i=0; i<MAX_ENTRIES; i++, offset += 12)
        {
            (*((int *) &new_buf[i*12])) = i*10;
            (*((unsigned int *) &new_buf[i*12+4])) = i*100;
            (*((unsigned int *) &new_buf[i*12+8])) = i*1000;
        }

        *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries
    
        ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
        assert(new_pid == 1);
        cout << "PASS: handle.NewNode([data]) && new_pid == 1" << endl;
    }

    {
        int key = 25;
        RID aux_rid = {0, 0};

        /* equality test on key=20 */
        ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,=," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        RID aux_rid = {0, 0};
        int key = MAX_ENTRIES*10; /* shouldn't exist. */

        /* equality test on key=20 */
        ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,=," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_eq, "a1"));
    cout << "PASS: DestroyIndex(data_test_eq,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_eq));
    cout << "PASS: deleteTable(" << data_test_eq << ")" << endl;
    // }}}

    /* single key equality test [k1 k2 ... k* ... kn], key: kn */ // {{{
    cout << "\n[ data_test_eq - single key equality test (last key) ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_eq, data_test_eq_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_eq, data_test_eq_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_eq, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_eq, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;

    {
        /* create a data node (339 entries): [ (0,0,0) (10,100,1000) (20,200,2000) ... (3380, 33800, 338000) ] */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;

        for(int i=0; i<MAX_ENTRIES; i++, offset += 12)
        {
            (*((int *) &new_buf[i*12])) = i*10;
            (*((unsigned int *) &new_buf[i*12+4])) = i*100;
            (*((unsigned int *) &new_buf[i*12+8])) = i*1000;
        }

        *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries
    
        ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
        assert(new_pid == 1);
        cout << "PASS: handle.NewNode([data]) && new_pid == 1" << endl;
    }

    {
        int key = 3380;
        RID aux_rid = {0, 0};

        /* equality test */
        ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,=," << key << ")" << endl;

        ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(key, aux_rid);
        assert((aux_rid.pageNum == (unsigned int) key*10) && (aux_rid.slotNum == (unsigned int) key*100));
        cout << "PASS: scan.GetNextEntry(aux_rid) && aux_rid == {" << key*100 << "," << key*1000 << "}" << endl;
        
        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_eq, "a1"));
    cout << "PASS: DestroyIndex(data_test_eq,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_eq));
    cout << "PASS: deleteTable(" << data_test_eq << ")" << endl;
    // }}}

    /* duplicate key equality test. [k1 k1 k2 k2 ... kn] key: k1, k8 */ // {{{
    cout << "\n[ data_test_eq - duplicate key equality test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_eq, data_test_eq_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_eq, data_test_eq_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_eq, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_eq, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;

    {
        /* create a data node (339 entries): [ (0,0,0) (0,1,1) (10,100,1000) (10,101,1001)... (3380, 33801, 338001) ] */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;

        for(int i=0; i<MAX_ENTRIES; i+=2)
        {
            (*((int *) &new_buf[i*12])) = i*10;
            (*((unsigned int *) &new_buf[i*12+4])) = i*100;
            (*((unsigned int *) &new_buf[i*12+8])) = i*1000;

            /* create a second copy of the key [note the post increment is +2]. */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                (*((int *) &new_buf[(i+1)*12])) = i*10;
                (*((unsigned int *) &new_buf[(i+1)*12+4])) = i*100 + 1;
                (*((unsigned int *) &new_buf[(i+1)*12+8])) = i*1000 + 1;
            }
        }

        offset = MAX_ENTRIES*12;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries
    
        ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
        assert(new_pid == 1);
        cout << "PASS: handle.NewNode([data]) && new_pid == 1" << endl;
    }

    {
        // test two duplicate entries

        int key = 0;
        RID aux_rid = {0, 0};

        ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,=," << key << ")" << endl;

        ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(key, aux_rid);
        assert((aux_rid.pageNum == (unsigned int) key*10) && (aux_rid.slotNum == (unsigned int) key*100));
        cout << "PASS: scan.GetNextEntry(aux_rid) && aux_rid == {" << key*100 << "," << key*1000 << "}" << endl;

        ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(key, aux_rid);
        assert((aux_rid.pageNum == (unsigned int) (key*10+1)) && (aux_rid.slotNum == (unsigned int) (key*100+1)));
        cout << "PASS: scan.GetNextEntry(aux_rid) && aux_rid == {" << key*100 << "," << key*1000 << "}" << endl;
        
        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;

        key = 80;

        ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,=," << key << ")" << endl;

        ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(key, aux_rid);
        assert((aux_rid.pageNum == (unsigned int) key*10) && (aux_rid.slotNum == (unsigned int) key*100));
        cout << "PASS: scan.GetNextEntry(aux_rid) && aux_rid == {" << key*100 << "," << key*1000 << "}" << endl;

        ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(key, aux_rid);
        assert((aux_rid.pageNum == (unsigned int) (key*10+1)) && (aux_rid.slotNum == (unsigned int) (key*100+1)));
        cout << "PASS: scan.GetNextEntry(aux_rid) && aux_rid == {" << key*100+1 << "," << key*1000+1 << "}" << endl;
        
        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;

    }

    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_eq, "a1"));
    cout << "PASS: DestroyIndex(data_test_eq,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_eq));
    cout << "PASS: deleteTable(" << data_test_eq << ")" << endl;
    // }}}


} // }}}

void ixTest_data_test_ne(IX_Manager *ixmgr) // {{{
{
    RM *rm = RM::Instance();
    IX_IndexHandle handle;
    IX_IndexScan scan;

    string data_test_ne = "data_test_ne";
    vector<Attribute> data_test_ne_attrs;
    data_test_ne_attrs.push_back((struct Attribute) { "a1", TypeInt, 0 });
    data_test_ne_attrs.push_back((struct Attribute) { "a2", TypeInt, 0 });


    /* single data node ne test [k1 k2 ... k* ... kn] */ // {{{
    cout << "\n[ data_test_ne - single data node ne test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_ne, data_test_ne_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_ne, data_test_ne_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_ne, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_ne, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;

    {
        /* create a data node (339 entries): [ (0,0,0) (1,10,100) (2,20,200) ... (338, 3380, 33800) ] */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;

        for(int i=0; i<MAX_ENTRIES; i++, offset += 12)
        {
            (*((int *) &new_buf[i*12])) = i;
            (*((unsigned int *) &new_buf[i*12+4])) = i*10;
            (*((unsigned int *) &new_buf[i*12+8])) = i*100;
        }

        *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries
    
        ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
        assert(new_pid == 1);
        cout << "PASS: handle.NewNode([data]) && new_pid == 1" << endl;
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }



    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_ne, "a1"));
    cout << "PASS: DestroyIndex(data_test_ne,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_ne));
    cout << "PASS: deleteTable(" << data_test_ne << ")" << endl;
    // }}}

    /* single data node (duplicate key) ne test [k1 k1 k2 k2 ... k* k* ... kn kn] */ // {{{
    cout << "\n[ data_test_ne - single data node (duplicate key) ne test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_ne, data_test_ne_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_ne, data_test_ne_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_ne, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_ne, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;

    {
        /* create a data node (339 entries): [ (0,0,0) (0,1,1) (1,10,100) (1,11,101)... (338, 3381, 33801) ] */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;

        for(int i=0; i<MAX_ENTRIES; i+=2)
        {
            (*((int *) &new_buf[i*12])) = i;
            (*((unsigned int *) &new_buf[i*12+4])) = i*10;
            (*((unsigned int *) &new_buf[i*12+8])) = i*100;

            /* create a second copy of the key [note the post increment is +2]. */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                (*((int *) &new_buf[(i+1)*12])) = i;
                (*((unsigned int *) &new_buf[(i+1)*12+4])) = i*10 + 1;
                (*((unsigned int *) &new_buf[(i+1)*12+8])) = i*100 + 1;
            }
        }

        offset = MAX_ENTRIES*12;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries
    
        ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
        assert(new_pid == 1);
        cout << "PASS: handle.NewNode([data]) && new_pid == 1" << endl;
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i+=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i+=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i+=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i+=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }



    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_ne, "a1"));
    cout << "PASS: DestroyIndex(data_test_ne,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_ne));
    cout << "PASS: deleteTable(" << data_test_ne << ")" << endl;
    // }}}

    /* two data node ne test [k11 k12 ... k1n]<-> [k21 k22 ... k2n] */ // {{{
    cout << "\n[ data_test_ne - two data node ne test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_ne, data_test_ne_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_ne, data_test_ne_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_ne, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_ne, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;


    {
        /* create two nodes. */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;
        int n_nodes = 2;
      
        for(int j=0; j<n_nodes; j++)
        {
            for(int i=0; i<MAX_ENTRIES; i++, offset += 12)
            {
                (*((int *) &new_buf[i*12])) = j*MAX_ENTRIES+i;
                (*((unsigned int *) &new_buf[i*12+4])) = (j*MAX_ENTRIES+i)*10;
                (*((unsigned int *) &new_buf[i*12+8])) = (j*MAX_ENTRIES+i)*100;
            }

            *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = j; // left is preceding node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = j+2; // right is going to be second node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries

            /* if first node, set the left node to 0. */
            if (j == 0)
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0; // no node for left.

            /* if final node, set the right node to 0. */
            if (j == (n_nodes-1))
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0; // no node for right.
    
            ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
            assert(new_pid == (unsigned int) j+1);
            cout << "PASS: handle.NewNode([data]) && new_pid == " << j+1 << endl;
        }
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<(2*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<(2*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<(2*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 339;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<(2*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 500;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<(2*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = (2*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<(2*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<(2*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }



    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_ne, "a1"));
    cout << "PASS: DestroyIndex(data_test_ne,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_ne));
    cout << "PASS: deleteTable(" << data_test_ne << ")" << endl;
    // }}}

    /* three data node ne test [k11 k12 ... k1n]<-> [k21 k22 ... k2n] */ // {{{
    cout << "\n[ data_test_ne - three data node ne test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_ne, data_test_ne_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_ne, data_test_ne_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_ne, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_ne, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;


    {
        /* create three nodes. */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;
        int n_nodes = 3;

      
        for(int j=0; j<n_nodes; j++)
        {
            for(int i=0; i<MAX_ENTRIES; i++, offset += 12)
            {
                (*((int *) &new_buf[i*12])) = j*MAX_ENTRIES+i;
                (*((unsigned int *) &new_buf[i*12+4])) = (j*MAX_ENTRIES+i)*10;
                (*((unsigned int *) &new_buf[i*12+8])) = (j*MAX_ENTRIES+i)*100;
            }

            *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = j; // left is preceding node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = j+2; // right is going to be second node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries

            /* if first node, set the left node to 0. */
            if (j == 0)
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0; // no node for left.

            /* if final node, set the right node to 0. */
            if (j == (n_nodes-1))
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0; // no node for right.
    
            ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
            assert(new_pid == (unsigned int) j+1);
            cout << "PASS: handle.NewNode([data]) && new_pid == " << j+1 << endl;
        }
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = MAX_ENTRIES;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 500;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }


    {
        int key = (3*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 3*MAX_ENTRIES;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = (3*MAX_ENTRIES)+200;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = (3*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,!=," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is equal. */
            if (i == key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key != " << key << " and rid != {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }



    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_ne, "a1"));
    cout << "PASS: DestroyIndex(data_test_ne,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_ne));
    cout << "PASS: deleteTable(" << data_test_ne << ")" << endl;
    // }}}

} // }}}

void ixTest_data_test_gt(IX_Manager *ixmgr) // {{{
{
    RM *rm = RM::Instance();
    IX_IndexHandle handle;
    IX_IndexScan scan;

    string data_test_gt = "data_test_gt";
    vector<Attribute> data_test_gt_attrs;
    data_test_gt_attrs.push_back((struct Attribute) { "a1", TypeInt, 0 });
    data_test_gt_attrs.push_back((struct Attribute) { "a2", TypeInt, 0 });


    /* single data node gt test [k1 k2 ... k* ... kn] */ // {{{
    cout << "\n[ data_test_gt - single data node gt test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_gt, data_test_gt_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_gt, data_test_gt_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_gt, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_gt, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;

    {
        /* create a data node (339 entries): [ (0,0,0) (1,10,100) (2,20,200) ... (338, 3380, 33800) ] */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;

        for(int i=0; i<MAX_ENTRIES; i++, offset += 12)
        {
            (*((int *) &new_buf[i*12])) = i;
            (*((unsigned int *) &new_buf[i*12+4])) = i*10;
            (*((unsigned int *) &new_buf[i*12+8])) = i*100;
        }

        *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries
    
        ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
        assert(new_pid == 1);
        cout << "PASS: handle.NewNode([data]) && new_pid == 1" << endl;
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than or equal. */
            if (i <= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than or equal. */
            if (i <= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;
        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;
        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }



    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_gt, "a1"));
    cout << "PASS: DestroyIndex(data_test_gt,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_gt));
    cout << "PASS: deleteTable(" << data_test_gt << ")" << endl;
    // }}}

    /* single data node (duplicate key) gt test [k1 k1 k2 k2 ... k* k* ... kn kn] */ // {{{
    cout << "\n[ data_test_gt - single data node (duplicate key) gt test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_gt, data_test_gt_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_gt, data_test_gt_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_gt, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_gt, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;

    {
        /* create a data node (339 entries): [ (0,0,0) (0,1,1) (1,10,100) (1,11,101)... (338, 3381, 33801) ] */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;

        for(int i=0; i<MAX_ENTRIES; i+=2)
        {
            (*((int *) &new_buf[i*12])) = i;
            (*((unsigned int *) &new_buf[i*12+4])) = i*10;
            (*((unsigned int *) &new_buf[i*12+8])) = i*100;

            /* create a second copy of the key [note the post increment is +2]. */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                (*((int *) &new_buf[(i+1)*12])) = i;
                (*((unsigned int *) &new_buf[(i+1)*12+4])) = i*10 + 1;
                (*((unsigned int *) &new_buf[(i+1)*12+8])) = i*100 + 1;
            }
        }

        offset = MAX_ENTRIES*12;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries
    
        ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
        assert(new_pid == 1);
        cout << "PASS: handle.NewNode([data]) && new_pid == 1" << endl;
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i+=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is less than or equal. */
            if (i <= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i+=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is less than or equal. */
            if (i <= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i+=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is less than or equal. */
            if (i <= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i+=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is less than or equal. */
            if (i <= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }



    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_gt, "a1"));
    cout << "PASS: DestroyIndex(data_test_gt,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_gt));
    cout << "PASS: deleteTable(" << data_test_gt << ")" << endl;
    // }}}

    /* two data node gt test [k11 k12 ... k1n]<-> [k21 k22 ... k2n] */ // {{{
    cout << "\n[ data_test_gt - two data node gt test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_gt, data_test_gt_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_gt, data_test_gt_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_gt, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_gt, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;


    {
        /* create two nodes. */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;
        int n_nodes = 2;
      
        for(int j=0; j<n_nodes; j++)
        {
            for(int i=0; i<MAX_ENTRIES; i++, offset += 12)
            {
                (*((int *) &new_buf[i*12])) = j*MAX_ENTRIES+i;
                (*((unsigned int *) &new_buf[i*12+4])) = (j*MAX_ENTRIES+i)*10;
                (*((unsigned int *) &new_buf[i*12+8])) = (j*MAX_ENTRIES+i)*100;
            }

            *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = j; // left is preceding node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = j+2; // right is going to be second node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries

            /* if first node, set the left node to 0. */
            if (j == 0)
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0; // no node for left.

            /* if final node, set the right node to 0. */
            if (j == (n_nodes-1))
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0; // no node for right.
    
            ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
            assert(new_pid == (unsigned int) j+1);
            cout << "PASS: handle.NewNode([data]) && new_pid == " << j+1 << endl;
        }
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        for(int i=0; i<(2*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than or equal. */
            if (i <= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        for(int i=0; i<(2*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than or equal. */
            if (i <= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        for(int i=0; i<(2*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than or equal. */
            if (i <= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 339;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        for(int i=0; i<(2*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than or equal. */
            if (i <= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 500;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        for(int i=0; i<(2*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than or equal. */
            if (i <= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = (2*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }



    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_gt, "a1"));
    cout << "PASS: DestroyIndex(data_test_gt,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_gt));
    cout << "PASS: deleteTable(" << data_test_gt << ")" << endl;
    // }}}

    /* three data node gt test [k11 k12 ... k1n]<-> [k21 k22 ... k2n] */ // {{{
    cout << "\n[ data_test_gt - three data node gt test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_gt, data_test_gt_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_gt, data_test_gt_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_gt, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_gt, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;


    {
        /* create three nodes. */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;
        int n_nodes = 3;

      
        for(int j=0; j<n_nodes; j++)
        {
            for(int i=0; i<MAX_ENTRIES; i++, offset += 12)
            {
                (*((int *) &new_buf[i*12])) = j*MAX_ENTRIES+i;
                (*((unsigned int *) &new_buf[i*12+4])) = (j*MAX_ENTRIES+i)*10;
                (*((unsigned int *) &new_buf[i*12+8])) = (j*MAX_ENTRIES+i)*100;
            }

            *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = j; // left is preceding node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = j+2; // right is going to be second node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries

            /* if first node, set the left node to 0. */
            if (j == 0)
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0; // no node for left.

            /* if final node, set the right node to 0. */
            if (j == (n_nodes-1))
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0; // no node for right.
    
            ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
            assert(new_pid == (unsigned int) j+1);
            cout << "PASS: handle.NewNode([data]) && new_pid == " << j+1 << endl;
        }
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than or equal. */
            if (i <= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than or equal. */
            if (i <= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than or equal. */
            if (i <= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = MAX_ENTRIES;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than or equal. */
            if (i <= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 500;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than or equal. */
            if (i <= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }


    {
        int key = (2*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 3));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than or equal. */
            if (i <= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 3*MAX_ENTRIES;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 3));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = (3*MAX_ENTRIES)+200;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 3));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = (3*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 3));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 3));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = -1000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than or equal. */
            if (i <= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_gt, "a1"));
    cout << "PASS: DestroyIndex(data_test_gt,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_gt));
    cout << "PASS: deleteTable(" << data_test_gt << ")" << endl;
    // }}}

} // }}}

void ixTest_data_test_ge(IX_Manager *ixmgr) // {{{
{
    RM *rm = RM::Instance();
    IX_IndexHandle handle;
    IX_IndexScan scan;

    string data_test_ge = "data_test_ge";
    vector<Attribute> data_test_ge_attrs;
    data_test_ge_attrs.push_back((struct Attribute) { "a1", TypeInt, 0 });
    data_test_ge_attrs.push_back((struct Attribute) { "a2", TypeInt, 0 });


    /* single data node ge test [k1 k2 ... k* ... kn] */ // {{{
    cout << "\n[ data_test_ge - single data node ge test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_ge, data_test_ge_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_ge, data_test_ge_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_ge, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_ge, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;

    {
        /* create a data node (339 entries): [ (0,0,0) (1,10,100) (2,20,200) ... (338, 3380, 33800) ] */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;

        for(int i=0; i<MAX_ENTRIES; i++, offset += 12)
        {
            (*((int *) &new_buf[i*12])) = i;
            (*((unsigned int *) &new_buf[i*12+4])) = i*10;
            (*((unsigned int *) &new_buf[i*12+8])) = i*100;
        }

        *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries
    
        ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
        assert(new_pid == 1);
        cout << "PASS: handle.NewNode([data]) && new_pid == 1" << endl;
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }



    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_ge, "a1"));
    cout << "PASS: DestroyIndex(data_test_ge,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_ge));
    cout << "PASS: deleteTable(" << data_test_ge << ")" << endl;
    // }}}

    /* single data node (duplicate key) ge test [k1 k1 k2 k2 ... k* k* ... kn kn] */ // {{{
    cout << "\n[ data_test_ge - single data node (duplicate key) ge test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_ge, data_test_ge_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_ge, data_test_ge_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_ge, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_ge, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;

    {
        /* create a data node (339 entries): [ (0,0,0) (0,1,1) (1,10,100) (1,11,101)... (338, 3381, 33801) ] */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;

        for(int i=0; i<MAX_ENTRIES; i+=2)
        {
            (*((int *) &new_buf[i*12])) = i;
            (*((unsigned int *) &new_buf[i*12+4])) = i*10;
            (*((unsigned int *) &new_buf[i*12+8])) = i*100;

            /* create a second copy of the key [note the post increment is +2]. */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                (*((int *) &new_buf[(i+1)*12])) = i;
                (*((unsigned int *) &new_buf[(i+1)*12+4])) = i*10 + 1;
                (*((unsigned int *) &new_buf[(i+1)*12+8])) = i*100 + 1;
            }
        }

        offset = MAX_ENTRIES*12;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries
    
        ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
        assert(new_pid == 1);
        cout << "PASS: handle.NewNode([data]) && new_pid == 1" << endl;
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i+=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i+=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i+=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<MAX_ENTRIES; i+=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }



    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_ge, "a1"));
    cout << "PASS: DestroyIndex(data_test_ge,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_ge));
    cout << "PASS: deleteTable(" << data_test_ge << ")" << endl;
    // }}}

    /* two data node ge test [k11 k12 ... k1n]<-> [k21 k22 ... k2n] */ // {{{
    cout << "\n[ data_test_ge - two data node ge test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_ge, data_test_ge_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_ge, data_test_ge_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_ge, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_ge, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;


    {
        /* create two nodes. */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;
        int n_nodes = 2;
      
        for(int j=0; j<n_nodes; j++)
        {
            for(int i=0; i<MAX_ENTRIES; i++, offset += 12)
            {
                (*((int *) &new_buf[i*12])) = j*MAX_ENTRIES+i;
                (*((unsigned int *) &new_buf[i*12+4])) = (j*MAX_ENTRIES+i)*10;
                (*((unsigned int *) &new_buf[i*12+8])) = (j*MAX_ENTRIES+i)*100;
            }

            *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = j; // left is preceding node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = j+2; // right is going to be second node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries

            /* if first node, set the left node to 0. */
            if (j == 0)
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0; // no node for left.

            /* if final node, set the right node to 0. */
            if (j == (n_nodes-1))
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0; // no node for right.
    
            ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
            assert(new_pid == (unsigned int) j+1);
            cout << "PASS: handle.NewNode([data]) && new_pid == " << j+1 << endl;
        }
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<(2*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<(2*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<(2*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 339;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<(2*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 500;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<(2*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = (2*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<(2*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_ge, "a1"));
    cout << "PASS: DestroyIndex(data_test_ge,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_ge));
    cout << "PASS: deleteTable(" << data_test_ge << ")" << endl;
    // }}}

    /* three data node ge test [k11 k12 ... k1n]<-> [k21 k22 ... k2n] */ // {{{
    cout << "\n[ data_test_ge - three data node ge test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_ge, data_test_ge_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_ge, data_test_ge_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_ge, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_ge, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;


    {
        /* create three nodes. */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;
        int n_nodes = 3;

      
        for(int j=0; j<n_nodes; j++)
        {
            for(int i=0; i<MAX_ENTRIES; i++, offset += 12)
            {
                (*((int *) &new_buf[i*12])) = j*MAX_ENTRIES+i;
                (*((unsigned int *) &new_buf[i*12+4])) = (j*MAX_ENTRIES+i)*10;
                (*((unsigned int *) &new_buf[i*12+8])) = (j*MAX_ENTRIES+i)*100;
            }

            *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = j; // left is preceding node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = j+2; // right is going to be second node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries

            /* if first node, set the left node to 0. */
            if (j == 0)
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0; // no node for left.

            /* if final node, set the right node to 0. */
            if (j == (n_nodes-1))
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0; // no node for right.
    
            ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
            assert(new_pid == (unsigned int) j+1);
            cout << "PASS: handle.NewNode([data]) && new_pid == " << j+1 << endl;
        }
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = MAX_ENTRIES;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 500;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }


    {
        int key = (2*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 3*MAX_ENTRIES;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 3));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = (3*MAX_ENTRIES)+200;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 3));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = (3*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 3));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;


        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 3));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = -1000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        for(int i=0; i<(3*MAX_ENTRIES); i++)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is less than. */
            if (i < key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key >= " << key << "  and rid >= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_ge, "a1"));
    cout << "PASS: DestroyIndex(data_test_ge,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_ge));
    cout << "PASS: deleteTable(" << data_test_ge << ")" << endl;
    // }}}

} // }}}

void ixTest_data_test_lt(IX_Manager *ixmgr) // {{{
{
    RM *rm = RM::Instance();
    IX_IndexHandle handle;
    IX_IndexScan scan;

    string data_test_lt = "data_test_lt";
    vector<Attribute> data_test_lt_attrs;
    data_test_lt_attrs.push_back((struct Attribute) { "a1", TypeInt, 0 });
    data_test_lt_attrs.push_back((struct Attribute) { "a2", TypeInt, 0 });

    /* single data node lt test [k1 k2 ... k* ... kn] */ // {{{
    cout << "\n[ data_test_lt - single data node lt test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_lt, data_test_lt_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_lt, data_test_lt_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_lt, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_lt, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;

    {
        /* create a data node (339 entries): [ (0,0,0) (1,10,100) (2,20,200) ... (338, 3380, 33800) ] */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;

        for(int i=0; i<MAX_ENTRIES; i++, offset += 12)
        {
            (*((int *) &new_buf[i*12])) = i;
            (*((unsigned int *) &new_buf[i*12+4])) = i*10;
            (*((unsigned int *) &new_buf[i*12+8])) = i*100;
        }

        *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries
    
        ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
        assert(new_pid == 1);
        cout << "PASS: handle.NewNode([data]) && new_pid == 1" << endl;
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = -8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }


    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_lt, "a1"));
    cout << "PASS: DestroyIndex(data_test_lt,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_lt));
    cout << "PASS: deleteTable(" << data_test_lt << ")" << endl;
    // }}}

    /* single data node (duplicate key) lt test [k1 k1 k2 k2 ... k* k* ... kn kn] */ // {{{
    cout << "\n[ data_test_lt - single data node (duplicate key) lt test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_lt, data_test_lt_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_lt, data_test_lt_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_lt, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_lt, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;

    {
        /* create a data node (339 entries): [ (0,0,0) (0,1,1) (1,10,100) (1,11,101)... (338, 3381, 33801) ] */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;

        for(int i=0; i<MAX_ENTRIES; i+=2)
        {
            (*((int *) &new_buf[i*12])) = i;
            (*((unsigned int *) &new_buf[i*12+4])) = i*10;
            (*((unsigned int *) &new_buf[i*12+8])) = i*100;

            /* create a second copy of the key [note the post increment is +2]. */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                (*((int *) &new_buf[(i+1)*12])) = i;
                (*((unsigned int *) &new_buf[(i+1)*12+4])) = i*10 + 1;
                (*((unsigned int *) &new_buf[(i+1)*12+8])) = i*100 + 1;
            }
        }

        offset = MAX_ENTRIES*12;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries
    
        ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
        assert(new_pid == 1);
        cout << "PASS: handle.NewNode([data]) && new_pid == 1" << endl;
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i-=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i-=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i-=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i-=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }


        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = -8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i-=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;
        
        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }



    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_lt, "a1"));
    cout << "PASS: DestroyIndex(data_test_lt,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_lt));
    cout << "PASS: deleteTable(" << data_test_lt << ")" << endl;
    // }}}

    /* two data node lt test [k11 k12 ... k1n]<-> [k21 k22 ... k2n] */ // {{{
    cout << "\n[ data_test_lt - two data node lt test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_lt, data_test_lt_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_lt, data_test_lt_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_lt, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_lt, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;


    {
        /* create two nodes. */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;
        int n_nodes = 2;
      
        for(int j=0; j<n_nodes; j++)
        {
            for(int i=0; i<MAX_ENTRIES; i++, offset += 12)
            {
                (*((int *) &new_buf[i*12])) = j*MAX_ENTRIES+i;
                (*((unsigned int *) &new_buf[i*12+4])) = (j*MAX_ENTRIES+i)*10;
                (*((unsigned int *) &new_buf[i*12+8])) = (j*MAX_ENTRIES+i)*100;
            }

            *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = j; // left is preceding node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = j+2; // right is going to be second node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries

            /* if first node, set the left node to 0. */
            if (j == 0)
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0; // no node for left.

            /* if final node, set the right node to 0. */
            if (j == (n_nodes-1))
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0; // no node for right.
    
            ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
            assert(new_pid == (unsigned int) j+1);
            cout << "PASS: handle.NewNode([data]) && new_pid == " << j+1 << endl;
        }
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for(int i=(2*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for(int i=(2*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for(int i=(2*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 339;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for(int i=(2*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 500;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for(int i=(2*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = (2*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for(int i=(2*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for(int i=(2*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_lt, "a1"));
    cout << "PASS: DestroyIndex(data_test_lt,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_lt));
    cout << "PASS: deleteTable(" << data_test_lt << ")" << endl;
    // }}}

    /* three data node lt test [k11 k12 ... k1n]<-> [k21 k22 ... k2n] */ // {{{
    cout << "\n[ data_test_lt - three data node lt test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_lt, data_test_lt_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_lt, data_test_lt_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_lt, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_lt, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;


    {
        /* create three nodes. */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;
        int n_nodes = 3;

      
        for(int j=0; j<n_nodes; j++)
        {
            for(int i=0; i<MAX_ENTRIES; i++, offset += 12)
            {
                (*((int *) &new_buf[i*12])) = j*MAX_ENTRIES+i;
                (*((unsigned int *) &new_buf[i*12+4])) = (j*MAX_ENTRIES+i)*10;
                (*((unsigned int *) &new_buf[i*12+8])) = (j*MAX_ENTRIES+i)*100;
            }

            *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = j; // left is preceding node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = j+2; // right is going to be second node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries

            /* if first node, set the left node to 0. */
            if (j == 0)
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0; // no node for left.

            /* if final node, set the right node to 0. */
            if (j == (n_nodes-1))
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0; // no node for right.
    
            ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
            assert(new_pid == (unsigned int) j+1);
            cout << "PASS: handle.NewNode([data]) && new_pid == " << j+1 << endl;
        }
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = MAX_ENTRIES;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 500;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }


    {
        int key = (2*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 3*MAX_ENTRIES;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 3));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = (3*MAX_ENTRIES)+200;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 3));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;


        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = (3*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 3));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 3));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = -1000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than or equal. */
            if (i >= key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key < " << key << " and rid < {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_lt, "a1"));
    cout << "PASS: DestroyIndex(data_test_lt,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_lt));
    cout << "PASS: deleteTable(" << data_test_lt << ")" << endl;
    // }}}

} // }}}

void ixTest_data_test_le(IX_Manager *ixmgr) // {{{
{
    RM *rm = RM::Instance();
    IX_IndexHandle handle;
    IX_IndexScan scan;

    string data_test_le = "data_test_le";
    vector<Attribute> data_test_le_attrs;
    data_test_le_attrs.push_back((struct Attribute) { "a1", TypeInt, 0 });
    data_test_le_attrs.push_back((struct Attribute) { "a2", TypeInt, 0 });

    /* single data node le test [k1 k2 ... k* ... kn] */ // {{{
    cout << "\n[ data_test_le - single data node le test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_le, data_test_le_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_le, data_test_le_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_le, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_le, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;

    {
        /* create a data node (339 entries): [ (0,0,0) (1,10,100) (2,20,200) ... (338, 3380, 33800) ] */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;

        for(int i=0; i<MAX_ENTRIES; i++, offset += 12)
        {
            (*((int *) &new_buf[i*12])) = i;
            (*((unsigned int *) &new_buf[i*12+4])) = i*10;
            (*((unsigned int *) &new_buf[i*12+8])) = i*100;
        }

        *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries
    
        ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
        assert(new_pid == 1);
        cout << "PASS: handle.NewNode([data]) && new_pid == 1" << endl;
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = -8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }


    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_le, "a1"));
    cout << "PASS: DestroyIndex(data_test_le,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_le));
    cout << "PASS: deleteTable(" << data_test_le << ")" << endl;
    // }}}

    /* single data node (duplicate key) le test [k1 k1 k2 k2 ... k* k* ... kn kn] */ // {{{
    cout << "\n[ data_test_le - single data node (duplicate key) le test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_le, data_test_le_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_le, data_test_le_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_le, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_le, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;

    {
        /* create a data node (339 entries): [ (0,0,0) (0,1,1) (1,10,100) (1,11,101)... (338, 3381, 33801) ] */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;

        for(int i=0; i<MAX_ENTRIES; i+=2)
        {
            (*((int *) &new_buf[i*12])) = i;
            (*((unsigned int *) &new_buf[i*12+4])) = i*10;
            (*((unsigned int *) &new_buf[i*12+8])) = i*100;

            /* create a second copy of the key [note the post increment is +2]. */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                (*((int *) &new_buf[(i+1)*12])) = i;
                (*((unsigned int *) &new_buf[(i+1)*12+4])) = i*10 + 1;
                (*((unsigned int *) &new_buf[(i+1)*12+8])) = i*100 + 1;
            }
        }

        offset = MAX_ENTRIES*12;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
        *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries
    
        ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
        assert(new_pid == 1);
        cout << "PASS: handle.NewNode([data]) && new_pid == 1" << endl;
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i-=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i-=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i-=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i-=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }


        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = -8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for(int i=MAX_ENTRIES-1; i>=0; i-=2)
        {
            aux_rid.pageNum = aux_rid.slotNum = 2; /* seed with bad values */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            /* duplicate key */
            if ((i+1) < (int) MAX_ENTRIES)
            {
                ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
                assert((aux_rid.pageNum == (unsigned int) i*10+1) && (aux_rid.slotNum == (unsigned int) i*100+1));
            }

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"} or {" << key*10+1 << "," << key*100+1 <<"}" << endl;
        
        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }



    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_le, "a1"));
    cout << "PASS: DestroyIndex(data_test_le,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_le));
    cout << "PASS: deleteTable(" << data_test_le << ")" << endl;
    // }}}

    /* two data node le test [k11 k12 ... k1n]<-> [k21 k22 ... k2n] */ // {{{
    cout << "\n[ data_test_le - two data node le test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_le, data_test_le_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_le, data_test_le_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_le, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_le, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;


    {
        /* create two nodes. */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;
        int n_nodes = 2;
      
        for(int j=0; j<n_nodes; j++)
        {
            for(int i=0; i<MAX_ENTRIES; i++, offset += 12)
            {
                (*((int *) &new_buf[i*12])) = j*MAX_ENTRIES+i;
                (*((unsigned int *) &new_buf[i*12+4])) = (j*MAX_ENTRIES+i)*10;
                (*((unsigned int *) &new_buf[i*12+8])) = (j*MAX_ENTRIES+i)*100;
            }

            *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = j; // left is preceding node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = j+2; // right is going to be second node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries

            /* if first node, set the left node to 0. */
            if (j == 0)
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0; // no node for left.

            /* if final node, set the right node to 0. */
            if (j == (n_nodes-1))
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0; // no node for right.
    
            ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
            assert(new_pid == (unsigned int) j+1);
            cout << "PASS: handle.NewNode([data]) && new_pid == " << j+1 << endl;
        }
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for(int i=(2*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for(int i=(2*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for(int i=(2*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 339;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for(int i=(2*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 500;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for(int i=(2*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = (2*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for(int i=(2*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for(int i=(2*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_le, "a1"));
    cout << "PASS: DestroyIndex(data_test_le,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_le));
    cout << "PASS: deleteTable(" << data_test_le << ")" << endl;
    // }}}

    /* three data node le test [k11 k12 ... k1n]<-> [k21 k22 ... k2n] */ // {{{
    cout << "\n[ data_test_le - three data node le test ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_le, data_test_le_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_le, data_test_le_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_le, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_le, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [root node]" << endl;


    {
        /* create three nodes. */
        char new_buf[PF_PAGE_SIZE];
        unsigned int new_pid;
        unsigned int offset = 0;
        int n_nodes = 3;

      
        for(int j=0; j<n_nodes; j++)
        {
            for(int i=0; i<MAX_ENTRIES; i++, offset += 12)
            {
                (*((int *) &new_buf[i*12])) = j*MAX_ENTRIES+i;
                (*((unsigned int *) &new_buf[i*12+4])) = (j*MAX_ENTRIES+i)*10;
                (*((unsigned int *) &new_buf[i*12+8])) = (j*MAX_ENTRIES+i)*100;
            }

            *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = j; // left is preceding node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = j+2; // right is going to be second node
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-12]) = offset;
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-8]) = DUMP_TYPE_DATA; // type data
            *((unsigned int *) &new_buf[PF_PAGE_SIZE-4]) = MAX_ENTRIES; // num of entries

            /* if first node, set the left node to 0. */
            if (j == 0)
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-20]) = 0; // no node for left.

            /* if final node, set the right node to 0. */
            if (j == (n_nodes-1))
                *((unsigned int *) &new_buf[PF_PAGE_SIZE-16]) = 0; // no node for right.
    
            ZERO_ASSERT(handle.NewNode(new_buf, new_pid));
            assert(new_pid == (unsigned int) j+1);
            cout << "PASS: handle.NewNode([data]) && new_pid == " << j+1 << endl;
        }
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = MAX_ENTRIES;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 500;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }


    {
        int key = (2*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 2));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 3*MAX_ENTRIES;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 3));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = (3*MAX_ENTRIES)+200;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 3));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;


        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = (3*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 3));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 3));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }

        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        int key = -1000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key, 1));
        cout << "PASS: scan.OpenScan(h,<=," << key << ")" << endl;

        for (int i=(3*MAX_ENTRIES)-1; i>=0; i--)
        {
            aux_rid.pageNum = aux_rid.slotNum = i+1; /* seed with bad values. */

            /* skip when key is greater than. */
            if (i > key) 
                continue;

            ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(i, aux_rid);
            assert((aux_rid.pageNum == (unsigned int) i*10) && (aux_rid.slotNum == (unsigned int) i*100));
        }
        
        cout << "PASS: scan.GetNextEntry(...) s.t. key <= " << key << " and rid <= {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    ZERO_ASSERT(ixmgr->CloseIndex(handle));
    cout << "PASS: CloseIndex(handle)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test_le, "a1"));
    cout << "PASS: DestroyIndex(data_test_le,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test_le));
    cout << "PASS: deleteTable(" << data_test_le << ")" << endl;
    // }}}

} // }}}

int main() 
{
  cout << "test..." << endl;
  IX_Manager *ixmgr = IX_Manager::Instance();

  cleanup();

  debug = 0;

  cout << "Dummy Insert and IndexScan Tests" << endl << endl;
  ixTest_data_test_eq(ixmgr);
  ixTest_data_test_ne(ixmgr);
  ixTest_data_test_gt(ixmgr);
  ixTest_data_test_ge(ixmgr);
  ixTest_data_test_lt(ixmgr);
  ixTest_data_test_le(ixmgr);

  cout << "OK" << endl;
}
