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

using namespace std;

int debug = 0;

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

void ixTest_data_test_eq(IX_Manager *ixmgr) // {{{
{
    RM *rm = RM::Instance();
    IX_IndexHandle handle;
    IX_IndexScan scan;

    string data_test_eq = "data_test_eq";
    vector<Attribute> data_test_eq_attrs;
    data_test_eq_attrs.push_back((struct Attribute) { "a1", TypeReal, 0 });
    data_test_eq_attrs.push_back((struct Attribute) { "a2", TypeInt, 0 });


    /* delete even entries (single data node). */ // {{{
    cout << "\n[ data_test_eq - single key equality test (missing key) ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test_eq, data_test_eq_attrs));
    cout << "PASS: createTable(" << output_schema(data_test_eq, data_test_eq_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test_eq, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test_eq, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    {
        /* XXX: create & convert to shuffled array for values 0 to 338; switch to InsertEntry */

        for(int i=0; i<(int)MAX_ENTRIES; i++)
        {
            float k = i;
            struct RID r = {i*10, i*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r));
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << MAX_ENTRIES << ", [k*100,k*1000])" << endl;
    }

    {
        /* delete all even entries. */
        float k;

        for (k = 0; k < (int)MAX_ENTRIES; k+=2)
        {
            struct RID r = {k*10, k*100};
            ZERO_ASSERT(handle.DeleteEntry(&k, r));
        }

        cout << "PASS: handle.DeleteEntries([0,2,...,338])" << endl;
    }

    {
        /* check all odd entries exist. */
        float k;

        for (k = 1; k < (int)MAX_ENTRIES; k+=2)
        {
            struct RID aux_rid = {0,0};
            ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &k));

            ZERO_ASSERT(scan.GetNextEntry(aux_rid));
            assert(aux_rid.pageNum == (unsigned int) k*10 && aux_rid.slotNum == (unsigned int) k*100);

            assert(scan.GetNextEntry(aux_rid) == IX_EOF);
            ZERO_ASSERT(scan.CloseScan());
        }

        cout << "PASS: handle.GetEntries([1,3,...,337])" << endl;
    }

    {
        /* delete all odd entries. */
        float k;

        for (k = 1; k < (int)MAX_ENTRIES; k+=2)
        {
            struct RID r = {k*10, k*100};
            ZERO_ASSERT(handle.DeleteEntry(&k, r));
        }

        cout << "PASS: handle.DeleteEntries([1,3,...,337])" << endl;
    }

    {
        /* no more elements should exist. */
        float k = 0;
        struct RID aux_rid = {0,0};

        ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &k));
        cout << "PASS: scan.OpenScan(...)" << endl;
        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry() == IX_EOF" << endl;
        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        float k = 20;
        struct RID r = {k*10, k*100};
        ZERO_ASSERT(handle.InsertEntry(&k, r));
        cout << "PASS: handle.InsertEntry(" << k << "," << r.pageNum << "," << r.slotNum << ")" << endl;

        k = 350;
        r.pageNum = k*10; r.slotNum = k*100;
        ZERO_ASSERT(handle.InsertEntry(&k, r));
        cout << "PASS: handle.InsertEntry(" << k << "," << r.pageNum << "," << r.slotNum << ")" << endl;

        /* missing key. */
        k = 169;
        struct RID aux_rid = {0,0};

        ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &k));
        cout << "PASS: scan.OpenScan([EQ_OP " << k << "])" << endl;
        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(" << k << ") == IX_EOF" << endl;
        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;

        /* keys should exist. */
        k = 20;
        aux_rid.pageNum = aux_rid.slotNum = 0;
        ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &k));
        cout << "PASS: scan.OpenScan([EQ_OP " << k << "])" << endl;
        ZERO_ASSERT(scan.GetNextEntry(aux_rid));
        assert(aux_rid.pageNum == (unsigned int) k*10 && aux_rid.slotNum == (unsigned int) k*100);
        cout << "PASS: scan.GetNextEntry(" << k << ")" << endl;
        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry() == IX_EOF" << endl;
        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;

        k = 350;
        aux_rid.pageNum = aux_rid.slotNum = 0;
        ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &k));
        cout << "PASS: scan.OpenScan([EQ_OP " << k << "])" << endl;
        ZERO_ASSERT(scan.GetNextEntry(aux_rid));
        assert(aux_rid.pageNum == (unsigned int) k*10 && aux_rid.slotNum == (unsigned int) k*100);
        cout << "PASS: scan.GetNextEntry(" << k << ")" << endl;
        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry() == IX_EOF" << endl;
        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;


        /* some NE_OP tests. */        
        k = 350;
        aux_rid.pageNum = aux_rid.slotNum = 0;
        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &k));
        cout << "PASS: scan.OpenScan([NE_OP " << k << "])" << endl;
        ZERO_ASSERT(scan.GetNextEntry(aux_rid));
        k=20; /* we should only get one record. */
        assert(aux_rid.pageNum == (unsigned int) k*10 && aux_rid.slotNum == (unsigned int) k*100);
        cout << "PASS: scan.GetNextEntry(" << k << ")" << endl;
        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry() == IX_EOF" << endl;
        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;

        /* some NE_OP tests. */        
        k = 20;
        aux_rid.pageNum = aux_rid.slotNum = 0;
        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &k));
        cout << "PASS: scan.OpenScan([NE_OP " << k << "])" << endl;
        ZERO_ASSERT(scan.GetNextEntry(aux_rid));
        k=350; /* we should only get one record. */
        assert(aux_rid.pageNum == (unsigned int) k*10 && aux_rid.slotNum == (unsigned int) k*100);
        cout << "PASS: scan.GetNextEntry(" << k << ")" << endl;
        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry() == IX_EOF" << endl;
        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;

        /* some NE_OP tests. */        
        k = 5;
        aux_rid.pageNum = aux_rid.slotNum = 0;
        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &k));
        cout << "PASS: scan.OpenScan([NE_OP " << k << "])" << endl;
        ZERO_ASSERT(scan.GetNextEntry(aux_rid));
        k=20; /* we should only get one record. */
        assert(aux_rid.pageNum == (unsigned int) k*10 && aux_rid.slotNum == (unsigned int) k*100);
        cout << "PASS: scan.GetNextEntry(" << k << ")" << endl;
        k=350; /* we should only get one record. */
        ZERO_ASSERT(scan.GetNextEntry(aux_rid));
        assert(aux_rid.pageNum == (unsigned int) k*10 && aux_rid.slotNum == (unsigned int) k*100);
        cout << "PASS: scan.GetNextEntry(" << k << ")" << endl;
        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry() == IX_EOF" << endl;
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
        /* XXX: create & convert to shuffled array for values 0 to 338; switch to InsertEntry */

        for(int i=0; i<(int)MAX_ENTRIES; i++)
        {
            struct RID r = {i*10, i*100};

            ZERO_ASSERT(handle.InsertEntry(&i, r));
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << MAX_ENTRIES << ", [k*100,k*1000])" << endl;
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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
        /* XXX: create & convert to shuffled array for values 0 to 338; switch to InsertEntry */

        for(int i=0; i<(int)MAX_ENTRIES; i+=2)
        {
            int k = i;
            struct RID r = {i*10,i*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r));

            if ((i+1) < (int) MAX_ENTRIES)
            {
                r.pageNum++;
                r.slotNum++;
                ZERO_ASSERT(handle.InsertEntry(&k, r));
            }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << MAX_ENTRIES << ", [k*100,k*1000])" << endl;
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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
        /* XXX: create & convert to shuffled array for values 0 to 338; switch to InsertEntry */

        for(int i=0; i<(int)(2*MAX_ENTRIES); i++)
        {
            int k = i;
            struct RID r = {i*10,i*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r));
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << MAX_ENTRIES << ", [k*100,k*1000])" << endl;
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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
        /* XXX: create & convert to shuffled array for values 0 to 338; switch to InsertEntry */

        for(int i=0; i<(int)3*MAX_ENTRIES; i++)
        {
            int k = i;
            struct RID r = {i*10,i*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r));
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << MAX_ENTRIES << ", [k*100,k*1000])" << endl;
    }

    {
        int key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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

        ZERO_ASSERT(scan.OpenScan(handle, NE_OP, &key));
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


int main() 
{
  cout << "test..." << endl;
  IX_Manager *ixmgr = IX_Manager::Instance();

  cleanup();

  cout << "Delete Tests" << endl << endl;
  ixTest_data_test_eq(ixmgr);
  //ixTest_data_test_ne(ixmgr);

  cout << "OK" << endl;
}
