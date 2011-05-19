#include <fstream>
#include <iostream>
#include <cassert>
#include <string>
#include <sstream>
#include <cstdlib>

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

void shuffle_sequence(int *a, int n_elems) // {{{
{
    for (int i = 0; i < n_elems; i++)
        a[i] = i;

    for (int i = 0; i < n_elems; i++)
    {
        int r = i + (rand() % (n_elems - i));
        int temp = a[i];
        a[i] = a[r];
        a[r] = temp;
    }
} // }}}

void shuffle_even_sequence(int *a, int n_elems) // {{{
{
    for (int i = 0; i < n_elems; i += 2)
    {
        a[i] = i;

        if((i+1) < n_elems)
            a[i+1] = i;
    }

    for (int i = 0; i < n_elems; i++)
    {
        int r = i + (rand() % (n_elems - i));
        int temp = a[i];
        a[i] = a[r];
        a[r] = temp;
    }
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
        int randarray[MAX_ENTRIES]; shuffle_sequence(randarray, MAX_ENTRIES);

        for(int i=0; i<(int)MAX_ENTRIES; i++)
        {
            float k = randarray[i]*10;
            struct RID r = {randarray[i]*100, randarray[i]*1000};

            ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << MAX_ENTRIES << ", [k*100,k*1000])" << endl;
    }

    {
        float key = 90;
        RID aux_rid = {0, 0};

        /* equality test */
        ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &key));
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
        int randarray[MAX_ENTRIES]; shuffle_sequence(randarray, MAX_ENTRIES);

        for(int i=0; i<(int)MAX_ENTRIES; i++)
        {
            float k = randarray[i]*10;
            struct RID r = {randarray[i]*100, randarray[i]*1000};

            ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << MAX_ENTRIES << ", [k*100,k*1000])" << endl;
    }

    {
        float key = 25;
        RID aux_rid = {0, 0};

        /* equality test on key=20 */
        ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &key));
        cout << "PASS: scan.OpenScan(h,=," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        RID aux_rid = {0, 0};
        float key = MAX_ENTRIES*10; /* shouldn't exist. */

        /* equality test on key=20 */
        ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &key));
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
        int randarray[MAX_ENTRIES]; shuffle_sequence(randarray, MAX_ENTRIES);

        for(int i=0; i<(int)MAX_ENTRIES; i++)
        {
            float k = randarray[i]*10;
            struct RID r = {randarray[i]*100, randarray[i]*1000};

            ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << MAX_ENTRIES << ", [k*100,k*1000])" << endl;
    }

    {
        float key = 3380;
        RID aux_rid = {0, 0};

        /* equality test */
        ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &key));
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
        int randarray[MAX_ENTRIES]; shuffle_sequence(randarray, MAX_ENTRIES);

        for(int i=0; i<(int)MAX_ENTRIES; i++)
        {
            if ((randarray[i] % 2) == 0)
            {
                 float k = randarray[i]*10;
                 struct RID r = {k*10, k*100};
                 ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
            }
        }

        for(int i=0; i<(int)MAX_ENTRIES; i++)
        {
            if ((randarray[i] % 2) == 1)
            {
                 float k = (randarray[i] - 1)*10;
                 struct RID r = {k*10+1, k*100+1};
                 ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
            }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << MAX_ENTRIES << ", [k*10,k*100])" << endl;
    }

    {
        // test two duplicate entries

        float key = 0;
        RID aux_rid = {0, 0};

        ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &key));
        cout << "PASS: scan.OpenScan(h,=," << key << ")" << endl;

        ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(key, aux_rid);
        assert((aux_rid.pageNum == (unsigned int) key*10) && (aux_rid.slotNum == (unsigned int) key*100));
        cout << "PASS: scan.GetNextEntry(aux_rid) && aux_rid == {" << key*10 << "," << key*100 << "}" << endl;

        ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(key, aux_rid);
        assert((aux_rid.pageNum == (unsigned int) (key*10+1)) && (aux_rid.slotNum == (unsigned int) (key*100+1)));
        cout << "PASS: scan.GetNextEntry(aux_rid) && aux_rid == {" << key*10+1 << "," << key*100+1 << "}" << endl;
        
        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;

        key = 80;

        ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &key));
        cout << "PASS: scan.OpenScan(h,=," << key << ")" << endl;

        ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(key, aux_rid);
        assert((aux_rid.pageNum == (unsigned int) key*10) && (aux_rid.slotNum == (unsigned int) key*100));
        cout << "PASS: scan.GetNextEntry(aux_rid) && aux_rid == {" << key*10 << "," << key*100 << "}" << endl;

        ZERO_ASSERT(scan.GetNextEntry(aux_rid)); if (debug) DUMP_KEYVAL(key, aux_rid);
        assert((aux_rid.pageNum == (unsigned int) (key*10+1)) && (aux_rid.slotNum == (unsigned int) (key*100+1)));
        cout << "PASS: scan.GetNextEntry(aux_rid) && aux_rid == {" << key*10+1 << "," << key*100+1 << "}" << endl;
        
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
    data_test_ne_attrs.push_back((struct Attribute) { "a1", TypeReal, 0 });
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
        int randarray[MAX_ENTRIES]; shuffle_sequence(randarray, MAX_ENTRIES);

        for(int i=0; i<(int)MAX_ENTRIES; i++)
        {
            float k = randarray[i];
            struct RID r = {randarray[i]*10, randarray[i]*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << MAX_ENTRIES << ", [k*100,k*1000])" << endl;
    }

    {
        float key = 90;
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
        float key = 0;
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
        float key = 338;
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
        float key = 8000;
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
        int randarray[MAX_ENTRIES]; shuffle_sequence(randarray, MAX_ENTRIES);

        for(int i=0; i<(int)MAX_ENTRIES; i++)
        {
            if ((randarray[i] % 2) == 0)
            {
                 float k = randarray[i];
                 struct RID r = {k*10, k*100};
                 ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
            }
        }

        for(int i=0; i<(int)MAX_ENTRIES; i++)
        {
            if ((randarray[i] % 2) == 1)
            {
                 float k = randarray[i] - 1;
                 struct RID r = {k*10+1, k*100+1};
                 ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
            }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << MAX_ENTRIES << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
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
        float key = 0;
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
        float key = 338;
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
        float key = 8000;
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
        int randarray[(2*MAX_ENTRIES)]; shuffle_sequence(randarray, (2*MAX_ENTRIES));

        for(int i=0; i<(int)(2*MAX_ENTRIES); i++)
        {
            float k = randarray[i];
            struct RID r = {randarray[i]*10, randarray[i]*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << (2*MAX_ENTRIES) << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
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
        float key = 0;
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
        float key = 338;
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
        float key = 339;
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
        float key = 500;
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
        float key = (2*MAX_ENTRIES)-1;
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
        float key = 8000;
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
        int randarray[(3*MAX_ENTRIES)]; shuffle_sequence(randarray, (3*MAX_ENTRIES));

        for(int i=0; i<(int)(3*MAX_ENTRIES); i++)
        {
            float k = randarray[i];
            struct RID r = {randarray[i]*10, randarray[i]*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << (3*MAX_ENTRIES) << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
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
        float key = 0;
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
        float key = 338;
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
        float key = MAX_ENTRIES;
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
        float key = 500;
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
        float key = (3*MAX_ENTRIES)-1;
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
        float key = 3*MAX_ENTRIES;
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
        float key = (3*MAX_ENTRIES)+200;
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
        float key = (3*MAX_ENTRIES)-1;
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
        float key = 8000;
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

void ixTest_data_test_gt(IX_Manager *ixmgr) // {{{
{
    RM *rm = RM::Instance();
    IX_IndexHandle handle;
    IX_IndexScan scan;

    string data_test_gt = "data_test_gt";
    vector<Attribute> data_test_gt_attrs;
    data_test_gt_attrs.push_back((struct Attribute) { "a1", TypeReal, 0 });
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
        int randarray[(1*MAX_ENTRIES)]; shuffle_sequence(randarray, (1*MAX_ENTRIES));

        for(int i=0; i<(int)(1*MAX_ENTRIES); i++)
        {
            float k = randarray[i];
            struct RID r = {randarray[i]*10, randarray[i]*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << (1*MAX_ENTRIES) << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        float key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        float key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;
        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        float key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        int randarray[MAX_ENTRIES]; shuffle_sequence(randarray, MAX_ENTRIES);

        for(int i=0; i<(int)MAX_ENTRIES; i++)
        {
            if ((randarray[i] % 2) == 0)
            {
                 float k = randarray[i];
                 struct RID r = {k*10, k*100};
                 ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
            }
        }

        for(int i=0; i<(int)MAX_ENTRIES; i++)
        {
            if ((randarray[i] % 2) == 1)
            {
                 float k = randarray[i] - 1;
                 struct RID r = {k*10+1, k*100+1};
                 ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
            }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << MAX_ENTRIES << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        float key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        float key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        float key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        int randarray[(2*MAX_ENTRIES)]; shuffle_sequence(randarray, (2*MAX_ENTRIES));

        for(int i=0; i<(int)(2*MAX_ENTRIES); i++)
        {
            float k = randarray[i];
            struct RID r = {randarray[i]*10, randarray[i]*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << (2*MAX_ENTRIES) << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        float key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        float key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        float key = 339;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        float key = 500;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        float key = (2*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        float key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        int randarray[(3*MAX_ENTRIES)]; shuffle_sequence(randarray, (3*MAX_ENTRIES));

        for(int i=0; i<(int)(3*MAX_ENTRIES); i++)
        {
            float k = randarray[i];
            struct RID r = {randarray[i]*10, randarray[i]*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << (3*MAX_ENTRIES) << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        float key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        float key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        float key = MAX_ENTRIES;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        float key = 500;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        float key = (2*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
        float key = 3*MAX_ENTRIES;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        float key = (3*MAX_ENTRIES)+200;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        float key = (3*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        float key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
        cout << "PASS: scan.OpenScan(h,>," << key << ")" << endl;

        cout << "PASS: scan.GetNextEntry(...) s.t. key > " << key << " and rid > {" << key*10 << "," << key*100 <<"}" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        float key = -1000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GT_OP, &key));
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
    data_test_ge_attrs.push_back((struct Attribute) { "a1", TypeReal, 0 });
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
        int randarray[(1*MAX_ENTRIES)]; shuffle_sequence(randarray, (1*MAX_ENTRIES));

        for(int i=0; i<(int)(1*MAX_ENTRIES); i++)
        {
            float k = randarray[i];
            struct RID r = {randarray[i]*10, randarray[i]*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << (1*MAX_ENTRIES) << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        int randarray[MAX_ENTRIES]; shuffle_sequence(randarray, MAX_ENTRIES);

        for(int i=0; i<(int)MAX_ENTRIES; i++)
        {
            if ((randarray[i] % 2) == 0)
            {
                 float k = randarray[i];
                 struct RID r = {k*10, k*100};
                 ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
            }
        }

        for(int i=0; i<(int)MAX_ENTRIES; i++)
        {
            if ((randarray[i] % 2) == 1)
            {
                 float k = randarray[i] - 1;
                 struct RID r = {k*10+1, k*100+1};
                 ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
            }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << MAX_ENTRIES << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        int randarray[(2*MAX_ENTRIES)]; shuffle_sequence(randarray, (2*MAX_ENTRIES));

        for(int i=0; i<(int)(2*MAX_ENTRIES); i++)
        {
            float k = randarray[i];
            struct RID r = {randarray[i]*10, randarray[i]*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << (2*MAX_ENTRIES) << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = 339;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = 500;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = (2*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        int randarray[(3*MAX_ENTRIES)]; shuffle_sequence(randarray, (3*MAX_ENTRIES));

        for(int i=0; i<(int)(3*MAX_ENTRIES); i++)
        {
            float k = randarray[i];
            struct RID r = {randarray[i]*10, randarray[i]*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << (3*MAX_ENTRIES) << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = MAX_ENTRIES;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = 500;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = (2*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = 3*MAX_ENTRIES;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        float key = (3*MAX_ENTRIES)+200;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        float key = (3*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
        float key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
        cout << "PASS: scan.OpenScan(h,>=," << key << ")" << endl;

        assert(scan.GetNextEntry(aux_rid) == IX_EOF);
        cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

        ZERO_ASSERT(scan.CloseScan());
        cout << "PASS: scan.CloseScan()" << endl;
    }

    {
        float key = -1000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, GE_OP, &key));
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
    data_test_lt_attrs.push_back((struct Attribute) { "a1", TypeReal, 0 });
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
        int randarray[(1*MAX_ENTRIES)]; shuffle_sequence(randarray, (1*MAX_ENTRIES));

        for(int i=0; i<(int)(1*MAX_ENTRIES); i++)
        {
            float k = randarray[i];
            struct RID r = {randarray[i]*10, randarray[i]*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << (1*MAX_ENTRIES) << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = -8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        int randarray[MAX_ENTRIES]; shuffle_sequence(randarray, MAX_ENTRIES);

        for(int i=0; i<(int)MAX_ENTRIES; i++)
        {
            if ((randarray[i] % 2) == 0)
            {
                 float k = randarray[i];
                 struct RID r = {k*10, k*100};
                 ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
            }
        }

        for(int i=0; i<(int)MAX_ENTRIES; i++)
        {
            if ((randarray[i] % 2) == 1)
            {
                 float k = randarray[i] - 1;
                 struct RID r = {k*10+1, k*100+1};
                 ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
            }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << MAX_ENTRIES << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = -8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        int randarray[(2*MAX_ENTRIES)]; shuffle_sequence(randarray, (2*MAX_ENTRIES));

        for(int i=0; i<(int)(2*MAX_ENTRIES); i++)
        {
            float k = randarray[i];
            struct RID r = {randarray[i]*10, randarray[i]*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << (2*MAX_ENTRIES) << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = 339;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = 500;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = (2*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        int randarray[(3*MAX_ENTRIES)]; shuffle_sequence(randarray, (3*MAX_ENTRIES));

        for(int i=0; i<(int)(3*MAX_ENTRIES); i++)
        {
            float k = randarray[i];
            struct RID r = {randarray[i]*10, randarray[i]*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << (3*MAX_ENTRIES) << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = MAX_ENTRIES;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = 500;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = (2*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = 3*MAX_ENTRIES;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = (3*MAX_ENTRIES)+200;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = (3*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
        float key = -1000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LT_OP, &key));
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
    data_test_le_attrs.push_back((struct Attribute) { "a1", TypeReal, 0 });
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
        int randarray[(1*MAX_ENTRIES)]; shuffle_sequence(randarray, (1*MAX_ENTRIES));

        for(int i=0; i<(int)(1*MAX_ENTRIES); i++)
        {
            float k = randarray[i];
            struct RID r = {randarray[i]*10, randarray[i]*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << (1*MAX_ENTRIES) << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = -8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        int randarray[MAX_ENTRIES]; shuffle_sequence(randarray, MAX_ENTRIES);

        for(int i=0; i<(int)MAX_ENTRIES; i++)
        {
            if ((randarray[i] % 2) == 0)
            {
                 float k = randarray[i];
                 struct RID r = {k*10, k*100};
                 ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
            }
        }

        for(int i=0; i<(int)MAX_ENTRIES; i++)
        {
            if ((randarray[i] % 2) == 1)
            {
                 float k = randarray[i] - 1;
                 struct RID r = {k*10+1, k*100+1};
                 ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
            }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << MAX_ENTRIES << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = -8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        int randarray[(2*MAX_ENTRIES)]; shuffle_sequence(randarray, (2*MAX_ENTRIES));

        for(int i=0; i<(int)(2*MAX_ENTRIES); i++)
        {
            float k = randarray[i];
            struct RID r = {randarray[i]*10, randarray[i]*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << (2*MAX_ENTRIES) << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = 339;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = 500;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = (2*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        int randarray[(3*MAX_ENTRIES)]; shuffle_sequence(randarray, (3*MAX_ENTRIES));

        for(int i=0; i<(int)(3*MAX_ENTRIES); i++)
        {
            float k = randarray[i];
            struct RID r = {randarray[i]*10, randarray[i]*100};

            ZERO_ASSERT(handle.InsertEntry(&k, r)); if (debug) { cout << "insert: "; DUMP_KEYVAL(k,r); }
        }

        cout << "PASS: handle.InsertEntries(k: 0 to " << (3*MAX_ENTRIES) << ", [k*10,k*100])" << endl;
    }

    {
        float key = 90;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = 0;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = 338;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = MAX_ENTRIES;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = 500;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = (2*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = 3*MAX_ENTRIES;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = (3*MAX_ENTRIES)+200;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = (3*MAX_ENTRIES)-1;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = 8000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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
        float key = -1000;
        struct RID aux_rid;

        ZERO_ASSERT(scan.OpenScan(handle, LE_OP, &key));
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

  cout << "Insert and IndexScan Tests" << endl << endl;
  ixTest_data_test_eq(ixmgr);
  ixTest_data_test_ne(ixmgr);
  ixTest_data_test_gt(ixmgr);
  ixTest_data_test_ge(ixmgr);
  ixTest_data_test_lt(ixmgr);
  ixTest_data_test_le(ixmgr);

  cout << "OK" << endl;
}
