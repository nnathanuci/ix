#include <fstream>
#include <iostream>
#include <cassert>
#include <string>
#include <sstream>

#include "ix.h"

#define ZERO_ASSERT(x) assert((x) == 0)
#define NONZERO_ASSERT(x) assert((x) != 0)

using namespace std;

/* temp dummy. */
RC IX_IndexHandle::InsertEntry(void *data, RID const &rid)
{
    return 0;
}

void cleanup() // {{{
{
  const char *files[4] = { "systemcatalog", "t1.a1", "t1.a2", "t1" };

  for(int i = 0; i < 4; i++)
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

void ixTest_Insert(IX_Manager *ixmgr)
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

    ZERO_ASSERT(scan.GetNextEntry(aux_rid));
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
}

void ixTest_data_test1(IX_Manager *ixmgr)
{
    RM *rm = RM::Instance();
    IX_IndexHandle handle;
    IX_IndexScan scan;

    string data_test1 = "data_test1";
    vector<Attribute> data_test1_attrs;
    data_test1_attrs.push_back((struct Attribute) { "a1", TypeInt, 0 });
    data_test1_attrs.push_back((struct Attribute) { "a2", TypeInt, 0 });


    int key = 0;
    RID rid = {0,0};
    RID aux_rid;

    /* create table, create index, insert an entry, scan/retrieve entry, delete index, delete table. */ // {{{
    cout << "\n[ data_test1 ]" << endl;
    ZERO_ASSERT(rm->createTable(data_test1, data_test1_attrs));
    cout << "PASS: createTable(" << output_schema(data_test1, data_test1_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex(data_test1, "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex(data_test1, "a1", handle));
    cout << "PASS: OpenIndex(t1,a1,h)" << endl;

    assert(handle.GetNumberOfPages() == 1);
    cout << "PASS: h.GetNumberOfPages == 1 [data_test1.a1 has root node]" << endl;

    {
        /* create a data node (339 entries): [ (0,0,0) (10,100,1000) (20,200,2000) ... (3380, 33800, 338000) ] */
        char data_buf[PF_PAGE_SIZE];
        unsigned int data_pid;
        unsigned int max_entries = (PF_PAGE_SIZE-20)/12; // (key, pageid, slotid)
        unsigned int offset = 0;

        for(int i=0; i<(int)max_entries; i++, offset += 12)
        {
            (*((int *) &data_buf[i*12])) = i*10;
            (*((unsigned int *) &data_buf[i*12+4])) = i*100;
            (*((unsigned int *) &data_buf[i*12+8])) = i*1000;
        }

        *((unsigned int *) &data_buf[PF_PAGE_SIZE-20]) = 0;
        *((unsigned int *) &data_buf[PF_PAGE_SIZE-16]) = 0;
        *((unsigned int *) &data_buf[PF_PAGE_SIZE-12]) = offset;
        *((unsigned int *) &data_buf[PF_PAGE_SIZE-8]) = 2; // type data
        *((unsigned int *) &data_buf[PF_PAGE_SIZE-4]) = max_entries; // num of entries
    
        ZERO_ASSERT(handle.NewNode(data_buf, data_pid));
        assert(data_pid == 0);
        cout << "PASS: handle.NewNode([data]) && data_pid == 0" << endl;
        handle.DumpNode(0, 0);
    }

    //ZERO_ASSERT(scan.OpenScan(handle, EQ_OP, &key));
    //cout << "PASS: scan.OpenScan(h,=,1)" << endl;

    //ZERO_ASSERT(scan.GetNextEntry(aux_rid));
    //assert(aux_rid.pageNum == 200 && aux_rid.slotNum == 3000);
    //cout << "PASS: scan.GetNextEntry(aux_rid) && aux_rid == {200,3000}" << endl;
    //
    //assert(scan.GetNextEntry(aux_rid) == IX_EOF);
    //cout << "PASS: scan.GetNextEntry(aux_rid) == IX_EOF" << endl;

    //ZERO_ASSERT(scan.CloseScan());
    //cout << "PASS: scan.CloseScan()" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex(data_test1, "a1"));
    cout << "PASS: DestroyIndex(data_test1,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(data_test1));
    cout << "PASS: deleteTable(" << data_test1 << ")" << endl;
    // }}}
}

int main() 
{
  cout << "test..." << endl;
  IX_Manager *ixmgr = IX_Manager::Instance();

  cleanup();

  cout << "Insert and IndexScan Tests" << endl << endl;
  //ixTest_Insert(ixmgr);
  ixTest_data_test1(ixmgr);

  cout << "OK" << endl;
}
