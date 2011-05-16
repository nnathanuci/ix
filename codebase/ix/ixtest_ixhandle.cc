#include <fstream>
#include <iostream>
#include <cassert>
#include <string>
#include <sstream>

#include "ix.h"

#define ZERO_ASSERT(x) assert((x) == 0)
#define NONZERO_ASSERT(x) assert((x) != 0)

using namespace std;

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


void ixTest_NewNode(IX_Manager *ixmgr)
{
    IX_IndexHandle ix_handle1;
    IX_IndexHandle ix_handle2;
    RM *rm = RM::Instance();

    char new_buf[PF_PAGE_SIZE] = {0};
    unsigned int new_pid = 0;
    unsigned int type = 0;

    string t1 = "t1";
    vector<Attribute> t1_attrs;
    t1_attrs.push_back((struct Attribute) { "a1", TypeInt, 0 });
    t1_attrs.push_back((struct Attribute) { "a2", TypeReal, 0 });
    t1_attrs.push_back((struct Attribute) { "a3", TypeVarChar, 500 });

    /* create table, create index, open indices, create new index node, delete index, delete table. */ // {{{
    cout << "\n[ index newnode test 1. ]" << endl;
    ZERO_ASSERT(rm->createTable(t1, t1_attrs));
    cout << "PASS: createTable(" << output_schema(t1, t1_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex("t1", "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex("t1", "a1", ix_handle1));
    cout << "PASS: OpenIndex(t1,a1,h1)" << endl;

    assert(ix_handle1.GetNumberOfPages() == 1);
    cout << "PASS: h1.GetNumberOfPages == 1 [root node]" << endl;

    memset(new_buf, 0, PF_PAGE_SIZE);
    type = DUMP_TYPE_DELETED; memcpy((new_buf+PF_PAGE_SIZE-8), &type, sizeof(type));
    ZERO_ASSERT(ix_handle1.NewNode(new_buf, new_pid));
    assert(new_pid == 1);
    cout << "PASS: h1.NewNode([0*4096],new_pid) && new_pid==0 [new node replaced root node]" << endl;

    ZERO_ASSERT(ixmgr->CloseIndex(ix_handle1));
    cout << "PASS: CloseIndex()" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex("t1", "a1"));
    cout << "PASS: DestroyIndex(t1,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(t1));
    cout << "PASS: deleteTable(" << t1 << ")" << endl;
    // }}}

    /* create table, create index, open indices, create new index node, delete index, delete table. */ // {{{
    cout << "\n[ index newnode test 2 (allocate a new node). ]" << endl;
    ZERO_ASSERT(rm->createTable(t1, t1_attrs));
    cout << "PASS: createTable(" << output_schema(t1, t1_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex("t1", "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex("t1", "a1", ix_handle1));
    cout << "PASS: OpenIndex(t1,a1,h1)" << endl;

    assert(ix_handle1.GetNumberOfPages() == 1);
    cout << "PASS: h1.GetNumberOfPages == 1 [root node]" << endl;

    memset(new_buf, 0, PF_PAGE_SIZE);
    type = DUMP_TYPE_INDEX; memcpy((new_buf+PF_PAGE_SIZE-8), &type, sizeof(type));
    ZERO_ASSERT(ix_handle1.WriteNode(0, new_buf));
    cout << "PASS: h1.WriteNode(0, new_buf[type=1]) [overwrite root node]" << endl;

    memset(new_buf, 0, PF_PAGE_SIZE);
    type = DUMP_TYPE_DELETED; memcpy((new_buf+PF_PAGE_SIZE-8), &type, sizeof(type));
    ZERO_ASSERT(ix_handle1.NewNode(new_buf, new_pid));
    assert(new_pid == 1);
    cout << "PASS: h1.NewNode([0*4096],new_pid) && new_pid==1 [new node appended]" << endl;

    ZERO_ASSERT(ixmgr->CloseIndex(ix_handle1));
    cout << "PASS: CloseIndex()" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex("t1", "a1"));
    cout << "PASS: DestroyIndex(t1,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(t1));
    cout << "PASS: deleteTable(" << t1 << ")" << endl;
    // }}}

    /* create table, create index, open indices, create multiple new index nodes, delete/realloc nodes, delete table. */ // {{{
    cout << "\n[ index newnode test 3 (allocate/delete/reallocate nodes). ]" << endl;
    ZERO_ASSERT(rm->createTable(t1, t1_attrs));
    cout << "PASS: createTable(" << output_schema(t1, t1_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex("t1", "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex("t1", "a1", ix_handle1));
    cout << "PASS: OpenIndex(t1,a1,h1)" << endl;

    assert(ix_handle1.GetNumberOfPages() == 1);
    cout << "PASS: h1.GetNumberOfPages == 1 [root node]" << endl;

    {
        /* create 10 occupied nodes. */
        for (unsigned int i = 0; i < 10; i++)
        {
            memset(new_buf, 0, PF_PAGE_SIZE);
            type = DUMP_TYPE_INDEX; memcpy((new_buf+PF_PAGE_SIZE-8), &type, sizeof(type));
            ZERO_ASSERT(ix_handle1.NewNode(new_buf, new_pid));
            /* new nodes should start at pid 1 and increase, since they're occupied. */
            assert(new_pid == i+1);
            
        }

        cout << "PASS: h1.NewNode([type=1])*10 [wrote new nodes from pid 1 to 10]" << endl;
    }

    {
        /* delete node 4,5,7. */
        ZERO_ASSERT(ix_handle1.DeleteNode(4, new_buf));
        cout << "PASS: h1.DeleteNode(4)" << endl;
        ZERO_ASSERT(ix_handle1.DeleteNode(5, new_buf));
        cout << "PASS: h1.DeleteNode(5)" << endl;
        ZERO_ASSERT(ix_handle1.DeleteNode(7, new_buf));
        cout << "PASS: h1.DeleteNode(7)" << endl;

        /* reallocate nodes 4, 5, 7 */
        memset(new_buf, 0, PF_PAGE_SIZE);
        type = DUMP_TYPE_INDEX; memcpy((new_buf+PF_PAGE_SIZE-8), &type, sizeof(type));

        ZERO_ASSERT(ix_handle1.NewNode(new_buf, new_pid));
        assert(new_pid == 4);
        cout << "PASS: h1.NewNode([type=1]) && new_pid == 4" << endl;
        ZERO_ASSERT(ix_handle1.NewNode(new_buf, new_pid));
        assert(new_pid == 5);
        cout << "PASS: h1.NewNode([type=1]) && new_pid == 5" << endl;
        ZERO_ASSERT(ix_handle1.NewNode(new_buf, new_pid));
        assert(new_pid == 7);
        cout << "PASS: h1.NewNode([type=1]) && new_pid == 7" << endl;
    }

    {
        /* new nodes 11, 12 */
        memset(new_buf, 0, PF_PAGE_SIZE);
        type = DUMP_TYPE_INDEX; memcpy((new_buf+PF_PAGE_SIZE-8), &type, sizeof(type));
        ZERO_ASSERT(ix_handle1.NewNode(new_buf, new_pid));
        assert(new_pid == 11);
        cout << "PASS: h1.NewNode([type=1]) && new_pid == 11" << endl;
        ZERO_ASSERT(ix_handle1.NewNode(new_buf, new_pid));
        assert(new_pid == 12);
        cout << "PASS: h1.NewNode([type=1]) && new_pid == 12" << endl;
    }


    ZERO_ASSERT(ixmgr->CloseIndex(ix_handle1));
    cout << "PASS: CloseIndex()" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex("t1", "a1"));
    cout << "PASS: DestroyIndex(t1,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(t1));
    cout << "PASS: deleteTable(" << t1 << ")" << endl;
    // }}}

}
int main() 
{
  cout << "test..." << endl;
  IX_Manager *ixmgr = IX_Manager::Instance();

  cleanup();

  cout << "NewNode Tests" << endl << endl;
  ixTest_NewNode(ixmgr);

  cout << "OK" << endl;
}
