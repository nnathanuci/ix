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


void ixTest_CreateDestroy(IX_Manager *ixmgr)
{
    RM *rm = RM::Instance();
    //IX_IndexHandle ix_handle;

    string t1 = "t1";
    vector<Attribute> t1_attrs;
    t1_attrs.push_back((struct Attribute) { "a1", TypeInt, 0 });
    t1_attrs.push_back((struct Attribute) { "a2", TypeReal, 0 });
    t1_attrs.push_back((struct Attribute) { "a3", TypeVarChar, 500 });

    /* create index without creating table. */ // {{{
    cout << "\n[ index creation (no table created). ]" << endl;
    NONZERO_ASSERT(ixmgr->CreateIndex("foo", "bar"));
    cout << "PASS: createIndex(foo,bar) [tbl doesn't exist]" << endl;
    cout << "\n[ index creation (no table exists). ]" << endl;
    NONZERO_ASSERT(ixmgr->DestroyIndex("foo", "bar"));
    cout << "PASS: destroyIndex(foo,bar) [tbl doesn't exist]" << endl;
    // }}}

    /* create table, delete index (doesnt exist), delete table. */ // {{{
    cout << "\n[ index deletion test. ]" << endl;
    ZERO_ASSERT(rm->createTable(t1, t1_attrs));
    cout << "PASS: createTable(" << output_schema(t1, t1_attrs) << ")" << endl;
    NONZERO_ASSERT(ixmgr->DestroyIndex("t1", "a1"));
    cout << "PASS: destroyIndex(t1,a1) [index doesn't exist]" << endl;
    ZERO_ASSERT(rm->deleteTable(t1));
    cout << "PASS: deleteTable(" << t1 << ")" << endl;
    // }}}

    /* create table, create indices, delete indices, delete table. */ // {{{
    cout << "\n[ index creation/deletion test. ]" << endl;
    ZERO_ASSERT(rm->createTable(t1, t1_attrs));
    cout << "PASS: createTable(" << output_schema(t1, t1_attrs) << ")" << endl;
    ZERO_ASSERT(ixmgr->CreateIndex("t1", "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;
    ZERO_ASSERT(ixmgr->CreateIndex("t1", "a2"));
    cout << "PASS: CreateIndex(t1,a2)" << endl;
    ZERO_ASSERT(ixmgr->DestroyIndex("t1", "a1"));
    cout << "PASS: DestroyIndex(t1,a1)" << endl;
    ZERO_ASSERT(ixmgr->DestroyIndex("t1", "a2"));
    cout << "PASS: DestroyIndex(t1,a2)" << endl;
    ZERO_ASSERT(rm->deleteTable(t1));
    cout << "PASS: deleteTable(" << t1 << ")" << endl;
    // }}}

}

void ixTest_OpenClose(IX_Manager *ixmgr)
{
    IX_IndexHandle ix_handle1;
    IX_IndexHandle ix_handle2;
    RM *rm = RM::Instance();

    string t1 = "t1";
    vector<Attribute> t1_attrs;
    t1_attrs.push_back((struct Attribute) { "a1", TypeInt, 0 });
    t1_attrs.push_back((struct Attribute) { "a2", TypeReal, 0 });
    t1_attrs.push_back((struct Attribute) { "a3", TypeVarChar, 500 });

    /* create table, create indices, open index twice, delete indices, delete table. */ // {{{
    cout << "\n[ opening index twice. ]" << endl;
    ZERO_ASSERT(rm->createTable(t1, t1_attrs));
    cout << "PASS: createTable(" << output_schema(t1, t1_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex("t1", "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex("t1", "a1", ix_handle1));
    cout << "PASS: OpenIndex(t1,a1,h1)" << endl;

    NONZERO_ASSERT(ixmgr->OpenIndex("t1", "a1", ix_handle1));
    cout << "PASS: OpenIndex(t1,a1,h1) [failed, already opened]" << endl;

    ZERO_ASSERT(ixmgr->CloseIndex(ix_handle1));
    cout << "PASS: CloseIndex(h1)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex("t1", "a1"));
    cout << "PASS: DestroyIndex(t1,a1)" << endl;

    ZERO_ASSERT(rm->deleteTable(t1));
    cout << "PASS: deleteTable(" << t1 << ")" << endl;
    // }}}

    /* create table, create indices, open indices, create new index nodes, delete indices, delete table. */ // {{{
    cout << "\n[ index creation/deletion test. ]" << endl;
    ZERO_ASSERT(rm->createTable(t1, t1_attrs));
    cout << "PASS: createTable(" << output_schema(t1, t1_attrs) << ")" << endl;

    ZERO_ASSERT(ixmgr->CreateIndex("t1", "a1"));
    cout << "PASS: CreateIndex(t1,a1)" << endl;
    ZERO_ASSERT(ixmgr->CreateIndex("t1", "a2"));
    cout << "PASS: CreateIndex(t1,a2)" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex("t1", "a1", ix_handle1));
    cout << "PASS: OpenIndex(t1,a1,h1)" << endl;
    assert(ix_handle1.GetNumberOfPages() == 1);
    cout << "PASS: h1.GetNumberOfPages == 1 [root node]" << endl;

    ZERO_ASSERT(ixmgr->OpenIndex("t1", "a2", ix_handle2));
    cout << "PASS: OpenIndex(t1,a1,h2)" << endl;
    assert(ix_handle2.GetNumberOfPages() == 1);
    cout << "PASS: h2.GetNumberOfPages == 1 [root node]" << endl;

    ZERO_ASSERT(ixmgr->CloseIndex(ix_handle1));
    cout << "PASS: CloseIndex(h1)" << endl;

    ZERO_ASSERT(ixmgr->CloseIndex(ix_handle2));
    cout << "PASS: CloseIndex(h2)" << endl;

    ZERO_ASSERT(ixmgr->DestroyIndex("t1", "a1"));
    cout << "PASS: DestroyIndex(t1,a1)" << endl;
    ZERO_ASSERT(ixmgr->DestroyIndex("t1", "a2"));
    cout << "PASS: DestroyIndex(t1,a2)" << endl;

    ZERO_ASSERT(rm->deleteTable(t1));
    cout << "PASS: deleteTable(" << t1 << ")" << endl;
    // }}}

}
int main() 
{
  cout << "test..." << endl;
  IX_Manager *ixmgr = IX_Manager::Instance();

  cleanup();

  cout << "CreateIndex and DestroyIndex Tests" << endl << endl;
  ixTest_CreateDestroy(ixmgr);

  cleanup();

  cout << "OpenIndex and CloseIndex Tests" << endl << endl;
  ixTest_OpenClose(ixmgr);

  cout << "OK" << endl;
}
