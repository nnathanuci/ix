#include "ix.h"
#include <cassert>

// class IX_Manager {
//  public:
//   static IX_Manager* Instance();
// 
//   RC CreateIndex(const string tableName,       // create new index
//                  const string attributeName);
//   RC DestroyIndex(const string tableName,      // destroy an index
//                   const string attributeName);
//   RC OpenIndex(const string tableName,         // open an index
//                const string attributeName,
//                IX_IndexHandle &indexHandle);
//   RC CloseIndex(IX_IndexHandle &indexHandle);  // close index
// 
//  protected:
//   IX_Manager   ();                             // Constructor
//   ~IX_Manager  ();                             // Destructor
// 
//  private:
//   static IX_Manager *_ix_manager;
// };

IX_Manager* IX_Manager::_ix_manager = 0;

IX_Manager* IX_Manager::Instance()
{
    if (!_ix_manager)
        _ix_manager = new IX_Manager();

    return _ix_manager;
}

IX_Manager::IX_Manager()
{
	pf = PF_Manager::Instance();
        rm = RM::Instance();
}


RC IX_Manager::CreateIndex(const string tableName, const string attributeName)
{
    Attribute attr;
    uint16_t attr_pos;
    PF_FileHandle handle;

    /* open table. */
    if (rm->openTable(tableName, handle))
        return -1;

    /* ensure attribute name exists. */
    if (rm->getTableAttribute(tableName, attributeName, attr, attr_pos))
        return -1;

    /* create the index file. */
    if (pf->CreateFile((tableName+"."+attributeName).c_str()))
        return -1;

    return 0;
}

RC IX_Manager::DestroyIndex(const string tableName, const string attributeName)
{
    Attribute attr;
    uint16_t attr_pos;
    PF_FileHandle handle;

    /* open table. */
    if (rm->openTable(tableName, handle))
        return -1;

    /* ensure attribute name exists. */
    if (rm->getTableAttribute(tableName, attributeName, attr, attr_pos))
        return -1;

    /* delete the index file. */
    if (pf->DestroyFile((tableName+"."+attributeName).c_str()))
        return -1;

    return 0;
}

RC IX_Manager::OpenIndex(const string tableName, const string attributeName, IX_IndexHandle &indexHandle)
{
    Attribute attr;
    uint16_t attr_pos;
    PF_FileHandle handle;
    unsigned int pid;

    /* open table. */
    if (rm->openTable(tableName, handle))
        return -1;

    /* retrieve the attribute. */
    if (rm->getTableAttribute(tableName, attributeName, attr, attr_pos))
        return -1;

    /* open the index. */
    if(indexHandle.OpenFile((tableName+"."+attributeName).c_str(), attr))
        return -1;

    /* create root node if doesn't exist already. */
    if (indexHandle.GetNumberOfPages() == 0)
    {
        char root_node[PF_PAGE_SIZE] = {0};
        if(indexHandle.NewNode(root_node, pid))
            return -1;

        /* ensure the root node begins at pid 0. */
        assert(pid == 0);
    }

    return 0;
}

RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle)
{
    return(indexHandle.CloseFile());
}
