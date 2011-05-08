#include "ix.h"

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
