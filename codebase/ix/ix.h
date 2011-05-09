#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../pf/pf.h"
#include "../rm/rm.h"

# define IX_EOF (-1)  // end of the index scan

using namespace std;

class IX_IndexHandle;

class IX_Manager {
 public:
  static IX_Manager* Instance();

  RC CreateIndex(const string tableName,       // create new index
		 const string attributeName);
  RC DestroyIndex(const string tableName,      // destroy an index
		  const string attributeName);
  RC OpenIndex(const string tableName,         // open an index
	       const string attributeName,
	       IX_IndexHandle &indexHandle);
  RC CloseIndex(IX_IndexHandle &indexHandle);  // close index
  
 protected:
  IX_Manager   ();                             // Constructor
  ~IX_Manager  ();                             // Destructor
 
 private:
  static IX_Manager *_ix_manager;
  PF_Manager *pf;
  RM *rm;
};


class IX_IndexHandle {
 public:
  IX_IndexHandle  () { pf = PF_Manager::Instance(); } // Constructor
  ~IX_IndexHandle ();                           // Destructor

  // The following two functions are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  RC InsertEntry(void *key, const RID &rid);  // Insert new index entry
  RC DeleteEntry(void *key, const RID &rid);  // Delete index entry

  /* helper functions to write/read nodes based on id. */
  /* RC WriteNode(int id, void *data); */
  /* RC ReadNode(int id, void *data); */

  RC OpenFile(const char *fileName, Attribute &a) { { if(pf->OpenFile(fileName, handle)) return -1; } attr = a; return 0; }
  RC CloseFile() { { if(pf->CloseFile(handle)) return -1; } return 0; }

 private:
  PF_Manager *pf;
  PF_FileHandle handle;
  Attribute attr;
};


class IX_IndexScan {
 public:
  IX_IndexScan();  								// Constructor
  ~IX_IndexScan(); 								// Destructor

  // for the format of "value", please see IX_IndexHandle::InsertEntry()
  RC OpenScan(const IX_IndexHandle &indexHandle, // Initialize index scan
	      CompOp      compOp,
	      void        *value);           

  RC GetNextEntry(RID &rid);  // Get next matching entry
  RC CloseScan();             // Terminate index scan
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
