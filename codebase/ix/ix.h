#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../pf/pf.h"
#include "../rm/rm.h"

# define IX_EOF (-1)  // end of the index scan

/* used only for the dump routines. */
#define DUMP_TYPE_DATA (0)
#define DUMP_TYPE_INDEX (1)
#define DUMP_TYPE_DELETED (2)
#define DUMP_NO_PID (0)


/* header fields */
#define DUMP_HEADER_START (PF_PAGE_SIZE - 20)
#define DUMP_LEFT_PID (DUMP_HEADER_START+0)
#define DUMP_RIGHT_PID (DUMP_HEADER_START+4)
#define DUMP_FREE_OFFSET (DUMP_HEADER_START+8)
#define DUMP_TYPE (DUMP_HEADER_START+12)
#define DUMP_NUM_ENTRIES (DUMP_HEADER_START+16)

/* macros to get the values. */
#define DUMP_GET_LEFT_PID(buf_start) (*((unsigned int *) &((buf_start)[DUMP_LEFT_PID])))
#define DUMP_GET_RIGHT_PID(buf_start) (*((unsigned int *) &((buf_start)[DUMP_RIGHT_PID])))
#define DUMP_GET_FREE_OFFSET(buf_start) (*((unsigned int *) &((buf_start)[DUMP_FREE_OFFSET])))
#define DUMP_GET_TYPE(buf_start) (*((unsigned int *) &((buf_start)[DUMP_TYPE])))
#define DUMP_GET_NUM_ENTRIES(buf_start) (*((unsigned int *) &((buf_start)[DUMP_NUM_ENTRIES])))
#define DUMP_GET_FREE_SPACE(buf_start) (DUMP_HEADER_START - DUMP_GET_FREE_OFFSET(buf_start))

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
  IX_IndexHandle  (); // Constructor
  ~IX_IndexHandle (); // Destructor

  // The following two functions are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  RC InsertEntry(void *key, const RID &rid);  // Insert new index entry
  RC DeleteEntry(void *key, const RID &rid);  // Delete index entry

  /* helper functions to write/read nodes based on id. */
  /* RC WriteNode(int id, void *data); */
  /* RC ReadNode(int id, void *data); */

  RC OpenFile(const char *fileName, Attribute &a);
  RC CloseFile();
  unsigned GetNumberOfPages();
  RC ReadNode(unsigned int pid, const void *data);
  RC WriteNode(unsigned int pid, const void *data);
  RC DeleteNode(unsigned int pid, const void *data);

  /* for now, NewNode appends all created nodes to the end of the page file. It returns by reference the pid.
     (this will later be refactored to scan for the next available free node, and return that pid instead.)
  */
  RC NewNode(const void *data, unsigned int &pid);

  /* node debugging utils. */
  void DumpNode(const char *node, unsigned int pid, unsigned int verbose);
  void DumpNodeTerse(const char *node, unsigned int pid);
  RC DumpNode(unsigned int pid, unsigned int verbose);

  Attribute attr;

 public:
  bool in_use;

 private:
  PF_Manager *pf;
  PF_FileHandle handle;
};


class IX_IndexScan {
 public:
  IX_IndexScan();  								// Constructor
  ~IX_IndexScan(); 								// Destructor

  // for the format of "value", please see IX_IndexHandle::InsertEntry()
  RC OpenScan(const IX_IndexHandle &indexHandle, // Initialize index scan
	      CompOp      compOp,
	      void        *value);           

  RC OpenScan(const IX_IndexHandle &indexHandle, // Initialize index scan
	      CompOp      compOp,
	      void        *value, int anchor_pid);           

  RC GetNextEntry(RID &rid);  // Get next matching entry
  RC GetNextEntryEQ(RID &rid);  // Get next matching entry
  RC GetNextEntryNE(RID &rid);  // Get next matching entry
  RC GetNextEntryGT(RID &rid);  // Get next matching entry
  RC GetNextEntryGE(RID &rid);  // Get next matching entry
  RC GetNextEntryLT(RID &rid);  // Get next matching entry
  RC GetNextEntryLE(RID &rid);  // Get next matching entry
  RC GetNextEntryNOOP(RID &rid);  // Get next matching entry
  RC CloseScan();             // Terminate index scan


  IX_IndexHandle handle;
  Attribute cond_attr;
  CompOp op;
  unsigned int start_pid;
  unsigned int next_pid;

  int k_int;
  float k_float;
  char k_varchar[PF_PAGE_SIZE];
  int k_len;

  char last_node[PF_PAGE_SIZE];
  int last_node_next;
  unsigned int last_node_pid;

  unsigned int n_matches;
};

// print out the error message for a given return code
void IX_PrintError (RC rc);

#endif
