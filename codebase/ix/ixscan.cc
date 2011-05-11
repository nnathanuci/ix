#include "ix.h"

//class IX_IndexScan {
// public:
//  IX_IndexScan();  								// Constructor
//  ~IX_IndexScan(); 								// Destructor
//
//  // for the format of "value", please see IX_IndexHandle::InsertEntry()
//  RC OpenScan(const IX_IndexHandle &indexHandle, // Initialize index scan
//	      CompOp      compOp,
//	      void        *value);           
//
//  RC GetNextEntry(RID &rid);  // Get next matching entry
//  RC CloseScan();             // Terminate index scan
//};


IX_IndexScan::IX_IndexScan()
{
}

IX_IndexScan::~IX_IndexScan()
{
}

RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle, CompOp compOp, void *value)
{
    return -1;
}

RC IX_IndexScan::GetNextEntry(RID &rid)  // Get next matching entry
{
    return -1;
}

RC IX_IndexScan::CloseScan()             // Terminate index scan
{
    return -1;
}
