/*
 * IndexNode.h
 *
 *  Created on: May 11, 2011
 *      Author: DA
 */
#ifndef INDEXNODE_H_
#define INDEXNODE_H_

#include <string.h>

#define PF_PAGE_SIZE (4096)
#define ENTRY_TRAILER_START	(PF_PAGE_SIZE - 20)
#define MAX_ENTRIES 		((PF_PAGE_SIZE - 20)/4)
#define TYPE_DATA			(0)
#define TYPE_INDEX			(1)
#define TYPE_DELETED 		(2)
#define NODE_HEADER_BYTES	(16)

extern int compare_entrys (const void* a, const void* b);

class Entry{
	unsigned int key;
	unsigned int r_page;
public:
    unsigned int getKey()  const		{return key		;}
    unsigned int getPage() const		{return r_page  ;}
    void setKey (unsigned int key   )	{this->key = key;}
    void setPage(unsigned int r_page) 	{this->r_page = r_page;}
    void write_entry(void* p)			{((unsigned int*) p)[0] = key;((unsigned int*) p)[1] = r_page;}
    void read_entry (void* p)			{key = ((int*) p)[0] ; r_page = ((int*) p)[1] ;}

    friend std::ostream& operator<<(std::ostream& os, const Entry& entry);
};

//http://www.java2s.com/Tutorial/Cpp/0180__Class/Usetypeidtotesttypeequality.htm }-> test class type.
class IndexNode {
	//1. left pid (0 means there is no left pid)
	unsigned int  left;
	//2. right pid (0 means there is no right pid
	unsigned int  right;
	//3. free space offset (initially 0) | used to calculate the amount of free space
	unsigned int  free_offset;
	//4. node type| 0: deleted node | 1: index node | 2: leaf node
	unsigned int  type;
	//5. number of items
	unsigned int  num_entries;
	//Account for trailer.
	Entry  entries [MAX_ENTRIES] ; /******/

public:
	IndexNode ()
	{
		free_offset		= NODE_HEADER_BYTES;
		left			= 0;
		right			= 0;
		num_entries		= 0;
		type			= TYPE_DATA;

		memset( entries, 0, sizeof(entries) );
		entries[0].setKey (0);
		entries[0].setPage(0);
	} //offset is past the leftmost ptr.
	~IndexNode() {}

	int insert(unsigned int value, unsigned int r_ptr);
	int remove(unsigned int value);
	void write_node(void* p);
	void read_node(void* p);

    unsigned int getFree_offset() const				{return free_offset;};
    unsigned int getLeft() const					{return left;};
    unsigned int getRight() const					{return right;};
    unsigned int getType() const					{return type;};
    void setFree_offset(unsigned int free_offset)	{this->free_offset = free_offset;};
    void setLeft(unsigned int left) 				{this->left = left;};
    void setRight(unsigned int right)				{this->right = right;};
    void setType(unsigned int type)					{this->type = type;};


	friend std::ostream& operator<<(std::ostream& os, const IndexNode& node);
};

#endif /* INDEXNODE_H_ */
