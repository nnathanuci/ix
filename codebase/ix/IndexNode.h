/*
 * IndexNode.h
 *
 *  Created on: May 11, 2011
 *      Author: DA
 */
#ifndef INDEXNODE_H_
#define INDEXNODE_H_

#include <string.h>
#include "ix.h"

#define ENTRY_TRAILER_START	(PF_PAGE_SIZE - 20)
#define MAX_ENTRIES 		((PF_PAGE_SIZE - 20)/8)
#define TYPE_DATA			(0)
#define TYPE_INDEX			(1)
#define TYPE_DELETED 		(2)

#define NODE_HEADER_BYTES	(0)
#define D					(10)
#define	ROOT_PAGE			(0)

class Entry;
extern bool compare_entrys (Entry a, Entry b);

class Entry{
	unsigned int 	r_page;
	unsigned int 	slot_id;	//Only data nodes have this.
	float 			key_real;
	int 			key_int;
	char* 			key_str;
	unsigned int	key_str_length;
	AttrType 		keymode;
	bool			data_entry;

public:
	inline Entry () 							{keymode = TypeInt;data_entry = true;}
	inline Entry (int k   , unsigned int p) 	{this->key_int = k;		this->r_page = p ; keymode = TypeInt; data_entry = false;}
	inline Entry (float k, unsigned int p)		{this->key_real = k;	this->r_page = p ; keymode = TypeReal;data_entry = false;}

	inline Entry (unsigned int len, char* k   , unsigned int p)
	{
		data_entry = false;
		this->key_str_length = len;
		this->key_str = k;
		this->r_page = p ;
	}

	inline unsigned int getPage()	const		{return r_page  ;}

    inline float getKeyReal()		const		{return key_real;}
    inline int getKey()				const		{return key_int	;}
    inline int getMode()			const		{return keymode	;}
    inline int getSlot()			const		{return slot_id	;}
    inline int isData()				const		{return data_entry	;}

    inline void setSlot(unsigned int sid)		{this->slot_id	= sid;}
    inline void setMode(AttrType mode)			{this->keymode	= mode;}
    inline void setKey (int key   )				{this->key_int	= key;}
    inline void setKey (float key)				{this->key_real	= key;}
    inline void setPage(unsigned int r_page) 	{this->r_page	= r_page;}
    inline void setIsData(bool is_data) 		{this->data_entry = is_data;}

    inline void write_int_entry(void* p)
    {
    	(( int*) p)[0]	= key_int;
    	(( int*) p)[1]	= r_page;

    	if (data_entry)
    		(( int*) p)[2]	= slot_id;
    }

    inline void read_int_entry (void* p, unsigned int type)
    {
    	key_int	= ((int*) p)[0] ;
    	r_page	= ((int*) p)[1];

    	if (type == TYPE_DATA)
    	{
    		data_entry = true;
    		slot_id = (( int*) p)[2];
    	}
    	else
    	{
    		data_entry = false;
    	}
    }

    inline void write_real_entry(void* p)
    {
    	(( float*) p)[0] = key_real;
    	(( int*) p)[1] = r_page;

    	if (data_entry)
    		(( float*) p)[2]	= slot_id;
    }
    inline void read_real_entry (void* p, unsigned int type)
    {
    	key_real	= ((float*) p)[0] ;
    	r_page		= ((  int*) p)[1] ;

    	if (type == TYPE_DATA)
    	{
    		data_entry = true;
    		slot_id = (( float*) p)[2];
    	}
    	else
    	{
    		data_entry = false;
    	}
    }

    friend std::ostream& operator<<(std::ostream& os, const Entry& entry);
};

class IndexNode {
	//Int real or char* ?
	AttrType keymode;
	//What page is this ?
	unsigned int page_num;
	//Handle to index file.
	IX_IndexHandle handle;
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
	//Insert node from bottom up.
	Entry*  newchildtry;

	static bool compare_entrys (const Entry a, const Entry b)
	{
		if (a.getMode() == TypeInt)
			return ( a.getKey() - b.getKey() ) < 0;
		else if (a.getMode() == TypeReal)
			return ( a.getKeyReal() - b.getKeyReal() ) < 0;
		return ( a.getKey() - b.getKey() ) < 0;
	}
public:
	IndexNode (IX_IndexHandle& handle_p, unsigned int page)
	{
		free_offset		= NODE_HEADER_BYTES;
		type			= TYPE_DATA;
		newchildtry 	= NULL;

		left			= 0;
		right			= 0;
		num_entries		= 0;
		page_num		= page;
		handle 			= handle_p;
		keymode			= handle.attr.type;

		for (int i = 0; i < MAX_ENTRIES;i++)
		{
			entries[i].setMode(keymode);
		}

		entries[0].setKey (0);
		entries[0].setPage(0);
	} //offset is past the leftmost ptr.
	~IndexNode() {}

	RC insert_entry(Entry key);
	RC remove_entry(Entry key);

	RC find_entry( Entry& e);

	RC find(int key, RID& ret);
	RC find(float key, RID& ret);
	RC tree_search_real(unsigned int nodePointer, Entry key, RID& ret);

	RC tree_search(unsigned int nodePointer, Entry e, RID& ret);

	RC insert(int value, unsigned int r_ptr, unsigned int slot);
	RC insert(float value, unsigned int r_ptr, unsigned int slot);

	RC insert_tree(unsigned int nodePointer, Entry entry);
	RC insert_tree_real(unsigned int nodePointer, Entry to_insert);
	RC get_leftmost_data_node(RID& r_val);

	RC remove(int value);

	void* write_node(void* p);
	void* new_page(void* p);
	IndexNode read_node(void* p , unsigned int page_num);

	inline unsigned int	getFree_offset()	const	{return free_offset;}
	inline unsigned int	getLeft()			const	{return left;}
	inline unsigned int	getRight()			const	{return right;}
	inline unsigned int	getType()			const	{return type;}
	inline unsigned int	getPage_num()		const	{return page_num;}
	inline AttrType		getKeymode()		const	{return keymode;}

	inline void setFree_offset(unsigned int free_offset)	{this->free_offset = free_offset;}
	inline void setLeft(unsigned int left) 					{this->left = left;}
	inline void setRight(unsigned int right)				{this->right = right;}
	inline void setType(unsigned int type)					{this->type = type;}
	inline void setPage_num(unsigned int page_num)			{this->page_num = page_num;}
	inline void setKeymode(AttrType keymode)				{this->keymode = keymode;}

	friend std::ostream& operator<<(std::ostream& os, const IndexNode& node);
};


#endif /* INDEXNODE_H_ */


