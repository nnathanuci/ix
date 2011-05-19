#include <iostream>
#include <cstdio>
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <iostream>
#include <algorithm>
#include <vector>

#include "IndexNode.h"
using namespace std;

std::ostream& operator<<(std::ostream& os, const Entry& entry)
{
	if(entry.getMode() == TypeInt)
	{
		os << "<V: " << entry.getKey() << " P->" << entry.getPage() ;
		if (entry.isData())
			os << " S: " << entry.getSlot( ) << "> ";
		os << "> ";
	}
	else if(entry.getMode() == TypeReal)
	{
		os << "<V: " << entry.getKeyReal() << " P->" << entry.getPage() ;
		if (entry.isData())
			os << " S: " << entry.getSlot( );
		os << "> ";
	}
    return os;
}

std::ostream& operator<<(std::ostream& os, const IndexNode& node)
{
	os << "[ L: " << node.left << " R: " << node.right << " F: " << node.free_offset << " T: " << node.type << " C: " << node.num_entries << " N: " << node.getPage_num() << " ]:::: ";
    for (unsigned int iter = 0; iter < node.num_entries ; iter++)
    	os << node.entries[iter];
    os << std::endl;
    return os;
}

/*Insert directly into the entry list of this node*/
RC IndexNode::insert_entry(Entry e)
{
	if ( (num_entries > 2*D ) )
		return -1;

	if (this->type == TYPE_DATA)
	{
		entries[num_entries].setIsData(true);
		entries[num_entries].setSlot( e.getSlot() );
		free_offset += sizeof(int);
	}
	else
		entries[num_entries].setIsData(false);

	if(e.getMode() == TypeInt)
		entries[num_entries].setKey ( e.getKey () );
	else if(e.getMode() == TypeReal)
		entries[num_entries].setKey ( e.getKeyReal() );

	entries[num_entries].setPage( e.getPage() );


	num_entries++;
	free_offset += 2*sizeof(int);
	std::stable_sort (entries, entries+num_entries, compare_entrys);
	return 0;
}

/*Remove directly from the entry list of this node*/
RC IndexNode::remove_entry(Entry e)
{
	Entry *found = NULL;//= (Entry*) bsearch ( &key, (entries), num_entries, sizeof(Entry), compare_entrys );

	if(e.getMode() == TypeInt)
		for (unsigned int i = 0; i < num_entries; i++)
			if (entries[i].getKey() == e.getKey())
				found = &entries[i];

	if(e.getMode() == TypeReal)
		for (unsigned int i = 0; i < num_entries; i++)
			if (entries[i].getKeyReal() == e.getKeyReal())
				found = &entries[i];

	if ( found == NULL )
		return -1;

	entries[found - entries] = entries[num_entries - 1];
	num_entries--;
	free_offset -= 2*sizeof(int);
	if (this->type == TYPE_DATA)
	{
		free_offset -= sizeof(int);
	}
	std::stable_sort (entries, entries + num_entries, compare_entrys);

	return 0;
}

RC IndexNode::find_entry( Entry& e )
{
	Entry *found = NULL;//= (Entry*) bsearch ( &key, (entries), num_entries, sizeof(Entry), compare_entrys );

	if(e.getMode() == TypeInt)
		for (unsigned int i = 0; i < num_entries; i++)
			if (entries[i].getKey() == e.getKey())
				found = &entries[i];

	if(e.getMode() == TypeReal)
		for (unsigned int i = 0; i < num_entries; i++)
			if (entries[i].getKeyReal() == e.getKeyReal())
				found = &entries[i];

	if ( found == NULL )
		return -1;

	e.setPage((*found).getPage());
	e.setSlot((*found).getSlot());

	return 0;
}

RC IndexNode::find(int key, RID& r_val)
{
	Entry e;
	e.setKey(key);
	RC ret = tree_search(ROOT_PAGE, e, r_val);
	return ret;
}

RC IndexNode::find(float key, RID& r_val)
{
	Entry e;
	e.setKey(key);
	e.setMode(TypeReal);
	return tree_search_real(ROOT_PAGE, e, r_val);
}
RC IndexNode::get_leftmost_data_node(RID& r_val)
{
	unsigned char* root[PF_PAGE_SIZE];
	handle.ReadNode(ROOT_PAGE, root);
	this->read_node(root, ROOT_PAGE);

	unsigned int left_page	= this->getLeft();

	while (left_page != ROOT_PAGE)
	{
		handle.ReadNode(left_page, root);
		this->read_node(root, left_page);
		left_page	= this->getLeft();
	}

	r_val.pageNum = this->getPage_num();
	return 0;
}

RC IndexNode::tree_search_real(unsigned int nodePointer, Entry key, RID& r_val)
{
	unsigned char* root[PF_PAGE_SIZE];
	handle.ReadNode(nodePointer, root);
	this->read_node(root, nodePointer);

	float leftmost_key    	= this->entries[0].getKeyReal();
	Entry rightmost_entry	= (this->entries[this->num_entries - 1]);

	RC ret = -2;
	if ( this->getType() == TYPE_DATA )
	{
		Entry e;
		e.setMode(TypeReal);
		e.setKey(key.getKeyReal());
		RC rc 	= find_entry(e);
		r_val.pageNum	= this->getPage_num();//e.getPage()
		r_val.slotNum	= e.getSlot();
		return rc;
	}
	else
	{
		if( key.getKeyReal() < leftmost_key )
			ret = this->tree_search_real(this->left, key, r_val);
		else if( key.getKeyReal() >= rightmost_entry.getKeyReal() )
			ret = this->tree_search_real(rightmost_entry.getPage(), key, r_val );
		else
		{
			float left		= leftmost_key;
			float right		= entries[1].getKeyReal();
			int iter		= 0;
			bool found	= false;

			while (!found && iter < (num_entries - 1))
			{
				if (left <= key.getKeyReal() &&  right > key.getKeyReal() )
				{
					ret = this->tree_search_real(entries[iter].getPage(), key, r_val);
					found = true;
				}

				iter++;
				left  = right;
				right = entries[iter+1].getKeyReal();
			}
		}
	}
	return ret;
}

RC IndexNode::tree_search(unsigned int nodePointer, Entry key, RID& r_val)
{
	unsigned char* root[PF_PAGE_SIZE];
	handle.ReadNode(nodePointer, root);
	this->read_node(root, nodePointer);

	int leftmost_key       = this->entries[0].getKey();
	Entry rightmost_entry  = (this->entries[this->num_entries - 1]);

	RC ret = -2;
	if ( this->getType() == TYPE_DATA )
	{
		Entry e;
		e.setKey(key.getKey());
		RC rc 	= find_entry(e);
		r_val.pageNum	= this->getPage_num();//e.getPage()
		r_val.slotNum	= e.getSlot();
		return rc;
	}
	else
	{
		if( key.getKey() < leftmost_key )
			ret = this->tree_search(this->left, key, r_val);
		else if( key.getKey() >= rightmost_entry.getKey() )
			ret = this->tree_search(rightmost_entry.getPage(), key, r_val );
		else
		{
			int left		= leftmost_key;
			int right		= entries[1].getKey();
			int iter		= 0;
			bool found	= false;

			while (!found && iter < (num_entries - 1))
			{
				if (left <= key.getKey() &&  right > key.getKey() )
				{
					ret = this->tree_search(entries[iter].getPage(), key, r_val);
					found = true;
				}

				iter++;
				left  = right;
				right = entries[iter+1].getKey();
			}
		}
	}
	return ret;
}

RC IndexNode::insert(int key, unsigned int r_ptr, unsigned int slot)
{
	unsigned char* read_buffer [PF_PAGE_SIZE]  = {0};
	Entry to_insert (key , r_ptr);
	to_insert.setSlot(slot);

	//No root.
	if (handle.GetNumberOfPages() == 0)
		new_page(read_buffer);

	RC ret =  IndexNode::insert_tree( ROOT_PAGE, to_insert);

	return ret;
}

RC IndexNode::insert(float key, unsigned int r_ptr, unsigned int slot)
{
	unsigned char* read_buffer [PF_PAGE_SIZE]  = {0};
	Entry to_insert (key , r_ptr);
	to_insert.setMode(TypeReal);
	to_insert.setSlot(slot);

	//No root.
	if (handle.GetNumberOfPages() == 0)
		new_page(read_buffer);

	return IndexNode::insert_tree_real( ROOT_PAGE, to_insert);
}

RC IndexNode::insert_tree(unsigned int nodePointer, Entry to_insert)
{
	unsigned char* read_buffer [PF_PAGE_SIZE]  = {0};
	unsigned char* write_buffer [PF_PAGE_SIZE] = {0};

	unsigned int search_parent = this->page_num;

	handle.ReadNode(nodePointer, read_buffer);
	this->read_node(read_buffer, nodePointer);

	if ( type == TYPE_DATA )
	{
		this->insert_entry( to_insert );
		if (num_entries <= 2*D ) //Usual case, insert entry at leaf node.
		{
			this->write_node(write_buffer);
			handle.WriteNode(nodePointer, write_buffer);
		}
		else //data node is full, must split.
		{
			unsigned char* buffer [PF_PAGE_SIZE]  = {0};
			bool new_root = false;

		    if(page_num == ROOT_PAGE) //Data page split was a root page.
		    {
		    	new_root = true;
		    	unsigned int pid = 0;
		    	handle.NewNode( (const void*)buffer, pid );
		    	IndexNode left_neighbor(handle, pid);
		    	left_neighbor = (*this);
		    	left_neighbor.setPage_num(pid);
		    	(*this) = left_neighbor;
		    }

			unsigned int pid = 0;
			handle.NewNode( (const void*)buffer, pid );
			IndexNode right_neighbor(handle, pid);

			unsigned int midpoint = (floor((float)this->num_entries/2));
			unsigned int entries_at_start = this->num_entries;

		    for (unsigned int iter = midpoint; iter < entries_at_start ; iter++)
		    	right_neighbor.insert_entry( this->entries[iter] );

		    for (unsigned int iter = entries_at_start - 1; iter >=  midpoint; iter--)
		    	this->remove_entry(entries[iter]);

		    if (this->getRight() != ROOT_PAGE)
		    {
				handle.ReadNode(this->getRight(), buffer);
				IndexNode right_of_right(handle, this->getRight());
				right_of_right.read_node(buffer, this->getRight());
				right_of_right.setLeft(right_neighbor.getPage_num());

			    right_of_right.write_node(buffer);
			    handle.WriteNode(right_of_right.getPage_num(), buffer);
		    }

		    right_neighbor.setRight(this->getRight());
		    right_neighbor.setLeft(this->getPage_num());
		    this->setRight(pid);

		    right_neighbor.write_node(buffer);
		    handle.WriteNode(right_neighbor.getPage_num(), buffer);

		    this->write_node(buffer);
		    handle.WriteNode(this->getPage_num(), buffer);

		    if(new_root)
		    {
		    	handle.ReadNode(ROOT_PAGE, buffer);
		    	IndexNode root(handle, ROOT_PAGE);
		    	root.setType(TYPE_INDEX);
		    	root.setLeft(this->page_num);

		    	Entry r;
		    	r.setKey(right_neighbor.entries[0].getKey());
		    	r.setPage(right_neighbor.getPage_num());
		    	r.setIsData(false);

		    	root.insert_entry(r);
		    	(*this) = root;

		    	this->write_node(write_buffer);
		    	handle.WriteNode(ROOT_PAGE, write_buffer);
		    }
		    else
		    {
		    	Entry* newChild = new Entry();
		    	newChild->setKey(right_neighbor.entries[0].getKey());
		    	newChild->setPage(right_neighbor.getPage_num());
		    	this->newchildtry = newChild;
		    }
		}
	}
	else
	{
		Entry leftmost_entry    = entries[0];
		Entry rightmost_entry  	= (this->entries[this->num_entries - 1]);

		if( to_insert.getKey() < leftmost_entry.getKey() )
			this->insert_tree(this->left, to_insert);
		else if( to_insert.getKey() >= rightmost_entry.getKey() )
			this->insert_tree(rightmost_entry.getPage(), to_insert );
		else
		{
			Entry left		= leftmost_entry;
			Entry right		= entries[1];
			int iter		= 0;
			bool inserted	= false;

			while (!inserted && iter < (num_entries - 1))
			{
				if (left.getKey() <= to_insert.getKey() &&  right.getKey() > to_insert.getKey() )
					this->insert_tree(entries[iter].getPage(), to_insert );

				iter++;
				left  = right;
				right.setKey(entries[iter+1].getKey());
			}
		}

		if (newchildtry != NULL)
		{
			to_insert = *newchildtry;
			insert_entry( to_insert );

			if (num_entries <= 2*D ) //Usual case,didn't split child.
			{
				this->write_node(write_buffer);
				handle.WriteNode(page_num, write_buffer);
				delete(newchildtry);
				this->newchildtry = NULL;
			}
			else //index node is full, must split.
			{
				unsigned char* buffer [PF_PAGE_SIZE]  = {0};
				bool new_root = false;

				if(page_num == ROOT_PAGE) //Data page split was a root page.
				{
					new_root = true;
					unsigned int pid = 0;
					handle.NewNode( (const void*)buffer, pid );
					IndexNode left_neighbor(handle, pid);
					left_neighbor = (*this);
					left_neighbor.setPage_num(pid);
					(*this) = left_neighbor;
				}

				unsigned int pid = 0;
				handle.NewNode( (const void*)buffer, pid );
				IndexNode right_neighbor(handle, pid);
				right_neighbor.setType(TYPE_INDEX);

				unsigned int midpoint = (floor((float)this->num_entries/2));
				unsigned int entries_at_start = this->num_entries;

				//Skip the index node to be added above.
				for (unsigned int iter = midpoint ; iter < entries_at_start ; iter++)
					right_neighbor.insert_entry( this->entries[iter] );

				for (unsigned int iter = entries_at_start - 1; iter >=  midpoint; iter--)
					this->remove_entry(entries[iter]);

				this->setRight(pid);
				right_neighbor.setLeft(right_neighbor.entries[0].getPage());

				right_neighbor.write_node(buffer);
				handle.WriteNode(right_neighbor.getPage_num(), buffer);

				this->write_node(buffer);
				handle.WriteNode(this->getPage_num(), buffer);

				if(new_root)
				{
					handle.ReadNode(ROOT_PAGE, buffer);
					IndexNode root(handle, ROOT_PAGE);
					root.setType(TYPE_INDEX);
					root.setLeft(this->page_num);

					Entry r;
					r.setKey(right_neighbor.entries[0].getKey());
					r.setPage(right_neighbor.getPage_num());
					r.setIsData(false);
					root.insert_entry(r);
					(*this) = root;

					this->write_node(write_buffer);
					handle.WriteNode(ROOT_PAGE, write_buffer);
				}
				else
				{
					Entry* newChild = new Entry();
					newChild->setKey(right_neighbor.entries[0].getKey());
					newChild->setPage(right_neighbor.getPage_num());
					this->newchildtry = newChild;
				}

				right_neighbor.remove_entry(entries[0]);
				right_neighbor.write_node(buffer);
				handle.WriteNode(right_neighbor.getPage_num(), buffer);
			}
		}
	}

	handle.ReadNode(search_parent, read_buffer);
	this->read_node(read_buffer, search_parent);

	return 0;
}


RC IndexNode::insert_tree_real(unsigned int nodePointer, Entry to_insert)
{
	unsigned char* read_buffer [PF_PAGE_SIZE]  = {0};
	unsigned char* write_buffer [PF_PAGE_SIZE] = {0};

	unsigned int search_parent = this->page_num;

	handle.ReadNode(nodePointer, read_buffer);
	this->read_node(read_buffer, nodePointer);

	to_insert.setMode(TypeReal);
	if ( type == TYPE_DATA )
	{
		this->insert_entry( to_insert );

		if (num_entries <= 2*D ) //Usual case, insert entry at leaf node.
		{
			this->write_node(write_buffer);
			handle.WriteNode(nodePointer, write_buffer);
		}
		else //data node is full, must split.
		{
			unsigned char* buffer [PF_PAGE_SIZE]  = {0};
			bool new_root = false;

		    if(page_num == ROOT_PAGE) //Data page split was a root page.
		    {
		    	new_root = true;
		    	unsigned int pid = 0;
		    	handle.NewNode( (const void*)buffer, pid );
		    	IndexNode left_neighbor(handle, pid);
		    	left_neighbor = (*this);
		    	left_neighbor.setPage_num(pid);
		    	left_neighbor.setKeymode(TypeReal);
		    	(*this) = left_neighbor;
		    }

			unsigned int pid = 0;
			handle.NewNode( (const void*)buffer, pid );
			IndexNode right_neighbor(handle, pid);
			right_neighbor.setKeymode(TypeReal);

			unsigned int midpoint = (floor((float)this->num_entries/2));
			unsigned int entries_at_start = this->num_entries;

		    for (unsigned int iter = midpoint; iter < entries_at_start ; iter++)
		    	right_neighbor.insert_entry( this->entries[iter] );

		    for (unsigned int iter = entries_at_start - 1; iter >=  midpoint; iter--)
		    	this->remove_entry(entries[iter]);

		    if (this->getRight() != ROOT_PAGE)
		    {
				handle.ReadNode(this->getRight(), buffer);
				IndexNode right_of_right(handle, this->getRight());
				right_of_right.setKeymode(TypeReal);
				right_of_right.read_node(buffer, this->getRight());
				right_of_right.setLeft(right_neighbor.getPage_num());

			    right_of_right.write_node(buffer);
			    handle.WriteNode(right_of_right.getPage_num(), buffer);
		    }

		    right_neighbor.setRight(this->getRight());
		    right_neighbor.setLeft(this->getPage_num());
		    this->setRight(pid);

		    right_neighbor.write_node(buffer);
		    handle.WriteNode(right_neighbor.getPage_num(), buffer);

		    this->write_node(buffer);
		    handle.WriteNode(this->getPage_num(), buffer);

		    if(new_root)
		    {
		    	handle.ReadNode(ROOT_PAGE, buffer);
		    	IndexNode root(handle, ROOT_PAGE);
		    	root.setType(TYPE_INDEX);
		    	root.setLeft(this->page_num);
		    	root.setKeymode(TypeReal);

		    	Entry r;
		    	r.setIsData(false);
		    	r.setMode(TypeReal);
		    	r.setKey(right_neighbor.entries[0].getKeyReal());

		    	r.setPage(right_neighbor.getPage_num());
		    	root.insert_entry(r);
		    	(*this) = root;

		    	this->write_node(write_buffer);
		    	handle.WriteNode(ROOT_PAGE, write_buffer);
		    }
		    else
		    {
		    	Entry* newChild = new Entry();
		    	newChild->setMode(TypeReal);
			    newChild->setKey(right_neighbor.entries[0].getKeyReal());

		    	newChild->setPage(right_neighbor.getPage_num());
		    	this->newchildtry = newChild;
		    }
		}
	}
	else
	{
		Entry leftmost_entry    = entries[0];
		leftmost_entry.setMode(TypeReal);
		Entry rightmost_entry  	= (this->entries[this->num_entries - 1]);
		rightmost_entry.setMode(TypeReal);

		if( to_insert.getKeyReal() < leftmost_entry.getKeyReal() )
			this->insert_tree_real(this->left, to_insert);
		else if( to_insert.getKeyReal() >= rightmost_entry.getKeyReal() )
			this->insert_tree_real(rightmost_entry.getPage(), to_insert );
		else
		{
			Entry left		= leftmost_entry;
			left.setMode(TypeReal);
			Entry right		= entries[1];
			right.setMode(TypeReal);
			int iter		= 0;
			bool inserted	= false;

			while (!inserted && iter < (num_entries - 1))
			{
				if (left.getKeyReal() <= to_insert.getKeyReal() &&  right.getKeyReal() > to_insert.getKeyReal() )
					this->insert_tree_real(entries[iter].getPage(), to_insert );

				iter++;
				left  = right;
				right.setKey(entries[iter+1].getKeyReal());
			}
		}

		if (newchildtry != NULL)
		{
			to_insert = *newchildtry;
			to_insert.setMode(TypeReal);
			insert_entry( to_insert );

			if (num_entries <= 2*D ) //Usual case,didn't split child.
			{
				this->write_node(write_buffer);
				handle.WriteNode(page_num, write_buffer);
				delete(newchildtry);
				this->newchildtry = NULL;
			}
			else //index node is full, must split.
			{
				unsigned char* buffer [PF_PAGE_SIZE]  = {0};
				bool new_root = false;

				if(page_num == ROOT_PAGE) //Data page split was a root page.
				{
					new_root = true;
					unsigned int pid = 0;
					handle.NewNode( (const void*)buffer, pid );
					IndexNode left_neighbor(handle, pid);
					left_neighbor.setKeymode(TypeReal);
					left_neighbor = (*this);
					left_neighbor.setPage_num(pid);
					(*this) = left_neighbor;
				}

				unsigned int pid = 0;
				handle.NewNode( (const void*)buffer, pid );
				IndexNode right_neighbor(handle, pid);
				right_neighbor.setType(TYPE_INDEX);
				right_neighbor.setKeymode(TypeReal);

				unsigned int midpoint = (floor((float)this->num_entries/2));
				unsigned int entries_at_start = this->num_entries;

				//Skip the index node to be added above.
				for (unsigned int iter = midpoint ; iter < entries_at_start ; iter++)
					right_neighbor.insert_entry( this->entries[iter] );

				for (unsigned int iter = entries_at_start - 1; iter >=  midpoint; iter--)
					this->remove_entry(entries[iter]);

				this->setRight(pid);
				right_neighbor.setLeft(right_neighbor.entries[0].getPage());

				right_neighbor.write_node(buffer);
				handle.WriteNode(right_neighbor.getPage_num(), buffer);

				this->write_node(buffer);
				handle.WriteNode(this->getPage_num(), buffer);

				if(new_root)
				{
					handle.ReadNode(ROOT_PAGE, buffer);
					IndexNode root(handle, ROOT_PAGE);
					root.setType(TYPE_INDEX);
					root.setLeft(this->page_num);
					root.setKeymode(TypeReal);

					Entry r;
					r.setMode(TypeReal);
					r.setKey(right_neighbor.entries[0].getKeyReal());
					r.setPage(right_neighbor.getPage_num());
					root.insert_entry(r);
					(*this) = root;

					this->write_node(write_buffer);
					handle.WriteNode(ROOT_PAGE, write_buffer);
				}
				else
				{
					Entry* newChild = new Entry();
					newChild->setMode(TypeReal);
					newChild->setKey(right_neighbor.entries[0].getKeyReal());
					newChild->setPage(right_neighbor.getPage_num());
					this->newchildtry = newChild;
				}

				right_neighbor.remove_entry(entries[0]);
				right_neighbor.write_node(buffer);
				handle.WriteNode(right_neighbor.getPage_num(), buffer);
			}
		}
	}

	handle.ReadNode(search_parent, read_buffer);
	this->read_node(read_buffer, search_parent);

	return 0;
}

RC IndexNode::remove(int key)
{
	unsigned char* read_buffer [PF_PAGE_SIZE]  = {0};
	unsigned char* write_buffer [PF_PAGE_SIZE] = {0};

	//No root.
	if (handle.GetNumberOfPages() == 0)
		return -1;

	handle.ReadNode(ROOT_PAGE, read_buffer);
	this->read_node(read_buffer, ROOT_PAGE);

	if ( type == TYPE_DATA )
	{
		//remove_entry(key);
	}
	else
	{
		cerr << ("XX Only data pages currently supported.\n");
	}

	this->write_node(write_buffer);
	handle.WriteNode(ROOT_PAGE, write_buffer);
	return 0;
}

void* IndexNode::new_page(void* p)
{
	unsigned int* page_trailer = ((unsigned int*) p ) + (ENTRY_TRAILER_START / sizeof(unsigned int)) ;

	*page_trailer = left = 0;
	page_trailer++;
	*page_trailer = right = 0;
	page_trailer++;
	*page_trailer = free_offset = 0;
	page_trailer++;
	*page_trailer = type = 0;
	page_trailer++;
	*page_trailer = num_entries = 0;
	page_trailer++;

	return p;
}

void* IndexNode::write_node(void* p)
{
	int* page = (int*) p;
	int* page_trailer = ((int*) p ) + (ENTRY_TRAILER_START / sizeof(int)) ;

	if (keymode == TypeInt)
	{
		for (unsigned int iter = 0; iter < num_entries ; iter++ )
		{
			entries[iter].write_int_entry(page);
			if (type == TYPE_DATA)
				page+=3;
			else
				page+=2;
		}
	}
	else if (keymode == TypeReal)
	{
		for (unsigned int iter = 0; iter < num_entries ; iter++)
		{
			//cout << "Write " << entries[iter] ;
			entries[iter].write_real_entry(page);

			if (type == TYPE_DATA)
				page+=3;
			else
				page+=2;
		}
	}

	*page_trailer = left;
	page_trailer++;
	*page_trailer = right;
	page_trailer++;
	*page_trailer = free_offset;
	page_trailer++;
	*page_trailer = type;
	page_trailer++;
	*page_trailer = num_entries;
	page_trailer++;

	return p;
}

IndexNode IndexNode::read_node(void* p , unsigned int page_num)
{
	this->setPage_num(page_num);
	int* page = (int*) p;
	int* page_trailer = ((int*) p ) + (ENTRY_TRAILER_START / sizeof(int)) ;

    left = *page_trailer;
    page_trailer++;
	right = *page_trailer;
	page_trailer++;
	free_offset = *page_trailer ;
	page_trailer++;
	type = *page_trailer;
	page_trailer++;
	num_entries = *page_trailer ;
	page_trailer++;

	if (keymode == TypeInt)
	{
		for (unsigned int iter = 0; iter < num_entries ; iter++ )
		{
			entries[iter].read_int_entry(page,type );

			if (type == TYPE_DATA)
				page+=3;
			else
				page+=2;
		}
	}
	else if (keymode == TypeReal)
	{
	    for (unsigned int iter = 0; iter < num_entries ; iter++ )
	    {
	    	entries[iter].read_real_entry(page, type);
	    	//cout << "Write " << entries[iter] ;
			if (type == TYPE_DATA)
				page+=3;
			else
				page+=2;
	    }
	}


    return *(this);
}
