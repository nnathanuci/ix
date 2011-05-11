#include <iostream>
#include <cstdio>
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include "IndexNode.h"

using namespace std;


std::ostream& operator<<(std::ostream& os, const Entry& entry)
{
    os << "<V: " << entry.getKey() << " P->" << entry.getPage() << "> ";
    return os;
}

int compare_entrys (const void* a, const void* b)
{
	Entry A = *( (Entry*) a);
	Entry B = *( (Entry*) b);
	return ( (int) A.getKey() - (int) B.getKey() );
}

std::ostream& operator<<(std::ostream& os, const IndexNode& node)
{
	os << "[ L: " << node.left << " R: " << node.right << " F: " << node.free_offset << " T: " << node.type << " C: " << node.num_entries << " ]:::: ";
    for (unsigned int iter = 0; iter < node.num_entries ; iter++)
    	os << node.entries[iter];
    os << std::endl;
    return os;
}


int IndexNode::insert(unsigned int key, unsigned int r_ptr)
{
	if ( type == TYPE_DATA )
	{
		/* insert into the node internally */
		entries[num_entries].setKey(key);
		entries[num_entries].setPage(r_ptr);
		qsort (entries, num_entries, sizeof(Entry), compare_entrys);
		num_entries++;
		free_offset += sizeof(unsigned int);
	}
	else
	{
		perror("Only data pages currently supported.");
	}

	return 0;
}

int IndexNode::remove(unsigned int key)
{
	if ( type == TYPE_DATA )
	{
		Entry *found = (Entry*) bsearch ( &key, (entries), num_entries, sizeof(Entry), compare_entrys );

		if ( found == NULL || (found == entries - 1) )
			return -1;

		entries[found - entries] = entries[num_entries - 1];
		num_entries--;
		free_offset -= sizeof(unsigned int);
		qsort (entries, num_entries, sizeof(Entry), compare_entrys);
	}
	else
	{

	}
	return 0;
}

void IndexNode::write_node(void* p)
{
	unsigned int* page = (unsigned int*) p;
	unsigned int* page_trailer = ((unsigned int*) p ) + (ENTRY_TRAILER_START / sizeof(unsigned int)) ;

    for (unsigned int iter = 0; iter <= num_entries ; iter++ , page+=2)
    	entries[iter].write_entry(page);

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


}

void IndexNode::read_node(void* p)
{
	unsigned int* page = (unsigned int*) p;
	unsigned int* page_trailer = ((unsigned int*) p ) + (ENTRY_TRAILER_START / sizeof(unsigned int)) ;

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

    for (unsigned int iter = 0; iter <= num_entries ; iter++ , page+=2)
    	entries[iter].read_entry(page);
}

int test_insert_delete_visual_1() {
	IndexNode node;
	node.insert(70,11);
	cout << node ;
	node.insert(21,12);
	cout << node ;
	node.insert(22,22);
	cout << node ;
	node.insert(35,33);
	cout << node ;
	node.insert(17,19);
	cout << node ;
	node.insert(23,11);
	cout << node ;
	node.remove(21);
	cout << node ;
	node.remove(35);
	cout << node ;
	node.remove(17);
	cout << node ;
	node.remove(23);
	cout << node ;
	node.remove(70);
	cout << node ;
	node.remove(22);
	cout << node ;

	return 0;
}

int test_read_write_to_bytes()
{
	IndexNode node, node_test;
	node.insert(70,11);
	node.insert(21,12);
	node.insert(22,22);
	node.insert(35,33);
	node.insert(17,19);
	node.insert(23,11);

	void* p = calloc(MAX_ENTRIES,sizeof(Entry));
	node.write_node(p);
	node_test.read_node(p);
	cout << node ;
	cout << node_test ;

	free(p);
	return 0;
}
/*
int main()
{
	test_read_write_to_bytes();
}
*/
