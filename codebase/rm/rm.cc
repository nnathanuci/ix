#include "rm.h"
#include <cstdio>
#include <fstream>
#include <iostream>
#include <cassert>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sstream>


RM* RM::_rm = 0;

RM* RM::Instance()
{
    if (!_rm)
        _rm = new RM();

    return _rm;
}

RM::RM()
{
    pf = PF_Manager::Instance();

    /* spurious output of diagnostic messages. */
    debug = false;

    /* open system catalogue */

    /* hardcode system catalogue attributes and table name. */
    {
        Attribute attr;

        attr.name = "table";
        attr.type = TypeVarChar;
        attr.length = (AttrLength) MAX_CAT_NAME_LEN;
        system_catalog_attrs.push_back(attr);

        attr.name = "attribute";
        attr.type = TypeVarChar;
        attr.length = (AttrLength) MAX_CAT_NAME_LEN;
        system_catalog_attrs.push_back(attr);

        attr.name = "type";
        attr.type = TypeInt;
        attr.length = sizeof(int);
        system_catalog_attrs.push_back(attr);

        attr.name = "length";
        attr.type = TypeInt;
        attr.length = sizeof(int);
        system_catalog_attrs.push_back(attr);

        attr.name = "position";
        attr.type = TypeInt;
        attr.length = sizeof(int);
        system_catalog_attrs.push_back(attr);

        system_catalog_tablename = SYSTEM_CAT_TABLENAME;
    }

    /* open the system catalog, cache the handle */
    {
        PF_FileHandle handle;

        /* we have to assume this is guaranteed to work. */
        assert(openTable(system_catalog_tablename, handle) == 0);
    }
}

RM::~RM()
{
    closeAllTables();
}

RC RM::AllocateControlPage(PF_FileHandle &fileHandle) // {{{
{
    /* buffer to write control page. */
    uint8_t page[PF_PAGE_SIZE];

    /* overlay buffer as a uint16_t array. */
    uint16_t *ctrl_page = (uint16_t *) page;

    /* blank the space for all pages. */
    for (unsigned int i = 0; i < CTRL_MAX_PAGES; i++)
        ctrl_page[i] = SLOT_MAX_SPACE;

    return(fileHandle.AppendPage(page));
} // }}}

RC RM::AllocateDataPage(PF_FileHandle &fileHandle) // {{{
{
    /* buffer to write blank page. */
    uint8_t page[PF_PAGE_SIZE] = {0};

    uint16_t *slot_page = (uint16_t *) page;

    /* all fields are 0, except: num_slots, for 1 unallocated slot, and slot 0 stores slot queue end marker. */
    slot_page[SLOT_NUM_SLOT_INDEX] = 1;
    slot_page[SLOT_GET_SLOT_INDEX(0)] = SLOT_QUEUE_END;

    return(fileHandle.AppendPage(page));
} // }}}

RC RM::getPageSpace(PF_FileHandle &fileHandle, unsigned int page_id, uint16_t &unused_space) // {{{
{
    /* buffer to read in the control page. */
    uint16_t ctrl_page[CTRL_MAX_PAGES];

    unsigned int ctrl_page_id;
    uint16_t page_id_offset;

    /* get number of allocated pages. */
    unsigned int n_pages = fileHandle.GetNumberOfPages();

    /* make sure the page_id is valid. */
    if (page_id >= n_pages || CTRL_IS_CTRL_PAGE(page_id))
        return -1;

    /* determine the absolute control page id for a given page. */
    ctrl_page_id = CTRL_GET_CTRL_PAGE(page_id);

    /* get the offset in the control page. */
    page_id_offset = CTRL_GET_CTRL_PAGE_OFFSET(page_id);


    /* read in control page. */
    if (fileHandle.ReadPage(ctrl_page_id, (void *) ctrl_page))
        return -1;

    /* return the unused space in the page. */
    unused_space = ctrl_page[page_id_offset];

    return 0;
} // }}}

RC RM::decreasePageSpace(PF_FileHandle &fileHandle, unsigned int page_id, uint16_t space) // {{{
{
    /* buffer to read in the control page. */
    uint16_t ctrl_page[CTRL_MAX_PAGES];

    unsigned int ctrl_page_id;
    uint16_t page_id_offset;

    /* get number of allocated pages. */
    unsigned int n_pages = fileHandle.GetNumberOfPages();

    /* make sure the page_id is valid. */
    if (page_id >= n_pages || CTRL_IS_CTRL_PAGE(page_id))
        return -1;


    /* determine the absolute control page id for a given page. */
    ctrl_page_id = CTRL_GET_CTRL_PAGE(page_id);

    /* get the offset in the control page. */
    page_id_offset = CTRL_GET_CTRL_PAGE_OFFSET(page_id);

    /* read in control page. */
    if (fileHandle.ReadPage(ctrl_page_id, (void *) ctrl_page))
        return -1;

    /* cannot decrease the space if more than what is unused. */
    if (space > ctrl_page[page_id_offset])
        return -1;

    /* decrease the space for page on control page. */
    ctrl_page[page_id_offset] -= space;

    /* write back the control page. */
    if (fileHandle.WritePage(ctrl_page_id, (void *) ctrl_page))
        return -1;

    return 0;
} // }}}

RC RM::increasePageSpace(PF_FileHandle &fileHandle, unsigned int page_id, uint16_t space) // {{{
{
    /* buffer to read in the control page. */
    uint16_t ctrl_page[CTRL_MAX_PAGES];

    unsigned int ctrl_page_id;
    uint16_t page_id_offset;

    /* get number of allocated pages. */
    unsigned int n_pages = fileHandle.GetNumberOfPages();

    /* make sure the page_id is valid. */
    if (page_id >= n_pages || CTRL_IS_CTRL_PAGE(page_id))
        return -1;

    /* determine the absolute control page id for a given page. */
    ctrl_page_id = CTRL_GET_CTRL_PAGE(page_id);

    /* get the offset in the control page. */
    page_id_offset = CTRL_GET_CTRL_PAGE_OFFSET(page_id);

    /* read in control page. */
    if (fileHandle.ReadPage(ctrl_page_id, (void *) ctrl_page))
        return -1;

    /* cannot increase space beyond maximum. */
    if ((space + ctrl_page[page_id_offset]) > SLOT_MAX_SPACE)
        return -1;

    /* increase the space for page on control page. */
    ctrl_page[page_id_offset] += space;

    /* write back the control page. */
    if (fileHandle.WritePage(ctrl_page_id, (void *) ctrl_page))
        return -1;

    return 0;
} // }}}

RC RM::getDataPage(PF_FileHandle &fileHandle, uint16_t length, unsigned int &page_id, uint16_t &unused_space) // {{{
{
    /* read control page as an array of uint16_t. */
    uint16_t ctrl_page[CTRL_MAX_PAGES];

    /* get number of allocated pages. */
    unsigned int n_pages = fileHandle.GetNumberOfPages();

    /* calculate number of control/data pages. */
    unsigned int n_ctrl_pages = CTRL_NUM_CTRL_PAGES(n_pages);
    unsigned int n_data_pages = CTRL_NUM_DATA_PAGES(n_pages);

    /* Layout of Control Structure in Heap:
       [C] [D * CTRL_MAX_PAGES] [C] [D * CTRL_MAX_PAGES] [C] [D] [D] ...
       (C: control page, D: data page)
    */
    for (unsigned int i = 0; i < n_ctrl_pages; i++)
    {
        /* get the page id for i-th control page. */
        unsigned int ctrl_page_id = CTRL_PAGE_ID(i);

        unsigned int n_allocated_pages = CTRL_MAX_PAGES;

        /* if we're on the last control page, only iterate over remaining allocated pages,
           since the control page might not control all possible pages.
        */
        if ((n_ctrl_pages - 1) == i)
            n_allocated_pages = n_data_pages % CTRL_MAX_PAGES;

        /* read in control page. */
        if (fileHandle.ReadPage(ctrl_page_id, (void *) ctrl_page))
            return -1;

        for (unsigned int j = 0; j < n_allocated_pages; j++)
        {
            /* found a free page, consume the space, and return the page id. */
            if (length <= ctrl_page[j])
            {
                /* set the page_id with the available space which is returned to the caller. */
                page_id = ctrl_page_id + (j + 1); // page index is offset by 1.

                /* set the unused_space variable which will be returned to the caller. */
                unused_space = ctrl_page[j];

                return 0;
            }
        }
    }

    /* allocate a control page only if there are no pages, or the last control page is completly full and the last page isn't a control page. */
    if ((n_pages == 0) || ((n_data_pages % CTRL_MAX_PAGES) == 0) && (CTRL_PAGE_ID(n_ctrl_pages - 1) != (n_pages - 1)))
    {
        if (AllocateControlPage(fileHandle))
            return -1;

        /* number of pages increase. */
        n_ctrl_pages++;
        n_pages++;
    }

    /* allocate a new data page. */
    if (AllocateDataPage(fileHandle))
        return -1;

    /* number of pages increase. */
    n_data_pages++;
    n_pages++;

    /* the last page is the index for a newly allocated page. */
    page_id = n_pages - 1;

    /* unused space is the full page since it is freshly allocated. */
    unused_space = SLOT_MAX_SPACE;

    return 0;    
} // }}}

unsigned int RM::getSchemaSize(const vector<Attribute> &attrs) // {{{
{
    uint16_t size = sizeof(uint16_t); /* field offset marker consumes uint16_t bytes */

    size += attrs.size() * sizeof(uint16_t); /* each field consumes uint16_t bytes. */

    for (unsigned int i = 0; i < attrs.size(); i++)
    {
        switch (attrs[i].type)
        {
            case TypeInt:
                 size += sizeof(int);
                 break;

            case TypeReal:
                 size += sizeof(float);
                 break;

            case TypeVarChar:
                 size += attrs[i].length;
                 break;
        }
    }

    return size;
} // }}}

RC RM::openTable(const string tableName, PF_FileHandle &fileHandle) // {{{
{
    PF_FileHandle handle;

    /* if table is open, retrieve and return handle. */
    if (open_tables.count(tableName))
    {
        fileHandle = open_tables[tableName];

        return 0;
    }

    if(tableName == system_catalog_tablename)
    {
        /* special case: system catalogue */

        /* it exists, we open it, and all is well. */
        if(pf->OpenFile(tableName.c_str(), handle))
        {
            /* need to create the system catalogue then. */
            if (pf->CreateFile(tableName.c_str()))
                return -1;

            /* attempt to open again and retrieve the handle. */
            if(pf->OpenFile(tableName.c_str(), handle))
                return -1;
        }

        /* populate the attributes in the catalog. */
        catalog[system_catalog_tablename] = system_catalog_attrs;

        /* add fields for quick lookup (table.fieldname) */
        for (unsigned int i = 0; i < system_catalog_attrs.size(); i++)
        {
            catalog_fields[system_catalog_tablename+"."+system_catalog_attrs[i].name] = system_catalog_attrs[i];
            catalog_fields_position[system_catalog_tablename+"."+system_catalog_attrs[i].name] = i;
        }

    }
    else
    {
        /* non system catalogue table not open: open table, retrieve and cache handle. */

        /* open file and retrieve handle. */
        if (pf->OpenFile(tableName.c_str(), handle))
            return -1;
    }

    /* cache handle for later use. */
    open_tables[tableName] = handle;

    /* return handle. */
    fileHandle = handle;

    return 0;
} // }}}

RC RM::closeTable(const string tableName) // {{{
{
    /* refuse to close the system catalog table, it shouldn't be manipulated from the outside. */
    if(tableName == system_catalog_tablename)
        return -1;

    /* check to make sure the table handle is cached. */
    if (open_tables.count(tableName))
    {
        /* read in the handle. */
        PF_FileHandle handle = open_tables[tableName];

        /* close the handle. */
        if (handle.CloseFile())
            return -1;

        /* delete the attributes for the table. */
        if(clearTableAttributes(tableName))
            return -1;

        /* delete the entry from the map. */
        open_tables.erase(tableName);

        return 0;
    }

    /* table isn't open. */
    return 0;
} // }}}

RC RM::closeAllTables() // {{{
{
    /* need a pass to close handles first, and then delete map entries, since iterating must be non-destructive. */
    for (map<string, PF_FileHandle>::const_iterator it = open_tables.begin(); it != open_tables.end(); ++it)
    {
        PF_FileHandle handle = it->second;

        if (handle.CloseFile())
            return -1;
    }

    /* all handles are now closed, delete them from the map. */
    open_tables.clear();
    catalog.clear();
    catalog_fields.clear();
    catalog_fields_position.clear();

    return 0;
} // }}}

RC RM::createTable(const string tableName, const vector<Attribute> &attrs) // {{{
{
    /* Handle used to write control page. */
    PF_FileHandle handle;

    /* auxillary attributes to check if the table exists in the catalog. */
    vector<Attribute> aux_attrs;

    /* check table name. */
    if (tableName.find_first_of("/.") != string::npos)
        return -1;

    /* check attribute name. */
    for (unsigned int i = 0; i < attrs.size(); i++)
        if (attrs[i].name.find_first_of("/.") != string::npos)
            return -1;

    /* no empty schema. */
    if (attrs.size() == 0)
        return -1;

    /* no empty varchar attributes. */
    for (unsigned int i = 0; i < attrs.size(); i++)
        if (attrs[i].type == TypeVarChar && attrs[i].length == 0)
            return -1;

    /* no duplicate attribute names. */
    map<string, int> dupes;
    for (unsigned int i = 0; i < attrs.size(); i++)
    {
        /* entry already exists, duplicate found. */
        if (dupes.count(attrs[i].name))
            return -1;

        dupes[attrs[i].name] = 1;
    }

    /* check schema fits in a page. */
    if (getSchemaSize(attrs) > PF_PAGE_SIZE)
        return -1;

    /* table exists. */
    if (catalog.count(tableName))
        return -1;

    /* check to see if any attributes are available given the table.
       a very rare circumstance when the return code is overloaded to distinguish that there's no attributes for this table.
    */
    if(getAttributes(tableName, aux_attrs) != 1)
        return -1;

    /* populate the attributes in the system catalog table. */
    insertTableAttributes(tableName, attrs);

    // not anymore, we're bootstrapping from the system catalogue from now on.
    /* create table. */
    //catalog[tableName] = attrs;

    ///* add fields for quick lookup (table.fieldname) */
    //for (unsigned int i = 0; i < attrs.size(); i++)
    //{
    //    catalog_fields[tableName+"."+attrs[i].name] = attrs[i];
    //    catalog_fields_position[tableName+"."+attrs[i].name] = i;
    //}

    ///* create the cache using getAttributes */
    //{
    //    vector<Attribute> aux_attrs;
    //    if(getAttributes(tableName, aux_attrs))
    //        return 1;
    //}

    /* create file & append a control page. */
    if (pf->CreateFile(tableName.c_str()))
        return -1;

    return 0;
} // }}}

void RM::syscat_attr_to_tuple(const void *tuple, const string tableName, const Attribute &attr, int attr_pos) // {{{
{
    uint8_t *tuple_ptr = (uint8_t *) tuple;
    int length;

    /* pack in table name length. */
    length = tableName.size();
    memcpy(tuple_ptr, &length, sizeof(length));
    tuple_ptr += sizeof(length);

    /* pack in the table name. */
    memcpy(tuple_ptr, tableName.c_str(), tableName.size());
    tuple_ptr += length;

    /* pack in attribute name length. */
    length = attr.name.size();
    memcpy(tuple_ptr, &length, sizeof(length));
    tuple_ptr += sizeof(length);

    /* pack in the attribute name length. */
    memcpy(tuple_ptr, attr.name.c_str(), attr.name.size());
    tuple_ptr += length;

    /* pack in the attribute type. */
    memcpy(tuple_ptr, &attr.type, sizeof(int));
    tuple_ptr += sizeof(attr.type);

    /* pack in the attribute length. */
    memcpy(tuple_ptr, &attr.length, sizeof(int));
    tuple_ptr += sizeof(attr.length);

    /* pack in the attribute position. */
    memcpy(tuple_ptr, &attr_pos, sizeof(int));
    tuple_ptr += sizeof(attr_pos);
} // }}}

void RM::syscat_tuple_to_attr(const void *tuple, Attribute &attr, int &attr_pos) // {{{
{
        uint8_t attr_name[MAX_CAT_NAME_LEN] = {0};
        uint8_t *tuple_ptr = (uint8_t *) tuple;
        int length;

        /* we can ignore the table name, since our equality gave us this. */
        memcpy(&length, tuple_ptr, sizeof(length));
        tuple_ptr += sizeof(length) + length;
       
        /* get attribute name, first copy the name length, copy the data to temp buffer, fix it up, assign to string. */
        memcpy(&length, tuple_ptr, sizeof(length));
        memcpy(attr_name, tuple_ptr + sizeof(length), length);
        /* nil terminate so we can copy to a string. */
        attr_name[length] = '\0';
        /* copies that don't work? */
        //attr.name = (const char *) attr_name;
        //attr.name.insert(0, (char *) attr_name, length+1);
        attr.name = string((char *) attr_name);
        tuple_ptr += sizeof(length) + length;
        
        /* read in the type info. */
        memcpy(&attr.type, tuple_ptr, sizeof(int));
        tuple_ptr += sizeof(int);

        /* read in the length info. */
        memcpy(&attr.length, tuple_ptr, sizeof(int));
        tuple_ptr += sizeof(int);

        /* read in the position info. */
        memcpy(&attr_pos, tuple_ptr, sizeof(int));
        tuple_ptr += sizeof(int);

        /* we're done. */
} // }}}

RC RM::insertTableAttributes(const string &tableName, const vector<Attribute> &attrs) // {{{
{
    /* pack each attribute record in tuple. */
    uint8_t tuple[PF_PAGE_SIZE];

    /* add fields to the system catalogue. */
    for (unsigned int i = 0; i < attrs.size(); i++)
    {
        /* rid can be thrown away after insert. */
        RID aux;

        /* populates the tuple preparing it for insertTuple. */
        syscat_attr_to_tuple(tuple, tableName, attrs[i], i);

        /* insert the tuple. */
        if(insertTuple(system_catalog_tablename, tuple, aux))
            return -1;
    }

    return 0;
} // }}}

/* deletes from the cache. */
RC RM::clearTableAttributes(const string &tableName) // {{{
{
    /* find fields in system catalogue for deletion in the system catalogue. */
    if(catalog.count(tableName))
    {
        /* retrieve the tuples, and then delete the record based on the RID. */
        uint8_t return_tuple[PF_PAGE_SIZE];
        uint8_t value[PF_PAGE_SIZE];

        /* attributes retrieved from scan, kept for debugging purposes. */
        vector<Attribute> attrs;

        /* RID is unused. */
        RID aux;

        /* auxillary attribute. */
        Attribute aux_attr;
        int aux_attr_pos;

        /* to check for errors. */
        RC next_tuple_rc;

        int tableName_length = tableName.size();

        /* prepare comparison value buffer to be the tablename. */
        memcpy(value, &tableName_length, sizeof(tableName_length));
        memcpy(value + sizeof(tableName_length), tableName.c_str(), tableName_length);

        /* set up iterator, comparing on the table name. */
        RM_ScanIterator rmsi;

        vector<string> attributes;
        attributes.push_back("table");
        attributes.push_back("attribute");
        attributes.push_back("type");
        attributes.push_back("length");
        attributes.push_back("position");

        /* scan the system catalogue, using the table field. */
        if(scan(system_catalog_tablename, "table", EQ_OP, &value, attributes, rmsi))
            return -1;

        /* scan retrieving each attribute that matches our tablename. */
        while((next_tuple_rc = rmsi.getNextTuple(aux, return_tuple)) != RM_EOF)
        {
            /* special error code. */
            if(next_tuple_rc == 1)
                return -1;

            syscat_tuple_to_attr(return_tuple, aux_attr, aux_attr_pos);

            attrs.push_back(aux_attr);

            /* erase the attribute in cache. */
            catalog_fields.erase(tableName+"."+aux_attr.name);
            catalog_fields_position.erase(tableName+"."+aux_attr.name);
        }

        rmsi.close();
  
        /* if no attributes then the table doesn't exist. */
        if(attrs.size() == 0)
            return 1;

        /* erase the table in cache. */
        catalog.erase(tableName);

        /* all attributes are removed from cache. */
        return 0;
    }

    /* table doesn't exist, no attributes to delete. */
    return 0;
} // }}}

RC RM::deleteTableAttributes(const string &tableName) // {{{
{
    /* find fields in system catalogue for deletion in the system catalogue. */
    if(catalog.count(tableName))
    {
        /* retrieve the tuples, and then delete the record based on the RID. */
        uint8_t return_tuple[PF_PAGE_SIZE];
        uint8_t value[PF_PAGE_SIZE];

        /* attributes retrieved from scan, kept for debugging purposes. */
        vector<Attribute> attrs;

        /* RIDs are first stored, and then a mass deletion is performed. Scanning and deleting is tricky. */
        vector<RID> rids;

        RID aux;

        /* auxillary attribute. */
        Attribute aux_attr;
        int aux_attr_pos;

        /* to check for errors. */
        RC next_tuple_rc;

        int tableName_length = tableName.size();

        /* prepare comparison value buffer to be the tablename. */
        memcpy(value, &tableName_length, sizeof(tableName_length));
        memcpy(value + sizeof(tableName_length), tableName.c_str(), tableName_length);

        /* set up iterator, comparing on the table name. */
        RM_ScanIterator rmsi;

        vector<string> attributes;
        attributes.push_back("table");
        attributes.push_back("attribute");
        attributes.push_back("type");
        attributes.push_back("length");
        attributes.push_back("position");

        /* scan the system catalogue, using the table field. */
        if(scan(system_catalog_tablename, "table", EQ_OP, &value, attributes, rmsi))
            return -1;

        /* scan retrieving each attribute that matches our tablename. */
        while((next_tuple_rc = rmsi.getNextTuple(aux, return_tuple)) != RM_EOF)
        {
            /* special error code. */
            if(next_tuple_rc == 1)
                return -1;

            syscat_tuple_to_attr(return_tuple, aux_attr, aux_attr_pos);

            /* copy the rid for later deletion. */
            rids.push_back(aux);

            attrs.push_back(aux_attr);

            /* erase the attribute in cache. */
            catalog_fields.erase(tableName+"."+aux_attr.name);
            catalog_fields_position.erase(tableName+"."+aux_attr.name);
        }

        rmsi.close();
  
        /* erase the table in cache. */
        catalog.erase(tableName);

        /* delete all the records. */
        for (unsigned int i = 0; i < rids.size(); i++)
            if (deleteTuple(system_catalog_tablename, rids[i]))
                return -1;

        /* all attributes are removed from cache. */
        return 0;
    }

    /* table doesn't exist, no attributes to delete. */
    return 0;
} // }}}

RC RM::getAttributes(const string tableName, vector<Attribute> &attrs) // {{{
{
    /* table doesnt exists, fetch the attributes. */
    if (!catalog.count(tableName))
    {
        uint8_t return_tuple[PF_PAGE_SIZE];
        uint8_t value[PF_PAGE_SIZE];

        /* RID is unused. */
        RID aux;

        /* auxillary attribute. */
        Attribute aux_attr;
        int aux_attr_pos;

        /* to check for errors. */
        RC next_tuple_rc;

        int tableName_length = tableName.size();

        /* prepare comparison value buffer to be the tablename. */
        memcpy(value, &tableName_length, sizeof(tableName_length));
        memcpy(value + sizeof(tableName_length), tableName.c_str(), tableName_length);

        /* set up iterator, comparing on the table name. */
        RM_ScanIterator rmsi;

        vector<string> attributes;
        attributes.push_back("table");
        attributes.push_back("attribute");
        attributes.push_back("type");
        attributes.push_back("length");
        attributes.push_back("position");

        /* scan the system catalogue, using the table field. */
        if(scan(system_catalog_tablename, "table", EQ_OP, &value, attributes, rmsi))
            return -1;

        /* scan retrieving each attribute that matches our tablename. */
        while((next_tuple_rc = rmsi.getNextTuple(aux, return_tuple)) != RM_EOF)
        {
            /* special error code. */
            if(next_tuple_rc == 1)
                return -1;

            syscat_tuple_to_attr(return_tuple, aux_attr, aux_attr_pos);

            attrs.push_back(aux_attr);

            /* cache the attribute. */
            catalog_fields[tableName+"."+aux_attr.name] = aux_attr;
            catalog_fields_position[tableName+"."+aux_attr.name] = aux_attr_pos;
        }

        rmsi.close();
  
        /* if no attributes then the table doesn't exist, return 1 to indicate this. */
        if(attrs.size() == 0)
            return 1;

        /* create the table in cache. */
        catalog[tableName] = attrs;  

        /* all attributes are now cached. */
        return 0;
    }

    /* pull it from the cache. */
    attrs = catalog[tableName];

    return 0;
} // }}}

RC RM::getTableAttribute(const string tableName, const string attributeName, Attribute &attr, uint16_t &attrPosition) // {{{
{
    /* table doesnt exist in cache. */
    if (!catalog.count(tableName))
    {
        /* auxillary structure for getting the attributes. */
        vector <Attribute> attrs;

        /* fetch all attributes first to cache, and then continue. */
        if(getAttributes(tableName, attrs))
            return 1;
    }

    /* check to make sure the attribute name exists. */
    if (!catalog_fields.count(tableName+"."+attributeName))
        return -1;

    if (!catalog_fields_position.count(tableName+"."+attributeName))
        return -1;

    attr = catalog_fields[tableName+"."+attributeName];
    attrPosition = catalog_fields_position[tableName+"."+attributeName];

    return 0;
} // }}}

RC RM::deleteTable(const string tableName) // {{{
{
    /* easiest way to delete is to first open the table. */
    PF_FileHandle handle;

    /* auxillary structure for getting the attributes. */
    vector <Attribute> attrs;

    /* cannot delete the system catalogue. */
    if (tableName == system_catalog_tablename)
        return -1;

    /* open table to get handle. */
    if (openTable(tableName, handle))
        return -1;

    /* retrieve table attributes. */
    if (getAttributes(tableName, attrs))
        return -1;

    /* delete table attributes. */
    if (deleteTableAttributes(tableName))
        return -1;

    //for (unsigned int i = 0; i < attrs.size(); i++)
    //{
    //    catalog_fields.erase(tableName+"."+attrs[i].name);
    //    catalog_fields_position.erase(tableName+"."+attrs[i].name);
    //}

    ///* delete table. */
    //catalog.erase(tableName);

    if (closeTable(tableName))
        return -1;

    return(pf->DestroyFile(tableName.c_str()));
} // }}}

void RM::tuple_to_record(const void *tuple, uint8_t *record, const vector<Attribute> &attrs) // {{{
{
   uint8_t *tuple_ptr = (uint8_t *) tuple;
   uint8_t *record_data_ptr = record;

   uint16_t num_fields = attrs.size();

   /* last_offset is the relative offset of where to append data in a record.
      The data begins after the directory, (sizeof(num_fields) + 2*num_fields).
   */
   uint16_t last_offset = REC_START_DATA_OFFSET(num_fields);

   /* record data pointer points to where data can be appended. */
   record_data_ptr += last_offset;

   /* write the field count. */
   memcpy(record, &num_fields, sizeof(num_fields));

   for (unsigned int i = 0; i < attrs.size(); i++)
   {
       /* field offset address for the attribute. */
       uint16_t field_offset = REC_FIELD_OFFSET(i);

       if (attrs[i].type == TypeInt)
       {
           /* write the int starting at the last stored field offset. */
           memcpy(record_data_ptr, tuple_ptr, sizeof(int));

           /* advance pointer in record and tuple. */
           record_data_ptr += sizeof(int);
           tuple_ptr += sizeof(int);

           /* determine the new ending offset. */
           last_offset += sizeof(int);
       }
       else if (attrs[i].type == TypeReal)
       {
           /* write the float starting at the last stored field offset. */
           memcpy(record_data_ptr, tuple_ptr, sizeof(float));

           /* advance pointer in record and tuple. */
           record_data_ptr += sizeof(float);
           tuple_ptr += sizeof(float);

           /* determine the new ending offset. */
           last_offset += sizeof(float);
       }
       else if (attrs[i].type == TypeVarChar)
       {
           int length;

           /* read in the length as int. */
           memcpy(&length, tuple_ptr, sizeof(length));

           /* advance pointer past the length field in tuple. */
           tuple_ptr += sizeof(length);

           /* append varchar data to record. */
           memcpy(record_data_ptr, tuple_ptr, length);

           /* advance pointer in record and tuple. */
           tuple_ptr += length;
           record_data_ptr += length;

           /* determine the new ending offset. */
           last_offset += length;
       }

       /* write the end address for the field offset. */
       memcpy(record + field_offset, &last_offset, sizeof(last_offset));
   }
} // }}}

void RM::record_to_tuple(uint8_t *record, const void *tuple, const vector<Attribute> &attrs) // {{{
{
   uint8_t *tuple_ptr = (uint8_t *) tuple;
   uint8_t *record_data_ptr;

   uint16_t num_fields;

   /* read in the number of fields. */
   memcpy(&num_fields, record, sizeof(num_fields));

   /* find beginning of data. */
   uint16_t last_offset = REC_START_DATA_OFFSET(num_fields);
   record_data_ptr = record + last_offset;

   for (int i = 0; i < num_fields; i++)
   {
       /* field offset address for the attribute. */
       uint16_t field_offset = REC_FIELD_OFFSET(i);

       if (attrs[i].type == TypeInt)
       {
           /* copy the int to the tuple. */
           memcpy(tuple_ptr, record_data_ptr, sizeof(int));

           /* advance pointer in record and tuple. */
           record_data_ptr += sizeof(int);
           tuple_ptr += sizeof(int);

           /* determine the new ending offset. */
           last_offset += sizeof(int);
       }
       else if (attrs[i].type == TypeReal)
       {
           /* copy the float to the tuple. */
           memcpy(tuple_ptr, record_data_ptr, sizeof(float));

           /* advance pofloater in record and tuple. */
           record_data_ptr += sizeof(float);
           tuple_ptr += sizeof(float);

           /* determine the new ending offset. */
           last_offset += sizeof(float);
       }
       else if (attrs[i].type == TypeVarChar)
       {
           int length;
           uint16_t length_data;

           /* determine length from reading in the field offset, and subtract the last_offset. */
           memcpy(&length_data, record + field_offset, sizeof(length_data));
           length_data -= last_offset;

           /* copy the length data to an int representation. */
           length = length_data;

           /* write length data to tuple, and advance to where data should be appended. */
           memcpy(tuple_ptr, &length, sizeof(length));
           tuple_ptr += sizeof(length);

           /* append varchar data to tuple from last_offset to last_offset+length. */
           memcpy(tuple_ptr, record_data_ptr, length);

           /* advance pointer in record and tuple. */
           tuple_ptr += length;
           record_data_ptr += length;

           /* determine the new ending offset. */
           last_offset += length;
       }
   }
} // }}}

void RM::record_attr_to_tuple(uint8_t *record, const void *tuple, const Attribute &attr, uint16_t attr_position) // {{{
{
   uint8_t *tuple_ptr = (uint8_t *) tuple;
   uint8_t *record_data_ptr;

   uint16_t num_fields;

   /* end of the field preceding the one we want to project. */
   uint16_t last_field_offset;

   /* end of field that we want to project. */
   uint16_t end_field_offset;

   /* read in the number of fields. */
   memcpy(&num_fields, record, sizeof(num_fields));

   /* determine the beginning offset of the attribute. */
   if (attr_position == 0)
       last_field_offset = REC_START_DATA_OFFSET(num_fields);
   else
       memcpy(&last_field_offset, record + REC_FIELD_OFFSET(attr_position - 1), sizeof(last_field_offset));

   /* read in the value at the field offset position at attr_position. */
   memcpy(&end_field_offset, record + REC_FIELD_OFFSET(attr_position), sizeof(end_field_offset));

   /* position to beginning of data. */
   record_data_ptr = record + last_field_offset;

   if (attr.type == TypeInt)
   {
       /* copy the int to the tuple. */
       memcpy(tuple_ptr, record_data_ptr, sizeof(int));
   }
   else if (attr.type == TypeReal)
   {
       /* copy the float to the tuple. */
       memcpy(tuple_ptr, record_data_ptr, sizeof(float));
   }
   else if (attr.type == TypeVarChar)
   {
       int length = end_field_offset - last_field_offset;

       /* write length data to tuple, and advance to where data should be appended. */
       memcpy(tuple_ptr, &length, sizeof(length));
       tuple_ptr += sizeof(length);

       /* append varchar data to tuple from last_offset to last_offset+length. */
       memcpy(tuple_ptr, record_data_ptr, length);
   }
} // }}}

uint16_t RM::activateSlot(uint16_t *slot_page, uint16_t activate_slot_id, uint16_t record_offset) // {{{
{
    /* update slot queue head. */
    if (SLOT_GET_SLOT(slot_page, activate_slot_id) == SLOT_QUEUE_END)
    {
        /* reached last slot in the queue, which means we need to allocate a new slot. */

        /* check to see if enough space is available to allocate a new slot. */
        if (SLOT_GET_FREE_SPACE(slot_page) < sizeof(uint16_t))
            return 0;

        /* assign new slot index which happens to be the current number of slots. */
        slot_page[SLOT_NEXT_SLOT_INDEX] = SLOT_GET_NUM_SLOTS(slot_page);

        /* update the number of slots in the directory. */
        slot_page[SLOT_NUM_SLOT_INDEX]++;

        /* deactivate slot and set it to end of queue marker. */
        slot_page[SLOT_GET_LAST_SLOT_INDEX(slot_page)] = SLOT_QUEUE_END;

        /* activate slot and update offset. */
        slot_page[SLOT_GET_SLOT_INDEX(activate_slot_id)] = record_offset;

        /* return 1 to indicate a new slot is created. */
        return 1;
    }
    else
    {
        /* the queue has more than 1 element, so we just copy the next element from the last allocated slot. */
        slot_page[SLOT_NEXT_SLOT_INDEX] = SLOT_GET_INACTIVE_SLOT(slot_page, activate_slot_id);

        /* activate slot and update offset. */
        slot_page[SLOT_GET_SLOT_INDEX(activate_slot_id)] = record_offset;

        /* no new slots allocated. */
        return 0;
    }
} // }}}

uint16_t RM::deactivateSlot(uint16_t *slot_page, uint16_t deactivate_slot_id) // {{{
{
    /* deactivate the slot, and rebuild the list. */
    slot_page[SLOT_GET_SLOT_INDEX(deactivate_slot_id)] = SLOT_QUEUE_END;

    /* original number of slots before deletion. */
    uint16_t orig_num_slots = SLOT_GET_NUM_SLOTS(slot_page);

    /* new number of slots. */
    uint16_t new_num_slots = SLOT_GET_NUM_SLOTS(slot_page);

    /* auxillary variable to find the last active slot in the directory by walking backwards. */
    uint16_t last_active_slot = SLOT_GET_NUM_SLOTS(slot_page) - 1;

    /* auxillary variable to help determine the count of slots. */
    uint16_t final_slot_index;

    /* if there's only one slot, then slot id 0 is being deleted, don't worry about this case. */
    if (orig_num_slots == 1)
    {
        slot_page[SLOT_NEXT_SLOT_INDEX] = 0;
        slot_page[SLOT_GET_SLOT_INDEX(0)] = SLOT_QUEUE_END;
 
        /* no slots deleted. */
        return 0;
    }

    /* find the last active slot starting from the end of the directory. */
    while (last_active_slot && SLOT_IS_INACTIVE(SLOT_GET_SLOT(slot_page, last_active_slot)))
        last_active_slot--;

    /* ensure the last active slot is actually active (we could've hit the first slot and terminated the loop early). */
    if (SLOT_IS_ACTIVE(SLOT_GET_SLOT(slot_page, last_active_slot)))
        final_slot_index = last_active_slot + 1;
    else
        final_slot_index = last_active_slot;

    /* set the slot count, appending a free slot. */
    new_num_slots = final_slot_index + 1;
    slot_page[SLOT_NUM_SLOT_INDEX] = final_slot_index + 1;

    /* check to see if there's any available inactive slots. */
    for (uint16_t i = 0; i < last_active_slot; i++)
    {
        if (SLOT_IS_INACTIVE(SLOT_GET_SLOT(slot_page, i)))
        {
            /* found an empty slot, we have all we need. */
            new_num_slots--;
            slot_page[SLOT_NUM_SLOT_INDEX]--;
            break;
        }
    }
    
    /* rebuild linked list of inactive slots. */

    /* set the next slot to end of queue. */
    slot_page[SLOT_NEXT_SLOT_INDEX] = SLOT_QUEUE_END;

    for (uint16_t i = 0; i < new_num_slots; i++)
    {
        /* only consider inactive slots. */
        if (SLOT_IS_INACTIVE(SLOT_GET_SLOT(slot_page, i)))
        {
            if (slot_page[SLOT_NEXT_SLOT_INDEX] == SLOT_QUEUE_END)
                slot_page[SLOT_GET_SLOT_INDEX(i)] = SLOT_QUEUE_END;
            else
                slot_page[SLOT_GET_SLOT_INDEX(i)] = PF_PAGE_SIZE + slot_page[SLOT_NEXT_SLOT_INDEX];

            /* point to new slot. */
            slot_page[SLOT_NEXT_SLOT_INDEX] = i;
        }
    }

    /* slot directory updated. */
    return (orig_num_slots - new_num_slots);
} // }}}

RC RM::insertTuple(const string tableName, const void *data, RID &rid) // {{{
{
    uint16_t record_length;
    unsigned int page_id;

    uint8_t raw_page[PF_PAGE_SIZE];
    uint16_t *slot_page = (uint16_t *) raw_page;

    /* free space management. */
    uint16_t free_space_offset;
    uint16_t free_space_avail;

    /* available space of a page. */
    uint16_t avail_space;

    /* slot used for insert. */
    uint16_t insert_slot;

    /* set to 1 if the slot directory is expanded. */
    int new_slot_flag = 0;

    /* buffer to store record. */
    uint8_t record[PF_PAGE_SIZE];

    /* attributes to determine data packing format. */
    vector<Attribute> attrs;

    /* Handle for database. */
    PF_FileHandle handle;

    /* retrieve table attributes. */
    if (getAttributes(tableName, attrs))
        return -1;

    /* unpack data and convert into record format. Assumed to be a safe operation. */
    tuple_to_record(data, record, attrs);

    /* determine the size of the record. */
    record_length = REC_LENGTH(record);

    /* open table for insertion. */
    if (openTable(tableName, handle))
        return -1;

    /* find data page for insertion, (guaranteed to fit record). */
    if (getDataPage(handle, record_length, page_id, avail_space))
        return -1;

    /* update page id to return. */ 
    rid.pageNum = page_id;

    if (handle.ReadPage(page_id, raw_page))
        return -1;

    if(debug)
    {
        debug_data_page(raw_page, "before insertTuple");
        cout << "available space on page: " << avail_space << endl;
    }

    /* get the free space offset, and determine available space. */
    free_space_avail = SLOT_GET_FREE_SPACE(slot_page);
    free_space_offset = SLOT_GET_FREE_SPACE_OFFSET(slot_page);

    /* It can fit in the free space. Append the record at the free space offset. */
    if (record_length <= free_space_avail)
    {
        uint16_t new_offset;

        /* free space offset must always align on an even boundary (debug check). */
        if (!(IS_EVEN(free_space_offset)))
            return -1;

        /* append the record to the page beginning at the free space offset. */
        memcpy(raw_page + free_space_offset, record, record_length);

        /* calculate new offset, which is the offset after the record is appended at the free space offset. */
        new_offset = free_space_offset + record_length;

        /* align it to even boundary (and write a fragment byte on the boundary). */
        if (IS_ODD(new_offset))
        {
            memset(raw_page + new_offset, SLOT_FRAGMENT_BYTE, 1);
            new_offset++;
        }

        /* update free space offset (aligned to an even boundary). */
        slot_page[SLOT_FREE_SPACE_INDEX] = new_offset;

        /* assign slot from the head of the slot queue. */
        insert_slot = slot_page[SLOT_NEXT_SLOT_INDEX];

        /* update slot number to return. */
        rid.slotNum = insert_slot;

        /* activate slot setting it to the old free space offset, where the record was inserted. */
        new_slot_flag = activateSlot(slot_page, insert_slot, free_space_offset);
    }
    else
    {
        /* auxillary record id. */
        RID aux;

        /* have enough usable space, but first need to compact and then insert. */
        if (reorganizePage(tableName, page_id))
            return -1;

        /* all variables have been invalidate, instead call insertTuple, it should deterministically choose the same page. */
        if (insertTuple(tableName, data, aux))
            return -1;

        /* this should always come to the same page. */
        assert(rid.pageNum == aux.pageNum);

        /* we're done, lets just return. */
        return 0;
    }

    /* write page back. */
    if (handle.WritePage(page_id, (void *) raw_page))
        return -1;

    /* update space in the control page, align the record length on even boundary */
    if (IS_ODD(record_length))
        record_length++;

    if (decreasePageSpace(handle, page_id, record_length + new_slot_flag*sizeof(uint16_t)))
        return -1;

    if(debug)
    {
        debug_data_page(raw_page, "after insertTuple");
        getPageSpace(handle, page_id, avail_space);
        cout << "available space on page: " << avail_space << endl;
    }

    return 0;
} // }}}

RC RM::readTuple(const string tableName, const RID &rid, void *data) // {{{
{
    /* data variables for reading in pages. */
    uint8_t raw_page[PF_PAGE_SIZE];
    uint16_t *slot_page = (uint16_t *) raw_page;

    /* offset in page where the record begins. */
    uint16_t record_offset;

    /* attributes to determine data packing format. */
    vector<Attribute> attrs;

    /* handle for database. */
    PF_FileHandle handle;

    /* retrieve table attributes. */
    if (getAttributes(tableName, attrs))
        return -1;

    /* open table to read in page. */
    if (openTable(tableName, handle))
        return -1;

    /* read in data page */
    if (handle.ReadPage(rid.pageNum, raw_page))
        return -1;

    /* ensure slot is active. */
    if (SLOT_IS_INACTIVE(SLOT_GET_SLOT(slot_page, rid.slotNum)))
        return -1;

    /* get the beginning offset from the slot associated with the record. */
    record_offset = SLOT_GET_SLOT(slot_page, rid.slotNum);  

    /* check if it's a record, otherwise follow redirect. */

    if (REC_IS_TUPLE_REDIR(raw_page + record_offset))
    {
        /* auxillary record id which is retrieved from the tuple redirection. */
        RID aux;

        /* point to the tuple redirection in the page. */

        /* get the slot id, and discard the tuple marker. */
        aux.slotNum = *((uint16_t *) (raw_page + record_offset)) - REC_TUPLE_MARKER;
        aux.pageNum = *((unsigned int *) (raw_page + record_offset + sizeof(uint16_t)));

        /* read in from the redirected tuple. */
        if (readTuple(tableName, aux, data))
            return -1;

        return 0;
    }

    /* copy record to tuple buffer. */
    record_to_tuple(raw_page + record_offset, data, attrs);

    /* we're done, wasn't that hard? */
    return 0;
} // }}}

RC RM::deleteTuple(const string tableName, const RID &rid) // {{{
{
    /* data variables for reading in pages. */
    uint8_t raw_page[PF_PAGE_SIZE];
    uint16_t *slot_page = (uint16_t *) raw_page;

    /* num slots deleted for after deactivating a slot. */
    uint16_t n_slots_deleted = 0;
 
    /* amount of data deleted. */
    uint16_t space_deleted;

    /* offset in page where the record begins. */
    uint16_t record_offset;
    uint16_t record_length;

    /* offset aligned to the nearest even byte. */
    uint16_t record_end_offset;

    /* beginning offset of free space. */
    uint16_t free_space_offset;

     /* attributes to determine data packing format. */
    vector<Attribute> attrs;

    /* handle for database. */
    PF_FileHandle handle;

    /* retrieve table attributes. */
    if (getAttributes(tableName, attrs))
        return -1;

    /* open table to read in page. */
    if (openTable(tableName, handle))
        return -1;

    /* read in data page */
    if (handle.ReadPage(rid.pageNum, raw_page))
        return -1;

    /* get the beginning offset of free space. */
    free_space_offset = SLOT_GET_FREE_SPACE_OFFSET(slot_page);

    /* ensure slot is active, otherwise pointless to delete. */
    if (SLOT_IS_INACTIVE(SLOT_GET_SLOT(slot_page, rid.slotNum)))
        return -1;

    /* get the beginning offset from the slot associated with the record. */
    record_offset = SLOT_GET_SLOT(slot_page, rid.slotNum);  

    /* check if record redirects. */
    if (REC_IS_TUPLE_REDIR(raw_page + record_offset))
    {
        /* auxillary record id which is retrieved from the tuple redirection. */
        RID aux;

        /* point to the tuple redirection in the page. */

        /* get the slot id, and discard the tuple marker. */
        aux.slotNum = *((uint16_t *) (raw_page + record_offset)) - REC_TUPLE_MARKER;
        aux.pageNum = *((unsigned int *) (raw_page + record_offset + sizeof(uint16_t)));

        /* delete the redirected tuple. */
        if (deleteTuple(tableName, aux))
            return -1;

        /* check to see if record ends on free space offset, otherwise write fragment. */
        if (record_offset + REC_TUPLE_REDIR_LENGTH == free_space_offset)
            slot_page[SLOT_FREE_SPACE_INDEX] = free_space_offset - REC_TUPLE_REDIR_LENGTH;
        else
            memset(raw_page + record_offset, SLOT_FRAGMENT_BYTE, REC_TUPLE_REDIR_LENGTH);

        n_slots_deleted = deactivateSlot(slot_page, rid.slotNum);

        if (increasePageSpace(handle, rid.pageNum, REC_TUPLE_REDIR_LENGTH + n_slots_deleted * sizeof(uint16_t)))
            return -1;

        if(debug)
        {
            uint16_t avail_space;
            getPageSpace(handle, rid.pageNum, avail_space);
            debug_data_page(raw_page, "after delete");
            cout << "available space on page: " << avail_space << endl;
        }

        return 0;
    }

    /* tuple is a record, get the length. */
    record_length = REC_LENGTH(raw_page + record_offset);

    /* determine ending offset of record. */
    record_end_offset = record_offset + record_length;

    /* align to the nearest even byte, we know this isn't in use since records/free space begins on even bytes. */
    record_end_offset += IS_ODD(record_end_offset) ? 1 : 0;

    if(debug)
    {
        cout << "GOING TO DELETE: " << record_length << " at slot: " << rid.slotNum << " with offset: " << record_offset << " to " << record_end_offset << endl;
        uint16_t avail_space;
        getPageSpace(handle, rid.pageNum, avail_space);
        debug_data_page(raw_page, "before delete");
        cout << "available space on page: " << avail_space << endl;
    }

    /* two cases: record to delete is last page that appears on record preceding free space, or earlier which leaves a fragment. */
    if (record_end_offset == free_space_offset)
    {
        /* move the free space pointer to beginning of deleted record. */
        free_space_offset = record_offset;

        /* delete the record by updating the free space offset. */
        slot_page[SLOT_FREE_SPACE_INDEX] = free_space_offset;
    }
    else
    {
        /* write mark all bytes as fragments from beginning to aligned ending boundary. */
        memset(raw_page + record_offset, SLOT_FRAGMENT_BYTE, record_end_offset - record_offset);
    }

   
    space_deleted = record_end_offset - record_offset;
    n_slots_deleted = deactivateSlot(slot_page, rid.slotNum);

    if (handle.WritePage(rid.pageNum, raw_page))
        return -1;

    if (increasePageSpace(handle, rid.pageNum, space_deleted + n_slots_deleted * sizeof(uint16_t)))
        return -1;

    if(debug)
    {
        uint16_t avail_space;
        getPageSpace(handle, rid.pageNum, avail_space);
        debug_data_page(raw_page, "after delete");
        cout << "available space on page: " << avail_space << endl;
    }

    return 0;
} // }}}

// Assume the rid does not change after update
RC RM::updateTuple(const string tableName, const void *data, const RID &rid) // {{{
{
    /* data variables for reading in pages. */
    uint8_t raw_page[PF_PAGE_SIZE];
    uint16_t *slot_page = (uint16_t *) raw_page;

    /* need to track space for growth/shrink/tuple redirection. */
    uint16_t n_used_space = 0;
    uint16_t n_deleted_space = 0;
    uint16_t avail_space = 0;

    /* offset in page where the old record begins. */
    uint16_t old_record_offset;

    /* nearest even end byte offset of old record. */
    uint16_t old_record_end_offset_even;

    /* length of old record. */
    uint16_t old_record_length;

    /* buffer to store the record to update. */
    uint8_t update_record[PF_PAGE_SIZE];

    /* length of updated record. */
    uint16_t update_record_length;

    /* align the record length to an even byte, for free space purposes. */
    uint16_t update_record_end_offset_even;

    /* beginning offset of free space. */
    uint16_t free_space_offset;

    /* the actual amount of free space. */
    uint16_t free_space  = 0;

    /* attributes to determine data packing format. */
    vector<Attribute> attrs;

    /* handle for database. */
    PF_FileHandle handle;

    /* retrieve table attributes. */
    if (getAttributes(tableName, attrs))
        return -1;

    /* unpack data and convert into record format. Assumed to be a safe operation. */
    tuple_to_record(data, update_record, attrs);
    update_record_length = REC_LENGTH(update_record);

    /* open table to read in page. */
    if (openTable(tableName, handle))
        return -1;

    /* read in data page */
    if (handle.ReadPage(rid.pageNum, raw_page))
        return -1;

    /* ensure slot is active. */
    if (SLOT_IS_INACTIVE(SLOT_GET_SLOT(slot_page, rid.slotNum)))
        return -1;


    /* get the beginning offset of free space. */
    free_space_offset = slot_page[SLOT_FREE_SPACE_INDEX];

    /* get the free space available in the page. */
    free_space = SLOT_GET_FREE_SPACE(slot_page);

    /* get the beginning offset from the slot associated with the record. */
    old_record_offset = SLOT_GET_SLOT(slot_page, rid.slotNum);

    if(debug)
    {
        stringstream ss;
        ss << "before update: slot: " << rid.slotNum << " newlength: " << update_record_length << " ]";
        debug_data_page(raw_page, ss.str().c_str());
        uint16_t new_space;
        getPageSpace(handle, rid.pageNum, new_space);
        cout << "available space on page: " << new_space << endl;
    }

    /* check if record redirects. */
    if (REC_IS_TUPLE_REDIR(raw_page + old_record_offset))
    {
        /* auxillary record id which is retrieved from the tuple redirection. */
        RID aux;

        /* point to the tuple redirection in the page. */

        /* get the slot id, and discard the tuple marker. */
        aux.slotNum = *((uint16_t *) (raw_page + old_record_offset)) - REC_TUPLE_MARKER;
        aux.pageNum = *((unsigned int *) (raw_page + old_record_offset + sizeof(uint16_t)));

        /* update the redirected tuple. */
        if (updateTuple(tableName, data, aux))
            return -1;

        return 0;
    }

    /* it's a record, so we can get the length. */
    old_record_length = REC_LENGTH(raw_page + old_record_offset);

    /* find even aligned ending offset for the old record. */
    old_record_end_offset_even = old_record_offset + old_record_length;
    if (IS_ODD(old_record_end_offset_even))
        old_record_end_offset_even++;


    /* don't have time to fix this, but this function can really be compressed by distinguishing between growing at the free offset and not. */

    /* if the new record is shrinking, then overwrite and write back page. */
    if (update_record_length <= old_record_length)
    {
        /* copy new record. */
        memcpy(raw_page + old_record_offset, update_record, update_record_length);

        update_record_end_offset_even = old_record_offset + update_record_length;

        /* mark the byte as a fragment if not on an even boundary. */
        if (IS_ODD(update_record_length))
        {
            /* write out a fragment byte in the unusable space. */
            memset(raw_page + old_record_offset + update_record_length, SLOT_FRAGMENT_BYTE, 1);

            /* align the end offset to nearest even byte. */
            update_record_end_offset_even++;
        }

        /* update the increase in space. */
        n_deleted_space = old_record_end_offset_even - update_record_end_offset_even;

        /* if the record ends on the free space offset boundary, then we just move the free space pointer, otherwise write a fragment. */
        if (old_record_end_offset_even == free_space_offset)
        {
            /* set it to the even aligned ending offset of the newly updated record. */
            slot_page[SLOT_FREE_SPACE_INDEX] = update_record_end_offset_even;
        }
        else
        {
            /* write out fragment. */
            memset(raw_page + update_record_end_offset_even, SLOT_FRAGMENT_BYTE, n_deleted_space);
        }

        /* write page back. */
        if (handle.WritePage(rid.pageNum, raw_page))
            return -1;

        /* update control page information. */
        if (increasePageSpace(handle, rid.pageNum, n_deleted_space))
            return -1;

        if(debug)
        {
            uint16_t new_space;
            debug_data_page(raw_page, "after update: record shrink");
            getPageSpace(handle, rid.pageNum, new_space);
            cout << "available space on page: " << new_space << endl;
        }

        /* done. */
        return 0;
    }
    else if (update_record_length > old_record_length)
    {
        /* check if record can grow in place, otherwise place a tuple redirect. */

        /* if the record ends on the free space offset, then we only need to know how much available free space we have. */
        if (old_record_end_offset_even == free_space_offset)
        {
            /* if it fits in free space, then we're good. */
            if (update_record_length <= (free_space + old_record_length))
            {
                /* overwrite record. */
                memcpy(raw_page + old_record_offset, update_record, update_record_length);

                update_record_end_offset_even = old_record_offset + update_record_length;

                /* mark the byte as a fragment if not on an even boundary. */
                if (IS_ODD(update_record_length))
                {
                    /* write out a fragment byte in the unusable space. */
                    memset(raw_page + old_record_offset + update_record_length, SLOT_FRAGMENT_BYTE, 1);
        
                    /* align the end offset to nearest even byte. */
                    update_record_end_offset_even++;
                }

                /* update used space. */
                n_used_space = update_record_end_offset_even - old_record_end_offset_even;

                /* update free space offset to end of the newly updated record. */
                slot_page[SLOT_FREE_SPACE_INDEX] = update_record_end_offset_even;

                decreasePageSpace(handle, rid.pageNum, n_used_space);

                /* write back page. */
                if (handle.WritePage(rid.pageNum, raw_page))
                    return -1;

                if(debug)
                {
                    uint16_t new_space;
                    debug_data_page(raw_page, "after update: record shrink");
                    getPageSpace(handle, rid.pageNum, new_space);
                    cout << "available space on page: " << new_space << endl;
                }

                return 0;
            }
            else if (update_record_length > (free_space + old_record_length))
            {
                /* doesn't fit in the free space, we need to see if compaction will work. */
                uint16_t needed_space = update_record_length - old_record_length;

                /* check if there's enough available space. */
                getPageSpace(handle, rid.pageNum, avail_space);

                if (needed_space > avail_space)
                {
                    /* cannot possibly grow the record on this page, we need to write a redirect. */

                    /* auxillary record id returned from insertTuple. */
                    RID aux;

                    /* the amount we need to decrease for the page. */
                    uint16_t n_freed_space = old_record_end_offset_even - (old_record_offset + REC_TUPLE_REDIR_LENGTH);

                    /* marker + slot_id, makes it distinguishable as a tuple redirect. */
                    uint16_t slot_id = REC_TUPLE_MARKER;

                    /* reclaim the fragment byte if the old record ended on an odd byte boundary. */
                    if (IS_ODD(old_record_length))
                        n_freed_space++;

                    /* insert the new tuple, get the new RID */
                    if (insertTuple(tableName, data, aux))
                        return -1;

                    /* add the slot_id to the marker. */
                    slot_id += aux.slotNum;

                    /* write out the tuple redirect. */
                    memcpy(raw_page + old_record_offset, &slot_id, sizeof(slot_id));
                    memcpy(raw_page + old_record_offset + sizeof(slot_id), &aux.pageNum, sizeof(aux.pageNum));

                    /* update free space pointer to where the tuple redirect ends. */
                    slot_page[SLOT_FREE_SPACE_INDEX] = old_record_offset + REC_TUPLE_REDIR_LENGTH;

                    /* update control information about new space. */
                    increasePageSpace(handle, rid.pageNum, n_freed_space);
    
                    /* write back page. */
                    if (handle.WritePage(rid.pageNum, raw_page))
                        return -1;

                    if(debug)
                    {
                        uint16_t new_space;
                        debug_data_page(raw_page, "after update: record redirect");
                        getPageSpace(handle, rid.pageNum, new_space);
                        cout << "available space on page: " << new_space << endl;
                    }
                }
                else if (needed_space <= avail_space)
                {
                    /* space that will be used up. */
                    uint16_t n_used_space = needed_space;

                    /* we have enough space, mark the old record as a fragment, compact, then append. */
                    memset(raw_page + old_record_offset, SLOT_FRAGMENT_BYTE, old_record_end_offset_even - old_record_offset);

                    /* update the free space pointer to the old_record_offset, since that's where the fragment ends. */
                    slot_page[SLOT_FREE_SPACE_INDEX] = old_record_offset;

                    /* compact the page to squeeze out some more space. */
                    reorganizePage(raw_page, rid, handle);

                    /* update free space offset since it changed after compaction. */
                    free_space_offset = slot_page[SLOT_FREE_SPACE_INDEX];

                    /* append record at free space pointer. */
                    memcpy(raw_page + free_space_offset, update_record, update_record_length);

                    /* update the slot with relocated offset. */
                    slot_page[SLOT_GET_SLOT_INDEX(rid.slotNum)] = free_space_offset;

                    /* write fragment byte in residue. */
                    if (IS_ODD(update_record_length))
                    {
                        memset(raw_page + free_space_offset + update_record_length, SLOT_FRAGMENT_BYTE, 1);

                        /* we consider this part of the needed space. */
                        n_used_space++;

                        /* record length is on even boundary. */
                        update_record_length++;
                    }

                    /* move free space offset to end of update record. */
                    slot_page[SLOT_FREE_SPACE_INDEX] += update_record_length;

                    /* update control information about new space. */
                    decreasePageSpace(handle, rid.pageNum, n_used_space);
    
                    /* write back page. */
                    if (handle.WritePage(rid.pageNum, raw_page))
                        return -1;

                    if(debug)
                    {
                        uint16_t new_space;
                        debug_data_page(raw_page, "after update: record appended at end of page");
                        getPageSpace(handle, rid.pageNum, new_space);
                        cout << "available space on page: " << new_space << endl;
                    }

                }
            }
        }
        else if (old_record_end_offset_even < free_space_offset)
	{
            /* additional space needed to fit the record. */
            uint16_t needed_space = update_record_length - old_record_length;

            /* used to scan adjacent fragment beginning directly at the end of the record. */
            uint16_t fragment_space = 0;

            /* flag to mark that we're using the residue byte in the record, since it's counted as already used space. */
            bool fragment_byte_used = false;
            
            /* determine the amount of adjacent space avaiable in the fragment. */
            while (raw_page[old_record_end_offset_even + fragment_space] == SLOT_FRAGMENT_BYTE)
            	fragment_space++;
            
            /* now include the residue byte. */
            if (IS_ODD(old_record_length))
                fragment_byte_used = true, fragment_space++;

            /* check to see if it can fit between the end of the old record and adjacent space. */
            if (needed_space <= fragment_space)
            {
                /* the fragment byte is given to us for free due to even alignment. */
                n_used_space = (fragment_byte_used) ? needed_space - 1 : needed_space;
           
                /* overwrite record and write into free space. */
                memcpy(raw_page + old_record_offset, update_record, update_record_length);
            
                update_record_end_offset_even = old_record_offset + update_record_length;
            
                /* mark the byte as a fragment if not on an even boundary. */
                if (IS_ODD(update_record_length))
                {
                    /* write out a fragment byte in the unusable space. */
                    memset(raw_page + old_record_offset + update_record_length, SLOT_FRAGMENT_BYTE, 1);

                    /* we also need to update the fact we're occupying that byte. */
                    n_used_space++;
            
                    /* align the end offset to nearest even byte. */
                    update_record_end_offset_even++;
                }
            
                decreasePageSpace(handle, rid.pageNum, n_used_space);
            
                /* write back page. */
                if (handle.WritePage(rid.pageNum, raw_page))
                    return -1;
            
                if(debug)
                {
                    uint16_t new_space;
                    debug_data_page(raw_page, "after update: record grow");
                    getPageSpace(handle, rid.pageNum, new_space);
                    cout << "available space on page: " << new_space << endl;
                }

                return 0;
            }
            else if (needed_space > fragment_space)
            {
                if (update_record_length <= free_space)
                {
                    uint16_t n_used_space = needed_space;

                    /* relocate record at the free space offset, turn old record into a fragment. */

                    /* write data to free space offset. */
                    memcpy(raw_page + free_space_offset, update_record, update_record_length);

                    /* update the slot information with the new offset. */
                    slot_page[SLOT_GET_SLOT_INDEX(rid.slotNum)] = free_space_offset;

                    /* determine end offset for new record. */
                    update_record_end_offset_even = free_space_offset + update_record_length;

                    /* mark the byte as a fragment if not on an even boundary. */
                    if (IS_ODD(update_record_length))
                    {
                        /* write out a fragment byte in the unusable space. */
                        memset(raw_page + old_record_offset + update_record_length, SLOT_FRAGMENT_BYTE, 1);
    
                        /* we also need to update the fact we're occupying that byte. */
                        n_used_space++;
                
                        /* align the end offset to nearest even byte. */
                        update_record_end_offset_even++;
                    }

                    /* mark the old record as a fragment. */
                    memset(raw_page + old_record_offset, SLOT_FRAGMENT_BYTE, old_record_end_offset_even - old_record_offset);

                    /* update free space offset to end of relocated record. */
                    slot_page[SLOT_FREE_SPACE_INDEX] = update_record_end_offset_even;

                    decreasePageSpace(handle, rid.pageNum, n_used_space);
                
                    /* write back page. */
                    if (handle.WritePage(rid.pageNum, raw_page))
                        return -1;

                    if(debug)
                    {
                        uint16_t new_space;
                        debug_data_page(raw_page, "after update: record grow (relocated to free space offset)");
                        getPageSpace(handle, rid.pageNum, new_space);
                        cout << "available space on page: " << new_space << endl;
                    }

                }
                else if (update_record_length > free_space)
                {
                    /* doesn't fit in the free space, we need to see if compaction will work. */
                    uint16_t needed_space = update_record_length - old_record_length;

                    /* check if there's enough available space. */
                    getPageSpace(handle, rid.pageNum, avail_space);

                    if (needed_space > avail_space)
                    {
                        /* cannot possibly grow the record on this page, we need to write a redirect. */

                        /* auxillary record id returned from insertTuple. */
                        RID aux;
    
                        /* the amount we need to decrease for the page. */
                        uint16_t n_freed_space = old_record_end_offset_even - (old_record_offset + REC_TUPLE_REDIR_LENGTH);
    
                        /* marker + slot_id, makes it distinguishable as a tuple redirect. */
                        uint16_t slot_id = REC_TUPLE_MARKER;
    
                        /* reclaim the fragment byte if the old record ended on an odd byte boundary. */
                        if (IS_ODD(old_record_length))
                            n_freed_space++;
    
                        /* insert the new tuple, get the new RID */
                        if (insertTuple(tableName, data, aux))
                            return -1;
    
                        /* add slot id to the marker. */
                        slot_id += aux.slotNum;

                        /* write out the tuple redirect. */
                        memcpy(raw_page + old_record_offset, &slot_id, sizeof(slot_id));
                        memcpy(raw_page + old_record_offset + sizeof(slot_id), &aux.pageNum, sizeof(aux.pageNum));
    
                        /* write fragment data in the remaining space. */
                        memset(raw_page + old_record_offset + sizeof(slot_id) + sizeof(aux.pageNum), SLOT_FRAGMENT_BYTE, n_freed_space);

                        /* update control information about new space. */
                        increasePageSpace(handle, rid.pageNum, n_freed_space);
        
                        /* write back page. */
                        if (handle.WritePage(rid.pageNum, raw_page))
                            return -1;
    
                        if(debug)
                        {
                            uint16_t new_space;
                            debug_data_page(raw_page, "after update: record redirect");
                            getPageSpace(handle, rid.pageNum, new_space);
                            cout << "available space on page: " << new_space << endl;
                        }
                    }
                    else if (needed_space <= avail_space)
                    {
                        /* space that will be used up. */
                        uint16_t n_used_space = needed_space;

                        /* we have enough space, mark the old record as a fragment, compact, then append. */
                        memset(raw_page + old_record_offset, SLOT_FRAGMENT_BYTE, old_record_end_offset_even - old_record_offset);
    
                        /* compact the page to squeeze out some more space. */
                        reorganizePage(raw_page, rid, handle);
    
                        /* update free space offset since it changed after compaction. */
                        free_space_offset = slot_page[SLOT_FREE_SPACE_INDEX];
    
                        /* append record at free space pointer. */
                        memcpy(raw_page + free_space_offset, update_record, update_record_length);
    
                        /* update the slot with relocated offset. */
                        slot_page[SLOT_GET_SLOT_INDEX(rid.slotNum)] = free_space_offset;
    
                        /* write fragment byte in residue. */
                        if (IS_ODD(update_record_length))
                        {
                            memset(raw_page + free_space_offset + update_record_length, SLOT_FRAGMENT_BYTE, 1);
    
                            /* we consider this part of the needed space. */
                            n_used_space++;
    
                            /* record length is on even boundary. */
                            update_record_length++;
                        }
    
                        /* move free space offset to end of update record. */
                        slot_page[SLOT_FREE_SPACE_INDEX] += update_record_length;
    
                        /* update control information about new space. */
                        decreasePageSpace(handle, rid.pageNum, n_used_space);
        
                        /* write back page. */
                        if (handle.WritePage(rid.pageNum, raw_page))
                            return -1;
    
                        if(debug)
                        {
                            uint16_t new_space;
                            debug_data_page(raw_page, "after update: record appended at end of page");
                            getPageSpace(handle, rid.pageNum, new_space);
                            cout << "available space on page: " << new_space << endl;
                        }
                    }
                }
            }
	}
    }

    /* done. */
    return 0;
} // }}}

void RM::debug_data_page(uint8_t *raw_page, const char *annotation) // {{{
{
    uint16_t begin_fragment_offset = SLOT_INVALID_ADDR; /* points to invalid address. */
    uint16_t record_slot = SLOT_INVALID_ADDR; /* points to invalid address. */

    uint16_t offset_to_slot_map[SLOT_HASH_SIZE];

    uint16_t *slot_page = (uint16_t *) raw_page;

    uint16_t num_slots = SLOT_GET_NUM_SLOTS(slot_page);
    uint16_t next_slot = slot_page[SLOT_NEXT_SLOT_INDEX];
    uint16_t free_space_offset = SLOT_GET_FREE_SPACE_OFFSET(slot_page);
    uint16_t free_space_avail = SLOT_GET_FREE_SPACE(slot_page);

    //if (debug)
    //{
    //    cout << "[BEGIN PAGE DUMP: " << annotation << " ]" << endl;
    //    cout << "free space offset: " << free_space_offset << endl;
    //    cout << "free space avail:  " << free_space_avail << endl;
    //    cout << "next slot:         " << next_slot << endl;
    //    cout << "num slots:         " << num_slots << endl;
    //}

    /* read in the slot directory, create a map. */

    /* invalidate offset map. */ 
    memset(offset_to_slot_map, 0xFF, PF_PAGE_SIZE);

    for (uint16_t i = 0; i < SLOT_GET_NUM_SLOTS(slot_page); i++)
    {
        /* slot_index points to the data position in the slot directory relative to the page. */
        uint16_t slot_index = SLOT_GET_SLOT_INDEX(i);

        /* slot_directory[i] offset. */
        uint16_t offset = slot_page[slot_index];

        if (SLOT_IS_ACTIVE(offset))
        {
            cout << "slot " << i << ": active, offset: " << SLOT_GET_SLOT(slot_page, i) << endl;
            offset_to_slot_map[SLOT_HASH_FUNC(offset)] = i;
        }
        else
        {
            cout << "slot " << i << ": inactive, next: " << SLOT_GET_INACTIVE_SLOT(slot_page, i) << endl;
        }
    }

    /* scan through finding all fragments and records until free space is reached.
       reading the slot page as 2 byte chunks, so it's expected that free_space_offset is aligned on even boundary.
    */

    assert(IS_EVEN(free_space_offset));
    
    for (uint16_t i = 0; i < free_space_offset; i++)
    {
        /* reached a fragment byte, determine if it's the start of a fragment. Using raw format so that we can count bytes. */
        if (raw_page[i] == SLOT_FRAGMENT_BYTE)
        {
            /* if not set, set the offset to the beginning of the fragment. */
            if (begin_fragment_offset == SLOT_INVALID_ADDR)
                begin_fragment_offset = i;
        }
        else
        {
            /* reached end of fragment, output it. */
            if (((int32_t) i - (int32_t) begin_fragment_offset) > 1)
                cout << "Fragment [start: " << begin_fragment_offset << "]: length=" << (i - begin_fragment_offset) << endl;

            if (REC_IS_TUPLE_REDIR(raw_page + i))
            {
                uint16_t slot_num = *((uint16_t *) (raw_page + i)) - REC_TUPLE_MARKER;
                unsigned int page_num = *((unsigned int *) (raw_page + i + sizeof(uint16_t)));

                cout << "Tuple Redirect [page: " << page_num << " slot: " << slot_num << "]" << endl;

                /* skip the redirect length. */
                i += (REC_TUPLE_REDIR_LENGTH - 1);
            }
            else if (REC_IS_RECORD(raw_page + i))
            {
                /* start of data record/tuple redirection. */
                record_slot = offset_to_slot_map[SLOT_HASH_FUNC(i)];
                cout << "Record [slot: " << record_slot << " start: " << i << "]: length=" << REC_LENGTH(raw_page + i) << endl;

                /* skip to end of record aligned on even byte, minus one for the post increment. */
                i += IS_EVEN(REC_LENGTH(raw_page + i)) ? REC_LENGTH(raw_page + i) - 1 : REC_LENGTH(raw_page + i);
            }

            /* reset the fragment pointer. */
            begin_fragment_offset = SLOT_INVALID_ADDR;
        }
    }

    //if(debug)
    //    cout << "[END PAGE DUMP: " << annotation << " ]" << endl;
} // }}}

RC RM::deleteTuples(const string tableName) // {{{
{
    /* Handle for database. */
    PF_FileHandle handle;

    /* open table, get handle. */
    if (openTable(tableName, handle))
        return -1;

    /* truncate database, gets a new file object. */
    if (handle.TruncateFile(tableName.c_str()))
        return -1;

    /* we're done. */
    return 0;
} // }}}

RC RM::reorganizePage(const string tableName, const unsigned pageNumber) // {{{
{
    /* data variables for reading in pages. */
    uint8_t raw_page[PF_PAGE_SIZE];
    uint16_t *slot_page = (uint16_t *) raw_page;

    /* handle for database. */
    PF_FileHandle handle;

    uint16_t begin_fragment_offset = SLOT_INVALID_ADDR; /* points to invalid address. */
    uint16_t offset_to_slot_map[SLOT_HASH_SIZE];
    uint16_t free_space_offset;

    uint16_t avail_space;

    /* new free space offset will point to where the free space begins after being compacted. */
    uint16_t new_free_space_offset = 0;

    /* open table to read in page. */
    if (openTable(tableName, handle))
        return -1;

    /* read in data page */
    if (handle.ReadPage(pageNumber, raw_page))
        return -1;
 
    debug_data_page(raw_page, "before reorg");
    /* get free space offset. */
    free_space_offset = SLOT_GET_FREE_SPACE_OFFSET(slot_page);

    /* read in the slot directory, create a map. */

    /* invalidate offset map. */ 
    memset(offset_to_slot_map, 0xFF, PF_PAGE_SIZE);

    /* build the offset to slot hash table. */
    for (uint16_t i = 0; i < SLOT_GET_NUM_SLOTS(slot_page); i++)
    {
        /* slot_index points to the data position in the slot directory relative to the page. */
        uint16_t slot_index = SLOT_GET_SLOT_INDEX(i);

        /* slot_directory[i] offset. */
        uint16_t offset = slot_page[slot_index];

        if (SLOT_IS_ACTIVE(offset))
            offset_to_slot_map[SLOT_HASH_FUNC(offset)] = i;
    }

    /* scan through finding all fragments and records until free space is reached.
       reading the slot page as 2 byte chunks, so it's expected that free_space_offset is aligned on even boundary.
    */

    assert(IS_EVEN(free_space_offset));
    
    for (uint16_t i = 0; i < free_space_offset; i++)
    {
        /* reached a fragment byte, determine if it's the start of a fragment. Using raw format so that we can count bytes. */
        if (raw_page[i] == SLOT_FRAGMENT_BYTE)
        {
            /* if not set, set the offset to the beginning of the fragment. */
            if (begin_fragment_offset == SLOT_INVALID_ADDR)
                begin_fragment_offset = i;
        }
        else
        {
            /* reached end of fragment, output it. */
            if (((int32_t) i - (int32_t) begin_fragment_offset) > 1)
            {
                uint16_t fragment_size = (i - begin_fragment_offset);

                /* determine if followed by record or tuple redirect. */
                if (REC_IS_TUPLE_REDIR(raw_page + i))
                {
                        
                    uint16_t length = REC_TUPLE_REDIR_LENGTH;
                    uint16_t mark_length;
                    uint16_t redir_slot;

                    /* move the data. */
                    memcpy(raw_page + begin_fragment_offset, raw_page + i, length);

                    /* get slot to update new offset. */
                    redir_slot = offset_to_slot_map[SLOT_HASH_FUNC(i)];

                    /* invalidate old offset so that it no longer points to a slot. */
                    offset_to_slot_map[SLOT_HASH_FUNC(i)] = SLOT_FRAGMENT_WORD;

                    /* update slot table with the new offset. */
                    slot_page[SLOT_GET_SLOT_INDEX(redir_slot)] = begin_fragment_offset;

                    /* update hash with new offset pointing to the slot. */
                    offset_to_slot_map[SLOT_HASH_FUNC(begin_fragment_offset)] = redir_slot;

                    /* update beginning fragment offset (skip the redirection data)  */
                    begin_fragment_offset += length;

                    /* new free space points to end of compacted space. */
                    new_free_space_offset = begin_fragment_offset;

                    /* mark residue data to continue identifying fragments, etc. */
                    if (fragment_size >= length)
                    {
                        /* since residual bytes of fragment will already be marked as fragmented bytes, only update everything past the end of the fragment. */
                        memset(raw_page + i, SLOT_FRAGMENT_BYTE, length);
                    }
                    else if (fragment_size < length)
                    {
                        /* mark residual in old tuple data as fragment bytes. */
                        mark_length = (i + length) - begin_fragment_offset;
                        memset(raw_page + begin_fragment_offset, SLOT_FRAGMENT_BYTE, mark_length);
                    }

                    /* move past tuple redirect now that it's become fragmented space, compensate for post increment. */
                    i += (length - 1);
                }
                else if (REC_IS_RECORD(raw_page + i))
                {
                    uint16_t length = REC_LENGTH(raw_page + i);
                    uint16_t mark_length;
                    uint16_t rec_slot;

                    /* align data. */
                    if (IS_ODD(length))
                        length++;

                    /* move the data. */
                    memcpy(raw_page + begin_fragment_offset, raw_page + i, length);

                    /* get slot to update new offset. */
                    rec_slot = offset_to_slot_map[SLOT_HASH_FUNC(i)];

                    /* invalidate old offset so that it no longer points to a slot. */
                    offset_to_slot_map[SLOT_HASH_FUNC(i)] = SLOT_FRAGMENT_WORD;

                    /* update slot table with the new offset. */
                    slot_page[SLOT_GET_SLOT_INDEX(rec_slot)] = begin_fragment_offset;

                    /* update hash with new offset pointing to the slot. */
                    offset_to_slot_map[SLOT_HASH_FUNC(begin_fragment_offset)] = rec_slot;

                    /* update beginning offset */
                    begin_fragment_offset += length;

                    /* new free space points to end of compacted space. */
                    new_free_space_offset = begin_fragment_offset;

                    if (fragment_size >= length)
                    {
                        /* since residual bytes of fragment will already be marked as fragmented bytes, only update everything past the end of the fragment. */
                        memset(raw_page + begin_fragment_offset, SLOT_FRAGMENT_BYTE, length);
                    }
                    else if (fragment_size < length)
                    {
                        /* mark residual in old tuple data as fragment bytes. */
                        mark_length = (i + length) - begin_fragment_offset;
                        memset(raw_page + begin_fragment_offset, SLOT_FRAGMENT_BYTE, mark_length);

                    }

                    /* move past tuple redirect now that it's become fragmented space, compensate for post increment. */
                    i += (length - 1);
                }
            }
            else if (REC_IS_TUPLE_REDIR(raw_page + i))
            {
                /* skip the redirect (compensate for post-increment). */
                i += (REC_TUPLE_REDIR_LENGTH - 1);

                /* new free space begins at end of tuple. */
                new_free_space_offset += REC_TUPLE_REDIR_LENGTH;

                /* reset the fragment pointer. */
                begin_fragment_offset = SLOT_INVALID_ADDR;
            }
            else if (REC_IS_RECORD(raw_page + i))
            {
                /* skip over record. */

                /* skip to end of record aligned on even byte */
                i += IS_EVEN(REC_LENGTH(raw_page + i)) ? REC_LENGTH(raw_page + i) : REC_LENGTH(raw_page + i) + 1;

                /* new free space offset begins at i. */
                new_free_space_offset = i;

                /* compensate for post increment. */
                i--;

                /* reset the fragment pointer. */
                begin_fragment_offset = SLOT_INVALID_ADDR;
            }
        }
    }

    slot_page[SLOT_FREE_SPACE_INDEX] = new_free_space_offset;

    if(debug)
    {
        debug_data_page(raw_page, "after reorg");
        getPageSpace(handle, pageNumber, avail_space);
        cout << "available space: " << avail_space << endl;
        assert(SLOT_GET_FREE_SPACE(slot_page) == avail_space);
    }
     
    /* write back page. */
    if (handle.WritePage(pageNumber, raw_page))
        return -1;

    return 0;
} // }}}

void RM::reorganizePage(uint8_t *raw_page, const RID &rid, PF_FileHandle handle) // {{{
{
    /* data variables for reading in pages. */
    uint16_t *slot_page = (uint16_t *) raw_page;

    uint16_t begin_fragment_offset = SLOT_INVALID_ADDR; /* points to invalid address. */
    uint16_t offset_to_slot_map[SLOT_HASH_SIZE];
    uint16_t free_space_offset;

    uint16_t avail_space;

    /* new free space offset will point to where the free space begins after being compacted. */
    uint16_t new_free_space_offset = 0;

    debug_data_page(raw_page, "before reorg");

    /* get free space offset. */
    free_space_offset = SLOT_GET_FREE_SPACE_OFFSET(slot_page);

    /* read in the slot directory, create a map. */

    /* invalidate offset map. */ 
    memset(offset_to_slot_map, 0xFF, PF_PAGE_SIZE);

    /* build the offset to slot hash table. */
    for (uint16_t i = 0; i < SLOT_GET_NUM_SLOTS(slot_page); i++)
    {
        /* slot_index points to the data position in the slot directory relative to the page. */
        uint16_t slot_index = SLOT_GET_SLOT_INDEX(i);

        /* slot_directory[i] offset. */
        uint16_t offset = slot_page[slot_index];

        if (SLOT_IS_ACTIVE(offset))
            offset_to_slot_map[SLOT_HASH_FUNC(offset)] = i;
    }

    /* scan through finding all fragments and records until free space is reached.
       reading the slot page as 2 byte chunks, so it's expected that free_space_offset is aligned on even boundary.
    */

    assert(IS_EVEN(free_space_offset));
    
    for (uint16_t i = 0; i < free_space_offset; i++)
    {
        /* reached a fragment byte, determine if it's the start of a fragment. Using raw format so that we can count bytes. */
        if (raw_page[i] == SLOT_FRAGMENT_BYTE)
        {
            /* if not set, set the offset to the beginning of the fragment. */
            if (begin_fragment_offset == SLOT_INVALID_ADDR)
                begin_fragment_offset = i;
        }
        else
        {
            /* reached end of fragment, output it. */
            if (((int32_t) i - (int32_t) begin_fragment_offset) > 1)
            {
                uint16_t fragment_size = (i - begin_fragment_offset);

                /* determine if followed by record or tuple redirect. */
                if (REC_IS_TUPLE_REDIR(raw_page + i))
                {
                        
                    uint16_t length = REC_TUPLE_REDIR_LENGTH;
                    uint16_t mark_length;
                    uint16_t redir_slot;

                    /* move the data. */
                    memcpy(raw_page + begin_fragment_offset, raw_page + i, length);

                    /* get slot to update new offset. */
                    redir_slot = offset_to_slot_map[SLOT_HASH_FUNC(i)];

                    /* invalidate old offset so that it no longer points to a slot. */
                    offset_to_slot_map[SLOT_HASH_FUNC(i)] = SLOT_FRAGMENT_WORD;

                    /* update slot table with the new offset. */
                    slot_page[SLOT_GET_SLOT_INDEX(redir_slot)] = begin_fragment_offset;

                    /* update hash with new offset pointing to the slot. */
                    offset_to_slot_map[SLOT_HASH_FUNC(begin_fragment_offset)] = redir_slot;

                    /* update beginning fragment offset (skip the redirection data)  */
                    begin_fragment_offset += length;

                    /* new free space points to end of compacted space. */
                    new_free_space_offset = begin_fragment_offset;

                    /* mark residue data to continue identifying fragments, etc. */
                    if (fragment_size >= length)
                    {
                        /* since residual bytes of fragment will already be marked as fragmented bytes, only update everything past the end of the fragment. */
                        memset(raw_page + i, SLOT_FRAGMENT_BYTE, length);
                    }
                    else if (fragment_size < length)
                    {
                        /* mark residual in old tuple data as fragment bytes. */
                        mark_length = (i + length) - begin_fragment_offset;
                        memset(raw_page + begin_fragment_offset, SLOT_FRAGMENT_BYTE, mark_length);
                    }

                    /* move past tuple redirect now that it's become fragmented space, compensate for post increment. */
                    i += (length - 1);
                }
                else if (REC_IS_RECORD(raw_page + i))
                {
                    uint16_t length = REC_LENGTH(raw_page + i);
                    uint16_t mark_length;
                    uint16_t rec_slot;

                    /* align data. */
                    if (IS_ODD(length))
                        length++;

                    /* move the data. */
                    memcpy(raw_page + begin_fragment_offset, raw_page + i, length);

                    /* get slot to update new offset. */
                    rec_slot = offset_to_slot_map[SLOT_HASH_FUNC(i)];

                    /* invalidate old offset so that it no longer points to a slot. */
                    offset_to_slot_map[SLOT_HASH_FUNC(i)] = SLOT_FRAGMENT_WORD;

                    /* update slot table with the new offset. */
                    slot_page[SLOT_GET_SLOT_INDEX(rec_slot)] = begin_fragment_offset;

                    /* update hash with new offset pointing to the slot. */
                    offset_to_slot_map[SLOT_HASH_FUNC(begin_fragment_offset)] = rec_slot;

                    /* update beginning offset */
                    begin_fragment_offset += length;

                    /* new free space points to end of compacted space. */
                    new_free_space_offset = begin_fragment_offset;

                    if (fragment_size >= length)
                    {
                        /* since residual bytes of fragment will already be marked as fragmented bytes, only update everything past the end of the fragment. */
                        memset(raw_page + begin_fragment_offset, SLOT_FRAGMENT_BYTE, length);
                    }
                    else if (fragment_size < length)
                    {
                        /* mark residual in old tuple data as fragment bytes. */
                        mark_length = (i + length) - begin_fragment_offset;
                        memset(raw_page + begin_fragment_offset, SLOT_FRAGMENT_BYTE, mark_length);

                    }

                    /* move past tuple redirect now that it's become fragmented space, compensate for post increment. */
                    i += (length - 1);
                }
            }
            else if (REC_IS_TUPLE_REDIR(raw_page + i))
            {
                /* skip the redirect (compensate for post-increment). */
                i += (REC_TUPLE_REDIR_LENGTH - 1);

                /* new free space begins at end of tuple. */
                new_free_space_offset += REC_TUPLE_REDIR_LENGTH;

                /* reset the fragment pointer. */
                begin_fragment_offset = SLOT_INVALID_ADDR;
            }
            else if (REC_IS_RECORD(raw_page + i))
            {
                /* skip over record. */

                /* skip to end of record aligned on even byte */
                i += IS_EVEN(REC_LENGTH(raw_page + i)) ? REC_LENGTH(raw_page + i) : REC_LENGTH(raw_page + i) + 1;

                /* new free space offset begins at i. */
                new_free_space_offset = i;

                /* compensate for post increment. */
                i--;

                /* reset the fragment pointer. */
                begin_fragment_offset = SLOT_INVALID_ADDR;
            }
        }
    }

    slot_page[SLOT_FREE_SPACE_INDEX] = new_free_space_offset;

    if(debug)
    {
        debug_data_page(raw_page, "after reorg");
        getPageSpace(handle, rid.pageNum, avail_space);
        cout << "available space: " << avail_space << endl;
    }
} // }}}

RC RM::readAttribute(const string tableName, const RID &rid, const string attributeName, void *data) // {{{
{
    /* data variables for reading in pages. */
    uint8_t raw_page[PF_PAGE_SIZE];
    uint16_t *slot_page = (uint16_t *) raw_page;

    /* offset in page where the record begins. */
    uint16_t record_offset;

    /* attribute of field to read. */
    Attribute attr;

    /* attribute position. */
    uint16_t attr_position;

    /* handle for database. */
    PF_FileHandle handle;

    /* retrieve table attributes. */
    if (getTableAttribute(tableName, attributeName, attr, attr_position))
        return -1;

    /* open table to read in page. */
    if (openTable(tableName, handle))
        return -1;

    /* read in data page */
    if (handle.ReadPage(rid.pageNum, raw_page))
        return -1;

    /* ensure slot is active. */
    if (SLOT_IS_INACTIVE(SLOT_GET_SLOT(slot_page, rid.slotNum)))
        return -1;

    /* get the beginning offset from the slot associated with the record. */
    record_offset = SLOT_GET_SLOT(slot_page, rid.slotNum);  

    /* copy record to tuple buffer. */
    record_attr_to_tuple(raw_page + record_offset, data, attr, attr_position);

    /* we're done, wasn't that hard? */
    return 0;
} // }}}

// scan returns an iterator to allow the caller to go through the results one by one.
RC RM::scan(const string tableName, const vector<string> &attributeNames, RM_ScanIterator &rm_ScanIterator) // {{{
{
    /* number of pages: control/data */
    unsigned int n_pages;
    unsigned int n_ctrl_pages;
    unsigned int n_data_pages;

    /* attribute of fields to read. */
    vector<Attribute> attrs;

    /* attribute positions. */
    vector<uint16_t> attrs_pos;

    /* handle for database. */
    PF_FileHandle handle;

    /* retrieve each attribute. */
    for (unsigned int i = 0; i < attributeNames.size(); i++)
    {
        Attribute attr;
        uint16_t attr_pos;

        if (getTableAttribute(tableName, attributeNames[i], attr, attr_pos))
            return -1;

        attrs.push_back(attr);
        attrs_pos.push_back(attr_pos);
    }

    /* open table to get number of pages. */
    if (openTable(tableName, handle))
        return -1;

    /* need to know the amount of data pages to scan. */
    n_pages = handle.GetNumberOfPages();
    n_ctrl_pages = CTRL_NUM_CTRL_PAGES(n_pages);
    n_data_pages = CTRL_NUM_DATA_PAGES(n_pages);

    /* setup the scan iterator. */
    rm_ScanIterator.attrs = attrs;
    rm_ScanIterator.attrs_pos = attrs_pos;
    rm_ScanIterator.n_pages = n_pages;
    rm_ScanIterator.n_data_pages = n_data_pages;
    rm_ScanIterator.n_ctrl_pages = n_ctrl_pages;
    rm_ScanIterator.page_id = 0;
    rm_ScanIterator.slot_id = 0;
    rm_ScanIterator.handle = handle;
    rm_ScanIterator.op = NO_OP;
    rm_ScanIterator.value = NULL;

    return 0;
}
// }}}

/* compOP is the comparison type, value is the value to compare with, and attributeNames are the attributes. */
RC RM::scan(const string tableName, const string conditionAttribute, const CompOp compOp, const void *value, const vector<string> &attributeNames, RM_ScanIterator &rm_ScanIterator) // {{{
{
    /* number of pages: control/data */
    unsigned int n_pages;
    unsigned int n_ctrl_pages;
    unsigned int n_data_pages;

    /* attribute for the condition. */
    Attribute cond_attr;

    /* position of condition attribute. */
    uint16_t cond_attr_pos;

    /* attribute of fields to read. */
    vector<Attribute> attrs;

    /* attribute positions. */
    vector<uint16_t> attrs_pos;

    /* handle for database. */
    PF_FileHandle handle;

    /* retrieve each attribute. */
    for (unsigned int i = 0; i < attributeNames.size(); i++)
    {
        Attribute attr;
        uint16_t attr_pos;

        if (getTableAttribute(tableName, attributeNames[i], attr, attr_pos))
            return -1;

        attrs.push_back(attr);
        attrs_pos.push_back(attr_pos);
    }

    /* open table to get number of pages. */
    if (openTable(tableName, handle))
        return -1;

    /* need to know the amount of data pages to scan. */
    n_pages = handle.GetNumberOfPages();
    n_ctrl_pages = CTRL_NUM_CTRL_PAGES(n_pages);
    n_data_pages = CTRL_NUM_DATA_PAGES(n_pages);

    /* setup the scan iterator. */
    rm_ScanIterator.attrs = attrs;
    rm_ScanIterator.attrs_pos = attrs_pos;
    rm_ScanIterator.n_pages = n_pages;
    rm_ScanIterator.n_data_pages = n_data_pages;
    rm_ScanIterator.n_ctrl_pages = n_ctrl_pages;
    rm_ScanIterator.page_id = 0;
    rm_ScanIterator.slot_id = 0;
    rm_ScanIterator.handle = handle;

    /* conditional attribute stuff. */
    rm_ScanIterator.op = compOp;
    rm_ScanIterator.value = (void *) value;

    /* get the actual attribute. */
    if (getTableAttribute(tableName, conditionAttribute, cond_attr, cond_attr_pos))
        return -1;

    rm_ScanIterator.cond_attr = cond_attr;
    rm_ScanIterator.cond_attr_pos = cond_attr_pos;

    return 0;
}

// }}}

RC RM::debug_data_page(const string tableName, unsigned int page_id, const char *annotation) // {{{
{
    /* data variables for reading in pages. */
    uint8_t raw_page[PF_PAGE_SIZE];

    /* handle for database. */
    PF_FileHandle handle;

    /* open table to read in page. */
    if (openTable(tableName, handle))
        return -1;

    /* read in data page */
    if (handle.ReadPage(page_id, raw_page))
        return -1;

    debug_data_page(raw_page, annotation);

    return 0;
} // }}}

void RM_ScanIterator::record_attrs_to_tuple(uint8_t *record, void *data)
{
    /* keep a pointer to where data needs to be packed. */
    uint8_t *tuple_ptr = (uint8_t *) data;

    /* leverage existing work by record_attr_to_tuple to pack everything into data. */

    for(unsigned int i = 0; i < attrs.size(); i++)
    {
        RM::record_attr_to_tuple(record, tuple_ptr, attrs[i], attrs_pos[i]);

        if (attrs[i].type == TypeInt)
        {
            /* skip the int. */
            tuple_ptr += sizeof(int);
        }
        else if (attrs[i].type == TypeReal)
        {
            /* skip the float. */
            tuple_ptr += sizeof(float);
        }
        else if (attrs[i].type == TypeVarChar)
        {
            int length;
            memcpy(&length, tuple_ptr, sizeof(length));

            /* skip the length field. */
            tuple_ptr += sizeof(length);

            /* skip varchar data. */
            tuple_ptr += length;
        }
    }
}

RC RM_ScanIterator::record_cond_attrs_to_tuple(uint8_t *record, void *data) // {{{
{
    /* keep a pointer to where data needs to be packed. */
    uint8_t *tuple_ptr = (uint8_t *) data;

    /* determine if the condition is met. */
    RM::record_attr_to_tuple(record, tuple_ptr, cond_attr, cond_attr_pos);

    if (cond_attr.type == TypeInt)
    {
        int lhs_val, rhs_val;
        memcpy(&lhs_val, tuple_ptr, sizeof(lhs_val));
        memcpy(&rhs_val, value, sizeof(rhs_val));
        VALUE_COMP_OP(op, lhs_val, rhs_val);
    }
    else if (cond_attr.type == TypeReal)
    {
        float lhs_val, rhs_val;
        memcpy(&lhs_val, tuple_ptr, sizeof(lhs_val));
        memcpy(&rhs_val, value, sizeof(rhs_val));
        VALUE_COMP_OP(op, lhs_val, rhs_val);
    }
    else if (cond_attr.type == TypeVarChar)
    {
        int lhs_length, rhs_length, cmp_value;
        memcpy(&lhs_length, tuple_ptr, sizeof(lhs_length));
        tuple_ptr += sizeof(lhs_length);
        memcpy(&rhs_length, value, sizeof(rhs_length));

        /* if the lengths don't match and we're testing for equality we immediately know this isn't equal. */
        if(op == EQ_OP && lhs_length != rhs_length)
            return 1;

        /* if the lengths are equal then just compare, otherwise they're not equal strings. */
        if(lhs_length == rhs_length)
        {
            /* compare the buffers up to the shortest one. */
            cmp_value = memcmp((uint8_t *) tuple_ptr, ((uint8_t *) value) + sizeof(rhs_length), MIN(lhs_length, rhs_length));

            /* ensure equality or inequality. */
            if((cmp_value == 0 && op == NE_OP) || (cmp_value != 0 && op == EQ_OP))
                return 1;
        }
    }

    /* get the data. */
    record_attrs_to_tuple(record, data);
 
    return 0;
} // }}}

RC RM_ScanIterator::getNextTupleCond(RID &rid, void *data) // {{{
{
    /* data variables for reading in pages. */
    uint8_t raw_page[PF_PAGE_SIZE];
    uint16_t *slot_page = (uint16_t *) raw_page;

    /* record offset. */
    uint16_t record_offset;

    /* need at least a control and data page to iterate. */
    if (n_pages < 2)
       return RM_EOF;

    if(page_id == 0)
        page_id = 1;

    if(page_id >= n_pages)
        return RM_EOF;
      
    /* bring in the page. */ 
    if (handle.ReadPage(page_id, raw_page))
        return -1;

    if(slot_id == 0 && (RM::Instance())->debug)
        RM::debug_data_page(raw_page, "scanning data page (cond)");

    /* update the slot count. */
    num_slots = SLOT_GET_NUM_SLOTS(slot_page);

    if (slot_id == num_slots)
    {
        /* make sure we skip control pages/select only the next data page. */
        page_id = CTRL_IS_CTRL_PAGE(page_id + 1) ? page_id + 2 : page_id + 1;
        slot_id = num_slots = 0;

        /* confirm that it's in bounds. */
        if(page_id >= n_pages)
            return RM_EOF;

        /* bring in the new page. */ 
        if (handle.ReadPage(page_id, raw_page))
            return -1;

        if((RM::Instance())->debug)
            RM::debug_data_page(raw_page, "scanning data page");
        num_slots = SLOT_GET_NUM_SLOTS(slot_page);
    }

    /* find first active slot, get the record offset. */
    while(slot_id < num_slots)
    {
        record_offset = SLOT_GET_SLOT(slot_page, slot_id);

        /* if all crtieria are met, then we can use this slot id to get a record. */
        if(slot_id < num_slots && SLOT_IS_ACTIVE(record_offset) && REC_IS_RECORD(raw_page + record_offset))
        {
            /* get the record, make sure it meets criteria. */
            if(!record_cond_attrs_to_tuple(raw_page + record_offset, data))
            {
                /* return the data. */
                rid.pageNum = page_id;
                rid.slotNum = slot_id;
         
                /* work on the next slot next time the iterator is called. */
                slot_id++;

                return 0;
            }
        }

        /* otherwise keep searching. */
        slot_id++;

        /* reached the end of slots, need to go to next data page. */
        if (slot_id == num_slots)
        {
            /* make sure we skip control pages/select only the next data page. */
            page_id = CTRL_IS_CTRL_PAGE(page_id + 1) ? page_id + 2 : page_id + 1;
            slot_id = num_slots = 0;

            /* confirm that it's in bounds. */
            if(page_id >= n_pages)
                return RM_EOF;

            /* bring in the new page. */ 
            if (handle.ReadPage(page_id, raw_page))
                return -1;

            if((RM::Instance())->debug)
                RM::debug_data_page(raw_page, "scanning data page");
            num_slots = SLOT_GET_NUM_SLOTS(slot_page);
        }
    }

    return 0;
} // }}}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) // {{{
{
    /* data variables for reading in pages. */
    uint8_t raw_page[PF_PAGE_SIZE];
    uint16_t *slot_page = (uint16_t *) raw_page;

    /* record offset. */
    uint16_t record_offset;

    /* use the conditional iterator if we have a condition. */
    if (op != NO_OP)
        return getNextTupleCond(rid, data);

    /* need at least a control and data page to iterate. */
    if (n_pages < 2)
       return RM_EOF;

    if(page_id == 0)
        page_id = 1;

    if(page_id >= n_pages)
        return RM_EOF;
      
    /* bring in the page. */ 
    if (handle.ReadPage(page_id, raw_page))
        return -1;

    if(slot_id == 0 && (RM::Instance())->debug)
        RM::debug_data_page(raw_page, "scanning data page");

    /* update the slot count. */
    num_slots = SLOT_GET_NUM_SLOTS(slot_page);

    if (slot_id == num_slots)
    {
        /* make sure we skip control pages/select only the next data page. */
        page_id = CTRL_IS_CTRL_PAGE(page_id + 1) ? page_id + 2 : page_id + 1;
        slot_id = num_slots = 0;

        /* confirm that it's in bounds. */
        if(page_id >= n_pages)
            return RM_EOF;

        /* bring in the new page. */ 
        if (handle.ReadPage(page_id, raw_page))
            return -1;

        if((RM::Instance())->debug)
            RM::debug_data_page(raw_page, "scanning data page");
        num_slots = SLOT_GET_NUM_SLOTS(slot_page);
    }

    /* find first active slot, get the record offset. */
    while(slot_id < num_slots)
    {
        record_offset = SLOT_GET_SLOT(slot_page, slot_id);

        /* if all crtieria are met, then we can use this slot id to get a record. */
        if(slot_id < num_slots && SLOT_IS_ACTIVE(record_offset) && REC_IS_RECORD(raw_page + record_offset))
        {
            /* get the record. */
            record_attrs_to_tuple(raw_page + record_offset, data);
         
            /* return the data. */
            rid.pageNum = page_id;
            rid.slotNum = slot_id;
         
            /* work on the next slot next time the iterator is called. */
            slot_id++;

            return 0;
        }

        /* otherwise keep searching. */
        slot_id++;

        /* reached the end of slots, need to go to next data page. */
        if (slot_id == num_slots)
        {
            /* make sure we skip control pages/select only the next data page. */
            page_id = CTRL_IS_CTRL_PAGE(page_id + 1) ? page_id + 2 : page_id + 1;
            slot_id = num_slots = 0;

            /* confirm that it's in bounds. */
            if(page_id >= n_pages)
                return RM_EOF;

            /* bring in the new page. */ 
            if (handle.ReadPage(page_id, raw_page))
                return -1;

            if((RM::Instance())->debug)
                RM::debug_data_page(raw_page, "scanning data page");
            num_slots = SLOT_GET_NUM_SLOTS(slot_page);
        }
    }

    return 0;
} // }}}

RC RM_ScanIterator::close() // {{{
{
    page_id = 0;
    slot_id = 0;
    num_slots = 0;

    return 0;
}; // }}}
