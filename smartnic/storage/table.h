#pragma once
#include "common/global.h"
#include "storage/catalog.h"

//class catalog_t;
class row_t;
class table_t{
    public:
	table_t(catalog_t* schema);

	uint64_t get_tuple_size();
	uint64_t get_field_cnt();
	catalog_t* get_schema();

	uint64_t get_table_size(){
	    return cur_tab_size;
	}

	const char* get_table_name(){
	    return table_name;
	}

	catalog_t schema;

    private:
	char table_name[TABLE_NAME_LENGTH];
	uint64_t cur_tab_size;
	int table_id;
	char pad[64 - sizeof(void*)*3];
};
