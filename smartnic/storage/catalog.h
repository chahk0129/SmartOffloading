#pragma once
#include "common/global.h"

class column_t{
    public:
	column_t(){ }
	column_t(uint64_t id, uint64_t size, uint64_t index, char* type, char* name): id(id), size(size), index(index){
	    strcpy(this->type, type);
	    strcpy(this->name, name);
	}

	uint64_t id;
	uint64_t size; 
	uint64_t index;
	uint64_t pad;
	char type[80]; 
	char name[80]; 
	// 160B chars + 32B = 192B = 3 cachelines
};


class catalog_t{
    public:
	catalog_t(): field_cnt(0), table_id(0), tuple_size(0){ }
	void init(const char* table_name, int field_cnt);

	void add_col(const char* col_name, uint64_t size, const char* type);

	int field_cnt;
	int table_id;
	char table_name[TABLE_NAME_LENGTH];

	int get_tuple_size()      { return tuple_size; }
	int get_field_cnt()        { return field_cnt; }
	int get_field_index(int id){ return _columns[id].index; }
	int get_field_size(int id) { return _columns[id].size; }
	char* get_field_type(int id);
	char* get_field_name(int id);
	int get_field_id(const char* name);
	char* get_field_type(char* name);
	int get_field_index(char* name);

	void print_schema();

	column_t _columns[NUM_COLUMNS];
	int tuple_size;
};


