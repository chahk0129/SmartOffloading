#include "storage/catalog.h"

void catalog_t::init(const char* table_name, int field_cnt){
    strcpy(this->table_name, table_name);
    this->field_cnt = 0;
    this->table_id = 0;
    this->tuple_size = 0;
    for(int i=0; i<NUM_COLUMNS; i++){
	memset(_columns[i].type, 0, 80);
	memset(_columns[i].name, 0, 80);
	_columns[i].id = 0;
	_columns[i].index = 0;
	_columns[i].size = 0;
    }
}


void catalog_t::add_col(const char* col_name, uint64_t size, const char* type){
    _columns[field_cnt].size = size;
    strcpy(_columns[field_cnt].type, type);
    strcpy(_columns[field_cnt].name, col_name);
    _columns[field_cnt].id = field_cnt;
    _columns[field_cnt].index = tuple_size;
    tuple_size += size;
    field_cnt++;
}

int catalog_t::get_field_id(const char* name){
    int i;
    for(i=0; i<field_cnt; i++){
	if(strcmp(name, _columns[i].name) == 0)
	    break;
    }
    assert(i < field_cnt);
    return i;
}

char* catalog_t::get_field_type(int id){
    return _columns[id].type;
}

char* catalog_t::get_field_name(int id){
    return _columns[id].name;
}

char* catalog_t::get_field_type(char* name){
    return get_field_type(get_field_id(name));
}

int catalog_t::get_field_index(char* name){
    return get_field_index(get_field_id(name));
}

void catalog_t::print_schema(){
    printf("\n[Catalog] %s\n", table_name);
    for(int i=0; i<field_cnt; i++){
	printf("\t%s\t%s\t%d\n", get_field_name(i), get_field_type(i), get_field_size(i));
    }
}
