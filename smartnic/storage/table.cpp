#include "storage/table.h"
#include "storage/catalog.h"
#include "storage/row.h"

table_t::table_t(catalog_t* schema){
    memcpy(&this->schema, schema, sizeof(catalog_t));
    strcpy(table_name, schema->table_name);
    this->table_id = schema->table_id;
}

uint64_t table_t::get_tuple_size(){
    return schema.get_tuple_size();
}

uint64_t table_t::get_field_cnt(){
    return schema.field_cnt;
}

catalog_t* table_t::get_schema(){
    return &schema;
}
