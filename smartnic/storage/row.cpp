#include "storage/table.h"
#include "storage/catalog.h"
#include "storage/row.h"
#include "worker/transport.h"
#include "worker/mr.h"
#include "worker/txn.h"
#include "common/helper.h"

row_t::row_t(int table_idx, uint64_t row_id): table_idx(table_idx), row_id(row_id) { }

void row_t::init(table_t* table, int pid, uint64_t row_id){
    #ifndef LOCKTABLE
    //lock.init();
    memset(&lock, 0, sizeof(lock_t));
    #endif
    this->row_id = row_id;
    this->pid = pid;
    this->table = table;
    catalog_t* schema = table->get_schema();
    int tuple_size = schema->get_tuple_size();
}

const char* row_t::get_table_name(table_t* table){
    return table->get_table_name();
}

table_t* row_t::get_table(){
    return table;
}

catalog_t* row_t::get_schema(){
    return get_table()->get_schema();
}

int row_t::get_tuple_size(catalog_t* schema){
    return schema->get_tuple_size();
}

int row_t::get_tuple_size(){
    return get_schema()->get_tuple_size();
}

int row_t::get_field_cnt(catalog_t* schema){
    return schema->field_cnt;
}

void row_t::set_value(catalog_t* schema, int id, int val){
    int data_size = schema->get_field_size(id);
    int pos = schema->get_field_index(id);
    memcpy(&data[pos], &val, data_size);
}

void row_t::set_value(catalog_t* schema, int id, void* ptr){
    int data_size = schema->get_field_size(id);
    int pos = schema->get_field_index(id);
    memcpy(&data[pos], ptr, data_size);
}

void row_t::set_value(catalog_t* schema, int id, void* ptr, int size){
    int pos = schema->get_field_index(id);
    memcpy(&data[pos], ptr, size);
}

void row_t::set_value(catalog_t* schema, const char* col_name, void* ptr){
    auto id = schema->get_field_id(col_name);
    set_value(schema, id, ptr);
}

void row_t::get_value(catalog_t* schema, int id, void* ptr){
    int data_size = schema->get_field_size(id);
    int pos = schema->get_field_index(id);
    memcpy(ptr, &data[pos], data_size);
}

char* row_t::get_value(catalog_t* schema, int id){
    int pos = schema->get_field_index(id);
    return &data[pos];
}

char* row_t::get_value(catalog_t* schema, char* col_name){
    int pos = schema->get_field_index(col_name);
    return &data[pos];
}

char* row_t::get_data(){
    return data;
}

void row_t::set_data(char* _data, int size){
    memcpy(data, _data, size);
}

void row_t::copy(row_t* src){
    set_data(src->get_data(), src->get_tuple_size());
}

void row_t::copy(catalog_t* schema, row_t* src, int idx){
    char* ptr = src->get_value(schema, idx);
    set_value(schema, idx, ptr);
}

void row_t::copy(catalog_t* schema, row_t* src){
    set_data(src->get_data(), src->get_tuple_size(schema));
}

void row_t::free_row(){
}

#ifndef LOCKTABLE
RC row_t::get_row(access_t type, txn_man_t* txn){
    RC rc = RCOK;
RETRY:
    auto _lock = lock;
    if(type == READ || type == SCAN){
	if(_lock.is_exclusive())
	    return ABORT;

	auto swap = _lock;
	swap.acquire_shared();

	if(!ATOM_CAS(((uint64_t*)&lock), (*(uint64_t*)&_lock), (*(uint64_t*)&swap)))
	    goto RETRY;
    }
    else{
	if(_lock.is_exclusive() || _lock.is_shared())
	    return ABORT;

	auto swap = _lock;
	swap.acquire_exclusive();

	if(!ATOM_CAS(((uint64_t*)&lock), (*(uint64_t*)&_lock), (*(uint64_t*)&swap)))
	    return ABORT;
    }

    return RCOK;
}

RC row_t::get_row(access_t type, txn_man_t* txn, uint64_t row_addr, int tid, int pid){
    RC rc = RCOK;
//    debug::notify_info("REMOTE ACCESS GET_ROW");
    txn->transport->read((uint64_t)this, row_addr, sizeof(uint64_t), tid, pid);
RETRY:
    auto _lock = lock;
    if(type == READ || type == SCAN){
	if(_lock.is_exclusive())
	    return ABORT;

	_lock.acquire_shared();
	if(!txn->transport->cas((uint64_t)this, row_addr, *((uint64_t*)&lock), *((uint64_t*)&_lock), sizeof(uint64_t), tid, pid)){
	    if(lock.is_exclusive())
		return ABORT;
	    goto RETRY;
	}
    }
    else{
	if(_lock.is_exclusive() || _lock.is_shared())
	    return ABORT;

	_lock.acquire_exclusive();
	if(!txn->transport->cas((uint64_t)this, row_addr, *((uint64_t*)&lock), *(uint64_t*)&_lock, sizeof(uint64_t), tid, pid))
	    return ABORT;
    }

    return RCOK;
}

void row_t::return_row(access_t type){
    if(type == READ || type == SCAN){
	ATOM_SUB(lock.reader, 1);
    }
    else if(type == WRITE){
	lock.release_exclusive();
    }
    else{ // XP
	lock.release_exclusive();
	//lock.writer = 0;
    }
}
	
void row_t::return_row(access_t type, txn_man_t* txn, uint64_t row_addr, int tid, int pid){
//    debug::notify_info("REMOTE ACCESS RETURN_ROW");
    bool ret = false;
    if(type == READ || type == SCAN){
	while(!ret){
	    auto _lock = lock;
	    _lock.release_shared();
	    ret = txn->transport->cas((uint64_t)this, row_addr, *((uint64_t*)&lock), *((uint64_t*)&_lock), sizeof(uint64_t), tid, pid);
	}
    }
    else if(type == WRITE){
	lock.release_exclusive();
	txn->transport->write((uint64_t)this, row_addr, sizeof(uint64_t), tid, pid);
    }
    else{ // XP
	lock.release_exclusive();
	txn->transport->write((uint64_t)this, row_addr, ROW_SIZE, tid, pid);
    }
}
#endif
    
