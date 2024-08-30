#include "storage/table.h"
#include "storage/catalog.h"
#include "storage/row.h"
#include "client/transport.h"
#include "client/mr.h"
#include "system/txn.h"
#include "concurrency/nowait.h"
#include "concurrency/waitdie.h"
#include "concurrency/row_lock.h"

row_t::row_t(int table_idx, uint64_t row_id): manager(), table_idx(table_idx), row_id(row_id) { }

void row_t::init(table_t* table, int pid, uint64_t row_id){
    memset(&manager, 0, sizeof(manager));
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

RC row_t::get_row(access_t type, txn_man_t* txn, uint64_t row_addr, int tid, int pid){
    RC rc = RCOK;
#if defined(NOWAIT) || defined(WAITDIE)
    lock_type_t lt = (type == READ || type == SCAN) ? LOCK_SH : LOCK_EX;
    txn->transport->read((uint64_t)this, row_addr, sizeof(manager), tid, pid);
    rc = manager.lock_get(lt, txn, row_addr, tid, pid);
#else
    
RETRY:
    auto _lock = manager;
    if(type == READ || type == SCAN){
	if(_lock.is_exclusive()){
	    rc = ABORT;
	    return rc;
	}

	_lock.acquire_shared();
	if(!txn->transport->cas((uint64_t)this, row_addr, *((uint64_t*)&manager), *((uint64_t*)&_lock), sizeof(uint64_t), tid, pid)){
	    if(manager.is_exclusive()){
		rc = ABORT;
		return rc;
	    }
	    goto RETRY;
	}
    }
    else{
	if(_lock.is_exclusive() || _lock.is_shared()){
	    rc = ABORT;
	    return rc;
	}

	_lock.acquire_exclusive();
	if(!txn->transport->cas((uint64_t)this, row_addr, *((uint64_t*)&manager), *(uint64_t*)&_lock, sizeof(uint64_t), tid, pid)){
	    rc = ABORT;
	    return rc;
	}
    }
#endif

    return rc;
}

void row_t::return_row(access_t type, txn_man_t* txn, uint64_t row_addr, int tid, int pid){
#if defined(NOWAIT) || defined(WAITDIE)
    if(type == READ || type == SCAN){
	manager.lock_release(LOCK_SH, txn, row_addr, tid, pid);
    }
    else if(type == WRITE){
	size_t locksize = sizeof(manager);
	txn->transport->write((uint64_t)this + locksize, row_addr + locksize, ROW_SIZE-locksize, tid, pid);
	manager.lock_release(LOCK_EX, txn, row_addr, tid, pid);
    }
    else{ // XP
	manager.lock_release(LOCK_EX, txn, row_addr, tid, pid);
    }
#else
    
    bool ret = false;
    if(type == READ || type == SCAN){
	while(!ret){
	    auto _lock = manager;
	    _lock.release_shared();
	    ret = txn->transport->cas((uint64_t)this, row_addr, *((uint64_t*)&manager), *((uint64_t*)&_lock), sizeof(uint64_t), tid, pid);
	}
    }
    else if(type == WRITE){
	manager.release_exclusive();
	size_t locksize = sizeof(manager);
	txn->transport->write((uint64_t)this + locksize, row_addr + locksize, ROW_SIZE-locksize, tid, pid);
	txn->transport->write((uint64_t)this, row_addr, locksize, tid, pid);
    }
    else{ // XP
	manager.release_exclusive();
	size_t locksize = sizeof(manager);
	txn->transport->write((uint64_t)this, row_addr, locksize, tid, pid);
    }
#endif
}


    
