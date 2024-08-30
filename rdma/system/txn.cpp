#include "system/txn.h"
#include "storage/row.h"
//#include "concurrency/row_lock.h"
#include "client/mr.h"
#include "client/transport.h"
#include "system/workload.h"
#include "index/tree.h"
#include "system/thread.h"
#include "common/stats.h"
#include "common/helper.h"
#include <algorithm>

#ifdef BREAKDOWN
thread_local uint64_t t_abort, t_index, t_commit, t_wait, t_total, t_backoff;
thread_local uint64_t start, end;
#endif
void txn_man_t::init(thread_t* thread, workload_t* workload, int tid){
    this->thread = thread;
    this->workload = workload;
    lock_ready = false;
    lock_abort = false;
    timestamp = 0;
    insert_cnt = 0;
    wr_cnt = 0;
    row_cnt = 0;
    accesses = new Access*[MAX_ROW_PER_TXN];
    for(int i=0; i<MAX_ROW_PER_TXN; i++)
	accesses[i] = nullptr;

    num_accesses_alloc = 0;
    write_buffer = new row_wrapper_t[MAX_ROW_PER_TXN];
    memset(write_buffer, 0, sizeof(row_wrapper_t) * MAX_ROW_PER_TXN);

    this->mem = workload->mem;
    this->transport = workload->transport;
}

void txn_man_t::set_txn_id(uint64_t txn_id){
    this->txn_id = txn_id;
}

uint64_t txn_man_t::get_txn_id(){
    return txn_id;
}

workload_t* txn_man_t::get_workload(){
    return workload;
}

int txn_man_t::get_tid(){
    return thread->get_tid();
}

void txn_man_t::set_ts(uint64_t timestamp){
    this->timestamp = timestamp;
}

uint64_t txn_man_t::get_ts(){
    return timestamp;
}

void txn_man_t::cleanup(RC rc, int tid){
    // go through accesses and release
    for(int rid=row_cnt-1; rid>=0; rid--){
	auto buffer = &write_buffer[rid];
	auto row = mem->row_buffer_pool(tid, rid);
	auto addr = accesses[rid]->addr;
	auto type = accesses[rid]->type;
	auto pid = accesses[rid]->pid;
	if(type == WRITE){
	    if(rc == ABORT)
		type = XP;
	    else
		memcpy(row, buffer->data, ROW_SIZE);
	}

	row->return_row(type, this, addr, tid, pid);
    }

    row_cnt = 0;
    wr_cnt = 0;
    insert_cnt = 0;
}

inline void txn_man_t::assign_lock_entry(Access* access){
//    auto lock_entry = new lock_entry_t(this, access);
//    access->lock_entry = lock_entry;
}

void txn_man_t::return_row(row_t* row, uint64_t row_addr, access_t type, int tid, int pid){
    row->return_row(type, this, row_addr, tid, pid);
}

row_t* txn_man_t::get_new_row(uint64_t addr, int tid, int pid){
    RC rc = RCOK;
    if(accesses[row_cnt] == nullptr){
	auto access = new Access;
	accesses[row_cnt] = access;
	num_accesses_alloc++;
    }

    row_t* row = mem->row_buffer_pool(tid, row_cnt);
    accesses[row_cnt]->type = WRITE;
    accesses[row_cnt]->addr = addr;
    accesses[row_cnt]->pid = pid;
    wr_cnt++;
    row_cnt++;

    return row;
}

row_t* txn_man_t::get_row(uint64_t row_addr, int tid, int pid, access_t type){
//row_t* txn_man_t::get_row(uint64_t row_addr, int rid, int tid, int pid, access_t type){
    RC rc = RCOK;
    if(accesses[row_cnt] == nullptr){
	auto access = new Access;
	accesses[row_cnt] = access;
	num_accesses_alloc++;
    }

    assert(row_cnt < MAX_ROW_PER_TXN);
    row_t* row = mem->row_buffer_pool(tid, row_cnt);
    //row_t* row = mem->row_buffer_pool(tid, rid);
    rc = row->get_row(type, this, row_addr, tid, pid);
    if(rc == ABORT){
	return nullptr;
    }

    accesses[row_cnt]->type = type;
    accesses[row_cnt]->addr = row_addr;
    //accesses[row_cnt]->rid = rid;
    accesses[row_cnt]->pid = pid;

    if(type == WRITE){
	//memcpy(accesses[row_cnt]->orig_row, row, ROW_SIZE);
	wr_cnt++;
    }
    row_cnt++;

    return row;
}

void txn_man_t::insert_row(row_t* row, table_t* table){
    insert_rows[insert_cnt++] = row;
}

RC txn_man_t::finish(RC rc, int tid){
    cleanup(rc, tid);
    return rc;
}

void txn_man_t::index_insert(row_t* row, tree_t<Key, Value>* index, Key key, int tid){
    /*
    auto item = new itemid_t();
    item->init();
    item->type = DT_row;
    item->location = row;
    item->valid = true;
    index->index_insert(key, item, get_tid());
    */
}

uint64_t txn_man_t::index_read(tree_t<Key, Value>* index, Key key, int tid){
    uint64_t item = 0;
    index->search(key, item, tid);
    // TODO: stats
    return item;
}

void txn_man_t::index_read(tree_t<Key, Value>* index, Key key, uint64_t& item, int tid){
    index->search(key, item, tid);
    // TODO: stats
}

void txn_man_t::release(){
    for(int i=0; i<num_accesses_alloc; i++){
	// TODO: release lock entry
	delete accesses[i];
    }
    delete accesses;
}


/*
RC txn_man_t::set_readlock(row_t* row, uint64_t row_addr, int tid, int pid){
RETRY:
    if(row->lock.is_exclusive())
	return ABORT;

    auto lock = row->lock;
    lock.acquire_shared();
    bool ret = transport->cas((uint64_t)row, row_addr, *((uint64_t*)&row->lock), *((uint64_t*)&lock), sizeof(uint64_t), tid, pid);
    if(!ret){
	if(row->lock.is_exclusive())
	    return ABORT;
	goto RETRY;
    }

    return RCOK;
}

RC txn_man_t::set_writelock(row_t* row, uint64_t row_addr, int tid, int pid){
    if(row->lock.is_exclusive() || row->lock.is_shared())
	return ABORT;

    auto lock = row->lock;
    lock.acquire_exclusive();
    bool ret = transport->cas((uint64_t)row, row_addr, *((uint64_t*)&row->lock), *((uint64_t*)&lock), sizeof(uint64_t), tid, pid);
    if(!ret)
	return ABORT;

    return RCOK;
}

void txn_man_t::unset_readlock(row_t* row, uint64_t row_addr, int tid, int pid){
    bool ret = false;
    while(!ret){
	auto lock = row->lock;
	lock.release_shared();
	ret = transport->cas((uint64_t)row, row_addr, *((uint64_t*)&row->lock), *((uint64_t*)&lock), sizeof(uint64_t), tid, pid);
    }
}

void txn_man_t::unset_writelock(row_t* row, uint64_t row_addr, int tid, int pid){
    row->lock.release_exclusive();
    transport->write((uint64_t)row, row_addr, sizeof(uint64_t), tid, pid);
}
*/
