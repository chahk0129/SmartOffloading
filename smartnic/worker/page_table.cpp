#include "common/huge_page.h"
#include "storage/row.h"
#include "worker/page_table.h"
#include "worker/transport.h"
#include "worker/mr.h"
#include "worker/txn.h"
#include "concurrency/nowait.h"
#include "concurrency/waitdie.h"
#include "concurrency/woundwait.h"
#include "common/helper.h"
#include "common/debug.h"

#include <cassert>

// local
table_entry_t::table_entry_t(uint32_t id, uint64_t local_addr, uint64_t remote_addr, int pid): state(false), id(id), local_addr(local_addr), remote_addr(remote_addr), pid(pid), timestamp(0), next(nullptr){
    #ifdef WOUNDWAIT
    manager = new woundwait_t();
    #elif defined WAITDIE
    manager = new waitdie_t();
    #elif defined NOWAIT
    manager = new nowait_t();
    #endif
}

// remote
table_entry_t::table_entry_t(uint32_t id, uint64_t local_addr, uint64_t remote_addr, int pid, bool flag): state(true), id(id), local_addr(local_addr), remote_addr(remote_addr), pid(pid), timestamp(0), next(nullptr){
    #ifdef WOUNDWAIT
    manager = new woundwait_t();
    #elif defined WAITDIE
    manager = new waitdie_t();
    #elif defined NOWAIT
    manager = new nowait_t();
    #endif
}


// table entry
RC table_entry_t::lock(lock_type_t type, txn_man_t* txn, Access* access, int tid){
    #if defined NOWAIT || defined WAITDIE || defined WOUNDWAIT
    return manager->lock_get(type, txn, access, tid);
    #else
    return manager.lock_get(type);
    #endif
}

// lock for DPU to host migration
RC table_entry_t::lock(int tid){
    #if defined NOWAIT || defined WAITDIE || defined WOUNDWAIT
    return manager->lock_get(tid);
    #else
    return manager.lock_get();
    #endif
}


void table_entry_t::release(access_t type, txn_man_t* txn, int client_id, int tid, uint64_t timestamp){
    #if defined NOWAIT || defined WAITDIE || defined WOUNDWAIT
    if(!is_remote())
	update_timestamp(timestamp);
    manager->lock_release(txn, client_id, tid);
    #else // ROW_LOCK
    if(type == READ || type == SCAN){
	if(!is_remote())
	    update_timestamp(timestamp);
	manager.lock_release(LOCK_SH);
    }
    else{
	if(!is_remote() && type != XP)
	    update_timestamp(timestamp);
	manager.lock_release(LOCK_EX);
    }
    #endif
}

// unlock for DPU to host migration
void table_entry_t::release(txn_man_t* txn, int client_id, int tid){
    #if defined NOWAIT || defined WAITDIE || defined WOUNDWAIT
    manager->lock_release(txn, client_id, tid);
    #else // ROW_LOCK
    manager.lock_release();
    #endif
}


// page table
page_table_t::page_table_t(): size(DEFAULT_BUFFER_SIZE), next_id(1){
    size_t tab_size = sizeof(uint64_t) * size;
    auto addr = huge_page_alloc(tab_size);
    //memset(addr, 0, tab_size);

    tab = reinterpret_cast<std::atomic<table_entry_t*>*>(addr);
    for(int i=0; i<size; i++)
	tab[i].store(nullptr);
}

page_table_t::page_table_t(size_t size): size(size), next_id(1){
    size_t tab_size = sizeof(uint64_t) * size;
    auto addr = huge_page_alloc(tab_size);
    //memset(addr, 0, tab_size);

    tab = reinterpret_cast<std::atomic<table_entry_t*>*>(addr);
    for(int i=0; i<size; i++)
	tab[i].store(nullptr);
}

void page_table_t::set(uint32_t id, uint64_t local_addr, uint64_t remote_addr, int pid, bool is_remote){
    table_entry_t* entry;
    if(is_remote)
	entry = new table_entry_t(id, local_addr, remote_addr, pid, true);
    else
	entry = new table_entry_t(id, local_addr, remote_addr, pid);

    int idx = id % size;
RETRY:
    auto cur = tab[idx].load();
    table_entry_t* prev = nullptr;
    while(cur){
	prev = cur;
	cur = cur->next.load();
    }

    if(!prev){ // this entry is empty 
	if(!tab[idx].compare_exchange_strong(cur, entry))
	    goto RETRY;
    }
    else{ // append
	if(!prev->next.compare_exchange_strong(cur, entry))
	    goto RETRY;
    }
}
	
RC page_table_t::get(table_entry_t*& entry, uint32_t id, access_t type, txn_man_t* txn, Access* access, int tid){
    int idx = id % size;
    auto cur = tab[idx].load();
    while(cur){
	if(cur->id == id){ // found
	    entry = cur;
	    access->entry = cur;
	    if(type == READ || type == SCAN){ // shared
		return cur->lock(LOCK_SH, txn, access, tid);
	    }
	    else{ // exclusive
		return cur->lock(LOCK_EX, txn, access, tid); 
	    }
	}
	cur = cur->next.load();
    }
    return ABORT;
}

uint64_t page_table_t::rpc_alloc(worker_transport_t* transport, worker_mr_t* mem, int tid, int pid){
    auto send_ptr = mem->request_buffer_pool(tid);
    auto request = create_message<request_t>((void*)send_ptr, tid, pid, request_type::TABLE_ALLOC_ROW);
    transport->send((uint64_t)request, sizeof(request_t), tid);

    auto recv_ptr = mem->response_buffer_pool(tid);
    auto response = create_message<response_t>((void*)recv_ptr);
    transport->recv((uint64_t)response, sizeof(response_t), tid);
    if(response->type != response_type::SUCCESS)
        debug::notify_error("Memory allocation for new row failed (%d)", response->type);
    return response->addr;
}
	
void page_table_t::replace(table_entry_t*& entry, txn_man_t* txn, int tid){
    table_entry_t* victim[VICTIM_SIZE];
    int victim_idx = 0;
    uint32_t id = entry->id;
    uint64_t tab_size = size;
    //uint64_t tab_size = next_id.load();

    //debug::notify_error("REPLACE");
    //exit(0);
    while(true){
	auto idx = rand() % tab_size;
	auto cur = tab[idx].load();
	while(cur){
	    if(cur->id != id && !cur->is_remote()){ // find non-matching entry
		/*
		// if failed to lock, try next entry
		if(cur->lock(tid) == ABORT){
		    cur = cur->next.load();
		    continue;
		}
		*/

		// ensure this is local
		if(cur->is_remote()){
		    //cur->release(txn, tid, tid);
		    cur = cur->next.load();
		    continue;
		}

		victim[victim_idx] = cur;
		victim_idx++;
		if(victim_idx == VICTIM_SIZE){
		    int target_idx = -1;
		    uint64_t target_ts = UINT64_MAX;
		    for(int i=0; i<victim_idx; i++){
			auto victim_ts = victim[i]->timestamp.load();
			if(victim_ts < target_ts){
			    target_ts = victim_ts;
			    target_idx = i;
			}
		    }

		    for(int i=0; i<victim_idx; i++){
			if(i == target_idx)
			    continue;
			// release locks for other entries as soon as possible 
			//victim[i]->release(txn, tid, tid);
		    }

		    auto target = victim[target_idx];
		    auto data = txn->mem->row_buffer_pool(tid);
		    memcpy(data, (void*)target->local_addr, ROW_SIZE);

		    uint64_t remote_addr;
		    if(target->remote_addr == 0){ // rpc alloc
			remote_addr = rpc_alloc(txn->transport, txn->mem, tid, target->pid);
		    }
		    else{ // reuse preallocated remote addr
			remote_addr = target->remote_addr;
		    }

		    txn->transport->write((uint64_t)data, remote_addr, ROW_SIZE, tid, target->pid);
		    target->remote_addr = remote_addr;
		    target->set_remote();
		    //target->release(txn, tid, tid);
		    //debug::notify_info("REPLACE \t\tby tid %d", tid);
		    return;
		}
	    }
	    cur = cur->next.load();
	}
    }
}
