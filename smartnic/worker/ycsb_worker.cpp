#include "common/global.h"
#include "common/hash.h"
#include "worker/ycsb.h"
#include "storage/catalog.h"
#include "storage/table.h"
#include "storage/row.h"
#include "index/tree.h"
#include "worker/transport.h"
#include "worker/mr.h"
#include "worker/thread.h"
#include "worker/txn.h"
#include "index/indirection.h"
#include "worker/page_table.h"

#include <vector>
#include <thread>
#include <iostream>

RC ycsb_worker_t::init(config_t* conf){
    debug::notify_info("Initializing YCSB worker");
    worker_t::init(conf);
    std::string path = "../benchmark/YCSB_schema.txt";
    init_schema(path);
    #ifdef LOCKTABLE 
    tab = new page_table_t();
    #else
    tab = new indirection_table_t();
    #endif
    free_pages.store(DEFAULT_BUFFER_SIZE);
    //free_pages.store(YCSB_PAGE_NUM/2);

    init_table();
    return RCOK;
}

RC ycsb_worker_t::init_schema(std::string path){
    worker_t::init_schema(path);
    table = tables["MAIN_TABLE"];
    index = indexes["MAIN_INDEX"];
    return RCOK;
}

RC ycsb_worker_t::init_table(){
    std::vector<std::thread> thd;
    for(int i=0; i<WORKER_THREAD_NUM; i++)
	thd.push_back(std::thread(thread_init_table, this, i));
    for(auto& t: thd) t.join();
    return RCOK;
}

int ycsb_worker_t::key_to_part(uint64_t key){
    return h(&key, sizeof(key), HASH_FUNC) % MR_PARTITION_NUM;
}

RC ycsb_worker_t::init_table_parallel(int tid){
    bind_core_worker(tid);

    uint64_t chunk = g_synth_table_size / WORKER_THREAD_NUM;
    uint64_t from = chunk * tid + 1;
    uint64_t to = chunk * (tid + 1) + 1;
    if(tid == WORKER_THREAD_NUM-1)
	to = g_synth_table_size + 1;

    for(uint64_t key=from; key<to; key++){
	uint32_t row_id = tab->get_next_id();
	int pid = key_to_part(key);
	row_t* new_row;
	bool alloc_local = free_pages.load() > 0 ? true : false;
	uint64_t local_addr = 0;
	uint64_t remote_addr = rpc_alloc(tid, pid);
	if(alloc_local){ // local alloc
	    free_pages.fetch_sub(1);
	    new_row = (row_t*)malloc(ROW_SIZE);
	    //new_row = (row_t*)malloc(sizeof(row_t));
	    local_addr = (uint64_t)new_row;
	}
	else{ // remote alloc
	//    row_addr = rpc_alloc(tid, pid);
	    new_row = mem->row_buffer_pool(tid);
	}

	memset(new_row, 0, ROW_SIZE);
	new_row->init(table, pid, row_id);
	auto schema = table->get_schema();
	new_row->set_primary_key(key);
	new_row->set_value(schema, 0, &key);

	int field_cnt = schema->get_field_cnt();
	for(int fid=0; fid<field_cnt; fid++){
	    char value[6] = "hello";
	    new_row->set_value(schema, fid, value);
	}

	if(!alloc_local){
	    transport->write((uint64_t)new_row, remote_addr, ROW_SIZE, tid, pid);
	    //transport->write((uint64_t)new_row, row_addr, ROW_SIZE, tid, pid);
	    #ifndef LOCKTABLE
	    remote_addr = set_masked_addr(remote_addr);
	    #endif
	}

	#ifdef LOCKTABLE
	tab->set(row_id, local_addr, remote_addr, pid, !alloc_local);
	#else
	if(alloc_local)
	    tab->set(row_id, local_addr);
	else
	    tab->set(row_id, remote_addr);
	tab->set(row_id, remote_addr);
	#endif
	uint64_t idx_key = key;
	uint32_t idx_value = row_id;

	index->insert(idx_key, idx_value, tid);
    }
    return RCOK;
}

RC ycsb_worker_t::get_txn_man(txn_man_t*& txn_man, thread_t* thd){
    txn_man = global_ycsb_txn_man;
    
    //txn_man = new ycsb_txn_man_t;
    //txn_man->init(thd, this, thd->get_tid());
    return RCOK;
}
