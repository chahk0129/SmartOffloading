#include "common/global.h"
#include "common/hash.h"
#include "benchmark/ycsb.h"
#include "system/workload.h"
#include "system/thread.h"
#include "system/txn.h"
#include "storage/catalog.h"
#include "storage/table.h"
#include "storage/row.h"
#include "index/tree.h"
#include "client/transport.h"
#include "client/mr.h"

#include <vector>
#include <thread>
#include <iostream>

RC ycsb_workload_t::init(config_t* conf){
    workload_t::init(conf);
    std::string path = "../benchmark/YCSB_schema.txt";
    init_schema(path);
    init_table();
    return RCOK;
}

RC ycsb_workload_t::init_schema(std::string path){
    workload_t::init_schema(path);
    table = tables["MAIN_TABLE"];
    index = indexes["MAIN_INDEX"];
    return RCOK;
}

RC ycsb_workload_t::init_table(){
    std::vector<std::thread> thd;
    for(int i=0; i<g_init_parallelism; i++)
	thd.push_back(std::thread(thread_init_table, this, i));
    for(auto& t: thd) t.join();
    return RCOK;
}

RC ycsb_workload_t::init_table_parallel(int tid){
    bind_core(tid);

    uint64_t chunk = g_synth_table_size / g_init_parallelism;
    uint64_t from = chunk * tid + 1;
    uint64_t to = chunk * (tid + 1) + 1;
    if(tid == g_init_parallelism-1)
	to = g_synth_table_size + 1;

    for(uint64_t key=from; key<to; key++){
	uint64_t row_id = asm_rdtsc();
	int pid = key_to_part(key);
	auto send_ptr = mem->request_buffer_pool(tid);
	auto request = create_message<request_t>((void*)send_ptr, tid, pid, request_type::TABLE_ALLOC_ROW);
	transport->send((uint64_t)request, sizeof(request_t), tid);

	auto recv_ptr = mem->response_buffer_pool(tid);
	auto response = create_message<response_t>((void*)recv_ptr);
	transport->recv((uint64_t)response, sizeof(response_t), tid);
	if(response->type != response_type::SUCCESS)
	    debug::notify_error("Memory allocation for new row failed (%d)", response->type);
	uint64_t row_addr = response->addr;
	uint64_t primary_key = key;
	auto schema = table->get_schema();
	auto new_row = mem->row_buffer_pool(tid, 0);
	memset(new_row, 0, ROW_SIZE);
	new_row->init(table, pid, row_id);
	new_row->set_primary_key(primary_key);
	new_row->set_value(schema, 0, &primary_key);

	int field_cnt = schema->get_field_cnt();
	for(int fid=0; fid<field_cnt; fid++){
	    char value[6] = "hello";
	    new_row->set_value(schema, fid, value);
	}

	transport->write((uint64_t)new_row, row_addr, ROW_SIZE, tid, pid);
	uint64_t idx_key = primary_key;
	uint64_t idx_value = row_addr;

	index->insert(idx_key, idx_value, tid);
    }
    return RCOK;
}

RC ycsb_workload_t::get_txn_man(txn_man_t*& txn_man, thread_t* thd){
    txn_man = new ycsb_txn_man_t;
    txn_man->init(thd, this, thd->get_tid());
    return RCOK;
}

int ycsb_workload_t::key_to_part(Key key){
    return h(&key, sizeof(key), HASH_FUNC) % MR_PARTITION_NUM;
}
