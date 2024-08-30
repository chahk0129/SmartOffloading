#pragma once 

#include "common/global.h"
#include "system/workload.h"
#include "system/txn.h"

class base_query_t;
class table_t;
class row_t;
class config_t;

class ycsb_workload_t: public workload_t{
    public:
	RC init(config_t* conf);
	RC init_table();
	RC init_table_parallel(int tid);
	RC init_schema(std::string path);
	RC get_txn_man(txn_man_t*& txn_man, thread_t* thd);

	table_t* table;
	tree_t<Key, Value>* index;

	int key_to_part(Key key);

    private:
	static void thread_init_table(void* This, int tid){
	    ((ycsb_workload_t*)This)->init_table_parallel(tid);
	}


};

class ycsb_request_t;

class ycsb_txn_man_t: public txn_man_t{
    public:
	void init(thread_t* thd, workload_t* workload, int tid);
	RC run_txn(base_query_t* base_query);

    private:
	RC read(ycsb_request_t* request, int tid);
	RC upsert(ycsb_request_t* request, int tid);
	RC insert(ycsb_request_t* request, int tid);
	RC scan(ycsb_request_t* request, int tid);

	ycsb_workload_t* workload;
	uint64_t row_cnt;
};
