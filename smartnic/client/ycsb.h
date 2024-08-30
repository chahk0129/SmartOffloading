#pragma once
#include "common/global.h"
#include "client/worker.h"
#include "client/txn.h"

class config_t;
class base_query_t;
class thread_t;
class table_t;
class txn_man_t;

class ycsb_worker_t: public worker_t{
    public:
	RC init(config_t* conf);
	RC init_schema(std::string path);

	RC get_txn_man(txn_man_t*& txn_man, thread_t* thd);

	table_t* table;
};

class ycsb_request_t;

class ycsb_txn_man_t: public txn_man_t{
    public:
	void init(thread_t* thd, worker_t* worker, int tid);
	#ifdef BATCH
	void run_network_thread();
	#elif defined BATCH2
	void run_send(int tid);
	void run_recv(int tid);
	void run_network_thread();
	#endif
	RC run_txn(base_query_t* base_query);

    private:
	ycsb_worker_t* worker;
	uint64_t row_cnt;
};

