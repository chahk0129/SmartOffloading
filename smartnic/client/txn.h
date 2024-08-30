#pragma once
#include "common/global.h"

class worker_t;
class thread_t;
class table_t;
class base_query_t;
class row_t;
class client_mr_t;
class client_transport_t;

class txn_man_t{
    public:
	thread_t* thread;
	worker_t* worker;

	client_mr_t* mem;
	client_transport_t* transport;

	int write_num;
	int write_buf[MAX_ROW_PER_TXN];

	// main functions
	virtual void init(thread_t* thread, worker_t* worker, int tid);
	void release();
	#if defined BATCH || defined BATCH2
	virtual void run_network_thread() = 0;
	virtual RC run_txn(base_query_t* query) = 0;
	#else
	virtual RC run_txn(base_query_t* query) = 0;
	#endif

	int get_tid();
	int get_network_tid();
};

