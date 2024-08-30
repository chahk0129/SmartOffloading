#pragma once
#include "common/global.h"

class ycsb_query_t;
class ycsb_request_t;
class tpcc_query_t;
class worker_t;

class base_query_t{
    public:
	virtual void init(int tid) = 0;
	uint64_t timestamp;
	uint64_t part_num;
	uint64_t* part_to_access;
	bool rerun;
};

// queries for each thread
class query_thread_t{
    public:
	void init(int tid);
	base_query_t* get_next_query();
	uint64_t q_idx;
#if WORKLOAD == YCSB 
	ycsb_query_t* queries;
#else 
	tpcc_query_t* queries;
#endif
	uint64_t query_cnt;
};

// task for each thread to avoid contention in a centralized query queue
class query_queue_t{
    public:
	void init(worker_t* worker);
	void init_per_thread(int tid);
	base_query_t* get_next_query(int tid);

    private:
	static void thread_init_query(void* This, int tid);

	query_thread_t** all_queries;
	worker_t* worker;
	int tid;
};


