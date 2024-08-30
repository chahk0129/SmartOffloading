#pragma once
#include "common/global.h"
#include <random>

class ycsb_txn_man_t;
class base_query_t;
class workload_t;

class thread_t{
    public:
	int tid;
	workload_t* workload;

	int get_tid();
	void init(int tid, workload_t* workload);

	void run();

    private:
};
