#pragma once
#include "common/global.h"
#include <random>

class base_query_t;
class worker_t;

class thread_t{
    public:
	int tid;
	worker_t* worker;

	int get_tid();

	void init(int tid, worker_t* worker);

	#if defined BATCH || defined BATCH2
	int get_network_tid();
	void run_network();
	#endif
	void run();

    private:
};
