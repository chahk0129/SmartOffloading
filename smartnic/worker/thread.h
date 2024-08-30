#pragma once

#include "common/global.h"

class worker_t;

class thread_t{
    public:
	int tid;
	worker_t* worker;

	int get_tid();
	void init(int tid, worker_t* worker);

	void run();

    private:
};
