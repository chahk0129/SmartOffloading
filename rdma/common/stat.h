#pragma once

#include <vector>
#include "common/global.h"

class stat_thread_t{
    public:
	stat_thread_t();
	void add_latency(uint64_t latency);
	void clear();

	// compute stats
	void summary(uint64_t& run_cnt, uint64_t& run_time, uint64_t& abort_cnt, uint64_t& time_commit, uint64_t& time_abort, uint64_t& time_backoff, uint64_t& time_index, uint64_t& time_wait, std::vector<uint64_t>& latency);


	uint64_t run_cnt;
	uint64_t run_time;
	uint64_t abort_cnt;

	uint64_t time_index;
	uint64_t time_abort;
	uint64_t time_commit;
	uint64_t time_wait;
	uint64_t time_backoff;

	std::vector<uint64_t> latency;
};

class stat_t{
    public:
	stat_t();
	void clear(int tid);
	void summary();

	stat_thread_t** _stats;
};

#define ADD_STAT(tid, name, value) \
    if (STATS_ENABLE) \
    	stat->_stats[tid]->name += value;

#define SUB_STAT(tid, name, value) \
    if (STATS_ENABLE) \
    	stat->_stats[tid]->name -= value;

#define ADD_LATENCY(tid, name, value) \
    if (STATS_ENABLE) \
    	stat->_stats[tid]->name.push_back(value);
//    	stat->_stats[tid]->add_latency(value);

#define CLEAR_STAT(tid) \
    if (STATS_ENABLE) \
    	stat->_stats[tid]->clear();
