#include <sched.h>
#include "system/query.h"
#include "system/workload.h"
#include "benchmark/ycsb_query.h"
#include "benchmark/tpcc_query.h"

#include <vector>
#include <thread>

// class query_queue_t
void query_queue_t::init(workload_t* workload){
    all_queries = new query_thread_t*[g_run_parallelism];
    this->workload = workload;

    std::vector<std::thread> thds;
    for(int i=0; i<g_run_parallelism; i++)
	thds.push_back(std::thread(thread_init_query, this, i));
    for(auto& t: thds) t.join();
}
		
void query_queue_t::init_per_thread(int tid){
    all_queries[tid] = new query_thread_t;
    all_queries[tid]->init(tid);
}

base_query_t* query_queue_t::get_next_query(int tid){
    return all_queries[tid]->get_next_query();
}

void query_queue_t::thread_init_query(void* This, int tid){
    query_queue_t* query_queue = (query_queue_t*)This;
    bind_core(tid);

    query_queue->init_per_thread(tid);
}

// class query_thread_t
void query_thread_t::init(int tid){
    q_idx = 0;
    query_cnt = MAX_TRANSACTION / g_run_parallelism + 100;
    //query_cnt = WARMUP / g_run_parallelism + MAX_TRANSACTION;

#if WORKLOAD == YCSB
    queries = new ycsb_query_t[query_cnt];
    for(uint32_t qid=0; qid<query_cnt; qid++){
	new(&queries[qid]) ycsb_query_t();
	queries[qid].init(tid);
    }
#elif WORKLOAD == TPCC
    queries = new tpcc_query_t[query_cnt];
    for(uint32_t qid=0; qid<query_cnt; qid++){
	new(&queries[qid]) tpcc_query_t();
	queries[qid].init(tid);
    }
#else
    debug::notify_error("Unknown workload type %d... Implement me!", WORKLOAD);
    assert(false);
#endif
}

base_query_t* query_thread_t::get_next_query(){
    if(q_idx >= query_cnt-1)
	q_idx = 0;

    return &queries[q_idx++];
}

