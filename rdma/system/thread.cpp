#include "system/thread.h"
#include "system/query.h"
#include "system/txn.h"
#include "system/workload.h"
#include "benchmark/ycsb_query.h"
#include "benchmark/ycsb.h"
#include "common/stat.h"
#include <functional>
#include <random>

void thread_t::init(int tid, workload_t* workload){
    this->tid = tid;
    this->workload = workload;
}

int thread_t::get_tid(){
    return tid;
}

void thread_t::run(){
    bind_core(tid);

    RC rc = RCOK;
    // get txn man from workload
    txn_man_t* m_txn;
    rc = workload->get_txn_man(m_txn, this);
    assert(rc == RCOK);

    base_query_t* m_query = nullptr;
    uint64_t txn_cnt = 0;
    uint64_t abort_cnt = 0;
    uint64_t cur_abort_cnt = 0;
    uint64_t start, end;
    uint64_t txn_start_time = 0;

    while(true){
	uint64_t start_time = asm_rdtsc();
	if(rc == RCOK){
	    m_query = query_queue->get_next_query(tid);
	    m_query->timestamp = start_time;
	    txn_start_time = start_time;
	    cur_abort_cnt = 0;
	}
	else{ // backoff
	    cur_abort_cnt++;
	    uint64_t backoff = cur_abort_cnt * BACKOFF;
	    usleep(backoff / 1000);
	}

	rc = RCOK;
	rc = m_txn->run_txn(m_query);

	uint64_t end_time = asm_rdtsc();
	uint64_t time_span = end_time - start_time;
	ADD_STAT(tid, run_time, time_span);
	if(rc == ABORT){
	    abort_cnt++;
	}
	else if(rc == RCOK){
	    ADD_LATENCY(tid, latency, end_time-txn_start_time);
	    txn_cnt++;
	}

	if(rc == FINISH)
	    return;

	if(!warmup_finish && (txn_cnt >= WARMUP / g_run_parallelism)){
	    CLEAR_STAT(tid);
	    //stat->clear(tid);
	    return;
	}

	if(warmup_finish && (stat->_stats[tid]->run_time / 1000000000.0 >= MAX_RUNTIME)){
	    workload->sim_done.store(true);
	}

	if(workload->sim_done.load()){
	    ADD_STAT(tid, run_cnt, txn_cnt);
	    ADD_STAT(tid, abort_cnt, abort_cnt);
	    return;
	}
    }
}
    

