#include "worker/thread.h"
#include "worker/mr.h"
#include "worker/transport.h"
#include "worker/txn.h"
#include "worker/ycsb.h"
#include "common/stat.h"

void thread_t::init(int tid, worker_t* worker){
    this->tid = tid;
    this->worker = worker;

}

int thread_t::get_tid(){
    return tid;
}

void thread_t::run(){
    bind_core_worker(tid);

    RC rc = RCOK;
    // get txn man from worker
    txn_man_t* m_txn;
    rc = worker->get_txn_man(m_txn, this);
    assert(rc == RCOK);

    //int batch_size = 32;
    #if defined BATCH || defined BATCH2
    //int batch_size = 4;
    int batch_size = 1;
    #else
    int batch_size = 8;
    #endif
    struct ibv_wc wc[batch_size];
    #ifdef BREAKDOWN
    uint64_t _start = asm_rdtsc();
    uint64_t _end;
    #endif
    
    while(true){
	int cnt = worker->transport->poll(wc, batch_size);

	if(cnt > batch_size){
	    debug::notify_error("recv size error");
	    exit(0);
	}
	for(int i=0; i<cnt; i++){
	    #ifdef BATCH
	    auto qp_id = wc[i].wr_id;
	    auto request = worker->mem->rpc_request_pool(qp_id);
	    m_txn->run_request(request, tid);
	    memset(request, 0, sizeof(rpc_commit_t));
	    worker->transport->prepost_recv_client((uint64_t)request, sizeof(rpc_commit_t), qp_id);
	    #elif defined BATCH2
	    auto qp_id = wc[i].wr_id;
	    auto request = worker->mem->rpc_request_pool(qp_id);
	    m_txn->run_request(request, tid);
	    memset(request, 0, sizeof(rpc_commit_t));
	    worker->transport->prepost_recv_client((uint64_t)request, sizeof(rpc_commit_t), qp_id);
	    #else
	    auto qp_id = wc[i].wr_id;
	    auto request = worker->mem->rpc_request_buffer_pool(qp_id);
	    m_txn->run_request((base_request_t*)request, tid);
	    worker->transport->prepost_recv_client((uint64_t)request, sizeof(rpc_request_t<Key>), qp_id);
	    #endif
	}

	#ifdef BREAKDOWN
	if(tid == 0){
	    _end = asm_rdtsc();
	    if((_end - _start) >= 10000000000){ // print every 10 sec
		stat->summary();
		stat->clear();
		_start = _end;
	    }
	}
	#endif
    }
}
