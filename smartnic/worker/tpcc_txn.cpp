#include "storage/row.h"
#include "storage/table.h"
#include "storage/catalog.h"
#include "index/tree.h"
#include "common/rpc.h"
#include "common/stat.h"
#include "worker/txn.h"
#include "worker/tpcc.h"
#include "worker/mr.h"
#include "worker/transport.h"
#include "worker/worker.h"
#include "worker/page_table.h"
#include <unordered_map>

void tpcc_txn_man_t::init(worker_t* worker){
    txn_man_t::init(worker);
    this->worker = (tpcc_worker_t*)worker;
}

RC tpcc_txn_man_t::run_request(base_request_t* _request, int tid){
#if defined BATCH || defined BATCH2
    return RCOK;
#else
    RC rc = RCOK;
    auto request = (rpc_request_t<Key>*)_request;
    int qp_id = request->qp_id;
    access_t type = request->type;
    uint64_t timestamp = request->timestamp;
    
    auto send_ptr = mem->rpc_response_buffer_pool(tid);
    auto response = create_message<rpc_response_t>(send_ptr, tid, rc);
    size_t response_size = sizeof(base_response_t);
    #ifdef BREAKDOWN
    if(t[qp_id].start == 0) // tx begin
	t[qp_id].start = asm_rdtsc();
    else{
	if(t[qp_id].end == 0){ // backoff end
	    t[qp_id].end = asm_rdtsc();
	    t[qp_id].backoff += (t[qp_id].end - t[qp_id].start);
	    t[qp_id].start = t[qp_id].end;
	}
    }
    #endif

    if(type == COMMIT_DATA){
	rc = finish_with_write(request->data, request->num, qp_id, tid);
	response->type = rc;
	transport->send_client((uint64_t)response, response_size, qp_id);
	#ifdef BREAKDOWN
        t[qp_id].end = asm_rdtsc();
        if(rc == ABORT){
            t[qp_id].abort += (t[qp_id].end - t[qp_id].start);
            t[qp_id].total += t[qp_id].abort + t[qp_id].index + t[qp_id].commit + t[qp_id].wait + t[qp_id].backoff;
            ADD_STAT(qp_id, time_abort, t[qp_id].total);
	    memset(&t[qp_id], 0, sizeof(breakdown_t));
	    t[qp_id].start = asm_rdtsc(); // backoff start
        }
        else{
            assert(rc == RCOK);
            t[qp_id].commit += (t[qp_id].end - t[qp_id].start);
            ADD_STAT(qp_id, time_commit, t[qp_id].commit);
            ADD_STAT(qp_id, time_index, t[qp_id].index);
            ADD_STAT(qp_id, time_wait, t[qp_id].wait);
	    ADD_STAT(qp_id, time_backoff, t[qp_id].backoff);
            assert(t[qp_id].abort == 0);
	    memset(&t[qp_id], 0, sizeof(breakdown_t));
        }
        #endif
	return rc;
    }
    else if(type == COMMIT){
	rc = finish(rc, qp_id, tid);
	response->type = rc;
	transport->send_client((uint64_t)response, response_size, qp_id);
	#ifdef BREAKDOWN
        t[qp_id].end = asm_rdtsc();
        if(rc == ABORT){
            t[qp_id].abort += (t[qp_id].end - t[qp_id].start);
            t[qp_id].total += t[qp_id].abort + t[qp_id].index + t[qp_id].commit + t[qp_id].wait + t[qp_id].backoff;
            ADD_STAT(qp_id, time_abort, t[qp_id].total);
	    memset(&t[qp_id], 0, sizeof(breakdown_t));
	    t[qp_id].start = asm_rdtsc(); // backoff start
        }
        else{
            assert(rc == RCOK);
            t[qp_id].commit += (t[qp_id].end - t[qp_id].start);
            ADD_STAT(qp_id, time_commit, t[qp_id].commit);
            ADD_STAT(qp_id, time_index, t[qp_id].index);
            ADD_STAT(qp_id, time_wait, t[qp_id].wait);
	    ADD_STAT(qp_id, time_backoff, t[qp_id].backoff);
            assert(t[qp_id].abort == 0);
	    memset(&t[qp_id], 0, sizeof(breakdown_t));
        }
        #endif
	return rc;
    }

    assert(type == READ || type == SCAN || type == WRITE);

    tree_t<Key, Value>* idx = nullptr;
    auto tpcc_type = request->tpcc_type;
    if(tpcc_type == TPCC_WAREHOUSE)
	idx = worker->i_warehouse;
    else if(tpcc_type == TPCC_DISTRICT)
	idx = worker->i_district;
    else if(tpcc_type == TPCC_CUSTOMER_LASTNAME)
	idx = worker->i_customer_last;
    else if(tpcc_type == TPCC_CUSTOMER_ID)
	idx = worker->i_customer_id;
    else if(tpcc_type == TPCC_ITEM)
	idx = worker->i_item;
    else if(tpcc_type == TPCC_STOCK)
	idx = worker->i_stock;
    else{
	debug::notify_error("Not supported tpcc txn type: %d ... Implement me!", tpcc_type);
	exit(0);
    }

    uint32_t row_id = 0;
    uint32_t page_id = 0;
    Key key = request->key;
    int pid = request->pid;

    bool exists = idx->search(key, row_id, page_id, tid);
    if(!exists){ // key does not exist in the db
	rc = ERROR;
	response->type = rc;
	transport->send_client((uint64_t)response, response_size, qp_id);
	debug::notify_info("qp %d --- ERROR (tid %d)", qp_id, tid);
	return rc;
    }

    #ifdef BREAKDOWN
    t[qp_id].end = asm_rdtsc();
    t[qp_id].index += (t[qp_id].end - t[qp_id].start);
    t[qp_id].start = t[qp_id].end;
    #endif

    #ifdef LOCKTABLE
    table_entry_t* entry = nullptr;
    auto row = get_row(rc, row_id, qp_id, tid, pid, type, worker->tab, entry, timestamp);
    #else
    uint64_t row_addr = worker->tab->get_addr(row_id);
    bool is_remote = is_masked_addr(row_addr);
    auto unmasked_addr = get_unmasked_addr(row_addr);
    auto row = get_row(rc, unmasked_addr, qp_id, tid, pid, type, is_remote);
    #endif

    // failed to acquire lock due to conflict
    if(row == nullptr){ // WAIT or ABORT
	if(rc == ABORT){
	    response->type = rc;
	    transport->send_client((uint64_t)response, response_size, qp_id);
	    #ifdef BREAKDOWN
            t[qp_id].end = asm_rdtsc();
            t[qp_id].abort += (t[qp_id].end - t[qp_id].start);
            t[qp_id].total += t[qp_id].abort + t[qp_id].index + t[qp_id].commit + t[qp_id].wait + t[qp_id].backoff;
            ADD_STAT(qp_id, time_abort, t[qp_id].total);
            memset(&t[qp_id], 0, sizeof(breakdown_t));
            t[qp_id].start = asm_rdtsc();
            #endif
	}
	#ifdef BREAKDOWN
        else{
            t[qp_id].end = asm_rdtsc();
            t[qp_id].commit += (t[qp_id].end - t[qp_id].start);
            t[qp_id].start = t[qp_id].end;
        }
        #endif
	return rc;
    }

    #ifdef LOCKTABLE
    if(entry->is_remote()){
	transport->read((uint64_t)row, entry->remote_addr, sizeof(row_t), tid, pid);
	#ifdef BUFFER
	if(test_probability()){ // migrate
	    auto new_row = (row_t*)malloc(sizeof(row_t));
	    memcpy(new_row, row, sizeof(row_t));
	    entry->update_timestamp(timestamp);
	    entry->local_addr = (uint64_t)new_row;
	    entry->set_local();
	    row = new_row;

	    worker->tab->replace(entry, this, tid);
	}
	#endif
    }
    #else
    if(is_remote){
	transport->read((uint64_t)row, unmasked_addr, sizeof(row_t), tid, pid);
    }
    #endif

    memcpy(response->data, row, sizeof(row_t));

    assert(rc == RCOK);
    response->type = RCOK;
    response_size += sizeof(row_t);
    transport->send_client((uint64_t)response, response_size, qp_id);
    #ifdef BREAKDOWN
    t[qp_id].end = asm_rdtsc();
    t[qp_id].commit += (t[qp_id].end - t[qp_id].start);
    t[qp_id].start = t[qp_id].end;
    #endif
    return rc;
    return RCOK;
#endif
}

