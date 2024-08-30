#include "storage/row.h"
#include "storage/table.h"
#include "storage/catalog.h"
#include "index/tree.h"
#include "common/rpc.h"
#include "common/stat.h"
#include "worker/txn.h"
#include "worker/ycsb.h"
#include "worker/mr.h"
#include "worker/transport.h"
#include "worker/worker.h"
#include "worker/page_table.h"
#include <unordered_map>

void ycsb_txn_man_t::init(worker_t* worker){
    txn_man_t::init(worker);
    this->worker = (ycsb_worker_t*)worker;
}

#if defined BATCH || defined BATCH2
//#ifdef BATCH
RC ycsb_txn_man_t::run_request(base_request_t* request, int tid){
    RC rc = RCOK;
    int qp_id = request->qp_id;

    auto send_ptr = mem->rpc_response_pool(tid);
    auto rpc_response = create_message<rpc_response_t>((void*)send_ptr, tid);

    int base_size = sizeof(int) * 2;
    int res_idx = 0;
    if(request->type == buffer_type_t::COMMIT_BUFFER){ // this request contains commit requests (writes)
	auto rpc_request = reinterpret_cast<rpc_commit_t*>(request);
	int num = rpc_request->num;
	for(int i=0; i<num; i++){
	    auto req = rpc_request->get_buffer(i);

	    rc = RCOK;
	    int client_id = req->tid;
	    //debug::notify_info("tid %d --- commit_w \tclient %d from qp %d", tid, client_id, qp_id);
	    rc = finish_with_write(req->data, qp_id, client_id, tid);

	    auto res = rpc_response->get_buffer(res_idx);
	    //res->update(client_id, rc, req->rid, req->time_id);
	    res->update(client_id, rc);
	    res_idx++;
	}
    }
    else{
	auto rpc_request = reinterpret_cast<rpc_request_t<Key>*>(request);
	int num = rpc_request->num;
	for(int i=0; i<num; i++){
	    auto req = rpc_request->get_buffer(i);

	    rc = RCOK;
	    int client_id = req->tid;
	    access_t type = req->type;
	    auto res = rpc_response->get_buffer(res_idx);

	    if(type == COMMIT){ // commit request for read-only transactions
		//debug::notify_info("tid %d --- commit_r \tclient %d from qp %d", tid, client_id, qp_id);
		rc = finish(rc, qp_id, client_id, tid);
		res->update(client_id, rc);
		//res->update(client_id, rc, req->rid, req->time_id);
		res_idx++;
		continue;
	    }

	    uint64_t timestamp = req->timestamp;
	    Key key = req->key;

	    uint32_t row_id = 0;
	    uint32_t page_id = 0;

	    //debug::notify_info("tid %d --- request  \tclient %d \tkey %lu \ttimestamp %lu from qp %d", tid, client_id, key, timestamp, qp_id);
	    int pid = worker->key_to_part(key);
	    bool exists = worker->index->search(key, row_id, page_id, tid);
	    if(!exists){ // such key does not exist in the db
		res->update(client_id, rc);
		//res->update(client_id, rc, req->rid, req->time_id);
		res_idx++;
		debug::notify_info("tid %d --- key %lu does not exist", tid, key);
		exit(0);
		continue;
	    }

	    #ifdef LOCKTABLE
	    table_entry_t* entry = nullptr;
	    //row_t* row = get_row(rc, row_id, qp_id, client_id, tid, pid, type, worker->tab, entry, timestamp, key, req->rid, req->time_id);
	    //row_t* row = get_row(rc, row_id, qp_id, client_id, tid, pid, type, worker->tab, entry, timestamp, key);
	    row_t* row = get_row(rc, row_id, qp_id, client_id, tid, pid, type, worker->tab, entry, timestamp);
    	    #else
	    uint64_t row_addr = worker->tab->get_addr(row_id);
	    bool is_remote = is_masked_addr(row_addr);
	    auto unmasked_addr = get_unmasked_addr(row_addr);
	    row_t* row = get_row(rc, unmasked_addr, qp_id, tid, pid, type, is_remote);
    	    #endif

	    // failed to acuiqre a lock due to conflict
	    if(row == nullptr){ // WAIT or ABORT
		if(rc == ABORT){
		    //rc = finish(rc, qp_id, client_id, tid);
		    res->update(client_id, rc);
		    //res->update(client_id, rc, req->rid, req->time_id);
		    res_idx++;
		}
//		else{ // WAIT
		    //res->set(); // mark local buffer to let client worker thread identify the request has been processed
//		    res->type = rc;
		    // waiting txns will be returned later
//		}
		continue;
	    }

    	    #ifdef LOCKTABLE
	    if(entry->is_remote()){
		transport->read((uint64_t)row, entry->remote_addr, sizeof(row_t), tid, pid);
	        #ifdef BUFFER
		if(test_probability()){ // migrate
		    row_t* new_row = (row_t*)malloc(sizeof(row_t));
		    memcpy(new_row, row, sizeof(row_t));
		    entry->set_local();
		    entry->update_timestamp(timestamp);
		    entry->local_addr = (uint64_t)new_row;
		    row = new_row;

		    worker->tab->replace(entry, this, tid);
		}
                #endif // end of BUFFER
	    }
            #else // ifndef LOCKTABLE
	    if(is_remote){
		transport->read((uint64_t)row, unmasked_addr, sizeof(row_t), tid, pid);
	    }
    	    #endif 

	    res->update(client_id, rc);
	    //res->update(client_id, RCOK);
	    //res->update(client_id, RCOK, req->rid, req->time_id);
	    //memcpy(response->data, row->get_data(), sizeof(row_t));
	    memcpy(res->data, row, sizeof(row_t));
	    res_idx++;

	}
    }

    rpc_response->num = res_idx;
    if(res_idx){
	//size_t response_size = sizeof(rpc_response_t);
	size_t response_size = base_size + sizeof(rpc_response_buffer_t) * res_idx;
	//transport->send_client((uint64_t)rpc_response, response_size, tid);
	transport->send_client((uint64_t)rpc_response, response_size, qp_id);
	/*
	for(int i=0; i<rpc_response->num; i++){
	    auto res = rpc_response->get_buffer(i);
	    debug::notify_info("tid %d --- response \tclient %d to qp %d", tid, res->tid, qp_id);
	}
	*/
    }
    return RCOK;
}

#else
RC ycsb_txn_man_t::run_request(base_request_t* _request, int tid){
    RC rc = RCOK;
    auto request = (rpc_request_t<Key>*)_request;
    int qp_id = request->qp_id;
    access_t type = request->type;
    uint64_t timestamp = request->timestamp;

    auto send_ptr = mem->rpc_response_buffer_pool(tid);
    rpc_response_t* response = create_message<rpc_response_t>(send_ptr, tid, rc);
    size_t response_size = sizeof(base_response_t);
    #ifdef BREAKDOWN
    if(t[qp_id].start == 0) // tx begins
        t[qp_id].start = asm_rdtsc();
    else{
	if(t[qp_id].end == 0){ // backoff end
	    t[qp_id].end = asm_rdtsc();
	    t[qp_id].backoff += (t[qp_id].end - t[qp_id].start);
	    t[qp_id].start = t[qp_id].start;
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
	    memset(&t[qp_id], 0, sizeof(breakdown_t));
            assert(t[qp_id].abort == 0);
        }
        #endif
	return rc;
    }

    assert(type == READ || type == SCAN || type == WRITE);

    //auto send_ptr = mem->rpc_response_buffer_pool(qp_id);
    uint32_t row_id = 0;
    uint32_t page_id = 0;
    Key key = request->key;
    int pid = worker->key_to_part(key);
    bool exists = worker->index->search(key, row_id, page_id, tid);
    if(!exists){ // such key does not exist in the db
	rc = ERROR;
	response->type = rc;
	transport->send_client((uint64_t)response, response_size, qp_id);
	debug::notify_info("qp %d --- ERROR by tid %d", qp_id, tid);
	return rc;
    }
    #ifdef BREAKDOWN
    t[qp_id].end = asm_rdtsc();
    t[qp_id].index += (t[qp_id].end - t[qp_id].start);
    t[qp_id].start = t[qp_id].end;
    #endif

    #ifdef LOCKTABLE
    table_entry_t* entry = nullptr;
    row_t* row = get_row(rc, row_id, qp_id, tid, pid, type, worker->tab, entry, timestamp);
    #else
    uint64_t row_addr = worker->tab->get_addr(row_id);
    bool is_remote = is_masked_addr(row_addr);
    auto unmasked_addr = get_unmasked_addr(row_addr);
    row_t* row = get_row(rc, unmasked_addr, qp_id, tid, pid, type, is_remote);
    #endif

    // failed to acuiqre lock due to conflict
    if(row == nullptr){ // WAIT or ABORT
	if(rc == ABORT){
	    //finish(rc, qp_id, tid);
	    response->type = rc;
	    transport->send_client((uint64_t)response, response_size, qp_id);
	    #ifdef BREAKDOWN
	    t[qp_id].end = asm_rdtsc();
	    t[qp_id].abort += (t[qp_id].end - t[qp_id].start);
	    t[qp_id].total += t[qp_id].abort + t[qp_id].index + t[qp_id].commit + t[qp_id].wait + t[qp_id].backoff;
	    ADD_STAT(qp_id, time_abort, t[qp_id].total);
	    memset(&t[qp_id], 0, sizeof(breakdown_t));
	    t[qp_id].start = asm_rdtsc(); // backoff start
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
	    row_t* new_row = (row_t*)malloc(sizeof(row_t));
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
	transport->read((uint64_t)row, unmasked_addr, ROW_SIZE, tid, pid);
    }
    #endif

    //memcpy(response->data, row->get_data(), sizeof(row_t));
    //memcpy(response->data, row->get_data(), ROW_SIZE);
    memcpy(response->data, row, sizeof(row_t));

    assert(rc == RCOK);
    response->type = rc;
    response_size += sizeof(row_t);
    transport->send_client((uint64_t)response, response_size, qp_id);
    #ifdef BREAKDOWN
    t[qp_id].end = asm_rdtsc();
    t[qp_id].commit += (t[qp_id].end - t[qp_id].start);
    t[qp_id].start = t[qp_id].end;
    #endif

    return rc;
}
#endif
