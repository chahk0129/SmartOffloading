#include "storage/row.h"
#include "storage/catalog.h"
#include "benchmark/ycsb.h"
#include "benchmark/ycsb_query.h"
#include "system/workload.h"
#include "system/thread.h"
#include "system/query.h"
#include "storage/table.h"
#include "storage/catalog.h"
#include "index/tree.h"
//#include "concurrency/row_lock.h"
#include "system/txn.h"
#include "common/rpc.h"
#include "common/stat.h"
#include "client/mr.h"
#include "client/transport.h"


void ycsb_txn_man_t::init(thread_t* thd, workload_t* workload, int tid){
    txn_man_t::init(thd, workload, tid);
    this->workload = (ycsb_workload_t*)workload;
    #ifdef BREAKDOWN
    t_index = t_commit = t_abort = t_wait = t_backoff = t_total = 0;
    #endif
}

RC ycsb_txn_man_t::run_txn(base_query_t* query){
    RC rc = RCOK;
    auto m_query = (ycsb_query_t*)query;
    auto m_wl = workload;
    int tid = thread->get_tid();
    set_ts(m_query->timestamp);
    uint64_t row_addr = 0;
    uint64_t page_addr = 0;
    #ifdef BREAKDOWN
    if(start == 0){ // tx begins
        start = asm_rdtsc();
    }
    else{
	assert(end == 0); // backoff end
	end = asm_rdtsc();
	t_backoff += (end - start);
	start = end;
    }
    #endif

    #ifdef INTERACTIVE
    int base = 100;
    int delay = 100;
    int interactive_delay;
    interactive_delay = (rand() % delay) + base;
    #endif

    for(uint32_t rid=0; rid<m_query->request_cnt; rid++){
	#ifdef INTERACTIVE
        usleep(interactive_delay);
        #endif

	ycsb_request_t* req = &m_query->requests[rid];
	int pid = m_wl->key_to_part(req->key);
	bool finish_req = false;
	uint32_t iter = 0;
	while(!finish_req){
	    bool exists = false;
	    if(iter == 0){
		exists = m_wl->index->search(req->key, row_addr, page_addr, tid);
	    }
	    else{
		exists = m_wl->index->search_next(req->key, row_addr, page_addr, tid);
		if(!exists){
		    debug::notify_error("Item for key %lu does not exist", req->key);
		    exit(0);
		}
	    }

	    #ifdef BREAKDOWN
	    end = asm_rdtsc();
	    t_index += (end - start);
	    start = end;
	    #endif
	    access_t type = req->type;
	    row_t* row = get_row(row_addr, tid, pid, type);
	    //row_t* row = get_row(row_addr, rid, tid, pid, type);
	    #ifdef BREAKDOWN
	    #ifdef WAITDIE 
	    end = asm_rdtsc();
	    t_wait += (end - start);
	    start = end;
	    #endif
	    #endif
	    if(row == nullptr){
		rc = ABORT;
		goto FINAL;
	    }

	    transport->read((uint64_t)row, row_addr, ROW_SIZE, tid, pid);
	    /*
	    if(type == READ){
		return_row(row, row_addr, type, tid, pid);
	    }
	    */

	    #ifdef INTERACTIVE
            usleep(interactive_delay);
            #endif

	    // computation
	    if(m_query->request_cnt > 1){
		if(type == READ || type == SCAN){
		    int fid = 0;
		    char* data = row->get_data();
		    __attribute__((unused)) uint64_t fval = *(uint64_t*)(&data[fid * 10]);
		}
		else{ // WRITE
		    int fid = 0;
		    char* data = row->get_data();
		    *(uint64_t*)(&data[fid * 10]) = 0;
		    memcpy(write_buffer[rid].data, row, ROW_SIZE);
		}
	    }
	    iter++;
	    if(type == READ || type == WRITE || iter == req->scan_len)
		finish_req = true;
	}
    }
    assert(rc == RCOK);
    //rc = RCOK;
FINAL:
    rc = finish(rc, tid);
    #ifdef BREAKDOWN
    end = asm_rdtsc();
    if(rc == ABORT){
	t_abort += (end - start);
	t_total += (t_index + t_commit + t_abort + t_wait + t_backoff);
	ADD_STAT(tid, time_abort, t_total);
	t_index = t_commit = t_abort = t_wait = t_backoff = t_total = end = 0;
	start = asm_rdtsc(); // backoff start
    }
    else{
	assert(t_abort == 0);
	t_commit += (end - start);
	ADD_STAT(tid, time_index, t_index);
	ADD_STAT(tid, time_wait, t_wait);
	ADD_STAT(tid, time_backoff, t_backoff);
	ADD_STAT(tid, time_commit, t_commit);
	t_index = t_commit = t_abort = t_wait = t_backoff = t_total = start = end = 0;
    }
    #endif

    return rc;
}

RC ycsb_txn_man_t::read(ycsb_request_t* request, int tid){
    RC rc = RCOK;
    /*
    uint64_t row_addr = 0;
    int pid = workload->key_to_part(request->key);
    bool exists = workload->index->search(request->key, row_addr, tid);
    if(!exists){
	debug::notify_info("key not found  %lu", request->key);
	rc = ERROR;
	return rc;
    }

    row_t* row_buffer = mem->row_buffer_pool(tid);
    transport->read((uint64_t)row_buffer, row_addr, sizeof(uint64_t), tid, pid);
    if(row_buffer == nullptr){
	rc = ERROR;
	return rc;
    }

    rc = set_readlock(row_buffer, row_addr, tid, pid);
    if(rc == ABORT)
	return rc;

    int fid = 0;
    catalog_t* schema = workload->table->get_schema();
    char* data = row_buffer->get_value(schema, fid);
    __attribute__((unused)) uint64_t fval = *(uint64_t*)(&data[fid * 10]);

    unset_readlock(row_buffer, row_addr, tid, pid);
    */
    return RCOK;
}

RC ycsb_txn_man_t::upsert(ycsb_request_t* request, int tid){
    RC rc = RCOK;
    /*
    uint64_t row_addr = 0;
    int pid = workload->key_to_part(request->key);
    bool exists = workload->index->search(request->key, row_addr, tid);
    if(!exists){
	return insert(request, tid);
    }

    row_t* row_buffer = mem->row_buffer_pool(tid);
    transport->read((uint64_t)row_buffer, row_addr, sizeof(uint64_t), tid, pid);
    if(row_buffer == nullptr){
        rc = ERROR;
        return rc;
    }

    rc = set_writelock(row_buffer, row_addr, tid, pid);
    if(rc == ABORT)
        return rc;

    transport->read((uint64_t)row_buffer, row_addr, ROW_SIZE, tid, pid);

    int fid = 0;
    catalog_t* schema = workload->table->get_schema();
    char* data = row_buffer->get_value(schema, fid);
    //strcpy(data, request->value);
    *(uint64_t*)(&data[fid * 10]) = 0;

    transport->write((uint64_t)row_buffer + sizeof(uint64_t), row_addr + sizeof(uint64_t), ROW_SIZE - sizeof(uint64_t), tid, pid);

    // release lock after writing the row data
    unset_writelock(row_buffer, row_addr, tid, pid);
    */
    return rc;
}

RC ycsb_txn_man_t::insert(ycsb_request_t* req, int tid){
    RC rc = RCOK;
    /*
    int pid = workload->key_to_part(req->key);
    auto send_ptr = mem->request_buffer_pool(tid);
    auto request = create_message<request_t>((void*)send_ptr, tid, pid, request_type::TABLE_ALLOC_ROW);
    transport->send((uint64_t)request, sizeof(request_t), tid);

    auto recv_ptr = mem->response_buffer_pool(tid);
    auto response = create_message<response_t>((void*)recv_ptr);
    transport->recv((uint64_t)response, sizeof(response_t), tid);
    if(response->type != response_type::SUCCESS)
	debug::notify_error("Memory allocation for new row failed (%d)", response->type);
    uint64_t row_addr = response->addr;
    uint64_t primary_key = req->key;
    auto schema = workload->table->get_schema();

    row_t* new_row = mem->row_buffer_pool(tid);
    memset(new_row, 0, ROW_SIZE);
    new_row->set_primary_key(primary_key);
    new_row->set_value(schema, 0, &primary_key);

    int field_cnt = schema->get_field_cnt();
    for(int fid=0; fid<field_cnt; fid++){
	char value[6] = "abcde";
	new_row->set_value(schema, fid, value);
    }

    transport->write((uint64_t)new_row, row_addr, ROW_SIZE, tid, pid);
    uint64_t idx_key = primary_key;
    uint64_t idx_value = row_addr;

    workload->index->insert(idx_key, idx_value, tid);
    */
    return rc;
}

RC ycsb_txn_man_t::scan(ycsb_request_t* request, int tid){
    RC rc = RCOK;
    /*
    uint64_t row_addr = 0;
    uint64_t page_addr = 0;

    uint64_t prev_key = request->key;
    int pid;
    for(int i=0; i<request->scan_len; i++){
	row_t* row = nullptr;
	bool exists = false;
	if(i == 0){
	    exists = workload->index->search(request->key, row_addr, page_addr, tid);
	    if(!exists)
		return rc;
	    pid = workload->key_to_part(request->key);
	}
	else{
	    uint64_t next_key;
	    exists = workload->index->search_next(prev_key, row_addr, page_addr, tid);
	    if(!exists)
		return rc;
	    pid = workload->key_to_part(next_key);
	}

	auto row_buffer = mem->row_buffer_pool(tid);
	transport->read((uint64_t)row_buffer, row_addr, sizeof(uint64_t), tid, pid);
	rc = set_readlock(row_buffer, row_addr, tid, pid);
	if(rc == ABORT)
	    return rc;

	transport->read((uint64_t)row_buffer, row_addr, ROW_SIZE, tid, pid);
	int fid = 0;
	catalog_t* schema = workload->table->get_schema();
	char* data = row_buffer->get_value(schema, fid);
	__attribute__((unused)) uint64_t fval = *(uint64_t*)(&data[fid * 10]);

	prev_key = row_buffer->get_primary_key();
	unset_readlock(row_buffer, row_addr, tid, pid);
    }
    */
    return rc;

}
