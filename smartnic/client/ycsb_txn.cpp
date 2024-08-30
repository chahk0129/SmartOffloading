#include "storage/catalog.h"
#include "storage/row.h"
#include "storage/table.h"

#include "client/worker.h"
#include "client/ycsb.h"
#include "client/ycsb_query.h"
#include "client/query.h"
#include "client/thread.h"
#include "client/txn.h"
#include "client/mr.h"
#include "client/transport.h"
#include "client/buffer.h"

#include "common/rpc.h"
#include "common/stat.h"

void ycsb_txn_man_t::init(thread_t* thd, worker_t* worker, int tid){
    txn_man_t::init(thd, worker, tid);
    this->worker = (ycsb_worker_t*)worker;
}

#ifdef BATCH
void ycsb_txn_man_t::run_network_thread(){
    auto m_wl = worker;
    auto tid = thread->get_tid();
    assert(tid < NETWORK_THREAD_NUM);
    //size_t NETWORK_SLEEP = 10; // sleep for 1 usec
    size_t NETWORK_SLEEP = 1; // sleep for 1 usec
    size_t base_size = sizeof(base_request_t) + sizeof(int);

    size_t request_size = sizeof(rpc_request_t<Key>);
    size_t response_size = sizeof(rpc_response_t);
    size_t commit_size = sizeof(rpc_commit_t);

    auto request_ptr = mem->rpc_request_pool(tid);
    auto request = create_message<rpc_request_t<Key>>((void*)request_ptr, buffer_type_t::REQUEST_BUFFER, tid); // initialize request buffer

    //for(int i=0; i<BATCH_THREAD_NUM+1; i++){
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
    //for(int i=0; i<WORKER_THREAD_NUM; i++){
	auto ptr = mem->rpc_response_pool(tid, i);
	transport->prepost_recv((uint64_t)ptr, response_size, tid, i);
    }
    //auto response_ptr = mem->rpc_response_pool(tid);
    //auto response = create_message<rpc_response_t>((void*)response_ptr, tid); // initialize response buffer
    //transport->prepost_recv((uint64_t)response, response_size, tid);
 
    auto commit_ptr = mem->rpc_commit_pool(tid);
    auto commit = create_message<rpc_commit_t>((void*)commit_ptr, buffer_type_t::COMMIT_BUFFER, tid); // initialize commit buffer

    int send_cnt = 0;
    int recv_cnt = 0;
    int recv_poll_size = BATCH_THREAD_NUM;
    //int recv_poll_size = WORKER_THREAD_NUM;

    auto request_buffer = mem->get_request_buffer(tid);
    auto response_buffer = mem->get_response_buffer(tid);
    bool need_send = false;

    struct ibv_wc wc[recv_poll_size];
    while(true){
	// wake up

	// check if there is any buffered txn request
	need_send = request_buffer->validate();
	//if(need_send){ // send next batch if previous batch has been processed
	if(need_send && !send_cnt){ // send next batch if previous batch has been processed
	    int request_idx = 0;
	    int commit_idx = 0;
	    for(int i=0; i<BATCH_THREAD_NUM; i++){
		auto frame = request_buffer->get_frame(i);
		if(frame->validate()){
		    assert(frame->tid % BATCH_THREAD_NUM == i);
		    if(frame->type != COMMIT_DATA){
			request->buffer[request_idx].update(frame->tid, frame->type, frame->timestamp, frame->key);
			request_idx++;
		    }
		    else{ // commit 
			commit->buffer[commit_idx].update(frame->tid, frame->num, frame->data);
			commit_idx++;
		    }
		    int local_client_id = frame->tid % BATCH_THREAD_NUM;
		    frame->reset();
		}
	    }

	    if(request_idx){
		request->num = request_idx;
		//size_t request_size = base_size + sizeof(rpc_request_buffer_t<Key>)*request_idx;
		//transport->send((uint64_t)request, request_size, tid);
		transport->send_async((uint64_t)request, request_size, tid);
		send_cnt++;
	    }
	    if(commit_idx){
		commit->num = commit_idx;
		//size_t commit_size = base_size + sizeof(rpc_commit_buffer_t)*commit_idx;
		//transport->send((uint64_t)commit, commit_size, tid);
		transport->send_async((uint64_t)commit, commit_size, tid);
		send_cnt++;
	    }
	}

	recv_cnt = transport->poll(recv_poll_size, tid, wc);
	//recv_cnt = transport->poll(recv_poll_size, tid, wc);
	if(recv_cnt > 0){
	    for(int i=0; i<recv_cnt; i++){
		auto wr_id = wc[i].wr_id;
		auto response = mem->rpc_response_pool(tid, wr_id);
		int num = response->num;

		for(int j=0; j<num; j++){
		    auto res = response->get_buffer(j);
		    int client_id = res->tid % BATCH_THREAD_NUM; // local tid
		    auto frame = response_buffer->get_frame(client_id);

		    while(frame->validate())
			asm("nop");

		    frame->update(res->tid, res->type, res->data);
		    //frame->update(local_buffer->tid, local_buffer->type, local_buffer->timestamp, local_buffer->data, local_buffer->time_id, local_buffer->rid);
		    //frame->update(local_buffer->tid, local_buffer->type, local_buffer->timestamp, local_buffer->data, local_buffer->time_id);
		    //local_buffer->reset(); // debug -- this may not be necessary when correctly implemented
		}

		//auto _response = create_message<rpc_response_t>((void*)response, tid); // initialize response buffer
		transport->prepost_recv((uint64_t)response, response_size, tid, wr_id);
	    }
	}

	if(send_cnt){
	    int poll_cnt = transport->poll_sendcq(send_cnt, tid);
	    send_cnt -= poll_cnt;
	}
	//send_cnt = 0;

	usleep(NETWORK_SLEEP);
    }
}

RC ycsb_txn_man_t::run_txn(base_query_t* query){
    RC rc = RCOK;
    auto m_query = (ycsb_query_t*)query;
    auto m_wl = worker;
    int tid = thread->get_tid();
    int local_tid = tid % BATCH_THREAD_NUM;
    int net_tid = thread->get_network_tid();
    uint64_t timestamp = m_query->timestamp;

    // get buffer pointers
    auto request_buffer = mem->get_request_buffer(net_tid);
    auto response_buffer = mem->get_response_buffer(net_tid);

    // get local buffer frames
    auto local_request = request_buffer->get_frame(local_tid);
    auto local_response = response_buffer->get_frame(local_tid);

    write_num = 0;
    row_t rows[m_query->request_cnt];

    for(uint32_t rid=0; rid<m_query->request_cnt; rid++){
	ycsb_request_t* req = &m_query->requests[rid];
	bool finish_req = false;
	uint32_t iter = 0;
	access_t type = req->type;
	Key key = req->key;

	while(!finish_req){
	    //while(local_request->validate());
	    assert(local_response->validate() == false);
	    // update local buffer data
	    //local_request->update(tid, type, timestamp, key, rid, time_id);
	    local_request->update(tid, type, timestamp, key);

	    // debug: wait until local request has been processed
	    //local_request->wait();

	    // wait until response arrives
	    local_response->wait();

	    rc = local_response->type;
	    if(rc == ABORT){
		write_num = 0;
		local_response->reset();
		return rc;
	    }
	    else if(rc == ERROR){
		write_num = 0;
		local_response->reset();
		debug::notify_error("worker tid %d -- TX %d \t ERROR", tid, local_tid);
		exit(0);
	    }
	    assert(rc == RCOK);

	    memcpy(&rows[rid], local_response->data, sizeof(row_t));
	    //memcpy(&local_commit->data[sizeof(row_t)*rid], local_response->data, sizeof(row_t));
	    local_response->reset();

	    int fid = 0;
	    char* data = rows[rid].get_data();
	    //char* data = ((row_t*)&local_commit->data[sizeof(row_t)*rid])->get_data();
	    if(m_query->request_cnt > 1){ // computation
		if(type == READ || type == SCAN){
		    __attribute__((unused)) uint64_t fval = *(uint64_t*)(&data[fid * 10]);
		}
		else{ // WRITE
		    write_num++;
		    *(uint64_t*)(&data[fid * 10]) = 0;
		}
	    }

	    iter++;
	    if(type == READ || type == WRITE || iter == req->scan_len)
		finish_req = true;
	}
	assert(local_request->validate() == false);

    }

    //while(local_request->validate());
    assert(local_response->validate() == false);

    if(write_num != 0){ // COMMIT WRITE
	// update local request buffer
	//local_request->update(tid, COMMIT_DATA, m_query->request_cnt, timestamp, (char*)rows, rid, time_id);
	local_request->update(tid, COMMIT_DATA, m_query->request_cnt, timestamp, (char*)rows);
	write_num = 0;
    }
    else{
	// update local request buffer
	//local_request->update(tid, COMMIT, timestamp, rid, time_id);
	local_request->update(tid, COMMIT, timestamp);
    }

    // debug: wait until local request has been processed
    //local_request->wait();

    // wait until local response arrives
    local_response->wait();

    // debug
    rc = local_response->type;
    local_response->reset();

    /*
    if(rc == ABORT){ // this txn has been wounded
	debug::notify_info("tid %d --- tx commit has been wounded qp %d (timestamp %lu)", tid, net_tid, timestamp);
	return rc;
    }
    */

    return rc;
}

#elif defined BATCH2
void ycsb_txn_man_t::run_send(int tid){
    auto m_wl = worker;
    size_t base_size = sizeof(base_request_t) + sizeof(int);

    size_t request_size = sizeof(rpc_request_t<Key>);
    size_t commit_size = sizeof(rpc_commit_t);

    auto request_ptr = mem->rpc_request_pool(tid);
    auto request = create_message<rpc_request_t<Key>>((void*)request_ptr, buffer_type_t::REQUEST_BUFFER, tid);
    
    auto commit_ptr = mem->rpc_commit_pool(tid);
    auto commit = create_message<rpc_commit_t>((void*)commit_ptr, buffer_type_t::COMMIT_BUFFER, tid);

#ifdef PER_THREAD_BUFFER
    auto buffer = mem->get_request_buffer(tid);
#else
    auto buffer = mem->get_request_buffer();
#endif
    bool need_send = false;

    //size_t NETWORK_SLEEP = 2; // sleep for X usec
    //size_t NETWORK_SLEEP = 8; // sleep for X usec
    size_t NETWORK_SLEEP = 10; // sleep for X usec
    //size_t NETWORK_SLEEP = 10; // sleep for X usec
    //size_t NETWORK_SLEEP = 5; // sleep for X usec

    bool send_commit = false;
    bool send_request = false;
    int send_cnt = 0;
    int request_idx = 0;
    int commit_idx = 0;
    int iter_commit = 0;
    int iter_request = 0;
    //int NUM_ITER = 50;
    int NUM_ITER = 5;
    //int NUM_ITER = 10;
    //int NUM_ITER = 10;
    //int NUM_ITER = 20;
    //int NUM_ITER = 10;
    //

    uint64_t t_buffer, t_send, t_wait;
    t_buffer = t_send = t_wait = 0;

    uint64_t start, end;
    start = asm_rdtsc();
    while(true){
	// wake up

#ifdef PER_THREAD_BUFFER
	for(int i=0; i<PER_BATCH_SIZE; i++){
#else
	for(int i=0; i<CLIENT_THREAD_NUM; i++){
#endif
	    auto frame = buffer->get_frame(i);
	    if(frame->acquire()){
		if(frame->type == COMMIT_DATA){
		    commit->buffer[commit_idx].update(frame->tid, frame->num, frame->data);
		    commit_idx++;
//		    send_commit = true;
#ifdef PER_THREAD_BUFFER
		    if(commit_idx == PER_BATCH_SIZE){
#else
		    if(commit_idx == BATCH_SIZE){
#endif
			send_commit = true;
			break;
		    }
		}
		else{ // regular txn request
		    request->buffer[request_idx].update(frame->tid, frame->type, frame->timestamp, frame->key);
		    request_idx++;
//		    send_request = true;
#ifdef PER_THREAD_BUFFER
		    if(request_idx == PER_BATCH_SIZE){
#else
		    if(request_idx == BATCH_SIZE){
#endif
			send_request = true;
			break;
		    }
		}
		// TODO: handle buffer overflow
	    }
	}

	end = asm_rdtsc();
	t_buffer += (end - start);
	start = end;

	if(send_commit){
	    commit->num = commit_idx;
	    size_t send_size = sizeof(rpc_commit_t);
	    transport->send((uint64_t)commit, send_size, tid);
	    commit_idx = 0;
	    iter_commit = 0;
	    //debug::notify_info("Sending full commits"); 
	    
	    end = asm_rdtsc();
	    t_send += (end - start);
	    start = end;
	}

	if(send_request){
	    request->num = request_idx;
	    size_t send_size = sizeof(rpc_request_t<Key>);
	    transport->send((uint64_t)request, send_size, tid);
	    request_idx = 0;
	    iter_request = 0;
	    //debug::notify_info("Sending full requests"); 
	    
	    end = asm_rdtsc();
	    t_send += (end - start);
	    start = end;
	}
	    

	iter_commit++;
	iter_request++;

	if((commit_idx > 0) && (iter_commit > NUM_ITER)){
	    commit->num = commit_idx;
	    size_t send_size = base_size + sizeof(rpc_commit_buffer_t) * commit_idx;
	    transport->send((uint64_t)commit, send_size, tid);
	    commit_idx = 0;
	    iter_commit = 0;
	    send_commit = true;

	    end = asm_rdtsc();
	    t_send += (end - start);
	    start = end;
	}

	if((request_idx > 0) && (iter_request > NUM_ITER)){
	    request->num = request_idx;
	    size_t send_size = base_size + sizeof(rpc_request_buffer_t<Key>) * request_idx;
	    transport->send((uint64_t)request, send_size, tid);
	    request_idx = 0;
	    iter_request = 0;
	    send_request = true;

	    end = asm_rdtsc();
	    t_send += (end - start);
	    start = end;
	}

	if(send_request){
#ifdef PER_THREAD_BUFFER
	    if(request->num == PER_BATCH_SIZE)
#else
	    if(request->num == BATCH_SIZE)
#endif
		usleep(NETWORK_SLEEP);
	    send_request = false;
	    end = asm_rdtsc();
	    t_wait += (end - start);
	    start = end;

	}
	else if(send_commit){
#ifdef PER_THREAD_BUFFER
	    if(commit->num == PER_BATCH_SIZE)
#else
	    if(commit->num == BATCH_SIZE)
#endif
		usleep(NETWORK_SLEEP);
	    send_commit = false;
	    end = asm_rdtsc();
	    t_wait += (end - start);
	    start = end;

	}
	/*
	if(send_request || send_commit){
	    usleep(NETWORK_SLEEP);
	    send_request = false;
	    send_commit = false;

	    end = asm_rdtsc();
	    t_wait += (end - start);
	    start = end;
	}
	*/

	if(g_run_finish)
	    break;
    }

    ADD_STAT(tid, time_send, t_send);
    ADD_STAT(tid, time_wait_send, t_wait);
    ADD_STAT(tid, time_buffer_send, t_buffer);
}

void ycsb_txn_man_t::run_recv(int tid){
    auto m_wl = worker;
    //debug::notify_info("tid %d running recv", tid);

    auto buffer = mem->get_response_buffer();
    //size_t NETWORK_SLEEP = 2;
    //size_t NETWORK_SLEEP = 8;
    size_t NETWORK_SLEEP = 10;
    //size_t NETWORK_SLEEP = 10;

    //auto ptr = mem->rpc_response_pool(tid);
    //transport->prepost_recv((uint64_t)ptr, sizeof(rpc_response_t), tid, tid);

    int recv_poll_size = 4;
    //int recv_poll_size = 1;
    struct ibv_wc wc[recv_poll_size];

    uint64_t t_buffer, t_recv, t_wait;
    t_buffer = t_recv = t_wait = 0;

    uint64_t start, end;
    start = asm_rdtsc();
    while(true){
	int cnt = transport->poll(recv_poll_size, tid, wc);

	end = asm_rdtsc();
	t_recv += (end - start);
	start = end;

	for(int i=0; i<cnt; i++){
	    auto wr_id = wc[i].wr_id;
	    auto response = mem->rpc_response_pool(wr_id);
	    int num = response->num;

	    for(int j=0; j<num; j++){
		auto res = response->get_buffer(j);
		int client_id = res->tid % CLIENT_THREAD_NUM;
		auto frame = buffer->get_frame(client_id);

		//assert(frame->validate() == false);
		while(frame->validate())
		    asm("nop");

		frame->update(res->tid, res->type, res->data);
	    }
	    end = asm_rdtsc();
	    t_buffer += (end - start);
	    start = end;

	    transport->prepost_recv((uint64_t)response, sizeof(rpc_response_t), wr_id%WORKER_THREAD_NUM, wr_id);
	    //transport->prepost_recv((uint64_t)response, sizeof(rpc_response_t), tid, wr_id);
	    //
	    end = asm_rdtsc();
	    t_recv += (end - start);
	    start = end;
	}


	if(cnt){
	    //usleep(NETWORK_SLEEP * cnt);
	    usleep(NETWORK_SLEEP);

	    end = asm_rdtsc();
	    t_wait += (end - start);
	    start = end;
	}


	if(g_run_finish)
	    break;
    }

    ADD_STAT(tid, time_wait_recv, t_wait);
    ADD_STAT(tid, time_recv, t_recv);
    ADD_STAT(tid, time_buffer_recv, t_buffer);
}

void ycsb_txn_man_t::run_network_thread(){
    auto tid = thread->get_tid();
    if(tid < WORKER_THREAD_NUM){ // producer
	run_send(tid);
    }
    else{ // consumer
	run_recv(tid % WORKER_THREAD_NUM);
    }
}

RC ycsb_txn_man_t::run_txn(base_query_t* query){
    RC rc = RCOK;
    auto m_query = (ycsb_query_t*)query;
    auto m_wl = worker;
    int tid = thread->get_tid();
    int client_id = tid % CLIENT_THREAD_NUM;
    
    uint64_t timestamp = m_query->timestamp;

    // get buffer pointers
#ifdef PER_THREAD_BUFFER
    int net_tid = client_id / PER_BATCH_SIZE;
    //int net_tid = client_id / WORKER_THREAD_NUM;
    int local_tid = client_id % PER_BATCH_SIZE;
    assert(net_tid < WORKER_THREAD_NUM);
    //debug::notify_info("tid %d \tnet_tid %d \tlocal_tid %d", tid, net_tid, local_tid);
    auto request_buffer = mem->get_request_buffer(net_tid);
    //while(!warmup_finish);
    // break
#else
    auto request_buffer = mem->get_request_buffer();
#endif
    auto response_buffer = mem->get_response_buffer();

    // get local buffer frames
#ifdef PER_THREAD_BUFFER
    auto request_frame = request_buffer->get_frame(local_tid);
    auto response_frame = response_buffer->get_frame(client_id);
#else
    auto request_frame = request_buffer->get_frame(client_id);
    auto response_frame = response_buffer->get_frame(client_id);
#endif

    write_num = 0;
    row_t rows[m_query->request_cnt];

    for(uint32_t rid=0; rid<m_query->request_cnt; rid++){
	ycsb_request_t* req = &m_query->requests[rid];
	bool finish_req = false;
	uint32_t iter = 0;
	access_t type = req->type;
	auto key = req->key;

	while(!finish_req){
	    assert(request_frame->validate() == false);
	    assert(response_frame->validate() == false);
	    request_frame->update(tid, type, timestamp, key);

	    // debug
	    // wait until request is processed 
	    //request_frame->wait();
	    // wait until response arrives
	    response_frame->wait();

	    rc = response_frame->type;
	    if(rc == ABORT){
		write_num = 0;
		response_frame->reset();
		return rc;
	    }
	    else if(rc == ERROR){
		write_num = 0;
		response_frame->reset();
		debug::notify_error("tid %d --- TX %d \t ERROR", tid, rid);
		exit(0);
		return rc;
	    }

	    assert(rc == RCOK);

	    memcpy(&rows[rid], response_frame->data, sizeof(row_t));
	    response_frame->reset();

	    int fid = 0;
	    char* data = rows[rid].get_data();
	    if(m_query->request_cnt > 1){ // computation
		if(type == READ || type == SCAN){
		    __attribute__((unused)) uint64_t fval = *(uint64_t*)(&data[fid * 10]);
		}
		else{ // WRITE
		    *(uint64_t*)(&data[fid * 10]) = 0;
		    write_num++;
		}
	    }

	    iter++;
	    if(type == READ || type == WRITE || iter == req->scan_len)
		finish_req = true;
	}
    }

    assert(request_frame->validate() == false);
    assert(response_frame->validate() == false);

    // COMMIT
    if(write_num != 0){  // WRITE
	request_frame->update(tid, COMMIT_DATA, m_query->request_cnt, timestamp, (char*)rows);
	write_num = 0;
    }
    else{
	request_frame->update(tid, COMMIT, timestamp);
    }

    // debug
    // wait until request is processed 
    //request_frame->wait();

    response_frame->wait();
    rc = response_frame->type;
    response_frame->reset();

    return rc;
}

#else // ifndef BATCH
RC ycsb_txn_man_t::run_txn(base_query_t* query){
    RC rc = RCOK;
    auto m_query = (ycsb_query_t*)query;
    auto m_wl = worker;
    int tid = thread->get_tid();
    uint64_t timestamp = m_query->timestamp;
    size_t request_size = sizeof(base_request_t) + sizeof(int) + sizeof(Key);
    size_t response_size = sizeof(rpc_response_t);
    write_num = 0;

    #ifdef INTERACTIVE
    int base = 100;
    int delay = 100;
    int interactive_delay;
    interactive_delay = (rand() % delay) + base;
    #endif

    assert(m_query->request_cnt <= REQUEST_PER_QUERY);
    auto send_ptr = mem->rpc_request_buffer_pool(tid);

    for(uint32_t rid=0; rid<m_query->request_cnt; rid++){
	assert(rid < REQUEST_PER_QUERY);

	#ifdef INTERACTIVE
	usleep(interactive_delay);
	#endif

        ycsb_request_t* req = &m_query->requests[rid];
	bool finish_req = false;
	uint32_t iter = 0;
	access_t type = req->type;

	while(!finish_req){
	    auto request = create_message<rpc_request_t<Key>>((void*)send_ptr, tid, type, req->key, timestamp);
	    transport->send((uint64_t)request, request_size, tid);

	    auto recv_ptr = mem->rpc_response_buffer_pool(tid, rid);
	    auto response = create_message<rpc_response_t>((void*)recv_ptr);
	    //auto response = create_message<rpc_response_t>((void*)recv_ptr, tid);
	    transport->recv((uint64_t)response, response_size, tid);

	    if(response->type == ABORT){
		write_num = 0;
		return ABORT;
	    }
	    else if(response->type == ERROR){
		debug::notify_error("tid %d -- TX %d \t ERROR", tid, rid);
		exit(0);
	    }

	    #ifdef INTERACTIVE
	    usleep(interactive_delay);
	    #endif
	    if(m_query->request_cnt > 1){ // computation
		row_t* row = (row_t*)response->data;
		if(type == READ || type == SCAN){
		    int fid = 0;
		    char* data = row->get_data();
		    __attribute__((unused)) uint64_t fval = *(uint64_t*)(&data[fid * 10]);
		}
		else{ // WRITE
		    write_buf[write_num] = rid;
		    write_num++;
		    int fid = 0;
		    char* data = row->get_data();
		    *(uint64_t*)(&data[fid * 10]) = 0;
		    //debug::notify_info("  data: %s", data);
		}
	    }
	    iter++;
	    if(type == READ || type == WRITE || iter == req->scan_len)
		finish_req = true;
	}
    }


    if(write_num != 0){ // COMMIT WRITE
	auto request = create_message<rpc_request_t<Key>>((void*)send_ptr, tid, COMMIT_DATA);
	request->num = write_num;
	for(int i=0; i<write_num; i++){
	    memcpy(&request->data[sizeof(row_t)*i], mem->rpc_response_buffer_pool(tid, write_buf[i])->data, sizeof(row_t));
	}
	size_t commit_size = request_size + sizeof(row_t) * write_num;
	transport->send((uint64_t)request, commit_size, tid);

	write_num = 0;
    }
    else{ // COMMIT READ
	auto request = create_message<rpc_request_t<Key>>((void*)send_ptr, tid, COMMIT);
	size_t commit_size = sizeof(base_request_t);
	transport->send((uint64_t)request, commit_size, tid);
    }

    auto recv_ptr = mem->rpc_response_buffer_pool(tid, 0);
    auto response = create_message<rpc_response_t>((void*)recv_ptr);
    transport->recv((uint64_t)response, response_size, tid);
    rc = response->type;

    return rc;
}
#endif
