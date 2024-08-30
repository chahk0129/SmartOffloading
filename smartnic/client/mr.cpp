#include "client/mr.h"

client_mr_t::client_mr_t(){
    #ifdef BATCH
    uint64_t request_size = sizeof(rpc_request_t<Key>);
    uint64_t response_size = sizeof(rpc_response_t) * CLIENT_THREAD_NUM;
    uint64_t commit_size = sizeof(rpc_commit_t);

    uint64_t mem_size_per_thread = request_size + response_size + commit_size;
    for(int i=0; i<NETWORK_THREAD_NUM; i++){
	memory_region[i] = new memory_region_t(mem_size_per_thread);
	memory_size[i] = memory_region[i]->size();
	memory_pool[i] = reinterpret_cast<uint64_t>(memory_region[i]->ptr());

	request_buffer[i] = reinterpret_cast<rpc_request_t<Key>*>(memory_pool[i]);
	commit_buffer[i] = reinterpret_cast<rpc_commit_t*>(memory_pool[i] + request_size);
	response_buffer[i] = new rpc_response_t*[CLIENT_THREAD_NUM];
	for(int j=0; j<CLIENT_THREAD_NUM; j++){
	    response_buffer[i][j] = reinterpret_cast<rpc_response_t*>(memory_pool[i] + request_size + commit_size + sizeof(rpc_response_t)*j);
	}

	txn_request_buffer[i] = new txn_request_buffer_t<Key>();
	txn_response_buffer[i] = new txn_response_buffer_t();
    }

    #elif defined BATCH2
    uint64_t request_size = sizeof(rpc_request_t<Key>) + CACHELINE_SIZE;
    uint64_t response_size = sizeof(rpc_response_t) + CACHELINE_SIZE;
    uint64_t commit_size = sizeof(rpc_commit_t) + CACHELINE_SIZE;

    uint64_t mem_size = request_size * WORKER_THREAD_NUM + response_size * CLIENT_THREAD_NUM + commit_size * WORKER_THREAD_NUM;
    memory_region = new memory_region_t(mem_size);
    memory_size = memory_region->size();
    memory_pool = reinterpret_cast<uint64_t>(memory_region->ptr());

    for(int i=0; i<WORKER_THREAD_NUM; i++){
	request_buffer[i] = reinterpret_cast<rpc_request_t<Key>*>(memory_pool + (request_size + commit_size) * i);
	commit_buffer[i] = reinterpret_cast<rpc_commit_t*>(memory_pool + (request_size + commit_size) * i + request_size);
    }

    for(int i=0; i<CLIENT_THREAD_NUM; i++){
	response_buffer[i] = reinterpret_cast<rpc_response_t*>(memory_pool + request_size*WORKER_THREAD_NUM + commit_size*WORKER_THREAD_NUM + response_size*i);
    }
#ifdef PER_THREAD_BUFFER
    for(int i=0; i<WORKER_THREAD_NUM; i++){
        txn_request_buffer[i] = new txn_request_buffer_t<Key>();
    }
#else
    txn_request_buffer = new txn_request_buffer_t<Key>();
#endif
    txn_response_buffer = new txn_response_buffer_t();

    #else
    uint64_t query_msg = sizeof(rpc_request_t<Key>) + sizeof(rpc_response_t) * MAX_ROW_PER_TXN + sizeof(idx_request_t<Key, Value>) + sizeof(idx_response_t<Value>);

    uint64_t mem_size_per_thread = query_msg; 
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
	memory_region[i] = new memory_region_t(mem_size_per_thread);
	memory_size[i] = memory_region[i]->size();
	memory_pool[i] = reinterpret_cast<uint64_t>(memory_region[i]->ptr());

	rpc_request_buffer[i] = reinterpret_cast<rpc_request_t<Key>*>(memory_pool[i]);
	rpc_response_buffer[i] = reinterpret_cast<rpc_response_t*>((uint64_t)rpc_request_buffer[i] + sizeof(rpc_request_t<Key>));

	idx_request_buffer[i] = reinterpret_cast<idx_request_t<Key, Value>*>((uint64_t)rpc_response_buffer[i] + sizeof(rpc_response_t));
	idx_response_buffer[i] = reinterpret_cast<idx_response_t<Value>*>((uint64_t)idx_request_buffer[i] + sizeof(idx_request_t<Key, Value>));
    }
    #endif
}

#ifdef BATCH
uint64_t client_mr_t::get_memory_pool(int tid){
    return memory_pool[tid];
}

uint64_t client_mr_t::get_memory_size(int tid){
    return memory_size[tid];
}

rpc_request_t<Key>* client_mr_t::rpc_request_pool(int tid){
    return request_buffer[tid];
}

rpc_response_t* client_mr_t::rpc_response_pool(int tid, int wid){
    return response_buffer[tid][wid];
}

/*
rpc_response_t* client_mr_t::rpc_response_pool(int tid){
    return response_buffer[tid][0];
}
*/
rpc_commit_t* client_mr_t::rpc_commit_pool(int tid){
    return commit_buffer[tid];
}

txn_request_buffer_t<Key>* client_mr_t::get_request_buffer(int tid){
    return txn_request_buffer[tid];
}

txn_response_buffer_t* client_mr_t::get_response_buffer(int tid){
    return txn_response_buffer[tid];
}

#elif defined BATCH2
uint64_t client_mr_t::get_memory_pool(){
    return memory_pool;
}

uint64_t client_mr_t::get_memory_size(){
    return memory_size;
}

rpc_request_t<Key>* client_mr_t::rpc_request_pool(int tid){
    return request_buffer[tid];
}

rpc_response_t* client_mr_t::rpc_response_pool(int tid){
    return response_buffer[tid];
}

rpc_commit_t* client_mr_t::rpc_commit_pool(int tid){
    return commit_buffer[tid];
}

#ifdef PER_THREAD_BUFFER
txn_request_buffer_t<Key>* client_mr_t::get_request_buffer(int tid){
    return txn_request_buffer[tid];
}
#else
txn_request_buffer_t<Key>* client_mr_t::get_request_buffer(){
    return txn_request_buffer;
}
#endif

txn_response_buffer_t* client_mr_t::get_response_buffer(){
    return txn_response_buffer;
}
#else
uint64_t client_mr_t::get_memory_pool(int tid){
    return memory_pool[tid];
}

uint64_t client_mr_t::get_memory_size(int tid){
    return memory_size[tid];
}

rpc_request_t<Key>* client_mr_t::rpc_request_buffer_pool(int tid){
    return rpc_request_buffer[tid];
}

rpc_response_t* client_mr_t::rpc_response_buffer_pool(int tid, int rid){
    return reinterpret_cast<rpc_response_t*>((uint64_t)rpc_response_buffer[tid] + sizeof(rpc_response_t) * rid);
}

idx_request_t<Key, Value>* client_mr_t::idx_request_buffer_pool(int tid){
    return idx_request_buffer[tid];
}

idx_response_t<Value>* client_mr_t::idx_response_buffer_pool(int tid){
    return idx_response_buffer[tid];
}
#endif
