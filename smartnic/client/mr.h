#pragma once
#include "common/global.h"
#include "common/rpc.h"
#include "common/memory_region.h"
#include "client/buffer.h"

class row_t;

class client_mr_t{
    private:
	#ifdef BATCH
	uint64_t memory_pool[NETWORK_THREAD_NUM];
	uint64_t memory_size[NETWORK_THREAD_NUM];
	memory_region_t* memory_region[NETWORK_THREAD_NUM];

	rpc_request_t<Key>* request_buffer[NETWORK_THREAD_NUM];
	rpc_response_t** response_buffer[NETWORK_THREAD_NUM];
	//rpc_response_t* response_buffer[NETWORK_THREAD_NUM];
	rpc_commit_t* commit_buffer[NETWORK_THREAD_NUM];

	txn_request_buffer_t<Key>* txn_request_buffer[NETWORK_THREAD_NUM];
	txn_response_buffer_t* txn_response_buffer[NETWORK_THREAD_NUM];

	#elif defined BATCH2
	uint64_t memory_pool;
	uint64_t memory_size;
	memory_region_t* memory_region;

	rpc_request_t<Key>* request_buffer[WORKER_THREAD_NUM];
	rpc_commit_t* commit_buffer[WORKER_THREAD_NUM];
	rpc_response_t* response_buffer[CLIENT_THREAD_NUM];
	//rpc_response_t* response_buffer[WORKER_THREAD_NUM];

#ifdef PER_THREAD_BUFFER
	txn_request_buffer_t<Key>* txn_request_buffer[WORKER_THREAD_NUM];
#else
	txn_request_buffer_t<Key>* txn_request_buffer;
#endif
	txn_response_buffer_t* txn_response_buffer;

	#else
	uint64_t memory_pool[CLIENT_THREAD_NUM];
	uint64_t memory_size[CLIENT_THREAD_NUM];
	memory_region_t* memory_region[CLIENT_THREAD_NUM];

	idx_request_t<Key, Value>* idx_request_buffer[CLIENT_THREAD_NUM];
	idx_response_t<Value>* idx_response_buffer[CLIENT_THREAD_NUM];
	rpc_request_t<Key>* rpc_request_buffer[CLIENT_THREAD_NUM];
	rpc_response_t* rpc_response_buffer[CLIENT_THREAD_NUM];
	#endif

    public:
	client_mr_t();


	#ifdef BATCH
	uint64_t get_memory_pool(int tid);
	uint64_t get_memory_size(int tid);

	txn_request_buffer_t<Key>* get_request_buffer(int tid);
	txn_response_buffer_t* get_response_buffer(int tid);
	rpc_request_t<Key>* rpc_request_pool(int tid);
	rpc_response_t* rpc_response_pool(int tid, int wid);
	//rpc_response_t* rpc_response_pool(int tid);
	rpc_commit_t* rpc_commit_pool(int tid);
	#elif defined BATCH2
	uint64_t get_memory_pool();
	uint64_t get_memory_size();

#ifdef PER_THREAD_BUFFER
	txn_request_buffer_t<Key>* get_request_buffer(int tid);
#else
	txn_request_buffer_t<Key>* get_request_buffer();
#endif
	txn_response_buffer_t* get_response_buffer();
	rpc_request_t<Key>* rpc_request_pool(int tid);
	rpc_response_t* rpc_response_pool(int tid);
	//rpc_response_t* rpc_response_pool(int tid);
	rpc_commit_t* rpc_commit_pool(int tid);

	#else
	uint64_t get_memory_pool(int tid);
	uint64_t get_memory_size(int tid);

	idx_request_t<Key, Value>* idx_request_buffer_pool(int tid);
	idx_response_t<Value>* idx_response_buffer_pool(int tid);
	rpc_request_t<Key>* rpc_request_buffer_pool(int tid);
	rpc_response_t* rpc_response_buffer_pool(int tid, int rid);
	#endif
};
