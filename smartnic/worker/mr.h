#pragma once
#include "common/global.h"
#include "common/rpc.h"
#include "common/memory_region.h"

class row_t;

class worker_mr_t{
    public:
	uint64_t client_memory_pool;
	uint64_t server_memory_pool;
	uint64_t client_memory_size;
	uint64_t server_memory_size;
	memory_region_t* client_memory_region;
	memory_region_t* server_memory_region;

	#ifdef BATCH
	base_request_t* rpc_request_buffer[NETWORK_THREAD_NUM];
	rpc_response_t* rpc_response_buffer[WORKER_THREAD_NUM*2];
	#elif defined BATCH2
	base_request_t* rpc_request_buffer[WORKER_THREAD_NUM];
	rpc_response_t* rpc_response_buffer[WORKER_THREAD_NUM*2];
	#else
	rpc_request_t<Key>* rpc_request_buffer[CLIENT_THREAD_NUM];
	rpc_response_t* rpc_response_buffer[WORKER_THREAD_NUM];
	#endif

	request_t* request_buffer[WORKER_THREAD_NUM];
	response_t* response_buffer[WORKER_THREAD_NUM];
	uint64_t page_buffer[WORKER_THREAD_NUM];
	uint64_t sibling_buffer[WORKER_THREAD_NUM];
	row_t* row_buffer[WORKER_THREAD_NUM];


	worker_mr_t();

	uint64_t get_client_memory_pool();
	uint64_t get_server_memory_pool();
	uint64_t get_client_memory_size();
	uint64_t get_server_memory_size();

	// mr for client communication
	#if defined BATCH || defined BATCH2
	base_request_t* rpc_request_pool(int tid);
	rpc_response_t* rpc_response_pool(int tid);
	rpc_response_t* rpc_notify_response_pool(int tid);
	#else
	rpc_request_t<Key>* rpc_request_buffer_pool(int tid);
	rpc_response_t* rpc_response_buffer_pool(int tid);
	#endif

	// mr for server communication
	request_t* request_buffer_pool(int tid);
	response_t* response_buffer_pool(int tid);
	uint64_t page_buffer_pool(int tid);
	uint64_t sibling_buffer_pool(int tid);
	row_t* row_buffer_pool(int tid);
};
