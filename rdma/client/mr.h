#pragma once
#include "common/global.h"
#include "common/rpc.h"
#include "common/memory_region.h"

class row_t;

class client_mr_t{
    public:
	uint64_t memory_pool[CLIENT_THREAD_NUM];
	uint64_t memory_size[CLIENT_THREAD_NUM];
	memory_region_t* memory_region[CLIENT_THREAD_NUM];

	client_mr_t();

	uint64_t get_memory_pool(int tid);
	uint64_t get_memory_size(int tid);

	request_t* request_buffer_pool(int tid);
	response_t* response_buffer_pool(int tid);
	uint64_t root_buffer_pool(int tid);
	uint64_t cas_buffer_pool(int tid);
	uint64_t page_buffer_pool(int tid);
	uint64_t sibling_buffer_pool(int tid);
	row_t* row_buffer_pool(int tid, int rid);
};
