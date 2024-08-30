#pragma once
#include "common/global.h"
#include "common/rpc.h"
#include "common/memory_region.h"

#define MAX_INDEX_NUM 9

class server_mr_t{
    public:
        uint64_t memory_pool;
        uint64_t memory_size;
        memory_region_t* memory_region;

        server_mr_t();

	uint64_t get_memory_pool();
	uint64_t get_memory_size();

        request_t* request_buffer_pool(int qp_id);
        response_t* response_buffer_pool(int qp_id);
	uint64_t* root_buffer_pool(int root_idx);
};

