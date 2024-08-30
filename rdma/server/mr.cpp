#include "server/mr.h"

server_mr_t::server_mr_t(){
    memory_region = new memory_region_t();
    memory_size = memory_region->size();
    memory_pool = reinterpret_cast<uint64_t>(memory_region->ptr());
}

uint64_t server_mr_t::get_memory_pool(){
    return memory_pool;
}

uint64_t server_mr_t::get_memory_size(){
    return memory_size;
}

uint64_t* server_mr_t::root_buffer_pool(int root_idx){
    return reinterpret_cast<uint64_t*>(memory_pool + ROOT_BUFFER_SIZE * root_idx);
}

request_t* server_mr_t::request_buffer_pool(int qp_id){
    return reinterpret_cast<request_t*>(memory_pool + ROOT_BUFFER_SIZE * MAX_INDEX_NUM + (sizeof(request_t) + sizeof(response_t) * qp_id)); // reserve MAX_INDEX_NUM root buffers for tpcc indexes 
}

response_t* server_mr_t::response_buffer_pool(int qp_id){
    return reinterpret_cast<response_t*>(memory_pool + ROOT_BUFFER_SIZE * MAX_INDEX_NUM + (sizeof(request_t) + sizeof(response_t) * qp_id) + sizeof(request_t)); // reserve MAX_INDEX_NUM root buffers for tpcc indexes 
}

