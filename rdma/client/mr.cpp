#include "client/mr.h"

client_mr_t::client_mr_t(){
    uint64_t mem_msg = sizeof(request_t) + sizeof(response_t);
    uint64_t mem_index = ROOT_BUFFER_SIZE + CAS_BUFFER_SIZE + PAGE_BUFFER_SIZE + SIBLING_BUFFER_SIZE;
    uint64_t mem_row = ROW_SIZE * MAX_ROW_PER_TXN;

    uint64_t mem_size_per_thread = mem_msg + mem_index + mem_row;
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
	memory_region[i] = new memory_region_t(mem_size_per_thread);
	memory_size[i] = memory_region[i]->size();
	memory_pool[i] = reinterpret_cast<uint64_t>(memory_region[i]->ptr());
    }
}

uint64_t client_mr_t::get_memory_pool(int tid){
    return memory_pool[tid];
}

uint64_t client_mr_t::get_memory_size(int tid){
    return memory_size[tid];
}

request_t* client_mr_t::request_buffer_pool(int tid){
    return reinterpret_cast<request_t*>(memory_pool[tid]);
}

response_t* client_mr_t::response_buffer_pool(int tid){
    return reinterpret_cast<response_t*>(memory_pool[tid] + sizeof(request_t));
}

uint64_t client_mr_t::root_buffer_pool(int tid){
    return (memory_pool[tid] + sizeof(request_t) + sizeof(response_t));
}

uint64_t client_mr_t::cas_buffer_pool(int tid){
    return (memory_pool[tid] + sizeof(request_t) + sizeof(response_t) + ROOT_BUFFER_SIZE);
}

uint64_t client_mr_t::page_buffer_pool(int tid){
    return (memory_pool[tid] + sizeof(request_t) + sizeof(response_t) + ROOT_BUFFER_SIZE + CAS_BUFFER_SIZE);
}

uint64_t client_mr_t::sibling_buffer_pool(int tid){
    return (memory_pool[tid] + sizeof(request_t) + sizeof(response_t) + ROOT_BUFFER_SIZE + CAS_BUFFER_SIZE + PAGE_BUFFER_SIZE);
}

row_t* client_mr_t::row_buffer_pool(int tid, int rid){
    return reinterpret_cast<row_t*>(memory_pool[tid] + sizeof(request_t) + sizeof(response_t) + ROOT_BUFFER_SIZE + CAS_BUFFER_SIZE + PAGE_BUFFER_SIZE + SIBLING_BUFFER_SIZE + ROW_SIZE * rid);
}


