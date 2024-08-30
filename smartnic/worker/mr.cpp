#include "worker/mr.h"

worker_mr_t::worker_mr_t(){
    #ifdef BATCH
    // for batched communication, commit buffer is larger than rpc_request buffer
    uint64_t request_msg = (sizeof(rpc_commit_t) + CACHELINE_SIZE) * NETWORK_THREAD_NUM; // # of request buffers match with the # of network threads in client --> can be polled with RDMA batch requests
    uint64_t response_msg = (sizeof(rpc_response_t) + CACHELINE_SIZE) * WORKER_THREAD_NUM * 2; // # of response buffers match with the # of worker threads in smartnic --> RDMA batch requests are processed one at a time
    #elif defined BATCH2
    // for batched communication, commit buffer is larger than rpc_request buffer
    uint64_t request_msg = (sizeof(rpc_commit_t) + CACHELINE_SIZE) * WORKER_THREAD_NUM; // # of request buffers match with the # of network threads in client --> can be polled with RDMA batch requests
    uint64_t response_msg = (sizeof(rpc_response_t) + CACHELINE_SIZE) * WORKER_THREAD_NUM * 2; // # of response buffers match with the # of worker threads in smartnic --> RDMA batch requests are processed one at a time
    #else
    uint64_t request_msg = (sizeof(rpc_request_t<Key>) + CACHELINE_SIZE) * CLIENT_THREAD_NUM;
    uint64_t response_msg = (sizeof(rpc_response_t) + CACHELINE_SIZE) * WORKER_THREAD_NUM;
    #endif
    uint64_t client_msg = request_msg + response_msg;

    client_memory_region = new memory_region_t(client_msg);
    client_memory_size = client_memory_region->size();
    client_memory_pool = reinterpret_cast<uint64_t>(client_memory_region->ptr());
    debug::notify_info("mr (%d B) from %lu to %lu", client_msg, client_memory_pool, client_memory_pool + client_memory_size);
    
    #ifdef BATCH
    uint64_t request_base = client_memory_pool;
    uint64_t response_base = client_memory_pool + request_msg;
    for(int i=0; i<NETWORK_THREAD_NUM; i++)
	rpc_request_buffer[i] = reinterpret_cast<base_request_t*>(request_base + (sizeof(rpc_commit_t) + CACHELINE_SIZE) * i);
	//rpc_request_buffer[i] = reinterpret_cast<base_request_t*>(client_memory_pool + (sizeof(rpc_commit_t) + CACHELINE_SIZE) * i);
    for(int i=0; i<WORKER_THREAD_NUM*2; i++){
	rpc_response_buffer[i] = reinterpret_cast<rpc_response_t*>(response_base + (sizeof(rpc_response_t) + CACHELINE_SIZE) * i);
	//rpc_response_buffer[i] = reinterpret_cast<rpc_response_t*>(client_memory_pool + (sizeof(rpc_commit_t) + CACHELINE_SIZE) * NETWORK_THREAD_NUM + (sizeof(rpc_response_t) + CACHELINE_SIZE) * i);
    }
    #elif defined BATCH2
    uint64_t request_base = client_memory_pool;
    uint64_t response_base = client_memory_pool + request_msg;
    for(int i=0; i<WORKER_THREAD_NUM; i++)
	rpc_request_buffer[i] = reinterpret_cast<base_request_t*>(request_base + (sizeof(rpc_commit_t) + CACHELINE_SIZE) * i);
	//rpc_request_buffer[i] = reinterpret_cast<base_request_t*>(client_memory_pool + (sizeof(rpc_commit_t) + CACHELINE_SIZE) * i);
    for(int i=0; i<WORKER_THREAD_NUM*2; i++){
	rpc_response_buffer[i] = reinterpret_cast<rpc_response_t*>(response_base + (sizeof(rpc_response_t) + CACHELINE_SIZE) * i);
	//rpc_response_buffer[i] = reinterpret_cast<rpc_response_t*>(client_memory_pool + (sizeof(rpc_commit_t) + CACHELINE_SIZE) * NETWORK_THREAD_NUM + (sizeof(rpc_response_t) + CACHELINE_SIZE) * i);
    }
    #else
    for(int i=0; i<CLIENT_THREAD_NUM; i++)
	rpc_request_buffer[i] = reinterpret_cast<rpc_request_t<Key>*>(client_memory_pool + (sizeof(rpc_request_t<Key>) + CACHELINE_SIZE) * i);
    for(int i=0; i<WORKER_THREAD_NUM; i++)
	rpc_response_buffer[i] = reinterpret_cast<rpc_response_t*>(client_memory_pool + (sizeof(rpc_request_t<Key>) + CACHELINE_SIZE) * CLIENT_THREAD_NUM + sizeof(rpc_response_t) * i);
    #endif

    uint64_t server_msg = (sizeof(request_t) + sizeof(response_t)) * WORKER_THREAD_NUM;
    uint64_t server_mem = (PAGE_BUFFER_SIZE + SIBLING_BUFFER_SIZE + ROW_SIZE + CACHELINE_SIZE) * WORKER_THREAD_NUM;

    server_memory_region = new memory_region_t(server_msg + server_mem);
    server_memory_size = server_memory_region->size();
    server_memory_pool = reinterpret_cast<uint64_t>(server_memory_region->ptr());

    uint64_t buf_size = sizeof(request_t) + sizeof(response_t) + PAGE_BUFFER_SIZE + SIBLING_BUFFER_SIZE + ROW_SIZE + CACHELINE_SIZE;
    for(int i=0; i<WORKER_THREAD_NUM; i++){
	request_buffer[i] = reinterpret_cast<request_t*>(server_memory_pool + buf_size * i);
	response_buffer[i] = reinterpret_cast<response_t*>(server_memory_pool + buf_size * i + sizeof(request_t));
	page_buffer[i] = reinterpret_cast<uint64_t>(server_memory_pool + buf_size * i + sizeof(request_t) + sizeof(response_t));
	sibling_buffer[i] = reinterpret_cast<uint64_t>(server_memory_pool + buf_size * i + sizeof(request_t) + sizeof(response_t) + PAGE_BUFFER_SIZE);
	row_buffer[i] = reinterpret_cast<row_t*>(server_memory_pool + buf_size * i + sizeof(request_t) + sizeof(response_t) + PAGE_BUFFER_SIZE + SIBLING_BUFFER_SIZE);
    }
}

uint64_t worker_mr_t::get_client_memory_pool(){
    return client_memory_pool;
}

uint64_t worker_mr_t::get_client_memory_size(){
    return client_memory_size;
}

uint64_t worker_mr_t::get_server_memory_pool(){
    return server_memory_pool;
}

uint64_t worker_mr_t::get_server_memory_size(){
    return server_memory_size;
}

#if defined BATCH || defined BATCH2
base_request_t* worker_mr_t::rpc_request_pool(int qp_id){
    return rpc_request_buffer[qp_id];
    //return reinterpret_cast<base_request_t*>(client_memory_pool + (sizeof(rpc_commit_t) + CACHELINE_SIZE) * tid);
}

rpc_response_t* worker_mr_t::rpc_response_pool(int tid){
    return rpc_response_buffer[tid * 2];
    //return reinterpret_cast<rpc_response_t*>(client_memory_pool + (sizeof(rpc_commit_t) + sizeof(rpc_response_t)*2 + CACHELINE_SIZE) * tid + sizeof(rpc_commit_t));
}

rpc_response_t* worker_mr_t::rpc_notify_response_pool(int tid){
    return rpc_response_buffer[tid * 2 + 1];
    //return reinterpret_cast<rpc_response_t*>(client_memory_pool + (sizeof(rpc_commit_t) + sizeof(rpc_response_t)*2 + CACHELINE_SIZE) * tid + sizeof(rpc_commit_t) + sizeof(rpc_response_t));
}

#else

rpc_request_t<Key>* worker_mr_t::rpc_request_buffer_pool(int qp_id){
    return rpc_request_buffer[qp_id];
    //return reinterpret_cast<rpc_request_t<Key>*>(client_memory_pool + (sizeof(rpc_request_t<Key>) + sizeof(rpc_response_t)) * tid);
}

rpc_response_t* worker_mr_t::rpc_response_buffer_pool(int tid){
    return rpc_response_buffer[tid];
    //return reinterpret_cast<rpc_response_t*>(client_memory_pool + (sizeof(rpc_request_t<Key>) + sizeof(rpc_response_t)) * tid + sizeof(rpc_request_t<Key>));
}
#endif

request_t* worker_mr_t::request_buffer_pool(int tid){
    return request_buffer[tid];
    //return reinterpret_cast<request_t*>(server_memory_pool + (sizeof(request_t)+ sizeof(response_t) + PAGE_BUFFER_SIZE + SIBLING_BUFFER_SIZE + ROW_SIZE + CACHELINE_SIZE) * tid);
}

response_t* worker_mr_t::response_buffer_pool(int tid){
    return response_buffer[tid];
    //return reinterpret_cast<response_t*>(server_memory_pool + (sizeof(request_t)+ sizeof(response_t) + PAGE_BUFFER_SIZE + SIBLING_BUFFER_SIZE + ROW_SIZE + CACHELINE_SIZE) * tid + sizeof(request_t));
}

uint64_t worker_mr_t::page_buffer_pool(int tid){
    return page_buffer[tid];
    //return (server_memory_pool + (sizeof(request_t)+ sizeof(response_t) + PAGE_BUFFER_SIZE + SIBLING_BUFFER_SIZE + ROW_SIZE + CACHELINE_SIZE) * tid + sizeof(request_t) + sizeof(response_t));
}

uint64_t worker_mr_t::sibling_buffer_pool(int tid){
    return sibling_buffer[tid];
    //return (server_memory_pool + (sizeof(request_t)+ sizeof(response_t) + PAGE_BUFFER_SIZE + SIBLING_BUFFER_SIZE + ROW_SIZE + CACHELINE_SIZE) * tid + sizeof(request_t) + sizeof(response_t) + PAGE_SIZE);
}

row_t* worker_mr_t::row_buffer_pool(int tid){
    return row_buffer[tid];
    //return reinterpret_cast<row_t*>(server_memory_pool + (sizeof(request_t)+ sizeof(response_t) + PAGE_BUFFER_SIZE + SIBLING_BUFFER_SIZE + ROW_SIZE + CACHELINE_SIZE) * tid + sizeof(request_t) + sizeof(response_t) + PAGE_SIZE + SIBLING_BUFFER_SIZE);
}


