#include "client/idx_wrapper.h"
#include "client/mr.h"
#include "client/transport.h"
#include "index/node.h"
#include "index/tree.h"
#include "net/config.h"

idx_wrapper_t::idx_wrapper_t(config_t* conf){
    mem = new client_mr_t();

    uint64_t memory_pool[CLIENT_THREAD_NUM];
    uint64_t memory_size[CLIENT_THREAD_NUM];
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
	memory_pool[i] = mem->get_memory_pool(i);
	memory_size[i] = mem->get_memory_size(i);
    }

    transport = new client_transport_t(conf, memory_pool, memory_size);

    // prepost RDMA RECVs to server QPs
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
	auto recv_ptr = mem->response_buffer_pool(i);
	transport->prepost_recv((uint64_t)recv_ptr, sizeof(response_t), i);
    }

    idx = new tree_t<uint64_t, uint64_t>(mem, transport);
}

void idx_wrapper_t::insert(uint64_t key, uint64_t value, int tid){
    idx->insert(key, value, tid);
}

void idx_wrapper_t::update(uint64_t key, uint64_t value, int tid){
    idx->insert(key, value, tid);
}

bool idx_wrapper_t::search(uint64_t key, uint64_t& value, int tid){
    return idx->search(key, value, tid);
}
