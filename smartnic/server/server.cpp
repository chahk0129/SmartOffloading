#include "common/rpc.h"
#include "server/server.h"
#include "server/mr.h"
#include "server/allocator.h"
#include "server/transport.h"

server_t::server_t(){
    if(!init_resources())
	exit(0);

    // initiate worker thread
    for(int i=0; i<SERVER_THREAD_NUM; i++)
	workers.emplace_back(&server_t::handle_message, this, i);

}

bool server_t::init_resources(){
    bool ret = false;

    // create memory region
    uint64_t memory_pool[MR_PARTITION_NUM];
    uint64_t memory_size[MR_PARTITION_NUM];
    for(int i=0; i<MR_PARTITION_NUM; i++){
	mem[i] = new server_mr_t();
	memory_pool[i] = mem[i]->get_memory_pool();
	memory_size[i] = mem[i]->get_memory_size();
    }

    // network transport
    transport = new server_transport_t(memory_pool, memory_size);

    // allocator (MR)
    for(int i=0; i<MR_PARTITION_NUM; i++){
	uint64_t msg_size = 0;
	uint64_t alloc_offset = 0;
	if(i == 0){ // rpc messages for MR[0]
	    msg_size = (sizeof(request_t) + sizeof(response_t)) * WORKER_THREAD_NUM;
	    alloc_offset = memory_pool[i] + msg_size;
	    allocator[i] = new allocator_t(alloc_offset, memory_size[i] - msg_size);
	}
	else{
	    alloc_offset = memory_pool[i];
	    allocator[i] = new allocator_t(alloc_offset, memory_size[i]);
	}
    }

    ret = transport->setup_connection();
    if(!ret){
	debug::notify_error("Failed to connect to Worker");
	return false;
    }

    // pre-post RDMA RECVs
    for(int i=0; i<WORKER_THREAD_NUM; i++){
	auto recv_ptr = mem[0]->request_buffer_pool(i);
	transport->prepost_recv((uint64_t)recv_ptr, sizeof(request_t), i);
    }

    return true;
}

void server_t::handle_message(int tid){
    debug::notify_info("Running Thread %d", tid);
    bind_core_worker(tid);
    struct ibv_wc wc[QP_DEPTH];
    while(true){
	int cnt = transport->poll(wc, QP_DEPTH);
	for(int i=0; i<cnt; i++){
	    auto qp_id = wc[i].wr_id;
	    auto request = mem[0]->request_buffer_pool(qp_id);
	    handle_request(request);
	    transport->prepost_recv((uint64_t)request, sizeof(request_t), qp_id);
	}
    }
}

void server_t::handle_request(request_t* request){
    auto send_ptr = mem[0]->response_buffer_pool(request->qp_id);
    response_t* response;
    switch(request->type){
        case request_type::IDX_ALLOC_NODE:{
//	    std::cout << "IDX_ALLOC_NODE request received " << std::endl;
            auto addr = allocator[request->pid]->alloc(PAGE_SIZE);
            if(addr)
                response = create_message<response_t>(send_ptr, request->qp_id, response_type::SUCCESS, addr);
            else
                response = create_message<response_t>(send_ptr, request->qp_id, response_type::FAIL, addr);
            break;
        }
	case request_type::IDX_DEALLOC_NODE:{
	    //idx_allocator->free(request->addr);
	    response = create_message<response_t>(send_ptr, request->qp_id, response_type::SUCCESS);
	    break;
	}
	case request_type::IDX_UPDATE_ROOT:{
	    assert(idx_cnt < MR_PARTITION_NUM);
	    uint64_t* addr = mem[request->pid]->root_buffer_pool(idx_cnt);
	    *addr = request->addr;
	    response = create_message<response_t>(send_ptr, request->qp_id, response_type::SUCCESS, (uint64_t)addr);
	    idx_cnt++;
	    break;
	}
	case request_type::TABLE_ALLOC_ROW:{
//	    std::cout << "TABLE_ALLOC_ROW request received " << std::endl;
	    auto addr = allocator[request->pid]->alloc(ROW_SIZE);
	    if(addr)
		response = create_message<response_t>(send_ptr, request->qp_id, response_type::SUCCESS, addr);
	    else
		response = create_message<response_t>(send_ptr, request->qp_id, response_type::FAIL, addr);
	    break;
	}
	case request_type::TABLE_DEALLOC_ROW:{
	    //row_allocator->free(request->addr);
	    response = create_message<response_t>(send_ptr, request->qp_id, response_type::SUCCESS);
	    break;
	}			      
        default:
            debug::notify_error("Unsupported request %d ... Implement me!", request->type);
    }

    transport->send((uint64_t)response, sizeof(response_t), request->qp_id);
}
