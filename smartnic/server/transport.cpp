#include "server/transport.h"

server_transport_t::server_transport_t(uint64_t* mem_pool, uint64_t* mem_size){
    bool ret = init(mem_pool, mem_size);
    if(!ret)
	exit(0);
}

bool server_transport_t::init(uint64_t* mem_pool, uint64_t* mem_size){
    bool ret = false;
    context.gid_idx = 0;
    context.ctx = open_device();
    if(!context.ctx)
	goto CLEANUP;

    context.pd = alloc_pd(context.ctx);
    if(!context.pd)
        goto CLEANUP;

    ret = query_attr(context.ctx, context.port_attr, context.gid_idx, context.gid);
    if(!ret)
	goto CLEANUP;

    // create CQs
    send_cq = create_cq(context.ctx);
    recv_cq = create_cq(context.ctx);
    if(!send_cq || !recv_cq)
	goto CLEANUP;

    // create QPs
    for(int i=0; i<WORKER_THREAD_NUM; i++){
	qp[i] = create_qp(context.pd, send_cq, recv_cq);
	if(!qp[i])
	    goto CLEANUP;
	if(!modify_qp_state_to_init(qp[i]))
	    goto CLEANUP;
    }

    // create MR
    for(int i=0; i<MR_PARTITION_NUM; i++){
	mr[i] = register_mr(context.pd, mem_pool[i], mem_size[i]);
	if(!mr[i])
	    goto CLEANUP;
    }

    return true;

CLEANUP:
    debug::notify_error("Error occurred during initialization --- cleaning up ...");

    for(int i=0; i<MR_PARTITION_NUM; i++){
	if(mr[i])
	    ibv_dereg_mr(mr[i]);
    }

    for(int i=0; i<WORKER_THREAD_NUM; i++){
        if(qp[i])
            ibv_destroy_qp(qp[i]);
    }

    if(send_cq)
        ibv_destroy_cq(send_cq);
    if(recv_cq)
        ibv_destroy_cq(recv_cq);

    if(context.pd)
        ibv_dealloc_pd(context.pd);

    if(context.ctx)
        ibv_close_device(context.ctx);
    return false;
}

bool server_transport_t::setup_connection(){
    struct server_worker_meta local;
    memset(&local, 0, sizeof(struct server_worker_meta));

    local.gid_idx = context.gid_idx;
    memcpy(&local.gid, &context.gid, sizeof(union ibv_gid));
    local.lid = context.port_attr.lid;
    for(int i=0; i<MR_PARTITION_NUM; i++)
	local.rkey[i] = mr[i]->rkey;

    for(int i=0; i<WORKER_THREAD_NUM; i++)
        local.qpn[i] = qp[i]->qp_num;

    server_listen(&local, &meta);
    for(int i=0; i<WORKER_THREAD_NUM; i++){
        if(!modify_qp_state_to_rtr(qp[i], meta.gid, meta.gid_idx, meta.lid, meta.qpn[i]))
            return false;
        if(!modify_qp_state_to_rts(qp[i]))
            return false;
    }

    debug::notify_info("Connected to Worker");
    return true;
}

// RPCs are done always with MR in 0 idx
void server_transport_t::prepost_recv(uint64_t ptr, int size, int qp_id){
    rdma_recv_prepost(qp[qp_id], ptr, size, mr[0]->lkey, (uint64_t)qp_id);
}

void server_transport_t::send(uint64_t ptr, int size, int qp_id){
    rdma_send(qp[qp_id], send_cq, ptr, size, mr[0]->lkey);
}

int server_transport_t::poll(struct ibv_wc* wc, int num){
    return poll_cq_(recv_cq, num, wc);
}

