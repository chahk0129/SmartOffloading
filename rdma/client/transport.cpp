#include "client/transport.h"

client_transport_t::client_transport_t(config_t* conf, uint64_t* mem_pool, uint64_t* mem_size): conf(conf){
    bool ret = init(mem_pool, mem_size);
    if(!ret)
	exit(0);
}

bool client_transport_t::init(uint64_t* mem_pool, uint64_t* mem_size){
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
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
	send_cq[i] = create_cq(context.ctx);
        recv_cq[i] = create_cq(context.ctx);
        if(!send_cq[i] || !recv_cq[i])
            goto CLEANUP;
    }

    // create QPs
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
        qp[i] = create_qp(context.pd, send_cq[i], recv_cq[i]);
        if(!qp[i])
            goto CLEANUP;
        if(!modify_qp_state_to_init(qp[i]))
            goto CLEANUP;
    }

    // create MR
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
        mr[i] = register_mr(context.pd, mem_pool[i], mem_size[i]);
        if(!mr[i])
            goto CLEANUP;
    }

    if(!setup_connection())
        goto CLEANUP;

    return true;

CLEANUP:
    debug::notify_error("Error occurred during initialization --- cleaning up ...");
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
        if(mr[i])
            ibv_dereg_mr(mr[i]);
    }

    for(int i=0; i<CLIENT_THREAD_NUM; i++){
        if(qp[i])
            ibv_destroy_qp(qp[i]);
        if(send_cq[i])
            ibv_destroy_cq(send_cq[i]);
        if(recv_cq[i])
            ibv_destroy_cq(recv_cq[i]);
    }

    if(context.pd)
        ibv_dealloc_pd(context.pd);

    if(context.ctx)
        ibv_close_device(context.ctx);
    return false;
}

bool client_transport_t::setup_connection(){
    struct server_client_meta local;
    memset(&local, 0, sizeof(struct server_client_meta));

    local.gid_idx = context.gid_idx;
    memcpy(&local.gid, &context.gid, sizeof(union ibv_gid));
    local.lid = context.port_attr.lid;
    for(int i=0; i<CLIENT_THREAD_NUM; i++)
        local.qpn[i] = qp[i]->qp_num;

    client_connect(conf->get_ip(0).c_str(), &local, &meta);
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
        if(!modify_qp_state_to_rtr(qp[i], meta.gid, meta.gid_idx, meta.lid, meta.qpn[i]))
            return false;
        if(!modify_qp_state_to_rts(qp[i]))
            return false;
    }

    debug::notify_info("Connected to Memory Worker");
    return true;
}

void client_transport_t::prepost_recv(uint64_t ptr, int size, int qp_id){
    rdma_recv_prepost(qp[qp_id], ptr, size, mr[qp_id]->lkey);
}

void client_transport_t::recv(uint64_t ptr, int size, int qp_id){
    rdma_recv(qp[qp_id], recv_cq[qp_id], ptr, size, mr[qp_id]->lkey);
}

void client_transport_t::send(uint64_t ptr, int size, int qp_id){
    rdma_send(qp[qp_id], send_cq[qp_id], ptr, size, mr[qp_id]->lkey);
}

void client_transport_t::write(uint64_t src, uint64_t dest, int size, int qp_id, int pid){
    rdma_write(qp[qp_id], send_cq[qp_id], src, dest, size, mr[qp_id]->lkey, meta.rkey[pid]);
}

void client_transport_t::read(uint64_t src, uint64_t dest, int size, int qp_id, int pid){
    rdma_read(qp[qp_id], send_cq[qp_id], src, dest, size, mr[qp_id]->lkey, meta.rkey[pid]);
}

bool client_transport_t::cas(uint64_t src, uint64_t dest, uint64_t cmp, uint64_t swap, int size, int qp_id, int pid){
    return rdma_cas(qp[qp_id], send_cq[qp_id], src, dest, cmp, swap, size, mr[qp_id]->lkey, meta.rkey[pid]);
}
