#include "client/transport.h"

client_transport_t::client_transport_t(config_t* conf, uint64_t* mem_pool, uint64_t* mem_size): conf(conf){
    bool ret = init(mem_pool, mem_size);
    if(!ret){
	debug::notify_info("Failed to setup client transport!");
	exit(0);
    }
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

    #ifdef BATCH
    // create CQs
    for(int i=0; i<NETWORK_THREAD_NUM; i++){
	send_cq[i] = create_cq(context.ctx);
        recv_cq[i] = create_cq(context.ctx);
        if(!send_cq[i] || !recv_cq[i])
            goto CLEANUP;
    }

    // create QPs
    for(int i=0; i<NETWORK_THREAD_NUM; i++){
        qp[i] = create_qp(context.pd, send_cq[i], recv_cq[i]);
        if(!qp[i])
            goto CLEANUP;
        if(!modify_qp_state_to_init(qp[i]))
            goto CLEANUP;
    }

    // create MR
    for(int i=0; i<NETWORK_THREAD_NUM; i++){
        mr[i] = register_mr(context.pd, mem_pool[i], mem_size[i]);
        if(!mr[i])
            goto CLEANUP;
    }
    #elif defined BATCH2
    // create CQs
    for(int i=0; i<WORKER_THREAD_NUM; i++){
	send_cq[i] = create_cq(context.ctx);
        if(!send_cq[i])
            goto CLEANUP;
    }
    recv_cq = create_cq(context.ctx);
    if(!recv_cq)
	goto CLEANUP;

    // create QPs
    for(int i=0; i<WORKER_THREAD_NUM; i++){
        qp[i] = create_qp(context.pd, send_cq[i], recv_cq);
        if(!qp[i])
            goto CLEANUP;
        if(!modify_qp_state_to_init(qp[i]))
            goto CLEANUP;
    }

    // create MR
    mr = register_mr(context.pd, *mem_pool, *mem_size);
    if(!mr)
	goto CLEANUP;
    /*
    for(int i=0; i<WORKER_THREAD_NUM; i++){
        mr[i] = register_mr(context.pd, mem_pool[i], mem_size[i]);
        if(!mr[i])
            goto CLEANUP;
    }
    */

    #else
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
    #endif

    if(!setup_connection())
        goto CLEANUP;

    return true;

CLEANUP:
    debug::notify_error("Error occurred during initialization --- cleaning up ...");
    #ifdef BATCH
    for(int i=0; i<NETWORK_THREAD_NUM; i++){
        if(mr[i])
            ibv_dereg_mr(mr[i]);
    }

    for(int i=0; i<NETWORK_THREAD_NUM; i++){
        if(qp[i])
            ibv_destroy_qp(qp[i]);
        if(send_cq[i])
            ibv_destroy_cq(send_cq[i]);
        if(recv_cq[i])
            ibv_destroy_cq(recv_cq[i]);
    }
    #elif defined BATCH2
    if(mr)
	ibv_dereg_mr(mr);
    /*
    for(int i=0; i<WORKER_THREAD_NUM; i++){
        if(mr[i])
            ibv_dereg_mr(mr[i]);
    }
    */

    for(int i=0; i<WORKER_THREAD_NUM; i++){
        if(qp[i])
            ibv_destroy_qp(qp[i]);
        if(send_cq[i])
            ibv_destroy_cq(send_cq[i]);
    }
    if(recv_cq)
	ibv_destroy_cq(recv_cq);

    #else
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
    #endif

    if(context.pd)
        ibv_dealloc_pd(context.pd);

    if(context.ctx)
        ibv_close_device(context.ctx);
    return false;
}

bool client_transport_t::setup_connection(){
    struct worker_client_meta local;
    memset(&local, 0, sizeof(struct worker_client_meta));

    local.gid_idx = context.gid_idx;
    memcpy(&local.gid, &context.gid, sizeof(union ibv_gid));
    local.lid = context.port_attr.lid;
    #ifdef BATCH 
    for(int i=0; i<NETWORK_THREAD_NUM; i++)
        local.qpn[i] = qp[i]->qp_num;
    #elif defined BATCH2
    for(int i=0; i<WORKER_THREAD_NUM; i++)
        local.qpn[i] = qp[i]->qp_num;
    #else
    for(int i=0; i<CLIENT_THREAD_NUM; i++)
        local.qpn[i] = qp[i]->qp_num;
    #endif

    client_connect(conf->get_ip(0).c_str(), &local, &meta);
    #ifdef BATCH
    for(int i=0; i<NETWORK_THREAD_NUM; i++){
        if(!modify_qp_state_to_rtr(qp[i], meta.gid, meta.gid_idx, meta.lid, meta.qpn[i]))
            return false;
        if(!modify_qp_state_to_rts(qp[i]))
            return false;
    }
    #elif defined BATCH2
    for(int i=0; i<WORKER_THREAD_NUM; i++){
        if(!modify_qp_state_to_rtr(qp[i], meta.gid, meta.gid_idx, meta.lid, meta.qpn[i]))
            return false;
        if(!modify_qp_state_to_rts(qp[i]))
            return false;
    }
    #else
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
        if(!modify_qp_state_to_rtr(qp[i], meta.gid, meta.gid_idx, meta.lid, meta.qpn[i]))
            return false;
        if(!modify_qp_state_to_rts(qp[i]))
            return false;
    }
    #endif

    debug::notify_info("Connected to Memory Worker");
    return true;
}

#ifdef BATCH2
void client_transport_t::prepost_recv(uint64_t ptr, int size, int qp_id, uint64_t wr_id){
    rdma_recv_prepost(qp[qp_id], ptr, size, mr->lkey, wr_id);
}

void client_transport_t::prepost_recv(uint64_t ptr, int size, int qp_id){
    rdma_recv_prepost(qp[qp_id], ptr, size, mr->lkey);
}

void client_transport_t::recv(uint64_t ptr, int size, int qp_id){
    rdma_recv(qp[qp_id], recv_cq, ptr, size, mr->lkey);
}

void client_transport_t::send(uint64_t ptr, int size, int qp_id){
    rdma_send(qp[qp_id], send_cq[qp_id], ptr, size, mr->lkey);
}

void client_transport_t::send_async(uint64_t ptr, int size, int qp_id){
    rdma_send(qp[qp_id], ptr, size, mr->lkey);
}

int client_transport_t::poll_sendcq(int num, int qp_id){
    struct ibv_wc wc[num];
    return poll_cq_once(send_cq[qp_id], num, wc);
}

int client_transport_t::poll(int num, int qp_id){
    struct ibv_wc wc[num];
    return poll_cq_once(recv_cq, num, wc);
}

int client_transport_t::poll(int num, int qp_id, struct ibv_wc* wc){
    return poll_cq_once(recv_cq, num, wc);
}
#else
void client_transport_t::recv(uint64_t ptr, int size, int qp_id){
    rdma_recv(qp[qp_id], recv_cq[qp_id], ptr, size, mr[qp_id]->lkey);
}

void client_transport_t::prepost_recv(uint64_t ptr, int size, int qp_id, uint64_t wr_id){
    rdma_recv_prepost(qp[qp_id], ptr, size, mr[qp_id]->lkey, wr_id);
}

void client_transport_t::prepost_recv(uint64_t ptr, int size, int qp_id){
    rdma_recv_prepost(qp[qp_id], ptr, size, mr[qp_id]->lkey);
}

void client_transport_t::send(uint64_t ptr, int size, int qp_id){
    rdma_send(qp[qp_id], send_cq[qp_id], ptr, size, mr[qp_id]->lkey);
}

void client_transport_t::send_async(uint64_t ptr, int size, int qp_id){
    rdma_send(qp[qp_id], ptr, size, mr[qp_id]->lkey);
}

int client_transport_t::poll_sendcq(int num, int qp_id){
    struct ibv_wc wc[num];
    return poll_cq_once(send_cq[qp_id], num, wc);
}

int client_transport_t::poll(int num, int qp_id){
    struct ibv_wc wc[num];
    return poll_cq_once(recv_cq[qp_id], num, wc);
}

int client_transport_t::poll(int num, int qp_id, struct ibv_wc* wc){
    return poll_cq_once(recv_cq[qp_id], num, wc);
}

#endif


