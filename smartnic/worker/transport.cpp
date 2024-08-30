#include "worker/transport.h"
#include "common/rpc.h"
#include "common/debug.h"

worker_transport_t::worker_transport_t(config_t* conf, uint64_t server_mem_pool, uint64_t server_mem_size, uint64_t client_mem_pool, uint64_t client_mem_size): conf(conf){
    bool ret = init(server_mem_pool, server_mem_size, client_mem_pool, client_mem_size);
    if(!ret)
	exit(0);
}

bool worker_transport_t::init(uint64_t server_mem_pool, uint64_t server_mem_size, uint64_t client_mem_pool, uint64_t client_mem_size){
    bool ret = false;
    context.gid_idx = 0;
    context.ctx = open_device(true);
    if(!context.ctx)
        goto CLEANUP;

    context.pd = alloc_pd(context.ctx);
    if(!context.pd)
        goto CLEANUP;

    ret = query_attr(context.ctx, context.port_attr, context.gid_idx, context.gid);
    if(!ret)
        goto CLEANUP;

    // create CQs for server
    for(int i=0; i<WORKER_THREAD_NUM; i++){
	server_cq[i] = create_cq(context.ctx);
	if(!server_cq[i])
	    goto CLEANUP;
    }

    #ifdef BATCH
    // create CQs for client
    for(int i=0; i<NETWORK_THREAD_NUM; i++){
	client_send_cq[i] = create_cq(context.ctx);
        if(!client_send_cq[i])
            goto CLEANUP;
    }
    #elif defined BATCH2
    // create CQs for client
    for(int i=0; i<WORKER_THREAD_NUM; i++){
	client_send_cq[i] = create_cq(context.ctx);
        if(!client_send_cq[i])
            goto CLEANUP;
    }
    #else
    // create CQs for client
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
	client_send_cq[i] = create_cq(context.ctx);
        if(!client_send_cq[i])
            goto CLEANUP;
    }
    #endif
    client_recv_cq = create_cq(context.ctx);
    if(!client_recv_cq)
	goto CLEANUP;

    // create QPs for server
    for(int i=0; i<WORKER_THREAD_NUM; i++){
        server_qp[i] = create_qp(context.pd, server_cq[i], server_cq[i]);
        if(!server_qp[i])
            goto CLEANUP;
        if(!modify_qp_state_to_init(server_qp[i]))
            goto CLEANUP;
    }

    #ifdef BATCH
    // create QPs for client
    for(int i=0; i<NETWORK_THREAD_NUM; i++){
        client_qp[i] = create_qp(context.pd, client_send_cq[i], client_recv_cq);
        if(!client_qp[i])
            goto CLEANUP;
        if(!modify_qp_state_to_init(client_qp[i]))
            goto CLEANUP;
    }
    #elif defined BATCH2
    for(int i=0; i<WORKER_THREAD_NUM; i++){
        client_qp[i] = create_qp(context.pd, client_send_cq[i], client_recv_cq);
        if(!client_qp[i])
            goto CLEANUP;
        if(!modify_qp_state_to_init(client_qp[i]))
            goto CLEANUP;
    }
    #else
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
        client_qp[i] = create_qp(context.pd, client_send_cq[i], client_recv_cq);
        if(!client_qp[i])
            goto CLEANUP;
        if(!modify_qp_state_to_init(client_qp[i]))
            goto CLEANUP;
    }
    #endif

    // create MR
    server_mr = register_mr(context.pd, server_mem_pool, server_mem_size);
    client_mr = register_mr(context.pd, client_mem_pool, client_mem_size);
    if(!server_mr || !client_mr)
	goto CLEANUP;

    if(!setup_connection_to_server())
        goto CLEANUP;

    if(!setup_connection_to_client())
	goto CLEANUP;

    return true;

CLEANUP:
    debug::notify_error("Error occurred during initialization --- cleaning up ...");
    if(client_mr)
	ibv_dereg_mr(client_mr);
    if(server_mr)
	ibv_dereg_mr(server_mr);

    for(int i=0; i<WORKER_THREAD_NUM; i++){
        if(server_qp[i])
            ibv_destroy_qp(server_qp[i]);
        if(server_cq[i])
            ibv_destroy_cq(server_cq[i]);
    }

    #ifdef BATCH
    for(int i=0; i<NETWORK_THREAD_NUM; i++){
        if(client_qp[i])
            ibv_destroy_qp(client_qp[i]);
        if(client_send_cq[i])
            ibv_destroy_cq(client_send_cq[i]);
    }
    #elif defined BATCH2
    for(int i=0; i<WORKER_THREAD_NUM; i++){
        if(client_qp[i])
            ibv_destroy_qp(client_qp[i]);
        if(client_send_cq[i])
            ibv_destroy_cq(client_send_cq[i]);
    }
    #else
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
        if(client_qp[i])
            ibv_destroy_qp(client_qp[i]);
        if(client_send_cq[i])
            ibv_destroy_cq(client_send_cq[i]);
    }
    #endif

    if(client_recv_cq)
        ibv_destroy_cq(client_recv_cq);

    if(context.pd)
        ibv_dealloc_pd(context.pd);

    if(context.ctx)
        ibv_close_device(context.ctx);
    return false;
}

bool worker_transport_t::setup_connection_to_server(){
    struct server_worker_meta local;
    memset(&local, 0, sizeof(struct server_worker_meta));

    local.gid_idx = context.gid_idx;
    memcpy(&local.gid, &context.gid, sizeof(union ibv_gid));
    local.lid = context.port_attr.lid;
    for(int i=0; i<WORKER_THREAD_NUM; i++)
        local.qpn[i] = server_qp[i]->qp_num;

    worker_connect(conf->get_ip(0).c_str(), &local, &server_meta);
    for(int i=0; i<WORKER_THREAD_NUM; i++){
        if(!modify_qp_state_to_rtr(server_qp[i], server_meta.gid, server_meta.gid_idx, server_meta.lid, server_meta.qpn[i]))
            return false;
        if(!modify_qp_state_to_rts(server_qp[i]))
            return false;
    }

    debug::notify_info("Connected to Memory Worker");
    return true;
}

bool worker_transport_t::setup_connection_to_client(){
    struct worker_client_meta local;
    memset(&local, 0, sizeof(struct worker_client_meta));

    local.gid_idx = context.gid_idx;
    memcpy(&local.gid, &context.gid, sizeof(union ibv_gid));
    local.lid = context.port_attr.lid;
    #ifdef BATCH
    for(int i=0; i<NETWORK_THREAD_NUM; i++)
        local.qpn[i] = client_qp[i]->qp_num;
    #elif defined BATCH2
    for(int i=0; i<WORKER_THREAD_NUM; i++)
        local.qpn[i] = client_qp[i]->qp_num;
    #else
    for(int i=0; i<CLIENT_THREAD_NUM; i++)
        local.qpn[i] = client_qp[i]->qp_num;
    #endif

    worker_listen(&local, &client_meta);

    #ifdef BATCH
    for(int i=0; i<NETWORK_THREAD_NUM; i++){
        if(!modify_qp_state_to_rtr(client_qp[i], client_meta.gid, client_meta.gid_idx, client_meta.lid, client_meta.qpn[i]))
            return false;
        if(!modify_qp_state_to_rts(client_qp[i]))
            return false;
    }
    #elif defined BATCH2
    for(int i=0; i<WORKER_THREAD_NUM; i++){
        if(!modify_qp_state_to_rtr(client_qp[i], client_meta.gid, client_meta.gid_idx, client_meta.lid, client_meta.qpn[i]))
            return false;
        if(!modify_qp_state_to_rts(client_qp[i]))
            return false;
    }
    #else
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
        if(!modify_qp_state_to_rtr(client_qp[i], client_meta.gid, client_meta.gid_idx, client_meta.lid, client_meta.qpn[i]))
            return false;
        if(!modify_qp_state_to_rts(client_qp[i]))
            return false;
    }
    #endif

    debug::notify_info("Connected to Compute Server");

    return true;
}

// RDMA ops for server communication
void worker_transport_t::prepost_recv(uint64_t ptr, int size, int qp_id){
//    debug::notify_info("REMOTE PRERECV");
    rdma_recv_prepost(server_qp[qp_id], ptr, size, server_mr->lkey);
}

void worker_transport_t::recv(uint64_t ptr, int size, int qp_id){
//    debug::notify_info("REMOTE RECV");
    rdma_recv(server_qp[qp_id], server_cq[qp_id], ptr, size, server_mr->lkey);
}

void worker_transport_t::send(uint64_t ptr, int size, int qp_id){
//    debug::notify_info("REMOTE SEND");
    rdma_send(server_qp[qp_id], server_cq[qp_id], ptr, size, server_mr->lkey);
}

void worker_transport_t::write(uint64_t src, uint64_t dest, int size, int qp_id, int pid){
//    debug::notify_info("REMOTE WRITE");
    rdma_write(server_qp[qp_id], server_cq[qp_id], src, dest, size, server_mr->lkey, server_meta.rkey[pid]);
}

void worker_transport_t::read(uint64_t src, uint64_t dest, int size, int qp_id, int pid){
//    debug::notify_info("REMOTE READ");
    rdma_read(server_qp[qp_id], server_cq[qp_id], src, dest, size, server_mr->lkey, server_meta.rkey[pid]);
}

bool worker_transport_t::cas(uint64_t src, uint64_t dest, uint64_t cmp, uint64_t swap, int size, int qp_id, int pid){
//    debug::notify_info("REMOTE CAS");
    return rdma_cas(server_qp[qp_id], server_cq[qp_id], src, dest, cmp, swap, size, server_mr->lkey, server_meta.rkey[pid]);
}

// RDMA ops for client communication
void worker_transport_t::prepost_recv_client(uint64_t ptr, int size, int qp_id){
//    debug::notify_info("CLIENT RECV");
    rdma_recv_prepost(client_qp[qp_id], ptr, size, client_mr->lkey, (uint64_t)qp_id);
}

void worker_transport_t::send_client(uint64_t ptr, int size, int qp_id){
//    debug::notify_info("CLIENT SEND size %d qp %d", size, qp_id);
    rdma_send(client_qp[qp_id], client_send_cq[qp_id], ptr, size, client_mr->lkey);
}

void worker_transport_t::send_client_(uint64_t ptr, int size, int qp_id){
//    debug::notify_info("CLIENT SEND size %d qp %d", size, qp_id);
    rdma_send_(client_qp[qp_id], ptr, size, client_mr->lkey);
}

void worker_transport_t::send_client_async(uint64_t ptr, int size, int qp_id){
//    debug::notify_info("CLIENT SEND size %d qp %d", size, qp_id);
    rdma_send(client_qp[qp_id], ptr, size, client_mr->lkey);
}

int worker_transport_t::poll_client(int qp_id, int num){
    struct ibv_wc wc[num];
    return poll_cq_once(client_send_cq[qp_id], num, wc);
}

int worker_transport_t::poll(struct ibv_wc* wc, int num){
    return poll_cq_once(client_recv_cq, num, wc);
    //return poll_cq_(client_recv_cq, num, wc);
}
