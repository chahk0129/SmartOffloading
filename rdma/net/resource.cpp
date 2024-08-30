#include "net/net.h"

struct ibv_context* open_device(){
    struct ibv_device** dev_list;
    struct ibv_device* dev;
    int flags;
    int dev_num;

    dev_list = ibv_get_device_list(&dev_num);
    if(!dev_list || !dev_num){
	debug::notify_error("Failed to get ibv_get_device_list");
	return nullptr;
    }

    for(int i=0; i<dev_num; i++){
	if(ibv_get_device_name(dev_list[i])[5] == '2'){ // open mlx5_2
	    dev = dev_list[i];
	    debug::notify_info("Opening %s\n", ibv_get_device_name(dev_list[i]));
	    break;
	}
    }

    if(!dev){
	debug::notify_error("Failed to find IB device");
	return nullptr;
    }

    auto ctx = ibv_open_device(dev);
    if(!ctx){
	debug::notify_error("Failed to ibv_open_device");
	return nullptr;
    }

    ibv_free_device_list(dev_list);
    return ctx;
}

struct ibv_pd* alloc_pd(struct ibv_context* ctx){
    auto pd = ibv_alloc_pd(ctx);
    if(!pd){
	debug::notify_error("Failed to ibv_alloc_pd");
	return nullptr;
    }
    return pd;
}

bool query_attr(struct ibv_context* ctx, struct ibv_port_attr& attr, int& gid_idx, union ibv_gid& gid){
    memset(&attr, 0, sizeof(attr));

    if(ibv_query_port(ctx, IB_PORT, &attr)){
	debug::notify_error("Failed to ibv_query_port");
	return false;
    }

    if(ibv_query_gid(ctx, IB_PORT, gid_idx, &gid)){
	debug::notify_error("Failed to ibv_query_gid");
	return false;
    }
    return true;
}

struct ibv_cq* create_cq(struct ibv_context* ctx){
    auto cq = ibv_create_cq(ctx, QP_DEPTH, NULL, NULL, 0);
    if(!cq){
	debug::notify_error("Failed to ibv_create_cq");
	return nullptr;
    }
    return cq;
}

struct ibv_qp* create_qp(struct ibv_pd* pd, struct ibv_cq* send_cq, struct ibv_cq* recv_cq){
    struct ibv_qp_init_attr attr;
    memset(&attr, 0, sizeof(attr));

    attr.cap.max_send_wr = QP_DEPTH;
    attr.cap.max_recv_wr = QP_DEPTH;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = 256;
    attr.qp_type = IBV_QPT_RC;

    attr.send_cq = send_cq;
    attr.recv_cq = recv_cq;
    auto qp = ibv_create_qp(pd, &attr);
    if(!qp){
	debug::notify_error("Failed to ibv_create_qp");
	return nullptr;
    }
    return qp;
}

struct ibv_mr* register_mr(struct ibv_pd* pd, uint64_t mm, uint64_t mm_size){
    int flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    auto mr = ibv_reg_mr(pd, (void*)mm, mm_size, flags);
    if(!mr){
	debug::notify_error("Failed to ibv_reg_mr");
	return nullptr;
    }
    return mr;
}

bool modify_qp_state_to_init(struct ibv_qp* qp){
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_INIT;
    attr.pkey_index = 0;
    attr.port_num = IB_PORT;
    attr.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    auto ret = ibv_modify_qp(qp, &attr, flags);
    if(ret){
	debug::notify_error("Failed to ibv_modify_qp to INIT");
	return false;
    }
    return true;
}

bool modify_qp_state_to_rtr(struct ibv_qp* qp, union ibv_gid gid, int gid_idx, uint32_t lid, uint32_t qpn){
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_1024;
    attr.rq_psn = 3185;

    attr.ah_attr.dlid = lid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = IB_PORT;
    attr.ah_attr.is_global = 1;
    memcpy(&attr.ah_attr.grh.dgid , &gid, sizeof(union ibv_gid));
    attr.ah_attr.grh.flow_label = 0;
    attr.ah_attr.grh.hop_limit = 1;
    attr.ah_attr.grh.sgid_index = gid_idx;
    attr.ah_attr.grh.traffic_class = 0;

    attr.max_dest_rd_atomic = 16;
    attr.min_rnr_timer = 12;
    attr.dest_qp_num = qpn;

    int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    auto ret = ibv_modify_qp(qp, &attr, flags);
    if(ret){
	debug::notify_error("Failed to ibv_modify_qp to RTR --- errno(%d) %s", errno, std::strerror(errno));
	return false;
    }
    return true;
}

bool modify_qp_state_to_rts(struct ibv_qp* qp){
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = 3185;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.max_rd_atomic = 16;
    attr.max_dest_rd_atomic = 16;

    int flags = IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;
    auto ret = ibv_modify_qp(qp, &attr, flags);
    if(ret){
	debug::notify_error("Failed to ibv_modify_qp to RTS");
	return false;
    }
    return true;
}

