#include "net/net.h"
#include <string.h>

bool post_send(struct ibv_qp* qp, uint64_t src, int size, uint32_t lkey){
    struct ibv_sge list;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wr_bad;

    memset(&list, 0, sizeof(list));
    memset(&wr, 0, sizeof(wr));

    list.addr = (uintptr_t)src;
    list.length = size;
    list.lkey = lkey;

    wr.sg_list = &list;
    wr.num_sge = 1;
    wr.opcode     = IBV_WR_SEND;
    #ifdef BATCH2
    wr.send_flags = IBV_SEND_SIGNALED;
    if(size <= 1024)
	wr.send_flags |= IBV_SEND_INLINE;
    #else
    if(size <= 1024)
	wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
    else
	wr.send_flags = IBV_SEND_SIGNALED;
    #endif

    if(ibv_post_send(qp, &wr, &wr_bad)){
        debug::notify_error("Failed to ibv_post_send (RDMA SEND)");
        return false;
    }
    return true;
}

bool post_send_(struct ibv_qp* qp, uint64_t src, int size, uint32_t lkey){
    struct ibv_sge list;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wr_bad;

    memset(&list, 0, sizeof(list));
    memset(&wr, 0, sizeof(wr));

    list.addr = (uintptr_t)src;
    list.length = size;
    list.lkey = lkey;

    wr.sg_list = &list;
    wr.num_sge = 1;
    wr.opcode     = IBV_WR_SEND;
    if(size <= 1024)
	wr.send_flags = IBV_SEND_INLINE;

    if(ibv_post_send(qp, &wr, &wr_bad)){
        debug::notify_error("Failed to ibv_post_send (RDMA SEND)");
        return false;
    }
    return true;
}



bool post_send_imm(struct ibv_qp* qp, uint64_t src, int size, uint32_t lkey, uint32_t imm_data){
    struct ibv_sge list;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wr_bad;

    memset(&list, 0, sizeof(list));
    memset(&wr, 0, sizeof(wr));

    list.addr = (uintptr_t)src;
    list.length = size;
    list.lkey = lkey;

    wr.sg_list = &list;
    wr.num_sge = 1;
    wr.imm_data   = imm_data;
    wr.opcode     = IBV_WR_SEND_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;

    if(ibv_post_send(qp, &wr, &wr_bad)){
        debug::notify_error("Failed to ibv_post_send (RDMA SEND)");
        return false;
    }
    return true;
}

bool post_send_batch(struct ibv_qp* qp, uint64_t src, int size, uint32_t lkey, int batch_size){
    struct ibv_sge list[batch_size];
    struct ibv_send_wr wr[batch_size];
    struct ibv_send_wr* wr_bad;

    memset(list, 0, sizeof(struct ibv_sge) * batch_size);
    memset(wr, 0, sizeof(struct ibv_send_wr) * batch_size);

    for(int i=0; i<batch_size; i++){
        list[i].addr = (uintptr_t)(src + i*size);
        list[i].length = size;
        list[i].lkey = lkey;

        wr[i].sg_list = &list[i];
        wr[i].num_sge = 1;
	if(i == batch_size-1){
	    wr[i].opcode = IBV_WR_SEND;
	    wr[i].next = nullptr;
	    wr[i].send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
	}
	else{
	    wr[i].opcode = IBV_WR_SEND;
	    wr[i].next = &wr[i+1];
	    wr[i].send_flags = 0;
	}
    }

    if(ibv_post_send(qp, &wr[0], &wr_bad)){
        debug::notify_error("Failed to ibv_post_send (RDMA SEND BATCH)");
        return false;
    }
    return true;
}

bool post_recv(struct ibv_qp* qp, uint64_t src, int size, uint32_t lkey, uint64_t wr_id){
    struct ibv_sge list;
    struct ibv_recv_wr wr;
    struct ibv_recv_wr* wr_bad;

    memset(&list, 0, sizeof(list));
    memset(&wr, 0, sizeof(wr));

    list.addr = (uintptr_t)src;
    list.length = size;
    list.lkey = lkey;

    wr.wr_id = wr_id;
    wr.sg_list = &list;
    wr.num_sge = 1;

    if(ibv_post_recv(qp, &wr, &wr_bad)){
        debug::notify_error("Failed to ibv_post_recv (RDMA RECV)");
        return false;
    }
    return true;
}


bool post_recv(struct ibv_qp* qp, uint64_t src, int size, uint32_t lkey){
    struct ibv_sge list;
    struct ibv_recv_wr wr;
    struct ibv_recv_wr* wr_bad;

    memset(&list, 0, sizeof(list));
    memset(&wr, 0, sizeof(wr));

    list.addr = (uintptr_t)src;
    list.length = size;
    list.lkey = lkey;

    wr.sg_list = &list;
    wr.num_sge = 1;

    if(ibv_post_recv(qp, &wr, &wr_bad)){
        debug::notify_error("Failed to ibv_post_recv (RDMA RECV)");
        return false;
    }
    return true;
}

bool post_recv_batch(struct ibv_qp* qp, uint64_t src, int size, uint32_t lkey, uint64_t wr_id, int batch_size){
    struct ibv_sge list[batch_size];
    struct ibv_recv_wr wr[batch_size];
    struct ibv_recv_wr* wr_bad;

    memset(list, 0, sizeof(struct ibv_sge) * batch_size);
    memset(wr, 0, sizeof(struct ibv_recv_wr) * batch_size);

    for(int i=0; i<batch_size; i++){
        list[i].addr = (uintptr_t)(src + i*size);
        list[i].length = size;
        list[i].lkey = lkey;

        wr[i].wr_id = wr_id;
        wr[i].sg_list = &list[i];
        wr[i].num_sge = 1;
        wr[i].next = (i == batch_size-1) ? NULL : &wr[i+1];
    }

    if(ibv_post_recv(qp, &wr[0], &wr_bad)){
        debug::notify_error("Failed to ibv_post_recv (RDMA RECV BATCH)"); 
        return false;
    }
    return true;
}

bool post_cas(struct ibv_qp* qp, uint64_t src, uint64_t dest, uint64_t cmp, uint64_t swp, int size, uint32_t lkey, uint32_t rkey){
    struct ibv_sge list;
    struct ibv_send_wr wr;
    struct ibv_send_wr* wr_bad;

    memset(&list, 0, sizeof(list));
    memset(&wr, 0, sizeof(wr));

    list.addr = (uintptr_t)src;
    list.length = size;
    list.lkey = lkey;

    wr.wr_id = 0;
    wr.sg_list = &list;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = dest;
    wr.wr.atomic.compare_add = cmp;
    wr.wr.atomic.swap = swp;
    wr.wr.atomic.rkey = rkey;

    if(ibv_post_send(qp, &wr, &wr_bad)){
        debug::notify_error("Failed to ibv_post_send (RDMA CAS)");
        return false;
    }
    return true;
}


bool post_read(struct ibv_qp* qp, uint64_t src, uint64_t dest, int size, uint32_t lkey, uint32_t rkey){
    struct ibv_sge list;
    struct ibv_send_wr wr;
    struct ibv_send_wr* wr_bad;

    memset(&list, 0, sizeof(list));
    memset(&wr, 0, sizeof(wr));

    list.addr = (uintptr_t)src;
    list.length = size;
    list.lkey = lkey;

    wr.wr_id = 0;
    wr.sg_list = &list;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    //wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
    wr.wr.rdma.remote_addr = dest;
    wr.wr.rdma.rkey = rkey;

    if(ibv_post_send(qp, &wr, &wr_bad)){
        debug::notify_error("Failed to ibv_post_send (RDMA READ)");
        return false;
    }
    return true;
}

bool post_read(struct ibv_qp* qp, uint64_t src, uint64_t dest, int size, uint32_t lkey, uint32_t rkey, int batch_size){
    struct ibv_sge list[batch_size];
    struct ibv_send_wr wr[batch_size];
    struct ibv_send_wr* wr_bad;

    memset(list, 0, sizeof(struct ibv_sge) * batch_size);
    memset(wr, 0, sizeof(struct ibv_send_wr) * batch_size);

    for(int i=0; i<batch_size; i++){
        list[i].addr = (uintptr_t)(src + i*size);
        list[i].length = size;
        list[i].lkey = lkey;

        wr[i].wr_id = 0;
        wr[i].sg_list = &list[i];
        wr[i].opcode = IBV_WR_RDMA_READ;
        wr[i].next = (i == batch_size-1) ? NULL : &wr[i+1];
        wr[i].send_flags = (i == batch_size-1) ? IBV_SEND_SIGNALED | IBV_SEND_INLINE : 0;
        wr[i].wr.rdma.remote_addr = dest + i*size;
        wr[i].wr.rdma.rkey = rkey;
    }

    if(ibv_post_send(qp, &wr[0], &wr_bad)){
        debug::notify_error("Failed to ibv_post_send (RDMA READ BATCH)"); 
        return false;
    }
    return true;
}

bool post_write(struct ibv_qp* qp, uint64_t src, uint64_t dest, int size, uint32_t lkey, uint32_t rkey){
    struct ibv_sge list;
    struct ibv_send_wr wr;
    struct ibv_send_wr* wr_bad;

    memset(&list, 0, sizeof(list));
    memset(&wr, 0, sizeof(wr));

    list.addr = (uintptr_t)src;
    list.length = size;
    list.lkey = lkey;

    wr.wr_id = 0;
    wr.sg_list = &list;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED;
    //wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
    wr.wr.rdma.remote_addr = dest;
    wr.wr.rdma.rkey = rkey;

    if(ibv_post_send(qp, &wr, &wr_bad)){
        debug::notify_error("Failed to ibv_post_send (RDMA WRITE)");
        return false;
    }
    return true;
}

bool post_write(struct ibv_qp* qp, uint64_t src, uint64_t dest, int size, uint32_t lkey, uint32_t rkey, int batch_size){
    struct ibv_sge list[batch_size];
    struct ibv_send_wr wr[batch_size];
    struct ibv_send_wr* wr_bad;

    memset(list, 0, sizeof(struct ibv_sge) * batch_size);
    memset(wr, 0, sizeof(struct ibv_send_wr) * batch_size);

    for(int i=0; i<batch_size; i++){
        list[i].addr = (uintptr_t)(src + i*size);
        list[i].length = size;
        list[i].lkey = lkey;

        wr[i].wr_id = 0;
        wr[i].sg_list = &list[i];
        wr[i].opcode = IBV_WR_RDMA_WRITE;
        wr[i].next = (i == batch_size-1) ? NULL : &wr[i+1];
        wr[i].send_flags = (i == batch_size-1) ? IBV_SEND_SIGNALED | IBV_SEND_INLINE : 0;
        wr[i].wr.rdma.remote_addr = dest + i*size;
        wr[i].wr.rdma.rkey = rkey;
    }

    if(ibv_post_send(qp, &wr[0], &wr_bad)){
        debug::notify_error("Failed to ibv_post_send (RDMA WRITE BATCH)"); 
        return false;
    }
    return true;
}

int poll_cq(struct ibv_cq* cq, int num, struct ibv_wc* wc){
    int cnt = 0;
    while(cnt < num)
	cnt += ibv_poll_cq(cq, 1, wc);

    if(wc->status != IBV_WC_SUCCESS){
	debug::notify_error("Failed to ibv_poll_cq ---- status %s (%d)", ibv_wc_status_str(wc->status), wc->status);
	return -1;
    }
    return cnt;
}

int poll_cq_(struct ibv_cq* cq, int num, struct ibv_wc* wc){
    int cnt = 0;
    do{
	cnt = ibv_poll_cq(cq, 1, wc);
    }while(cnt == 0);

    return cnt;
}


int poll_cq_once(struct ibv_cq* cq, int num, struct ibv_wc* wc){
    int cnt = ibv_poll_cq(cq, num, wc);
    if(cnt < 0)
	debug::notify_error("Failed to ibv_poll_cq once ---- status %s (%d)", ibv_wc_status_str(wc->status), wc->status);

    return cnt;
}

bool rdma_cas(struct ibv_qp* qp, struct ibv_cq* cq, uint64_t src, uint64_t dest, uint64_t cmp, uint64_t swp, int size, uint32_t lkey, uint32_t rkey){
    struct ibv_wc wc;
    post_cas(qp, src, dest, cmp, swp, size, lkey, rkey);
    poll_cq(cq, 1, &wc);
    return cmp == *(uint64_t*)src;
    //return ret;
}

void rdma_read(struct ibv_qp* qp, struct ibv_cq* cq, uint64_t src, uint64_t dest, int size, uint32_t lkey, uint32_t rkey){
    struct ibv_wc wc;
    post_read(qp, src, dest, size, lkey, rkey);
    poll_cq(cq, 1, &wc);
}

void rdma_write(struct ibv_qp* qp, struct ibv_cq* cq, uint64_t src, uint64_t dest, int size, uint32_t lkey, uint32_t rkey){
    struct ibv_wc wc;
    post_write(qp, src, dest, size, lkey, rkey);
    poll_cq(cq, 1, &wc);
}

void rdma_send(struct ibv_qp* qp, struct ibv_cq* cq, uint64_t ptr, int size, uint32_t lkey){
    struct ibv_wc wc;
    post_send(qp, ptr, size, lkey);
    poll_cq(cq, 1, &wc);
}

void rdma_send_(struct ibv_qp* qp, uint64_t ptr, int size, uint32_t lkey){
    post_send(qp, ptr, size, lkey);
}

void rdma_send(struct ibv_qp* qp, uint64_t ptr, int size, uint32_t lkey){
    post_send(qp, ptr, size, lkey);
}

void rdma_recv(struct ibv_qp* qp, struct ibv_cq* cq, uint64_t ptr, int size, uint32_t lkey){
    struct ibv_wc wc;
    post_recv(qp, ptr, size, lkey);
    poll_cq(cq, 1, &wc);
}

void rdma_recv(struct ibv_qp* qp, struct ibv_cq* cq, uint64_t ptr, int size, uint32_t lkey, uint64_t wr_id){
    struct ibv_wc wc;
    post_recv(qp, ptr, size, lkey, wr_id);
    poll_cq(cq, 1, &wc);
}

void rdma_recv_prepost(struct ibv_qp* qp, uint64_t ptr, int size, uint32_t lkey){
    post_recv(qp, ptr, size, lkey);
}

void rdma_recv_prepost(struct ibv_qp* qp, uint64_t ptr, int size, uint32_t lkey, uint64_t wr_id){
    post_recv(qp, ptr, size, lkey, wr_id);
}
