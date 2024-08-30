#pragma once
#include "common/global.h"
#include "net/config.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>

#include <malloc.h>
#include <cstdint>
#include <unistd.h>

#define QP_DEPTH 64
//#define QP_DEPTH 128
#define IB_PORT 1
#define TCP_PORT 2123

struct rdma_ctx{
    struct ibv_context* ctx;
    struct ibv_pd* pd;
    struct ibv_port_attr port_attr;
    int gid_idx;
    union ibv_gid gid;
};

struct server_client_meta{
    int sock;
    int gid_idx;
    union ibv_gid gid;
    uint32_t lid;
    uint32_t rkey[MR_PARTITION_NUM];
    uint32_t qpn[CLIENT_THREAD_NUM];
};

// src/net/resource.cpp
struct ibv_context* open_device();
struct ibv_pd* alloc_pd(struct ibv_context* ctx);
struct ibv_cq* create_cq(struct ibv_context* ctx);
struct ibv_qp* create_qp(struct ibv_pd* pd, struct ibv_cq* send_cq, struct ibv_cq* recv_cq);
struct ibv_mr* register_mr(struct ibv_pd* pd, uint64_t mm, uint64_t mm_size);

bool query_attr(struct ibv_context* ctx, struct ibv_port_attr& attr, int& gid_idx, union ibv_gid& gid);
bool modify_qp_state_to_init(struct ibv_qp* qp);
bool modify_qp_state_to_rtr(struct ibv_qp* qp, union ibv_gid gid, int gid_idx, uint32_t lid, uint32_t qpn);
bool modify_qp_state_to_rts(struct ibv_qp* qp);

// src/net/connection.cpp
void server_listen(struct server_client_meta* local, struct server_client_meta* remote);
void client_connect(const char* ip, struct server_client_meta* local, struct server_client_meta* remote);

bool connect_qp(int sock, struct server_client_meta* local, struct server_client_meta* remote);

int socket_data_sync(int sock, int size, char* local, char* remote);
int socket_connect(const char* ip);

// src/net/operation.cpp
bool post_send(struct ibv_qp* qp, uint64_t src, int size, uint32_t lkey);
bool post_send_batch(struct ibv_qp* qp, uint64_t src, int size, uint32_t lkey, int batch_size);
bool post_send_imm(struct ibv_qp* qp, uint64_t src, int size, uint32_t lkey, uint32_t imm_data);

bool post_recv(struct ibv_qp* qp, uint64_t src, int size, uint32_t lkey);
bool post_recv(struct ibv_qp* qp, uint64_t src, int size, uint32_t lkey, uint64_t wr_id);
bool post_recv_batch(struct ibv_qp* qp, uint64_t src, int size, uint32_t lkey, uint64_t wr_id, int batch_size);

bool post_read(struct ibv_qp* qp, uint64_t src, uint64_t dest, int size, uint32_t lkey, uint32_t rkey);
bool post_read(struct ibv_qp* qp, uint64_t src, uint64_t dest, int size, uint32_t lkey, uint32_t rkey, int batch_size);

bool post_write(struct ibv_qp* qp, uint64_t src, uint64_t dest, int size, uint32_t lkey, uint32_t rkey);
bool post_write(struct ibv_qp* qp, uint64_t src, uint64_t dest, int size, uint32_t lkey, uint32_t rkey, int batch_size);

bool post_cas(struct ibv_qp* qp, uint64_t src, uint64_t dest, uint64_t cmp, uint64_t swp, int size, uint32_t lkey, uint32_t rkey);

int poll_cq(struct ibv_cq* cq, int num, struct ibv_wc* wc);
int poll_cq_(struct ibv_cq* cq, int num, struct ibv_wc* wc);
int poll_cq_once(struct ibv_cq* cq, int num, struct ibv_wc* wc);

bool rdma_cas(struct ibv_qp* qp, struct ibv_cq* cq, uint64_t src, uint64_t dest, uint64_t cmp, uint64_t swp, int size, uint32_t lkey, uint32_t rkey);
void rdma_read(struct ibv_qp* qp, struct ibv_cq* cq, uint64_t src, uint64_t dest, int size, uint32_t lkey, uint32_t rkey);
void rdma_write(struct ibv_qp* qp, struct ibv_cq* cq, uint64_t src, uint64_t dest, int size, uint32_t lkey, uint32_t rkey);
void rdma_send(struct ibv_qp* qp, struct ibv_cq* cq, uint64_t ptr, int size, uint32_t lkey);
void rdma_recv(struct ibv_qp* qp, struct ibv_cq* cq, uint64_t ptr, int size, uint32_t lkey);
void rdma_recv(struct ibv_qp* qp, struct ibv_cq* cq, uint64_t ptr, int size, uint32_t lkey, uint64_t wr_id);
void rdma_recv_prepost(struct ibv_qp* qp, uint64_t ptr, int size, uint32_t lkey);
void rdma_recv_prepost(struct ibv_qp* qp, uint64_t ptr, int size, uint32_t lkey, uint64_t wr_id);
