#pragma once
#include "net/net.h"
#include "common/global.h"

class server_transport_t{
    public:
	server_transport_t(uint64_t* mem_pool, uint64_t* mem_size);
	bool init(uint64_t* mem_pool, uint64_t* mem_size);
	bool setup_connection();

	void prepost_recv(uint64_t ptr, int size, int qp_id);
	void send(uint64_t ptr, int size, int qp_id);
	int poll(struct ibv_wc* wc, int num);

    private:
	struct rdma_ctx context;
	struct server_client_meta meta;
	struct ibv_qp* qp[CLIENT_THREAD_NUM];
	struct ibv_cq* send_cq;
	struct ibv_cq* recv_cq;
	struct ibv_mr* mr[MR_PARTITION_NUM];
};




