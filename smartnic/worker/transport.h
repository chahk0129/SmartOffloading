#pragma once
#include "net/net.h"
#include "net/config.h"
#include "common/global.h"

class worker_transport_t{
    public:
        worker_transport_t(config_t* conf, uint64_t server_mem_pool, uint64_t server_mem_size, uint64_t client_mem_pool, uint64_t client_mem_size);
        bool init(uint64_t server_mem_pool, uint64_t server_mem_size, uint64_t client_mem_pool, uint64_t client_mem_size);
        bool setup_connection_to_server();
        bool setup_connection_to_client();

	// RDMA wrappers for server communication
        void prepost_recv(uint64_t ptr, int size, int qp_id);
	void recv(uint64_t ptr, int size, int qp_id);
        void send(uint64_t ptr, int size, int qp_id);
	void read(uint64_t src, uint64_t dest, int size, int qp_id, int pid);
	void write(uint64_t src, uint64_t dest, int size, int qp_id, int pid);
	bool cas(uint64_t src, uint64_t dest, uint64_t cmp, uint64_t swap, int size, int qp_id, int pid);

	// RDMA wrappers for client communication
        void prepost_recv_client(uint64_t ptr, int size, int qp_id);
        void send_client(uint64_t ptr, int size, int qp_id);
        void send_client_(uint64_t ptr, int size, int qp_id);
        void send_client_async(uint64_t ptr, int size, int qp_id);
	int poll_client(int qp_id, int num);
	int poll(struct ibv_wc* wc, int num);

    private:
        struct rdma_ctx context;
        struct server_worker_meta server_meta;
        struct worker_client_meta client_meta;

	// QPs and CQs for server communication
        struct ibv_qp* server_qp[WORKER_THREAD_NUM];
        struct ibv_cq* server_cq[WORKER_THREAD_NUM];

	// QPs and CQs for client communication
	#ifdef BATCH
        struct ibv_qp* client_qp[NETWORK_THREAD_NUM];
        struct ibv_cq* client_send_cq[NETWORK_THREAD_NUM];
	#elif defined BATCH2
        struct ibv_qp* client_qp[WORKER_THREAD_NUM];
        struct ibv_cq* client_send_cq[WORKER_THREAD_NUM];
	#else
        struct ibv_qp* client_qp[CLIENT_THREAD_NUM];
        struct ibv_cq* client_send_cq[CLIENT_THREAD_NUM];
	#endif
        struct ibv_cq* client_recv_cq;

	// MRs
        struct ibv_mr* server_mr;
	struct ibv_mr* client_mr;

	config_t* conf;

};
