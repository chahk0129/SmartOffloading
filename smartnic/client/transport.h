#pragma once
#include "net/net.h"
#include "net/config.h"
#include "common/global.h"

class client_transport_t{
    public:
        client_transport_t(config_t* conf, uint64_t* mem_pool, uint64_t* mem_size);
        bool init(uint64_t* mem_pool, uint64_t* mem_size);
        bool setup_connection();

	void prepost_recv(uint64_t ptr, int size, int qp_id, uint64_t wr_id);
        void prepost_recv(uint64_t ptr, int size, int qp_id);
	void recv(uint64_t ptr, int size, int qp_id);
        void send(uint64_t ptr, int size, int qp_id);
        void send_async(uint64_t ptr, int size, int qp_id);
	//void read(uint64_t src, uint64_t dest, int size, int qp_id, int pid);
	//void write(uint64_t src, uint64_t dest, int size, int qp_id, int pid);
	//bool cas(uint64_t src, uint64_t dest, uint64_t cmp, uint64_t swap, int size, int qp_id, int pid);
	int poll_sendcq(int num, int qp_id);
	int poll(int num, int qp_id);
	int poll(int num, int qp_id, struct ibv_wc* wc);

    private:
        struct rdma_ctx context;
        struct worker_client_meta meta;
	#ifdef BATCH
        struct ibv_qp* qp[NETWORK_THREAD_NUM];
        struct ibv_cq* send_cq[NETWORK_THREAD_NUM];
        struct ibv_cq* recv_cq[NETWORK_THREAD_NUM];
        struct ibv_mr* mr[NETWORK_THREAD_NUM];
	#elif defined BATCH2
        struct ibv_qp* qp[WORKER_THREAD_NUM];
        struct ibv_cq* send_cq[WORKER_THREAD_NUM];
        struct ibv_cq* recv_cq;
        struct ibv_mr* mr;
        //struct ibv_mr* mr[WORKER_THREAD_NUM];
	#else
        struct ibv_qp* qp[CLIENT_THREAD_NUM];
        struct ibv_cq* send_cq[CLIENT_THREAD_NUM];
        struct ibv_cq* recv_cq[CLIENT_THREAD_NUM];
        struct ibv_mr* mr[CLIENT_THREAD_NUM];
	#endif

	config_t* conf;
};




