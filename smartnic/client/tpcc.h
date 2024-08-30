#pragma once
#include "common/global.h"
#include "client/worker.h"
#include "client/txn.h"

class config_t;
class base_query_t;
class thread_t;
class table_t;
class txn_man_t;
class tpcc_query_t;

class tpcc_worker_t: public worker_t{
    public:
	RC init(config_t* conf);
	RC init_schema(std::string path);

	RC get_txn_man(txn_man_t*& txn_man, thread_t* thd);

	table_t* t_warehouse;
        table_t* t_district;
        table_t* t_customer;
        table_t* t_history;
        table_t* t_neworder;
        table_t* t_order;
        table_t* t_orderline;
        table_t* t_item;
        table_t* t_stock;
   
	int key_to_part(Key key);
};

class tpcc_txn_man_t: public txn_man_t{
    public:
	#ifdef BATCH
	void run_network_thread();
	#elif defined BATCH2
	void run_send(int tid);
	void run_recv(int tid);
	void run_network_thread();
	#endif

	void init(thread_t* thd, worker_t* worker, int tid);
	RC run_txn(base_query_t* base_query);

    private:
	tpcc_worker_t* worker;
        RC run_payment(tpcc_query_t* m_query);
        RC run_neworder(tpcc_query_t* m_query);
        RC run_orderstatus(tpcc_query_t* m_query);
        RC run_delivery(tpcc_query_t* m_query);
        RC run_stocklevel(tpcc_query_t* m_query);
};

