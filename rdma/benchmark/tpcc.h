#pragma once

#include "common/global.h"
#include "system/workload.h"
#include "system/txn.h"

class base_query_t;
class table_t;
class row_t;
class tpcc_query_t;
class config_t;

class tpcc_workload_t: public workload_t{
    public:
	RC init(config_t* conf);
	RC init_table();
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

	tree_t<Key, Value>* i_item;
	tree_t<Key, Value>* i_warehouse;
	tree_t<Key, Value>* i_district;
	tree_t<Key, Value>* i_customer_id;
	tree_t<Key, Value>* i_customer_last;
	tree_t<Key, Value>* i_stock;
	tree_t<Key, Value>* i_order; 		// key = (w_id, d_id, o_id)
	tree_t<Key, Value>* i_orderline; 	// key = (w_id, d_id, o_id)
	tree_t<Key, Value>* i_orderline_wd; 	// key = (w_id, d_id)

	// XXX: HACK
	// only one txn can be delivering a warehouse at a time
	// *_delivering[w_id] --> the warehouse is delivering
	bool** delivering;

	uint64_t rpc_alloc_row(int pid, int tid);

	int key_to_part(Key key);

    private:
	uint64_t num_wh;
	void init_tab_item(int tid);
	void init_tab_wh(uint32_t wid, int tid);
	void init_tab_dist(uint64_t w_id, int tid);
	void init_tab_stock(uint64_t w_id, int tid);
	void init_tab_cust(uint64_t d_id, uint64_t w_id, int tid);
	void init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id, int tid);
	void init_tab_order(uint64_t d_id, uint64_t w_id, int tid);

	void init_permutation(uint64_t* perm_c_id, uint64_t wid);

	static void* thread_init_warehouse(void* This, int tid);
};

class tpcc_txn_man_t: public txn_man_t{
    public:
	void init(thread_t* thd, workload_t* workload, int tid);
	RC run_txn(base_query_t* base_query);

    private:
	tpcc_workload_t* workload;
	RC run_payment(tpcc_query_t* m_query);
	RC run_neworder(tpcc_query_t* m_query);
	RC run_orderstatus(tpcc_query_t* m_query);
	RC run_delivery(tpcc_query_t* m_query);
	RC run_stocklevel(tpcc_query_t* m_query);
};
