#pragma once

#include "common/global.h"
#include "worker/worker.h"
#include "worker/txn.h"

class base_query_t;
class table_t;
class row_t;
class tpcc_query_t;
class config_t;
class indirection_table_t;
class page_table_t;

class tpcc_worker_t: public worker_t{
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
        tree_t<Key, Value>* i_order;            // key = (w_id, d_id, o_id)
        tree_t<Key, Value>* i_orderline;        // key = (w_id, d_id, o_id)
        tree_t<Key, Value>* i_orderline_wd;     // key = (w_id, d_id)

	#ifdef LOCKTABLE
	page_table_t* tab;
	#else
	indirection_table_t* tab;
	#endif
	std::atomic<int64_t> free_pages;

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
        static void* thread_init_warehouse_parallel(void* This, int tid, int wid_from, int wid_to);
};

class tpcc_txn_man_t: public txn_man_t{
    public:
        void init(worker_t* worker);
        RC run_request(base_request_t* reuqest, int tid);

    private:
        tpcc_worker_t* worker;
};

