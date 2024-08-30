#pragma once
#include "common/global.h"
//#include "common/rpc.h"
#include "worker/worker.h"
#include "worker/txn.h"

class base_request_t;
class base_query_t;
class table_t;
class row_t;
class config_t;
class indirection_table_t;
class page_table_t;

class ycsb_worker_t: public worker_t{
    public:
        RC init(config_t* conf);
        RC init_table();
        RC init_table_parallel(int tid);
        RC init_schema(std::string path);
	RC get_txn_man(txn_man_t*& txn_man, thread_t* thd);

        int key_to_part(uint64_t key);

        table_t* table;

        tree_t<Key, Value>* index;
	// indirection table (row_id <-> row_addr)
	#ifdef LOCKTABLE
	page_table_t* tab;
	#else
	indirection_table_t* tab;
	#endif
	std::atomic<int64_t> free_pages;

    private:
        static void thread_init_table(void* This, int tid){
            ((ycsb_worker_t*)This)->init_table_parallel(tid);
        }


};

//class rpc_request_t<Key, Value>;

class ycsb_txn_man_t: public txn_man_t{
    public:
        void init(worker_t* worker);

	RC run_request(base_request_t* request, int tid);
    private:
//        RC read(rpc_request_t<Key>* request, rpc_response_type* response, int tid);
//        RC upsert(rpc_request_t<Key>* request, int tid);
  //      RC insert(rpc_request_t<Key>* request, int tid);
    //    RC scan(rpc_request_t<Key>* request, int tid);

	ycsb_worker_t* worker;
	uint64_t row_cnt;
};

