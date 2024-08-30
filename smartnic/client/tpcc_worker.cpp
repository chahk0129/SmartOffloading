#include "client/tpcc.h"
#include "client/mr.h"
#include "client/transport.h"
#include "client/thread.h"
#include "client/txn.h"
#include "storage/table.h"
#include "common/hash.h"
#include "benchmark/tpcc_helper.h"

RC tpcc_worker_t::init(config_t* conf){
    worker_t::init(conf);
    std::string path = "../benchmark/";
    #if TPCC_SMALL
    path += "TPCC_short_schema.txt";
    #else
    path += "TPCC_full_schema.txt";
    #endif
    std::cout << "Reading schema file: " << path << std::endl;
    init_schema(path);
    std::cout << "TPCC schema initialized" << std::endl;
    return RCOK;
}

RC tpcc_worker_t::init_schema(std::string path){
    worker_t::init_schema(path);
    t_warehouse = tables["WAREHOUSE"];
    t_district = tables["DISTRICT"];
    t_customer = tables["CUSTOMER"];
    t_history = tables["HISTORY"];
    t_neworder = tables["NEW-ORDER"];
    t_order = tables["ORDER"];
    t_orderline = tables["ORDER-LINE"];
    t_item = tables["ITEM"];
    t_stock = tables["STOCK"];

    tpcc_buffer = new drand48_data*[g_num_wh];
    for(int i=0; i<g_num_wh; i++){
	tpcc_buffer[i] = new drand48_data;
    }
    return RCOK;
}

RC tpcc_worker_t::get_txn_man(txn_man_t*& txn_man, thread_t* thd){
    txn_man = new tpcc_txn_man_t;
    txn_man->init(thd, this, thd->get_tid());
    return RCOK;
}

int tpcc_worker_t::key_to_part(Key key){
    return h(&key, sizeof(key), HASH_FUNC) % MR_PARTITION_NUM;
}
