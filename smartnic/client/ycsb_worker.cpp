#include "client/ycsb.h"
#include "client/ycsb_query.h"
#include "client/mr.h"
#include "client/transport.h"
#include "client/txn.h"
#include "storage/table.h"
#include "client/thread.h"
#include "common/rpc.h"
#include "common/debug.h"

RC ycsb_worker_t::init(config_t* conf){
    worker_t::init(conf);
    std::string path = "../benchmark/YCSB_schema.txt";
    init_schema(path);
    return RCOK;
}

RC ycsb_worker_t::init_schema(std::string path){
    worker_t::init_schema(path);
    table = tables["MAIN_TABLE"];
    //index = indexes["MAIN_INDEX"];
    return RCOK;
}

RC ycsb_worker_t::get_txn_man(txn_man_t*& txn_man, thread_t* thd){
    txn_man = new ycsb_txn_man_t;
    txn_man->init(thd, this, thd->get_tid());
    return RCOK;
}
