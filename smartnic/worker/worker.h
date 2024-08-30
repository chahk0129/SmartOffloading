#pragma once
#include <map>
#include <atomic>
#include <string>
#include "common/rpc.h"
#include "common/global.h"

template <typename Key_t, typename Value_t> class tree_t;
class config_t;
class worker_mr_t;
class worker_transport_t;
class table_t;
class row_t;
class thread_t;
class txn_man_t;

class worker_t{
    public:
	// tables indexed by table name
	std::map<std::string, table_t*> tables;
	std::map<std::string, tree_t<Key, Value>*> indexes;

	// netowrk transport
        worker_transport_t* transport;
        // memory space for network transport
        worker_mr_t* mem;
        // network configuration
        config_t* conf;

	// initialize tables and indexes
	virtual RC init(config_t* conf);
	virtual RC init_table() = 0;
	virtual RC init_schema(std::string path);
	virtual RC get_txn_man(txn_man_t*& txn_man, thread_t* thd) = 0;
	
	RC init_index();

	uint64_t rpc_alloc(int tid, int pid);
	// microbench
	void handle_message(int tid);
	void handle_request(rpc_request_t<Key>* request, int tid);
};
