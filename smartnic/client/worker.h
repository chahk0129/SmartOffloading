#pragma once
#include "common/global.h"
#include <chrono>
#include <vector>

class row_t;
class table_t;
class txn_man_t;
class catalog_t;
class thread_t;

class client_mr_t;
class client_transport_t;
class config_t;

class worker_t{
    public:
	
	// tables are maintained to get schema in local computation
	std::map<std::string, table_t*> tables;
	// indexes are maintained in remote worker

	// network transport
	client_mr_t* mem;
	// memory space for network transport
	client_transport_t* transport;
	// network configuration
	config_t* conf;

	std::atomic<bool> sim_done;

	// initialize 
	virtual RC init(config_t* conf, bool is_txn=true);
	virtual RC init_schema(std::string path);
	virtual RC get_txn_man(txn_man_t*& txn_man, thread_t* thd) = 0;
};

