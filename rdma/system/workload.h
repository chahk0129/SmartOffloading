#ifndef __WORKLOAD_H__
#define __WORKLOAD_H__

#include <atomic>
#include <map>
#include <string>
#include "common/global.h"

class row_t;
class table_t;
class client_t;
class txn_man_t;
class catalog_t;
class thread_t;

template <typename, typename>
class tree_t;

class client_transport_t;
class client_mr_t;
class config_t;

class workload_t{
    public:
	// tables indexed by table name
	std::map<std::string, table_t*> tables;
	std::map<std::string, tree_t<Key, Value>*> indexes;

	// netowrk transport
	client_transport_t* transport;
	// memory space for network transport
	client_mr_t* mem;
	// network configuration
	config_t* conf;

	std::atomic<bool> sim_done;

	// initialize the tables and indexes
	virtual RC init(config_t* conf);
	virtual RC init_schema(std::string path);
	virtual RC init_table() = 0;
	virtual RC get_txn_man(txn_man_t*& txn_man, thread_t* thd) = 0;

    protected:
	void index_insert(tree_t<Key, Value>* index, Key key, uint64_t value, int tid);

};
#endif
