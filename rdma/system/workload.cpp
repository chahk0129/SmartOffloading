#include "common/global.h"
#include "common/hash.h"
#include "storage/row.h"
#include "storage/table.h"
#include "storage/catalog.h"
#include "client/mr.h"
#include "client/transport.h"
#include "index/node.h"
#include "index/tree.h"
#include "system/txn.h"
#include "system/workload.h"

RC workload_t::init(config_t* conf){
    this->conf = conf;
    sim_done.store(false);

    mem = new client_mr_t();
    uint64_t mem_pool[CLIENT_THREAD_NUM];
    uint64_t mem_size[CLIENT_THREAD_NUM];
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
	mem_pool[i] = mem->get_memory_pool(i);
	mem_size[i] = mem->get_memory_size(i);
    }

    transport = new client_transport_t(conf, mem_pool, mem_size);
    return RCOK;
}

RC workload_t::init_schema(std::string path){
    std::string line;
    std::ifstream ifs(path);
    catalog_t* schema;
    int cur_pid = 0;
    while(getline(ifs, line)){
	if(line.compare(0, 6, "TABLE=") == 0){
	    std::string table_name = &line[6];
	    schema = new catalog_t();
	    int col_count = 0;
	    // Read all fields for this table
	    std::vector<std::string> lines;
	    getline(ifs, line);
	    while(line.length() > 1){
		lines.push_back(line);
		getline(ifs, line);
	    }

	    schema->init(table_name.c_str(), lines.size());
	    for(int i=0; i<lines.size(); i++){
		int pos = 0;
		int element_num = 0;
		int size = 0;
		std::string l = lines[i];
		std::string type, name, token;
		while(l.length() != 0){
		    pos = l.find(",");
		    if(pos == std::string::npos)
			pos = l.length();
		    token = l.substr(0, pos);
		    l.erase(0, pos+1);
		    switch(element_num){
			case 0: size = atoi(token.c_str()); break;
			case 1: type = token; break;
			case 2: name = token; break;
			default: assert(false);
		    }
		    element_num++;
		}
		assert(element_num == 3);
		schema->add_col(name.c_str(), size, type.c_str());
		col_count++;
	    }

	    table_t* tab = new table_t(schema);
	    tables[table_name] = tab;
	}
	else if(line.compare(0, 6, "INDEX=") == 0){
	    std::string index_name = &line[6];
	    getline(ifs, line);

	    std::vector<std::string> items;
	    std::string token;
	    int pos;
	    while(line.length() != 0){
		pos = line.find(",");
		if(pos == std::string::npos)
		    pos = line.length();
		token = line.substr(0, pos);
		items.push_back(token);
		line.erase(0, pos+1);
	    }

	    std::string table_name(items[0]);
	    tree_t<Key, Value>* index = new tree_t<Key, Value>(mem, transport);
	    indexes[index_name] = index;
	    // TODO pid for TPCC
	    cur_pid++;
	}
    }

    ifs.close();
    return RCOK;
}

void workload_t::index_insert(tree_t<Key, Value>* index, Key key, uint64_t addr, int tid){
    index->insert(key, addr, tid);
}
