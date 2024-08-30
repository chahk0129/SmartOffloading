#include "client/worker.h"
#include "client/mr.h"
#include "client/transport.h"
#include "client/txn.h"
#include "storage/catalog.h"
#include "storage/table.h"
#include "net/config.h"

RC worker_t::init(config_t* conf, bool is_txn){
    this->conf = conf;
    sim_done.store(false);

    // create memory region
    mem = new client_mr_t();
    #ifdef BATCH
    uint64_t memory_pool[NETWORK_THREAD_NUM];
    uint64_t memory_size[NETWORK_THREAD_NUM];
    for(int i=0; i<NETWORK_THREAD_NUM; i++){
        memory_pool[i] = mem->get_memory_pool(i);
        memory_size[i] = mem->get_memory_size(i);
    }
    transport = new client_transport_t(conf, memory_pool, memory_size);
    #elif defined BATCH2
    uint64_t memory_pool = mem->get_memory_pool();
    uint64_t memory_size = mem->get_memory_size();
    transport = new client_transport_t(conf, &memory_pool, &memory_size);
    #else
    uint64_t memory_pool[CLIENT_THREAD_NUM];
    uint64_t memory_size[CLIENT_THREAD_NUM];
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
        memory_pool[i] = mem->get_memory_pool(i);
        memory_size[i] = mem->get_memory_size(i);
    }
    transport = new client_transport_t(conf, memory_pool, memory_size);
    #endif

    #if defined BATCH2
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
	auto ptr = mem->rpc_response_pool(i);
	transport->prepost_recv((uint64_t)ptr, sizeof(rpc_response_t), i%WORKER_THREAD_NUM, i);
    }
    #endif

    return RCOK;
}

RC worker_t::init_schema(std::string path){
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
//            tree_t<Key, Value>* index = new tree_t<Key, Value>(mem, transport);
//            indexes[index_name] = index;
            // TODO pid for TPCC
            cur_pid++;
        }
    }

    ifs.close();
    return RCOK;
}
