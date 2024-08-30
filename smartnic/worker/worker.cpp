#include "worker/worker.h"
#include "worker/mr.h"
#include "worker/transport.h"
#include "index/node.h"
#include "index/tree.h"
#include "net/config.h"
#include "storage/catalog.h"
#include "storage/row.h"
#include "storage/table.h"
#include "worker/txn.h"

RC worker_t::init(config_t* conf){
    this->conf = conf;

    // create memory region
    mem = new worker_mr_t();

    // network transport
    uint64_t server_mem_pool = mem->get_server_memory_pool();
    uint64_t server_mem_size = mem->get_server_memory_size();
    uint64_t client_mem_pool = mem->get_client_memory_pool();
    uint64_t client_mem_size = mem->get_client_memory_size();
    transport = new worker_transport_t(conf, server_mem_pool, server_mem_size, client_mem_pool, client_mem_size);

    // prepost RDMA RECVs
    // prepost for server
    for(int i=0; i<WORKER_THREAD_NUM; i++){
	auto ptr = mem->response_buffer_pool(i);
	transport->prepost_recv((uint64_t)ptr, sizeof(request_t), i);
    }

    // prepost for client
    #if defined BATCH
    for(int i=0; i<NETWORK_THREAD_NUM; i++){
	auto ptr = mem->rpc_request_pool(i);
	transport->prepost_recv_client((uint64_t)ptr, sizeof(rpc_commit_t), i);
    }
    #elif defined BATCH2
    for(int i=0; i<WORKER_THREAD_NUM; i++){
	auto ptr = mem->rpc_request_pool(i);
	transport->prepost_recv_client((uint64_t)ptr, sizeof(rpc_commit_t), i);
    }
    #else
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
	auto ptr = mem->rpc_request_buffer_pool(i);
	transport->prepost_recv_client((uint64_t)ptr, sizeof(rpc_request_t<Key>), i);
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
            tree_t<Key, Value>* index = new tree_t<Key, Value>(mem, transport);
            indexes[index_name] = index;
            // TODO pid for TPCC
            cur_pid++;
        }
    }
    ifs.close();
    return RCOK;
}

uint64_t worker_t::rpc_alloc(int tid, int pid){
    auto send_ptr = mem->request_buffer_pool(tid);
    auto request = create_message<request_t>((void*)send_ptr, tid, pid, request_type::TABLE_ALLOC_ROW);
    transport->send((uint64_t)request, sizeof(request_t), tid);

    auto recv_ptr = mem->response_buffer_pool(tid);
    auto response = create_message<response_t>((void*)recv_ptr);
    transport->recv((uint64_t)response, sizeof(response_t), tid);
    if(response->type != response_type::SUCCESS)
	debug::notify_error("Memory allocation for new row failed (%d)", response->type);
    return response->addr;
}

RC worker_t::init_index(){
    tree_t<Key, Value>* index = new tree_t<Key, Value>(mem, transport);
    indexes["TEST"] = index;
    return RCOK;
}
