#include <unordered_set>
#include "system/query.h"
#include "benchmark/ycsb.h"
#include "benchmark/ycsb_query.h"
#include "benchmark/zipf.h"
#include "common/hash.h"
#include "common/debug.h"

void ycsb_query_t::init(int tid){
    request_cnt = REQUEST_PER_QUERY;
    requests = new ycsb_request_t[request_cnt];
    generate_request();
}

uint64_t ycsb_query_t::to_key(uint64_t key){
    return h(&key, sizeof(key), HASH_FUNC) % g_synth_table_size + 1;
}

access_t ycsb_query_t::generate_access(int r){
    if(ycsb_workload_type == YCSB_WORKLOAD_A){
	if(r < 50)
	    return WRITE;
	else
	    return READ;
    }
    else if(ycsb_workload_type == YCSB_WORKLOAD_B)
	if(r < 5)
	    return WRITE;
	else
	    return READ;
    else if(ycsb_workload_type == YCSB_WORKLOAD_C)
	return READ;
    else if(ycsb_workload_type == YCSB_WORKLOAD_E)
	if(r < 95)
	    return SCAN;
	else
	    return WRITE;
    else{
	debug::notify_error("Unsupported workload type... implement me!");
	exit(0);
    }
    return READ;
}

void ycsb_query_t::generate_request(){
    auto seed = (asm_rdtsc() & (0x0000ffffffffffffull));
    struct zipf_gen_state state;
    mehcached_zipf_init(&state, g_synth_table_size, zipfian, seed);

    std::unordered_set<Key> all_keys;
    all_keys.reserve(request_cnt);
    uint64_t table_size = g_synth_table_size;
    int access_cnt = 0;
    uint64_t rid = 0;
    uint64_t tmp;
    for(tmp = 0; tmp<request_cnt; tmp++){
	auto req = &requests[rid];
	int r = rand() % 100;
	req->type = generate_access(r);
	Key key;
    RETRY:
	auto dist = mehcached_zipf_next(&state);
	key = to_key(dist);
	/*
	if(tmp == 0){
	    key = g_synth_table_size - 1;
	}
	else{
	    auto dist = mehcached_zipf_next(&state);
	    key = to_key(dist);
	}
	*/
	req->key = key;
	req->value = rand() % (1 << 8);
	// make sure a single row is not accessed twice
	if(req->type == READ || req->type == WRITE){
	    if(all_keys.find(req->key) == all_keys.end()){
		all_keys.insert(req->key);
		access_cnt++;
	    }
	    else{
		goto RETRY;
		//tmp--;
		//continue;
	    }
	}
	else{
	    req->scan_len = 20;
	    bool conflict = false;
	    for(uint32_t i=0; i<req->scan_len; i++){
		uint64_t primary_key = key + i;
		if(all_keys.find(primary_key) != all_keys.end()){
		    conflict = true;
		    break;
		}
	    }
	    if(conflict){
		goto RETRY;
		//tmp--;
		//continue;
	    }
	    else{
		for(uint32_t i=0; i<req->scan_len; i++)
		    all_keys.insert(key + i);
		access_cnt += req->scan_len;
	    }
	}
	rid++;
    }
}


