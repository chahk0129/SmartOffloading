#include "common/options.h"
#include "common/cxxopts.hpp"
#include "common/global.h"
#include "common/hash.h"
#include "benchmark/zipf.h"
#include "client/idx_wrapper.h"
#include "net/config.h"
#include "index/tree.h"
#include "index/node.h"
#include <vector>
#include <thread>
#include <algorithm>

idx_wrapper_t* idx;

struct operation_t{
    access_t type;
    uint64_t key;
    uint64_t value;
    uint32_t scan_len;
};

void parse_args(int argc, char* argv[]){
    options_t opt;
    try{
	cxxopts::Options options("RDMA-Index", "Benchmark framework for RDMA-based indexing.");
	options.add_options()
	    ("workload", "Workload type (load, a, b, c, e)", cxxopts::value<std::string>())
	    ("num", "Size of workload to run", cxxopts::value<uint64_t>()->default_value(std::to_string(opt.num)))
	    ("threads", "Numver of client threads in a compute server to run", cxxopts::value<uint64_t>()->default_value(std::to_string(opt.threads)))
	    ("zipfian", "Key distribution skew factor to use", cxxopts::value<double>()->default_value(std::to_string(opt.zipfian)))
	    ("help", "Print help")
	    ;

	auto result = options.parse(argc, argv);
	if(result.count("help")){
	    std::cout << options.help() << std::endl;
	    exit(0);
	}

	if(result.count("num"))
	    opt.num = result["num"].as<uint64_t>();
	else{
	    std::cout << "Missing the size of workload" << std::endl;
	    std::cout << options.help() << std::endl;
	    exit(0);
	}

	if(result.count("workload"))
            opt.workload = result["workload"].as<std::string>();

        if(result.count("threads"))
            opt.threads = result["threads"].as<uint64_t>();

        if(result.count("zipfian"))
            opt.zipfian= result["zipfian"].as<double>();

    }catch(const cxxopts::OptionException& e){
	std::cout << "Error parsing options: " << e.what() << std::endl;
	exit(0);
    }

    if(opt.threads <= 0){
        std::cout << "Number of threads should be larger than 0: " << opt.threads  << std::endl;
        exit(0);
    }

    std::cerr << opt << std::endl;

    if(opt.workload.compare("a") == 0){
        ycsb_workload_type = YCSB_WORKLOAD_A;
        std::cout << "Workload type: YCSB A" << std::endl;
    }
    else if(opt.workload.compare("b") == 0){
        ycsb_workload_type = YCSB_WORKLOAD_B;
        std::cout << "Workload type: YCSB B" << std::endl;
    }
    else if(opt.workload.compare("c") == 0){
        ycsb_workload_type = YCSB_WORKLOAD_C;
        std::cout << "Workload type: YCSB C" << std::endl;
    }
    else if(opt.workload.compare("e") == 0){
        ycsb_workload_type = YCSB_WORKLOAD_E;
        std::cout << "Workload type: YCSB E" << std::endl;
    }
    else{
        std::cout << "Invalid YCSB workload type: " << opt.workload << std::endl;
        exit(0);
    }

    if(opt.zipfian != 0)
        zipfian = opt.zipfian;

    if(opt.num != 0){
        g_synth_table_size = opt.num;
    }
    else{
        std::cout << "Workload size is not defined!" << std::endl;
        exit(0);
    }

    g_run_parallelism = opt.threads;

    std::cout << "Workload size: " << g_synth_table_size << std::endl;
    std::cout << "# of client threads: " << g_run_parallelism << std::endl;
}

uint64_t to_key(uint64_t key){
    return h(&key, sizeof(key), HASH_FUNC) % g_synth_table_size + 1;
}

access_t generate_access(int r){
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

void generate_run_ops(operation_t* ops){
    auto seed = (asm_rdtsc() & (0x0000ffffffffffffull));
    struct zipf_gen_state state;
    mehcached_zipf_init(&state, g_synth_table_size, zipfian, seed);

    for(int i=0; i<g_synth_table_size; i++){
	int r = rand() % 100;
	ops[i].type = generate_access(r);
	auto dist = mehcached_zipf_next(&state);
	ops[i].key = to_key(dist);
	ops[i].value = ops[i].key;

	if(ops[i].type == SCAN)
	    ops[i].scan_len = rand() % 40 + 10;
    }
}

void generate_init_ops(operation_t* ops){
    for(int i=0; i<g_synth_table_size; i++){
	ops[i].type = WRITE;
	ops[i].key = i+1;
	ops[i].value = i+1;
    }
    std::random_shuffle(ops, ops + g_synth_table_size);
}


void generate_workload(operation_t* init_ops, operation_t* run_ops){
    generate_init_ops(init_ops);
    generate_run_ops(run_ops);
}

void load(operation_t* ops){
    auto load = [ops](int tid){
	bind_core(tid);
	int chunk = g_synth_table_size / g_run_parallelism;
	int from = chunk * tid;
	int to = chunk * (tid + 1);
	if(to > g_synth_table_size)
	    to = g_synth_table_size;

	for(int i=from; i<to; i++){
	    idx->insert(ops[i].key, ops[i].value, tid);
	}
    };

    int build_thread_num = 64;
    auto fast_load = [ops, build_thread_num](int tid){
	bind_core(tid);
	int chunk = g_synth_table_size / build_thread_num;
	int from = chunk * tid;
	int to = chunk * (tid + 1);
	if(to > g_synth_table_size)
	    to = g_synth_table_size;

	for(int i=from; i<to; i++){
	    idx->insert(ops[i].key, ops[i].value, tid);
	}
    };

    std::vector<std::thread> threads;
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    if(ycsb_workload_type == YCSB_WORKLOAD_LOAD)
	for(int i=0; i<g_run_parallelism; i++)
	    threads.push_back(std::thread(load, i));
    else
	for(int i=0; i<build_thread_num; i++)
	    threads.push_back(std::thread(fast_load, i));
    for(auto& t: threads) t.join();
    clock_gettime(CLOCK_MONOTONIC, &end);

    uint64_t elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec) * 1000000000;
    std::cout << "Load time (sec)     : " << elapsed / 1000000000.0 << std::endl;
    std::cout << "Load tput (mops/sec): " << g_synth_table_size * 1000.0 / elapsed << std::endl;;
}

void run(operation_t* ops){
    int not_found[g_run_parallelism];
    memset(not_found, 0, sizeof(int) * g_run_parallelism);
    auto func = [ops, &not_found](int tid){
	bind_core(tid);
	int chunk = g_synth_table_size / g_run_parallelism;
	int from = chunk * tid;
	int to = chunk * (tid + 1);
	if(to > g_synth_table_size) to = g_synth_table_size;

	for(int i=from; i<to; i++){
	    if(ops[i].type == WRITE)
		idx->insert(ops[i].key, ops[i].value, tid);
	    else{
		uint64_t value;
		auto ret = idx->search(ops[i].key, value, tid);
		if(!ret || (value != ops[i].value)){
		    not_found[tid]++;
		}
	    }
	}
    };

    std::vector<std::thread> threads;
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for(int i=0; i<g_run_parallelism; i++)
	threads.push_back(std::thread(func, i));
    for(auto& t: threads) t.join();
    clock_gettime(CLOCK_MONOTONIC, &end);

    uint64_t elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec) * 1000000000;
    std::cout << "Run time (sec)     : " << elapsed / 1000000000.0 << std::endl;
    std::cout << "Run tput (mops/sec): " << g_synth_table_size * 1000.0 / elapsed << std::endl;;

    int _not_found = 0;
    for(int i=0; i<g_run_parallelism; i++)
	_not_found += not_found[i];
    std::cout << "Not found keys     : " << _not_found << std::endl;
}

int main(int argc, char* argv[]){
    parse_args(argc, argv);

    std::string path = "../host.txt";
    config_t* config = new config_t(path);

    idx = new idx_wrapper_t(config);

    std::cout << "Creating workload ...";
    auto init_ops = new operation_t[g_synth_table_size];
    auto run_ops = new operation_t[g_synth_table_size];
    generate_workload(init_ops, run_ops);
    std::cout << " done!" << std::endl;

    load(init_ops);
    if(ycsb_workload_type != YCSB_WORKLOAD_LOAD)
	run(run_ops);
    return 0;
}


