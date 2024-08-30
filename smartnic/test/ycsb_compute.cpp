#include "common/options.h"
#include "common/cxxopts.hpp"
#include "common/global.h"
#include "common/stat.h"
#include "common/helper.h"
#include "net/config.h"
#include "client/thread.h"
#include "client/query.h"
#include "client/ycsb.h"


#include <random>
#include <vector>
#include <thread>
#include <iostream>
#include <string>
#include <atomic>
#include <memory>

thread_t** threads;
thread_t** net_threads;

void parse_args(int argc, char* argv[]){
    options_t opt;
    try{
        cxxopts::Options options("RDMA-DB", "Benchmark framework for RDMA-based Concurrency Control.");
        options.add_options()
            ("workload", "Workload type (load, a, b, c, e, mixed)", cxxopts::value<std::string>())
            ("num", "Size of workload to run in million records", cxxopts::value<uint64_t>()->default_value(std::to_string(opt.num)))
            ("threads", "Number of client threads in a compute server to run", cxxopts::value<uint64_t>()->default_value(std::to_string(opt.threads)))
            ("zipfian", "Key distribution skew factor to use", cxxopts::value<double>()->default_value(std::to_string(opt.zipfian)))
            ("latency", "Enable latency measurement", cxxopts::value<bool>()->default_value((opt.latency ? "true" : "false")))
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
            std::cout << "Missing a size of workload" << std::endl;
            std::cout << options.help() << std::endl;
            exit(0);
        }

        if(result.count("workload"))
            opt.workload = result["workload"].as<std::string>();

        if(result.count("threads"))
            opt.threads = result["threads"].as<uint64_t>();

        if(result.count("zipfian"))
            opt.zipfian= result["zipfian"].as<double>();

        if(result.count("latency"))
            opt.latency = result["latency"].as<bool>();

    }catch(const cxxopts::OptionException& e){
        std::cout << "Error parsing options: " << e.what() << std::endl;
        exit(0);
    }

    if(opt.threads <= 0){
        std::cout << "Number of threads should be larger than 0: " << opt.threads << std::endl;
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

    if(opt.num != 0)
	g_synth_table_size = opt.num;
    else{
        std::cout << "Workload size is not defined!" << std::endl;
        exit(0);
    }

    if(opt.latency){
	g_measure_latency = true;
    }

    //g_init_parallelism = opt.threads;
    g_run_parallelism = opt.threads;

    std::cout << "Workload size: " << g_synth_table_size << std::endl;
    std::cout << "# of client threads: " << g_run_parallelism << std::endl;
}

void f(int tid){
    threads[tid]->run();
}

#if defined BATCH || defined BATCH2
void n(int tid){
    net_threads[tid]->run_network();
}
#endif

int main(int argc, char* argv[]){
    parse_args(argc, argv);

    std::string path = "../dpu.txt";
    auto conf = new config_t(path);

    std::cout << "Initializing worker ... " << std::endl;;
    auto worker = new ycsb_worker_t;
    worker->init(conf);

    stat = new stat_t();

    std::cout << "Initializing threads ... " << std::endl;
    #ifdef BATCH
    // run network threads for batching
    std::cout << "Running " << NETWORK_THREAD_NUM << " network threads ..." << std::endl;
    std::vector<std::thread> network_threads;
    net_threads = new thread_t* [NETWORK_THREAD_NUM];
    for(int i=0; i<NETWORK_THREAD_NUM; i++){
	net_threads[i] = new thread_t;
	net_threads[i]->init(i, (worker_t*)worker);
	network_threads.push_back(std::thread(n, i));
    }
    #elif defined BATCH2
    // run network threads for batching
    std::cout << "Running " << WORKER_THREAD_NUM * 2<< " network threads ..." << std::endl;
    std::vector<std::thread> network_threads;
    net_threads = new thread_t* [WORKER_THREAD_NUM * 2];
    for(int i=0; i<WORKER_THREAD_NUM * 2; i++){
	net_threads[i] = new thread_t;
	net_threads[i]->init(i, (worker_t*)worker);
	network_threads.push_back(std::thread(n, i));
    }

    #endif

    std::cout << "Initializing threads ... " << std::endl;
    int thread_cnt = g_run_parallelism;
    threads = new thread_t* [thread_cnt];
    for(int i=0; i<thread_cnt; i++){
	threads[i] = new thread_t;
	threads[i]->init(i, (worker_t*)worker);
    }

    std::cout << "Creating queries ... " << std::endl;
    query_queue = new query_queue_t;
    query_queue->init(worker);

    if(WARMUP > 0){
	std::cout << "WARMUP start with " << thread_cnt << " threads!" << std::endl;
	std::vector<std::thread> warmup_threads;
        for(int i=0; i<thread_cnt; i++)
            warmup_threads.push_back(std::thread(f, i));
        for(auto& t: warmup_threads) t.join();
        std::cout << "WARMUP finished!" << std::endl;
    }

    warmup_finish = true;

    std::vector<std::thread> run_threads;
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);

    for(int i=0; i<thread_cnt; i++)
        run_threads.push_back(std::thread(f, i));
    for(auto& t: run_threads) t.join();

    clock_gettime(CLOCK_MONOTONIC, &end);
    uint64_t elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*  1000000000;
    std::cout << "Elapsed time (sec)   : " << elapsed / 1000000000.0 << std::endl;

    g_run_finish = true;
    #ifdef BATCH
    // network threads for batching
    for(auto& t: network_threads) t.join();
    #elif defined BATCH2
    // network threads for batching
    for(auto& t: network_threads) t.join();
    #endif

    if(STATS_ENABLE)
        stat->summary();
    return 0;
}

