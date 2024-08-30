#include "common/options.h"
#include "common/cxxopts.hpp"
#include "common/global.h"
#include "common/stat.h"
#include "common/helper.h"
#include "net/config.h"
#include "client/thread.h"
#include "client/query.h"
#include "client/tpcc.h"


#include <random>
#include <vector>
#include <thread>
#include <iostream>
#include <string>
#include <atomic>
#include <memory>

thread_t** threads;

void parse_args(int argc, char* argv[]){
    options_t opt;
    try{
        cxxopts::Options options("SmartNIC-DB", "Benchmark framework for SmartNIC-based Concurrency Control.");
        options.add_options()
            ("threads", "Number of client threads in a compute server to run", cxxopts::value<uint64_t>()->default_value(std::to_string(opt.threads)))
            ("latency", "Enable latency measurement", cxxopts::value<bool>()->default_value((opt.latency ? "true" : "false")))
            ("help", "Print help")
            ;

        auto result = options.parse(argc, argv);
        if(result.count("help")){
            std::cout << options.help() << std::endl;
            exit(0);
        }

	if(result.count("threads"))
            opt.threads = result["threads"].as<uint64_t>();
	else{
	    std::cout << "The number of compute threads needs to be identified!" << std::endl;
	    std::cout << options.help() << std::endl;
	    exit(0);
	}

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

    if(opt.latency){
	g_measure_latency = true;
    }

    g_init_parallelism = opt.threads;
    g_run_parallelism = opt.threads;

    std::cout << "# of compute threads: " << g_run_parallelism << std::endl;
}

void f(int tid){
    threads[tid]->run();
}

int main(int argc, char* argv[]){
    parse_args(argc, argv);

#if defined BATCH || defined BATCH2
    std::cout << "Batching is not supported in TPCC benchmark" << std::endl;
    exit(0);
#endif

    std::string path = "../dpu.txt";
    auto conf = new config_t(path);

    std::cout << "Initializing worker ... " << std::endl;;
    auto worker = new tpcc_worker_t;
    worker->init(conf);

    stat = new stat_t();

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
    if(STATS_ENABLE)
        stat->summary();
    return 0;
}

