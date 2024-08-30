#include "common/global.h"
#include "common/stat.h"
#include "common/helper.h"
#include "worker/thread.h"
#include "worker/ycsb.h"
#include "worker/worker.h"
#include "worker/txn.h"
#include "net/config.h"
#include <chrono>

thread_t** threads;

void f(int tid){
    threads[tid]->run();
}

int main(int argc, char* argv[]){
    g_synth_table_size = atoi(argv[1]);
    std::string host = "../host.txt";
    auto conf = new config_t(host);

    std::cout << "Initializing worker ... " << std::endl;
    auto worker = new ycsb_worker_t;
    worker->init(conf);

    stat = new stat_t();
    global_ycsb_txn_man = new ycsb_txn_man_t;
    global_ycsb_txn_man->init((worker_t*)worker);

    std::cout << "Initializing threads ... " << std::endl;
    int thread_cnt = WORKER_THREAD_NUM;
    threads = new thread_t*[thread_cnt];
    for(int i=0; i<thread_cnt; i++){
	threads[i] = new thread_t;
	threads[i]->init(i, (worker_t*)worker);
    }

    std::cout << "Ready to run YCSB benchmark!" << std::endl;

    std::vector<std::thread> run_threads;
    for(int i=1; i<WORKER_THREAD_NUM; i++)
	run_threads.push_back(std::thread(f, i));
    f(0);
    for(auto& t: run_threads) t.join();

    return 0;
}
