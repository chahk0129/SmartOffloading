#include "common/stat.h"
#include <algorithm>
#include <iostream>
#include <fstream>

stat_thread_t::stat_thread_t(): run_cnt(0), run_time(0), abort_cnt(0){
    latency.clear();
    latency.resize(MAX_TRANSACTION);
    latency.resize(0);

    time_abort = 0;
    time_commit = 0;
    time_index = 0;
    time_wait = 0;
    time_backoff = 0;

}

void stat_thread_t::clear(){
    run_cnt = 0;
    run_time = 0;
    abort_cnt = 0;

    time_abort = 0;
    time_commit = 0;
    time_index = 0;
    time_wait = 0;
    time_backoff = 0;
    latency.clear();

}

void stat_thread_t::add_latency(uint64_t _latency){
    latency.push_back(_latency);
}

void stat_thread_t::summary(uint64_t& run_cnt, uint64_t& run_time, uint64_t& abort_cnt, uint64_t& time_commit, uint64_t& time_abort, uint64_t& time_backoff, uint64_t& time_index, uint64_t& time_wait, std::vector<uint64_t>& latency){
    run_cnt += this->run_cnt;
    run_time += this->run_time;
    abort_cnt += this->abort_cnt;

    time_commit += this->time_commit;
    time_abort += this->time_abort;
    time_index += this->time_index;
    time_wait += this->time_wait;
    time_backoff += this->time_backoff;

    for(auto it=this->latency.begin(); it!=this->latency.end(); it++)
	latency.push_back(*it);
}

stat_t::stat_t(){
    _stats = new stat_thread_t*[g_run_parallelism];
    for(int i=0; i<g_run_parallelism; i++)
	_stats[i] = new stat_thread_t();
}

void stat_t::clear(int tid){
    _stats[tid]->clear();
}

void stat_t::summary(){
    uint64_t run_cnt = 0;
    uint64_t run_time = 0;
    uint64_t abort_cnt = 0;

    // breakdown
    uint64_t time_commit, time_abort, time_index, time_wait, time_backoff;
    time_commit = time_abort = time_index = time_wait = time_backoff = 0;

    std::vector<uint64_t> latency;
    for(int i=0; i<g_run_parallelism; i++){
	_stats[i]->summary(run_cnt, run_time, abort_cnt, time_commit, time_abort, time_backoff, time_index, time_wait, latency);
    }

    std::sort(latency.begin(), latency.end());

    run_time = run_time / 1000000000.0 / g_run_parallelism;
    double tput = (double)run_cnt / run_time; // ops/sec
    std::cout << "Throughput (ops/sec): " << tput << std::endl;
    std::cout << "Processed           : " << run_cnt << " for " << run_time << " sec" << std::endl;
    std::cout << "Aborted             : " << abort_cnt << " for " << run_time << " sec" << std::endl;
    std::cout << "Abort rate          : " << (double)abort_cnt / (abort_cnt + run_cnt) << std::endl;
    uint64_t total_breakdown = time_commit + time_abort + time_index + time_wait + time_backoff;
    std::cout << "    Total time: " << total_breakdown << std::endl;
    std::cout << "    Index     : " << (double)time_index / total_breakdown * 100 << " %" << std::endl;
    std::cout << "    Commit    : " << (double)time_commit / total_breakdown * 100 << " %" << std::endl;
    std::cout << "    Abort     : " << (double)time_abort / total_breakdown * 100 << " %" << std::endl;
    std::cout << "    Wait      : " << (double)time_wait / total_breakdown * 100 << " %" << std::endl;
    std::cout << "    Backoff   : " << (double)time_backoff / total_breakdown * 100 << " %" << std::endl;

    if(g_measure_latency){
	std::ofstream ofs("latency.txt");
	if(!ofs.is_open()){
	    std::cerr << "Failed to open latency file" << std::endl;
	    return;
	}

	for(auto it=latency.begin(); it!=latency.end(); it++)
	    ofs << *it << "\n";

	ofs.close();
    }

}
