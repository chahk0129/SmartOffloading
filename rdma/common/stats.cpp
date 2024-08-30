#include <iostream>
#include <fstream>
#include "common/global.h"
#include "common/helper.h"
#include "common/stats.h"

#define BILLION 1000000000UL

void stats_thread_t::init(int thd_id) {
    clear();
    all_debug1 = new uint64_t;
    all_debug2 = new uint64_t;
}

void stats_thread_t::clear() {
    ALL_METRICS(INIT_VAR, INIT_VAR, INIT_VAR)
}

void stats_tmp_t::init() {
    clear();
}

void stats_tmp_t::clear() {
    TMP_METRICS(INIT_VAR, INIT_VAR)
}

void stats_t::init() {
    if (!STATS_ENABLE)
	return;
    _stats = new stats_thread_t* [g_run_parallelism];
    tmp_stats = new stats_tmp_t* [g_run_parallelism];
    dl_detect_time = 0;
    dl_wait_time = 0;
    deadlock = 0;
    cycle_detect = 0;
}

void stats_t::init(int thread_id) {
    if (!STATS_ENABLE)
	return;
    _stats[thread_id] = new stats_thread_t;
    tmp_stats[thread_id] = new stats_tmp_t;

    _stats[thread_id]->init(thread_id);
    tmp_stats[thread_id]->init();
}

void stats_t::clear(int tid) {
    if (STATS_ENABLE) {
	_stats[tid]->clear();
	tmp_stats[tid]->clear();
	dl_detect_time = 0;
	dl_wait_time = 0;
	cycle_detect = 0;
	deadlock = 0;
    }
}

void stats_t::add_debug(int thd_id, uint64_t value, uint32_t select) {
    /*
    if (g_prt_lat_distr && warmup_finish) {
	uint64_t tnum = _stats[thd_id]->txn_cnt;
	if (select == 1)
	    _stats[thd_id]->all_debug1[tnum] = value;
	else if (select == 2)
	    _stats[thd_id]->all_debug2[tnum] = value;
    }
    */
}

void stats_t::commit(int thd_id) {
    if (STATS_ENABLE) {
	_stats[thd_id]->time_man += tmp_stats[thd_id]->time_man;
	_stats[thd_id]->time_index += tmp_stats[thd_id]->time_index;
	_stats[thd_id]->time_wait += tmp_stats[thd_id]->time_wait;
	tmp_stats[thd_id]->init();
    }
}

void stats_t::abort(int thd_id) {
    if (STATS_ENABLE)
	tmp_stats[thd_id]->init();
}

void stats_t::print() {
    ALL_METRICS(INIT_TOTAL_VAR, INIT_TOTAL_VAR, INIT_TOTAL_VAR)
	for (int tid = 0; tid < g_run_parallelism; tid ++) {
	    ALL_METRICS(SUM_UP_STATS, SUM_UP_STATS, MAX_STATS)
		printf("[tid=%u] txn_cnt=%lu,abort_cnt=%lu, user_abort_cnt=%lu\n",
			tid, _stats[tid]->txn_cnt, _stats[tid]->abort_cnt,
			_stats[tid]->user_abort_cnt);
	}
    total_latency = total_latency / total_txn_cnt;
    total_commit_latency = total_commit_latency / total_txn_cnt;
    total_time_man = total_time_man - total_time_wait;
    if (output_file != NULL) {
	std::ofstream outf(output_file);
	if (outf.is_open()) {
	    outf << "[summary] throughput=" << total_txn_cnt / total_run_time *
		BILLION * g_run_parallelism << ", ";
		//BILLION * THREAD_CNT << ", ";
	    ALL_METRICS(WRITE_STAT_X, WRITE_STAT_Y, WRITE_STAT_Y)
		outf << "deadlock_cnt=" << deadlock << ", ";
	    outf << "cycle_detect=" << cycle_detect << ", ";
	    outf << "dl_detect_time=" << dl_detect_time / BILLION << ", ";
	    outf << "dl_wait_time=" << dl_wait_time / BILLION << "\n";
	    outf.close();
	}
    }
    std::cout << "[summary] throughput=" << total_txn_cnt / total_run_time *
	BILLION * THREAD_CNT << ", ";
    ALL_METRICS(PRINT_STAT_X, PRINT_STAT_Y, PRINT_STAT_Y)
	std::cout << "deadlock_cnt=" << deadlock << ", ";
    std::cout << "cycle_detect=" << cycle_detect << ", ";
    std::cout << "dl_wait_time=" << dl_wait_time / BILLION << "\n";
}

void stats_t::print_lat_distr() {
    FILE * outf;
    if (output_file != NULL) {
	outf = fopen(output_file, "a");
	for (uint32_t tid = 0; tid < g_run_parallelism; tid ++) {
	    fprintf(outf, "[all_debug1 thd=%d] ", tid);
	    for (uint32_t tnum = 0; tnum < _stats[tid]->txn_cnt; tnum ++)
		fprintf(outf, "%ld,", _stats[tid]->all_debug1[tnum]);
	    fprintf(outf, "\n[all_debug2 thd=%d] ", tid);
	    for (uint32_t tnum = 0; tnum < _stats[tid]->txn_cnt; tnum ++)
		fprintf(outf, "%ld,", _stats[tid]->all_debug2[tnum]);
	    fprintf(outf, "\n");
	}
	fclose(outf);
    }
}

