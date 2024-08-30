#include "common/global.h"
#include "common/stat.h"

// benchmark config
uint64_t g_init_parallelism = DEFAULT_INIT_THREADS;
uint64_t g_run_parallelism = DEFAULT_RUN_THREADS;
bool g_measure_latency = true;
std::atomic<uint32_t> wramup_cnt = g_init_parallelism;
stat_t* stat;
bool warmup_finish = false;
query_queue_t* query_queue;

// YCSB Workload
double g_sampling_rate = DEFAULT_SAMPLING_RATE;
uint64_t g_synth_table_size = SYNTH_TABLE_SIZE;
double zipfian = ZIPFIAN_THETA;
ycsb_workload_type_t ycsb_workload_type = YCSB_WORKLOAD_C;

// TPCC Workload
uint32_t g_part_cnt = PART_CNT;
uint64_t g_num_wh = NUM_WH;
bool g_wh_update = WH_UPDATE;
double g_perc_payment = PERC_PAYMENT;
double g_perc_delivery = PERC_DELIVERY;
double g_perc_orderstatus = PERC_ORDERSTATUS;
double g_perc_stocklevel = PERC_STOCKLEVEL;
double g_perc_neworder = 1 - (g_perc_payment + g_perc_delivery + g_perc_orderstatus + g_perc_stocklevel);
uint32_t g_dist_per_wh = DIST_PER_WARE;
#if TPCC_SMALL
uint32_t g_max_items = 10000;
uint32_t g_cust_per_dist = 2000;
#else
uint32_t g_max_items = 100000;
uint32_t g_cust_per_dist = 3000;
#endif


char* output_file;

