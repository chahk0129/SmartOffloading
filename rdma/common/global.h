#pragma once

#include <unistd.h>
#include <cstdint>
#include <cstring>
#include <string>
#include <cassert>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <set>
#include <vector>
#include <time.h>
#include <sys/time.h>
#include <math.h>
#include <atomic>

#include "common/global_address.h"
//#include "stats.h"
//#include "stat.h"


//#define NOWAIT
#define WAITDIE

using Key = uint64_t;
using Value = uint64_t;

class query_queue_t;
class stat_t;

static bool is_masked_addr(uint64_t addr){
    return (bool)(addr & 0x1L);
}

static uint64_t set_masked_addr(uint64_t addr){
    return (addr | 0x1L);
}

static uint64_t get_unmasked_addr(uint64_t addr){
    return (addr & ~0x1L);
}

inline void compiler_barrier(){
    asm volatile("" ::: "memory");
}

static inline unsigned long long asm_rdtsc(){
#ifdef __x86_64__
    unsigned hi, lo;
    asm volatile("rdtsc": "=a"(lo), "=d"(hi));
    unsigned long long ret = ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
    return (unsigned long long)((double)ret / 2.9);
    //return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
#elif __aarch64__
    unsigned long long val;
    asm volatile("mrs %0, cntvct_el0" : "=r" (val));
    return (unsigned long long)((double)val / 2.0);
    //return val;
#else
    return 0;
#endif
}

static void bind_core(uint16_t core){
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if(rc != 0)
        std::cerr << "Failed to bind core" << std::endl;
}

enum connection_type{
    SERVER = 0,
    WORKER,
    CLIENT
};

template <typename Value_t>
struct result_t{
    bool is_leaf;
    uint32_t level;
    uint64_t sibling;
    uint64_t child;
    Value_t value;
};

enum txn_result_t{
    TX_ERROR = 0,
    TX_ABORT,
    TX_PROCEED,
    TX_COMMIT,
};

enum access_t{
    READ, 
    WRITE, 
    XP, 
    SCAN, 
    CM
};

enum lock_type_t{
    LOCK_EX,
    LOCK_SH,
    LOCK_NONE
};

enum RC{
    RCOK, 
    COMMIT, 
    ABORT, 
    WAIT, 
    ERROR, 
    FINISH
};

enum request_type_t{
    REQ_INSERT = 0,
    REQ_UPDATE,
    REQ_READ,
    REQ_SCAN,
};


enum status_t: unsigned int{
    RUNNING,
    ABORTED,
    COMMITTED,
    HOLDING,
};

// storage config
#define TABLE_NAME_LENGTH	32
#define NUM_COLUMNS 		25
#define ROW_LENGTH		1000
#define ROW_SIZE		1048

// index config
#define MAX_TREE_LEVEL 		10
#define PAGE_SIZE 		1024
#define INDEX_PAGE_SIZE 	(1024 * 1024 * PAGE_SIZE)
//#define CACHELINE_SIZE 64

// mr config
#define ROOT_BUFFER_SIZE	8
#define CAS_BUFFER_SIZE 	8
#define PAGE_BUFFER_SIZE 	PAGE_SIZE
#define SIBLING_BUFFER_SIZE 	PAGE_SIZE

// txn config 
#define MAX_ROW_PER_TXN 	1000 
#define MAX_TRANSACTION 	1000
#define WARMUP			1000
//#define WARMUP			10000
#define STATS_ENABLE 		true
#define BACKOFF 		10000

// client config
#define CLIENT_THREAD_NUM 	128
#define HASH_FUNC 		1

// server config
#define SERVER_NUM 		1
#define SERVER_THREAD_NUM 	1
//#define MR_PARTITION_NUM	50
#define MR_PARTITION_NUM	25
//#define MR_PARTITION_NUM	10


#define THREAD_CNT 		64
#define ROLL_BACK 		true
#define MAX_RUNTIME 		30

// benchmark config
#define DEFAULT_INIT_THREADS	128
#define DEFAULT_RUN_THREADS	64
#define DEFAULT_SAMPLING_RATE	0.1
extern uint64_t g_init_parallelism;
extern uint64_t g_run_parallelism;
extern double g_sampling_rate;
extern bool g_measure_latency;
extern double zipfian;
extern std::atomic<uint32_t> warmup_cnt;
extern bool warmup_finish;
extern query_queue_t* query_queue;

extern char* output_file;
extern stat_t* stat;
//extern class stats_t stats;


#ifdef YCSB
#define WORKLOAD (YCSB)
#elif defined TPCC
#define WORKLOAD (TPCC)
#endif

// YCSB config
#define SYNTH_TABLE_SIZE 	10000000
#define ZIPFIAN_THETA		0.9
#define REQUEST_PER_QUERY	8
enum ycsb_workload_type_t{
    YCSB_WORKLOAD_A = 0,
    YCSB_WORKLOAD_B,
    YCSB_WORKLOAD_C,
    YCSB_WORKLOAD_D,
    YCSB_WORKLOAD_E,
    YCSB_WORKLOAD_LOAD,
};
extern uint64_t global_ycsb_key_space;
extern uint64_t g_synth_table_size;
extern ycsb_workload_type_t ycsb_workload_type;


// TPCC config
#define TPCC_SMALL 		false
//#define TPCC_SMALL 		true
#define PART_CNT		1
#define WH_UPDATE		true
//#define NUM_WH			64
//#define NUM_WH			128
#define NUM_WH			1
#define PERC_PAYMENT		0.5
#define PERC_DELIVERY		0
#define PERC_ORDERSTATUS	0
#define PERC_STOCKLEVEL		0
// PERC_NEWORDER = 1 - PERC_PAYMENT - PERC_DELIVERY - PERC_ORDERSTATUS - PERC_STOCKLEVEL

#define FIRST_PART_LOCAL	true
#define FIRSTNAME_MIN_LEN	8
#define FIRSTNAME_LEN		16
#define LASTNAME_LEN		16
#define DIST_PER_WARE		10

enum tpcc_txn_type_t{
    TPCC_PAYMENT,
    TPCC_NEW_ORDER,
    TPCC_DELIVERY,
    TPCC_ORDER_STATUS,
    TPCC_STOCK_LEVEL,
    TPCC_ALL,
};
extern tpcc_txn_type_t g_tpcc_txn_type;
extern uint32_t g_part_cnt;
extern uint64_t g_num_wh;
extern bool g_wh_update;
extern double g_perc_payment;
extern double g_perc_delivery;
extern double g_perc_orderstatus;
extern double g_perc_stocklevel;
extern double g_perc_neworder;
extern uint32_t g_dist_per_wh;
extern uint32_t g_max_items;
extern uint32_t g_cust_per_dist;


