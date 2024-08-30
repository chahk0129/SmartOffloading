#pragma once

#include <unistd.h>
#include <cstdint>
#include <cstring>
#include <string>
#include <cassert>
#include <cstdio>
#include <iostream>
#include <random>
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

using Key = uint64_t;
using Value = uint32_t;

class query_queue_t;
class stat_t;
class ycsb_txn_man_t;
class tpcc_txn_man_t;

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
#elif __aarch64__
    unsigned long long val;
    asm volatile("mrs %0, cntvct_el0" : "=r" (val));
    return (unsigned long long)((double)val / 0.2);
#else
    return 0;
#endif
}

#define THREAD_PER_CORE 1
static void bind_core_client(uint16_t core){
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core/THREAD_PER_CORE, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if(rc != 0)
        std::cerr << "Failed to bind core" << std::endl;
}

static void bind_core_worker(uint16_t core){
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(),           sizeof(cpu_set_t), &cpuset);
    if(rc != 0)
        std::cerr << "Failed to bind core" << std::endl;
}


extern std::mt19937 rng;
static bool test_probability(){
    return (rng() % 100) < 10;
    //return rng() & 1;
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
    uint32_t sibling;
    uint32_t child;
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
    READ_DATA,
    WRITE_DATA,
    XP, 
    SCAN, 
    CM,
    COMMIT,
    COMMIT_DATA,
    ABORT_ALL,
};

enum lock_type_t{
    LOCK_NONE,
    LOCK_SH,
    LOCK_EX,
};

enum lock_status_t{
    LOCK_READY,
    LOCK_WAIT,
    LOCK_DROPPED,
};

enum txn_status_t{
    RUNNING,
    ABORTING,
    COMMITTING,
};

enum RC{
    RCOK, 
    //COMMIT, 
    ABORT, 
    WAIT, 
    RESUME,
    ERROR, 
    FINISH
};

enum request_type_t{
    REQ_INSERT = 0,
    REQ_UPDATE,
    REQ_READ,
    REQ_SCAN,
};

enum ycsb_workload_type_t{
    YCSB_WORKLOAD_A = 0,
    YCSB_WORKLOAD_B,
    YCSB_WORKLOAD_C,
    YCSB_WORKLOAD_D,
    YCSB_WORKLOAD_E,
};

// storage config
#define TABLE_NAME_LENGTH 	32
#define NUM_COLUMNS 		25
#define ROW_LENGTH		1000
#define ROW_SIZE 		1040

// index config
#define MAX_TREE_LEVEL 		10
#define PAGE_SIZE 		1024
#define CACHELINE_SIZE		64

// mr config
#define ROOT_BUFFER_SIZE 	8
#define CAS_BUFFER_SIZE 	8
#define PAGE_BUFFER_SIZE 	PAGE_SIZE
#define SIBLING_BUFFER_SIZE 	PAGE_SIZE

// txn config 
#define MAX_TRANSACTION 	1000
#define WARMUP 			10000
//#define WARMUP 			1000
#define STATS_ENABLE 		true
#define THREAD_CNT 		64
//#define MAX_RUNTIME 		10
#define MAX_RUNTIME 		30
#define REFRESH_TIME 		3
#define BACKOFF 30000
//#define BACKOFF 10000
//#define BACKOFF 15000
//#define BACKOFF 30000
#define TIME

// client config
#define CLIENT_THREAD_NUM 	128
//#define CLIENT_THREAD_NUM 	2
#define HASH_FUNC		1
//#define BATCH_THREAD_NUM 	4
#define BATCH_THREAD_NUM 	8
#define NETWORK_THREAD_NUM 	(CLIENT_THREAD_NUM / BATCH_THREAD_NUM)
//#define BATCH2
#define BATCH_SIZE 		8
#define PER_THREAD_BUFFER
#define PER_BATCH_SIZE 		(CLIENT_THREAD_NUM / WORKER_THREAD_NUM)
//#define BATCH_SIZE 		16
//#define BATCH

// server config
#define SERVER_NUM 		1
#define SERVER_THREAD_NUM 	1
#define MR_PARTITION_NUM 	25

// worker config
#define WORKER_THREAD_NUM	8
#define LOCKTABLE
#define BUFFER

// benchmark config
#define DEFAULT_INIT_THREADS 	128
#define DEFAULT_RUN_THREADS	64
#define DEFAULT_SAMPLING_RATE	0.1
extern uint64_t g_init_parallelism;
extern uint64_t g_run_parallelism;
extern double g_sampling_rate;
extern bool g_measure_latency;
extern bool g_run_finish;
extern double zipfian;
extern std::atomic<uint32_t> warmup_cnt;
extern bool warmup_finish;
extern query_queue_t* query_queue;

extern char* output_file;
extern stat_t* stat;

#ifdef YCSB
#define WORKLOAD 		YCSB
#define MAX_ROW_PER_TXN		REQUEST_PER_QUERY
#elif defined TPCC
#define WORKLOAD 		TPCC
#define MAX_ROW_PER_TXN		100
#else
#define MAX_ROW_PER_TXN 	100
#endif

// YCSB config
#define SYNTH_TABLE_SIZE 	(10000000)
#define YCSB_KEY_SIZE		(10000000) // 10M
#define ZIPFIAN_THETA		(0.9)
//#define YCSB_PAGE_NUM 		10000000 // 1GB
#if YCSB
#define DEFAULT_BUFFER_SIZE 	(9000000) // 8GB
#elif defined TPCC
#define DEFAULT_BUFFER_SIZE	7000000 // for up to 64 WHs 
//#define DEFAULT_BUFFER_SIZE	(3000000) // for 128 WHs (16GB equipped BF-2)
//#define DEFAULT_BUFFER_SIZE 	(10000000) // 10GB
#endif
#define PROBE_DISTANCE 		4
#define VICTIM_SIZE 		4
//#define REQUEST_PER_QUERY 	64
//#define REQUEST_PER_QUERY 	32
//#define REQUEST_PER_QUERY 	16
//#define REQUEST_PER_QUERY 	4
#define REQUEST_PER_QUERY 	8

extern uint64_t g_synth_table_size;
extern uint64_t global_ycsb_key_space;
extern ycsb_workload_type_t ycsb_workload_type;
extern ycsb_txn_man_t* global_ycsb_txn_man;

// TPCC config
#define TPCC_SMALL		false
//#define TPCC_SMALL		true
#define PART_CNT		1
#define WH_UPDATE		true
//#define NUM_WH			8
//#define NUM_WH			128
//#define NUM_WH			4
#define NUM_WH			1
//#define PERC_PAYMENT		1.0
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
extern tpcc_txn_man_t* global_tpcc_txn_man;

// CC
//#define NOWAIT
//#define WAITDIE
#define WOUNDWAIT


//#define INTERACTIVE
