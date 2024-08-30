#pragma once
#include "common/rpc.h"
#include "common/global.h"
#include <vector>
#include <thread>
#include <chrono>

class worker_t;
class thread_t;
class table_t;
template <typename, typename>
class tree_t;
class row_t;
class worker_mr_t;
class worker_transport_t;
class base_query_t;
class base_request_t;
class page_table_t;
class table_entry_t;

#ifdef BREAKDOWN
struct breakdown_t{
    uint64_t start;
    uint64_t end;
    uint64_t abort;
    uint64_t index;
    uint64_t commit;
    uint64_t wait;
    uint64_t total;
    uint64_t backoff;

    // debug
    uint64_t critical_section;
    uint64_t notification;
    char dummy[64]; // total 2 cachelines
};
extern breakdown_t t[CLIENT_THREAD_NUM];
#endif

class Access{
    public:
	access_t type;
	int qp_id;
	int client_id;
	table_entry_t* entry;
	int rid;
	#ifndef LOCKTABLE
	uint64_t addr;
	int pid;
	bool is_remote;
	#endif
	lock_status_t lock_status;
	std::atomic<txn_status_t>* txn_status;
	uint64_t timestamp;
};

class txn_man_t{
    public:
	worker_t* worker;

	worker_mr_t* mem;
	worker_transport_t* transport;

	int row_cnt[CLIENT_THREAD_NUM];
	Access** accesses[CLIENT_THREAD_NUM];

	// main functions
	virtual void init(worker_t* worker);
	void release();

	virtual RC run_request(base_request_t* request, int tid) = 0;

    #if defined BATCH || defined BATCH2
	RC finish(RC rc, int qp_id, int client_id, int tid);
	RC finish_with_write(char* new_row, int qp_id, int client_id, int tid);
	RC cleanup(RC rc, int qp_id, int client_id, int tid);

	#ifdef LOCKTABLE
	row_t* get_row(RC& rc, uint32_t row_id, int qp_id, int client_id, int tid, int pid, access_t type, page_table_t* tab, table_entry_t*& entry, uint64_t timestamp); // lock-table locking
	#else // in-tuple locking
	row_t* get_row(RC& rc, uint64_t row_addr, int qp_id, int client_id, int tid, int pid, access_t type, bool is_remote);
	#endif

	void inc_row_cnt(Access* access);
	void notify(Access* access, int tid);
	bool wound(Access* access);
	void prepare_abort(Access* access);
	RC prepare_commit(Access* access);
	void flush(Access* access);
    #else // ifndef BATCH

	RC finish(RC rc, int qp_id, int tid);
	RC finish_with_write(char* new_row, int num, int qp_id, int tid);
	RC cleanup(RC rc, int qp_id, int tid);

	#ifdef LOCKTABLE
	row_t* get_row(RC& rc, uint32_t row_id, int qp_id, int tid, int pid, access_t type, page_table_t* tab, table_entry_t*& entry, uint64_t timestamp); // lock-table locking
	#else // in-tuple locking
	row_t* get_row(RC& rc, uint64_t row_addr, int qp_id, int tid, int pid, access_t type, bool is_remote);
        #endif

	void inc_row_cnt(Access* access);
	void wait(int qp_id, int tid);
	void notify(Access* access, int tid);
	bool wound(Access* access);
	void prepare_abort(Access* access);
	RC prepare_commit(Access* access);
	void flush(Access* acess);

    #endif // end of BATCH

	// helper functions
	worker_t* get_worker();
	int get_row_cnt(int client_id);

};

