#pragma once
#include "common/global.h"
#include <vector>
#include <chrono>

class workload_t;
class thread_t;
class table_t;
class base_query_t;
template <typename, typename>
class tree_t;
class row_t;
class client_mr_t;
class client_transport_t;

#ifdef BREAKDOWN
extern thread_local uint64_t t_abort, t_commit, t_index, t_wait, t_total, t_backoff;
extern thread_local uint64_t start, end;
#endif
class Access{
    public:
	access_t type;
	uint64_t addr;
	int pid;
	//int rid;

	void cleanup();
};

struct row_wrapper_t{
    char data[ROW_SIZE];
};

class txn_man_t{
    public:
	thread_t* thread;
	workload_t* workload;
	uint64_t abort_cnt;
	uint64_t insert_cnt;
	uint64_t txn_id;
	uint64_t timestamp;

	bool volatile lock_ready;
	bool volatile lock_abort;
	status_t volatile status; // RUNNING, COMMITTED, ABORTED, HOLDING

	int row_cnt;
	int wr_cnt;
	Access** accesses;
	int num_accesses_alloc;
	row_t* insert_rows[MAX_ROW_PER_TXN];
	row_wrapper_t* write_buffer;


	client_mr_t* mem;
	client_transport_t* transport;


//	RC set_readlock(row_t* row, uint64_t row_addr, int tid, int pid);
//	RC set_writelock(row_t* row, uint64_t row_addr, int tid, int pid);
//	void unset_readlock(row_t* row, uint64_t row_addr, int tid, int pid);
//	void unset_writelock(row_t* row, uint64_t row_addr, int tid, int pid);

	// main functions
	virtual void init(thread_t* thread, workload_t* workload, int tid);
	void release();
	virtual RC run_txn(base_query_t* query) = 0;
	RC finish(RC rc, int tid);
	void cleanup(RC rc, int tid);
	void return_row(row_t* row, uint64_t addr, access_t type, int tid, int pid);
	
	// helper functions
	//   getters
	int get_tid();
	workload_t* get_workload();
	uint64_t get_txn_id();
	uint64_t get_ts();
	//   setters
	void set_txn_id(uint64_t txn_id);
	void set_ts(uint64_t ts);
	void reassign_ts();

	row_t* get_row(uint64_t row_addr, int tid, int pid, access_t type);
	//row_t* get_row(uint64_t row_addr, int rid, int tid, int pid, access_t type);
	row_t* get_new_row(uint64_t addr, int tid, int pid);

    protected:
	void insert_row(row_t* row, table_t* table);
	void index_insert(row_t* row, tree_t<Key, Value>* index, Key key, int tid);
	void index_read(tree_t<Key, Value>* index, Key key, uint64_t& item, int tid);
	uint64_t index_read(tree_t<Key, Value>* index, Key key, int tid);

    private:
	void assign_lock_entry(Access* access);

};

