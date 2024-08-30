#pragma once
#include <atomic>
#include <cstdint>
#include "common/global.h"
#include "concurrency/row_lock.h"

class woundwait_t;
class waitdie_t;
class nowait_t;
class lock_t;
class txn_man_t;
class worker_transport_t;
class worker_mr_t;

class table_entry_t{
    public:
    	#ifdef WOUNDWAIT
	woundwait_t* manager;
    	#elif defined WAITDIE
    	waitdie_t* manager;
    	#elif defined NOWAIT
    	nowait_t* manager;
    	#else
    	lock_t manager;
    	#endif

	bool state; // local or remote
	uint32_t id;
	int pid;
	uint64_t local_addr;
	uint64_t remote_addr;
	std::atomic<uint64_t> timestamp;
	std::atomic<table_entry_t*> next;

	table_entry_t(uint32_t id, uint64_t local_addr, uint64_t remote_addr, int pid); // local
	table_entry_t(uint32_t id, uint64_t local_addr, uint64_t remote_addr, int pid, bool flag); // remote

	bool is_remote(){
	    return state == true;
	}

	void set_remote(){
	    state = true;
	}

	void set_local(){
	    state = false;
	}

	void update_timestamp(uint64_t cur_ts){
	    auto ts = timestamp.load();
	    if(ts < cur_ts){
		timestamp.compare_exchange_weak(ts, cur_ts);
	    }
	}

	RC lock(lock_type_t type, txn_man_t* txn, Access* access, int tid);
	RC lock(int tid);
	void release(access_t type, txn_man_t* txn, int client_id, int tid, uint64_t timestamp);
	void release(txn_man_t* txn, int client_id, int tid);
};

class page_table_t{
    public:
	page_table_t();
	page_table_t(size_t size);

	void set(uint32_t id, uint64_t local_addr, uint64_t remote_addr, int pid, bool is_remote);
	RC get(table_entry_t*& entry, uint32_t id, access_t type, txn_man_t* txn, Access* access, int tid);
	void replace(table_entry_t*& replace, txn_man_t* txn, int tid);
	uint64_t rpc_alloc(worker_transport_t* transport, worker_mr_t* mem, int tid, int pid);

	uint64_t get_next_id(){
	    return next_id.fetch_add(1);
	}

    private:
	uint64_t size;
	std::atomic<uint64_t> next_id;
	std::atomic<table_entry_t*>* tab;
};
