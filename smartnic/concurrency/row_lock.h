#pragma once
#include <cstdint>
#include <atomic>
#include "common/global.h"

class txn_man_t;

class lock_t{
    public:
	lock_t(): writer(0), reader(0){ }

	void init();

	// local locktable operations
	RC lock_get(lock_type_t type);
	RC lock_get();
	void lock_release(lock_type_t type);
	void lock_release();

	// tuple-lock operations
	RC lock_get(lock_type_t type, txn_man_t* txn, uint64_t addr, int tid, int pid);
	void lock_release(lock_type_t type, txn_man_t* txn, uint64_t addr, int tid, int pid);
	RC lock_get(int qp_id, int tid);
	void lock_release(int qp_id, int tid);

	bool is_exclusive(){
	    if(writer != 0 && reader == 0)
		return true;
	    return false;
	}

	bool is_shared(){
	    if(reader != 0 && writer == 0)
		return true;
	    return false;
	}

	void acquire_shared(){
	    reader++;
	}

	void acquire_exclusive(){
	    writer++;
	}

	void release_shared(){
	    reader--;
	}

	void release_exclusive(){
	    writer--;
	}

	uint32_t writer;
	uint32_t reader;

};
