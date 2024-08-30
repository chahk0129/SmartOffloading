#pragma once

#include "common/debug.h"
#include "common/global.h"

class txn_man_t;

class lock_t{
    public:
	lock_t(): writer(0), reader(0){ }

	RC lock_get(lock_type_t type, txn_man_t* txn, uint64_t addr, int tid, int pid);
	void lock_release(lock_type_t type, txn_man_t* txn, uint64_t addr, int tid, int pid);

	uint32_t writer;
	uint32_t reader;

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

	void print(){
	    debug::notify_info("    # of readers: %u", reader);
	    debug::notify_info("    # of writers: %u", writer);
	}
};
