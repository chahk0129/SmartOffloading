#pragma once
#include "common/debug.h"
//#include "common/common.h"
#include <cassert>

class allocator_t{
    public:
	allocator_t(const uint64_t start, uint64_t size): start(start){
	    end = start + size;
	    cur = start;
	}

	~allocator_t(){ }

	uint64_t alloc(uint64_t size){
	    if(cur + size >= end){
		debug::notify_error("Running out of shared memory space");
		assert(false);
	    }

	    uint64_t addr = cur;
	    cur += size;
	    return addr;
	}
	
	// TODO: free

    private:
	uint64_t start;
	uint64_t end;

	uint64_t cur;
};

