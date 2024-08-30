#pragma once
#include "common/huge_page.h"
#include "common/debug.h"

//#define MR_SIZE (1024 * 1024 * 1024)
constexpr uint64_t MR_SIZE = (uint64_t)1024 * 1024 * 1024 * 4; 
//constexpr uint64_t MR_SIZE = (uint64_t)1024 * 1024 * 1024 * 4; // 10GB

class memory_region_t{
    public:
	memory_region_t(): _size(MR_SIZE){
	    addr = huge_page_alloc(_size);
	    if(!addr){
		debug::notify_error("failed to allocate memory_region");
		exit(0);
	    }
	    memset(addr, 0, _size);
	}

	memory_region_t(size_t _size): _size(_size){
	    addr = huge_page_alloc(_size);
	    if(!addr){
		debug::notify_error("failed to allocate memory_region");
		exit(0);
	    }
	    memset(addr, 0, _size);
	}

	~memory_region_t(){
	    huge_page_dealloc(addr, _size);
	}

	void* ptr(){
	    return addr;
	}

	size_t size(){
	    return _size;
	}

    private:
	void* addr;
	uint64_t _size;
};

