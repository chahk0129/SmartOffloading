#pragma once
#include <sys/mman.h>
#include <memory.h>
#include <iostream>

inline void* huge_page_alloc(size_t size){
    void* addr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if(addr == MAP_FAILED)
	std::runtime_error("Failed to alloc huge_page");
    return addr;
}

inline void huge_page_dealloc(void* addr, size_t size){
    int ret = munmap(addr, size);
    if(ret)
	std::runtime_error("Failed to dealloc huge_page");
}
