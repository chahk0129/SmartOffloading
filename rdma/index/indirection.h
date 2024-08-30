#pragma once
#include <atomic>
#include <cstdint>
#include "common/huge_page.h"

#define DEFAULT_TABLE_SIZE 100000000


class indirection_table_t{
    public:
	indirection_table_t(): size(DEFAULT_TABLE_SIZE), next_id(1){
	    size_t tab_size = sizeof(uint64_t) * size;
	    auto addr = huge_page_alloc(tab_size);
	    memset(addr, 0, tab_size);

	    tab = reinterpret_cast<std::atomic<uint64_t>*>(addr);
	}

	indirection_table_t(size_t size): size(size), next_id(1){
	    size_t tab_size = sizeof(uint64_t) * size;
	    auto addr = huge_page_alloc(tab_size);
	    memset(addr, 0, tab_size);

	    tab = reinterpret_cast<std::atomic<uint64_t>*>(addr);
	}


	void clear_node_addr(const uint32_t node_id){
	    tab[node_id].store(0);
	}

	uint64_t get_node_addr(const uint32_t node_id){
	    return tab[node_id].load();
	}

	bool set_node_addr(const uint32_t node_id, uint64_t addr){
	    uint64_t empty = 0;
	    return tab[node_id].compare_exchange_strong(empty, addr);
	}

	int get_next_node_id(){
	    return next_id.fetch_add(1);
	}

    private:
	size_t size;
	std::atomic<uint64_t> next_id;
	std::atomic<uint64_t>* tab;
};

