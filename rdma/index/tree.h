#pragma once

#include "common/global.h"
#include "index/indirection.h"

#include <algorithm>
#include <atomic>

class client_mr_t;
class client_transport_t;

template <typename Key_t, typename Value_t>
class tree_t{
    public:
	tree_t(client_mr_t* mem, client_transport_t* transport, int pid=0);
	~tree_t() { }

	// utility helper functions
        void print();
        void sanity_check();

        void insert(Key_t key, Value_t value, int tid);
        bool search(const Key_t& key, Value_t& value, int tid);
        bool search(const Key_t& key, Value_t& value, uint64_t& page_addr, int tid);
	bool search_next(const Key_t& key, Value_t& value, uint64_t& page_addr, int tid);
	int scan(const Key_t& key, Value_t*& values, int num, int tid);

    private:
	// tree worker functions
        bool page_search(uint64_t page_addr, const Key_t& key, result_t<Value_t>& result, int tid);
	bool page_search_lastlevel(uint64_t page_addr, const Key_t& key, result_t<Value_t>& result, int tid);
        bool scan(uint64_t page_addr, const Key_t& key, Value_t*& values, int num, int& cnt, int tid);
        bool store(uint64_t page_addr, const Key_t& key, const Value_t& value, uint64_t root_addr, int tid);
        void internal_insert(const Key_t& key, uint64_t value, uint8_t level, int tid);
        void internal_store(uint64_t page_addr, const Key_t& key, uint64_t value, uint64_t root_addr, uint8_t level, int tid);

	bool update_new_root(uint64_t left, const Key_t& key, uint64_t right, uint8_t level, uint64_t old_root_addr, int tid);
	void initialize_root();
	uint64_t get_root_addr(int tid);

	// alloc/dealloc host memory
	uint64_t rpc_alloc(int tid);
	void rpc_dealloc(int tid, uint64_t addr);

	void print_verbose();


	uint64_t _root_addr;
	int pid;

	client_mr_t* mem;
	client_transport_t* transport;
};
