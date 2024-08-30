#pragma once

#include "common/global.h"
#include "index/indirection.h"
#include "worker/scalable-cache.h"

#include <algorithm>
#include <atomic>

#define INDEX_PAGE_SIZE (1024 * 1024)
#define INDEX_CACHE_SIZE (1024 * 1024)

class worker_mr_t;
class worker_transport_t;

template <typename Key_t, typename Value_t>
class tree_t{
    public:
	tree_t(worker_mr_t* mem, worker_transport_t* transport, int pid=0);
	~tree_t() { }

	// utility helper functions
        void print();
        void sanity_check();
	void cache_stats();

        void insert(Key_t key, Value_t value, int tid);
        bool search(const Key_t& key, Value_t& value, int tid);
        bool search(const Key_t& key, Value_t& value, uint32_t& page_id, int tid);
	bool search_next(const Key_t& key, Value_t& value, uint32_t& page_id, int tid);
	int scan(const Key_t& key, Value_t* values, int num, int tid);

    private:
	// tree worker functions
        bool page_search(uint32_t page_addr, const Key_t& key, result_t<Value_t>& result, int tid);
        bool page_search_lastlevel(uint32_t page_addr, const Key_t& key, result_t<Value_t>& result, int tid);
        bool scan(uint32_t page_id, const Key_t& key, Value_t*& values, int num, int& cnt, int tid);
        bool store(uint32_t page_id, const Key_t& key, const Value_t& value, uint32_t root_id, int tid);
        void internal_insert(const Key_t& key, uint32_t value, uint8_t level, int tid);
        void internal_store(uint32_t page_id, const Key_t& key, uint32_t value, uint32_t root_id, uint8_t level, int tid);

	bool insert_to_cache(uint32_t node_id, uint64_t node_addr, int tid);
	bool update_new_root(uint32_t left, const Key_t& key, uint32_t right, uint8_t level, uint32_t old_root, int tid);
	bool update_root(uint32_t old_root, uint32_t new_root);
	void initialize_root();
	uint32_t get_root_id(){
	    return _root_id.load();
	}

	// alloc/dealloc host memory
	uint64_t rpc_alloc(int tid);
	void rpc_dealloc(int tid, uint64_t addr);

	void print_verbose();


	// root id of the tree index
	std::atomic<uint32_t> _root_id;
	int pid;

	// indirection table (node_id <-> node_addr)
	indirection_table_t* tab;
	// # of free index pages
	std::atomic<int64_t> free_pages;
#ifdef CACHE
	tstarling::ThreadSafeScalableCache<uint32_t, uint64_t>* cache;
#endif

	worker_mr_t* mem;
	worker_transport_t* transport;
};
