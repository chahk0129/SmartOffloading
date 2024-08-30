#pragma once
#include "common/global.h"

template <typename Key_t, typename Value_t> class tree_t;
class config_t;
class client_mr_t;
class client_transport_t;

class idx_wrapper_t{
    public:
	idx_wrapper_t(config_t* conf);

	// network
	client_transport_t* transport;

	// memory
	client_mr_t* mem;

	void insert(uint64_t key, uint64_t value, int tid);
	void update(uint64_t key, uint64_t value, int tid);
	bool search(uint64_t key, uint64_t& value, int tid);

    private:
	tree_t<uint64_t, uint64_t>* idx;
};
