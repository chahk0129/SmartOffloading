#pragma once
#include <cstdint>
#include <atomic>
#include <pthread.h>
#include "common/global.h"

class txn_man_t;
struct lock_entry_t;
class Access;

class nowait_t{
    public:
	nowait_t();
	RC lock_get(lock_type_t type, txn_man_t* txn, Access* access, int tid);
	RC lock_get(int tid);
	void lock_release(txn_man_t* txn, int client_id, int tid);

    private:
	bool lock_conflict(lock_type_t l1, lock_type_t l2);
	lock_entry_t* get_entry();
	void return_entry(lock_entry_t* entry);

	pthread_mutex_t* latch;

	lock_type_t lock_type;
	uint32_t owner_cnt;
	uint32_t waiter_cnt;

	// owners is a single linked list
	lock_entry_t* owners; 

	// waiters is a double linked list that is maintained in timestamp order
	lock_entry_t* waiters_head; 
	lock_entry_t* waiters_tail;
};
