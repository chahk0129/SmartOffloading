#include "concurrency/nowait.h"
#include "concurrency/entry.h"
#include "common/helper.h"
#include "worker/txn.h"

nowait_t::nowait_t(): owner_cnt(0), waiter_cnt(0), lock_type(LOCK_NONE), owners(nullptr), waiters_head(nullptr), waiters_tail(nullptr){
    latch = new pthread_mutex_t;
    pthread_mutex_init(latch, NULL);
}

RC nowait_t::lock_get(lock_type_t type, txn_man_t* txn, Access* access, int tid){
    RC rc = RCOK;
    pthread_mutex_lock(latch);

    bool conflict = lock_conflict(lock_type, type);
    if(conflict){ // lock conflicts -- cannot be added to the owner list
	pthread_mutex_unlock(latch);
	return ABORT;
    }
     // no conflict -- add it to owners list
    auto entry = get_entry();
    entry->type = type;
    entry->client_id = access->client_id;
    entry->access = access;
    STACK_PUSH(owners, entry);
    owner_cnt++;
    lock_type = type;

    pthread_mutex_unlock(latch);
    return rc;
}

// lock for DPU to host migration
RC nowait_t::lock_get(int tid){
    RC rc = RCOK;
    auto entry = get_entry();
    entry->type = LOCK_EX;
    entry->client_id = tid;
    entry->access = nullptr;

    pthread_mutex_lock(latch);

    bool conflict = lock_conflict(lock_type, entry->type);
    if(conflict){ // lock conflicts -- cannot be added to the owner list
	pthread_mutex_unlock(latch);
	return_entry(entry);
	return ABORT;
    }
     // no conflict -- add it to owners list
    STACK_PUSH(owners, entry);
    owner_cnt++;
    lock_type = entry->type;

    pthread_mutex_unlock(latch);
    return rc;
}


void nowait_t::lock_release(txn_man_t* txn, int client_id, int tid){
    RC rc = RCOK;
    lock_entry_t* prev = nullptr;
    pthread_mutex_lock(latch);
    auto en = owners;

    // find the entry in the owners list
    while(en && en->client_id != client_id){
	prev = en;
	en = en->next;
    }

    if(prev)
	prev->next = en->next;
    else
	owners = en->next;
    return_entry(en);
    owner_cnt--;
    if(owner_cnt == 0)
	lock_type = LOCK_NONE;

    pthread_mutex_unlock(latch);
}

bool nowait_t::lock_conflict(lock_type_t l1, lock_type_t l2){
    if(l1 == LOCK_NONE || l2 == LOCK_NONE)
	return false;
    else if(l1 == LOCK_EX || l2 == LOCK_EX)
	return true;
    return false;
}

lock_entry_t* nowait_t::get_entry(){
    lock_entry_t* entry = new lock_entry_t;
    return entry;
}

void nowait_t::return_entry(lock_entry_t* entry){
    delete entry;
}
