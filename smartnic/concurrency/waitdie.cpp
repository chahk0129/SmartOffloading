#include "concurrency/waitdie.h"
#include "concurrency/entry.h"
#include "worker/txn.h"
#include "common/helper.h"
#include "common/stat.h"
#include "common/debug.h"

waitdie_t::waitdie_t(): owner_cnt(0), waiter_cnt(0), lock_type(LOCK_NONE), owners(nullptr), waiters_head(nullptr), waiters_tail(nullptr){
    latch = new pthread_mutex_t;
    pthread_mutex_init(latch, NULL);
}

RC waitdie_t::lock_get(lock_type_t type, txn_man_t* txn, Access* access, int tid){
    RC rc = RCOK;
    uint64_t timestamp = access->timestamp;

    // preallocate lock entry to avoid mem allocation from being the bottleneck
    auto entry = get_entry();
    entry->type = type;
    entry->client_id = access->client_id;
    entry->access = access;

    lock_entry_t* en = nullptr;
    lock_entry_t* prev = nullptr;

#ifdef BREAKDOWN
    uint64_t start = asm_rdtsc();
#endif
    pthread_mutex_lock(latch);

#if 1
    if(waiter_cnt == 0){ // no waiters
	bool conflict = lock_conflict(lock_type, type); // shared locks or empty
	if(!conflict){
	    STACK_PUSH(owners, entry);
	    owner_cnt++;
	    lock_type = type;
	    goto final;
	}
    }

    // lock conflicts -- cannot be added to owner list
    en = owners;
    while(en){
	if(!en->access){ // buffer replacement happening
	    rc = ABORT;
	    goto final;
	}
	else if(en->access->timestamp < timestamp){ // compare timestamps
	    // cur tx has lower priority, cannot wait
	    rc = ABORT;
	    goto final;
	}
	// check all the owners
	en = en->next;
    }

    // insert txn to the right position
    // waiter list is always in timestamp order
    en = waiters_head;
    while(en && timestamp < en->access->timestamp){
	en = en->next;
    }

    if(en){
	LIST_INSERT_BEFORE(en, entry);
	if(en == waiters_head)
	    waiters_head = entry;
    }
    else{
	LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
    }

    waiter_cnt++;
    rc = WAIT;

#endif
#if 0
    bool conflict = lock_conflict(lock_type, type);
    if(!conflict){
	// previous txns are waiting, this txn should also wait
	if(waiters_head)
	    conflict = true;

	if(waiters_head && timestamp < waiters_head->access->timestamp){
	    conflict = true;
	}
    }

    if(!conflict){ // shared locks or empty
	// add it to owners list
	STACK_PUSH(owners, entry);
	owner_cnt++;
	lock_type = type;
    }
    else{ // lock conflicts -- cannot be added to the owner list
	bool wait = true;
	auto en = owners;
	while(en){
	    if(!en->access){ // buffer replacement happening
		rc = ABORT;
		goto final;
	    }
	    else if(en->access->timestamp < timestamp){ // compare timestamps
		// cur tx has lower priority, cannot wait
		rc = ABORT;
		goto final;
	    }
	    // check all the owners
	    en = en->next;
	}

	// insert txn to the right position
	// waiter list is always in timestamp order
	en = waiters_head;
	while(en && timestamp < en->access->timestamp)
	    en = en->next;

	if(en){
	    LIST_INSERT_BEFORE(en, entry);
	    if(en == waiters_head)
		waiters_head = entry;
	}
	else{
	    LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
	}
	waiter_cnt++;
	rc = WAIT;
    }
#endif
final:
    pthread_mutex_unlock(latch);
#ifdef BREAKDOWN
    uint64_t end = asm_rdtsc();
    uint64_t t_lock_critical_section = end - start;
    ADD_STAT(tid, time_lock_critical_section, t_lock_critical_section);
    ADD_STAT(tid, count_lock_critical_section, 1);
#endif

    // reclaim abort txn's lock entry
    if(rc == ABORT)
	return_entry(entry);

    return rc;
}

// lock for DPU to host migration
RC waitdie_t::lock_get(int tid){
    //debug::notify_info("LOCKGET for DPU to host migration");
    RC rc = RCOK;

    auto entry = get_entry();
    entry->type = LOCK_SH;
    entry->client_id = tid;

    pthread_mutex_lock(latch);
    bool conflict = lock_conflict(lock_type, entry->type);
    if(!conflict){ // lock conflicts -- cannot be added to the owner list
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


void waitdie_t::lock_release(txn_man_t* txn, int client_id, int tid){
    RC rc = RCOK;
    lock_entry_t* prev = nullptr;
    lock_entry_t* temp = nullptr;
    std::vector<Access*> buffer;

#ifdef BREAKDOWN
    uint64_t start = asm_rdtsc();
#endif
    pthread_mutex_lock(latch);
    auto en = owners;

    // find the entry in the owners list
    while(en && en->client_id != client_id){
	prev = en;
	en = en->next;
    }

    if(en){ // find the entry in the owner list
	if(prev)
	    prev->next = en->next;
	else
	    owners = en->next;
	temp = en;
	owner_cnt--;
	if(owner_cnt == 0)
	    lock_type = LOCK_NONE;
    }
    else{ // not in owners list, try waiters list
	en = waiters_head;
	while(en && en->client_id != client_id)
	    en = en->next;
	
	LIST_REMOVE(en);
	if(en == waiters_head)
	    waiters_head = en->next;
	if(en == waiters_tail)	
	    waiters_tail = en->prev;

	temp = en;
	waiter_cnt--;
    }

    if(owner_cnt == 0){
	lock_type = LOCK_NONE;
    }

    // check next proceeding txns
    bring_next(txn, buffer, tid);
    //bring_next(txn, tid);

    pthread_mutex_unlock(latch);
#ifdef BREAKDOWN
    uint64_t end = asm_rdtsc();
    uint64_t t_unlock_critical_section = end - start;
    start = end;
#endif

    // notify next txns to proceed (outside critical section)
    for(auto it=buffer.begin(); it!=buffer.end(); it++){
	txn->notify(*it, tid);
    }

#ifdef BREAKDOWN
    end = asm_rdtsc();
    uint64_t t_notification = end - start;
    ADD_STAT(tid, time_unlock_critical_section, t_unlock_critical_section);
    ADD_STAT(tid, time_notification, t_notification);
    ADD_STAT(tid, count_unlock_critical_section, 1);
#endif

    // reclaim complete txn's lock entry
    if(temp)
	return_entry(temp);
}

bool waitdie_t::lock_conflict(lock_type_t l1, lock_type_t l2){
    if(l1 == LOCK_NONE || l2 == LOCK_NONE)
	return false;
    else if(l1 == LOCK_EX || l2 == LOCK_EX)
	return true;
    return false;
}

lock_entry_t* waitdie_t::get_entry(){
    lock_entry_t* entry = new lock_entry_t;
    return entry;
}

void waitdie_t::return_entry(lock_entry_t* entry){
    delete entry;
}

void waitdie_t::bring_next(txn_man_t* txn, std::vector<Access*>& buffer, int tid){
    lock_entry_t* entry = nullptr;
    // if any waiter can join the owners, just do it
    while(waiters_head && !lock_conflict(lock_type, waiters_head->type)){
	LIST_GET_HEAD(waiters_head, waiters_tail, entry);
	waiter_cnt--;

	// promote this waiter to owner
	STACK_PUSH(owners, entry);
	owner_cnt++;

	lock_type = entry->type;
	buffer.push_back(entry->access);
	entry = nullptr;
    }
}
void waitdie_t::bring_next(txn_man_t* txn, int tid){
    lock_entry_t* entry = nullptr;
    // if any waiter can join the owners, just do it
    while(waiters_head && !lock_conflict(lock_type, waiters_head->type)){
	LIST_GET_HEAD(waiters_head, waiters_tail, entry);
	waiter_cnt--;

	// promote this waiter to owner
	STACK_PUSH(owners, entry);
	owner_cnt++;

	lock_type = entry->type;
	txn->notify(entry->access, tid);
	entry = nullptr;
    }
}
