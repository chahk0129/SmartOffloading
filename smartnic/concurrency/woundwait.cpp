#include "concurrency/woundwait.h"
#include "concurrency/entry.h"
#include "worker/txn.h"
#include "common/helper.h"
#include "common/stat.h"
#include "common/debug.h"

woundwait_t::woundwait_t(): owner_cnt(0), waiter_cnt(0), lock_type(LOCK_NONE), owners(nullptr), waiters_head(nullptr), waiters_tail(nullptr){
    latch = new pthread_mutex_t;
    pthread_mutex_init(latch, NULL);
}

RC woundwait_t::lock_get(lock_type_t type, txn_man_t* txn, Access* access, int tid){
    RC rc = RCOK;
    uint64_t timestamp = access->timestamp;
    std::vector<Access*> buffer;

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

    if(waiter_cnt == 0){ // no waiters
	bool conflict = lock_conflict(lock_type, type); // shared locks or empty
	if(!conflict){
	    STACK_PUSH(owners, entry);
	    owner_cnt++;
	    lock_type = type;
	    goto final;
	}
    }

    en = owners;
    while(en){
	if(!en->access){ // buffer replacement happening
	    rc = ABORT;
	    goto final;
	}
	else if(timestamp < en->access->timestamp && lock_conflict(lock_type, type)){ // cur txn has higher priority
	    if(!txn->wound(en->access)){ // this txn has already entered commit phase
		rc = ABORT;
		goto final;
	    }

	    // remove from owner
	    if(prev)
		prev->next = en->next;
	    else{
		if(owners == en)
		    owners = en->next;
		else
		    assert(false);
	    }

	    // drop lock for wounded txn
	    en->access->lock_status = lock_status_t::LOCK_DROPPED;
	    owner_cnt--;
	    if(owner_cnt == 0)
		lock_type = LOCK_NONE;
	}
	else{
	    prev = en;
	}
	en = en->next;
    }

    if(owner_cnt == 0){ // all the txns have been wounded
	STACK_PUSH(owners, entry);
	owner_cnt++;
	lock_type = type;
	assert(rc == RCOK);
    }
    else{ // some txns are running, insert to wait list
	// wait list is always in timestamp order
	if(waiters_head && waiters_tail){ // optimization
	    if(timestamp < waiters_head->access->timestamp){ // new txn has highest priority
	        LIST_INSERT_BEFORE(waiters_head, entry);
	        waiters_head = entry;
	    }
	    else if(waiters_tail->access->timestamp < timestamp){ // new txn has lowest priority
	        LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
	    }
	    else{
		auto head_ts = waiters_head->access->timestamp;
		auto tail_ts = waiters_tail->access->timestamp;
		auto head_diff = std::max(head_ts, timestamp) - std::min(head_ts, timestamp);
		auto tail_diff = std::max(tail_ts, timestamp) - std::min(tail_ts, timestamp);
		if(head_diff < tail_diff){ // traverse from head
		    en = waiters_head->next;
		    while(en->access->timestamp < timestamp){
			en = en->next;
		    }

		    if(en){
			LIST_INSERT_BEFORE(en, entry);
		    }
		    else{
			assert(false); // this should not be happening
			LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
		    }
		}
		else{ // traverse from tail
		    en = waiters_tail->prev;
		    while(timestamp <= en->access->timestamp){
			en = en->prev;
		    }

		    if(en){
			LIST_INSERT_AFTER(en, entry);
		    }
		    else{
			assert(false); // this should not be happening
			entry->next = waiters_head;
			waiters_head->prev = entry;
			waiters_head = entry;
		    }
		}
		/*
	        en = waiters_head->next;
	        while(en->access->timestamp < timestamp){
		    en = en->next;
	        }

	        if(en){
		    LIST_INSERT_BEFORE(en, entry);
	        }
	        else{
		    LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
	        }
		*/
	    }
	}
	else{
	    en = waiters_head;
	    while(en && (en->access->timestamp < timestamp)){
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
	}

	waiter_cnt++;
	rc = WAIT;
    }

    // optimization: try promoting locks (notify when it passes the critical section)
    bring_next(txn, buffer, tid);
    //bring_next(txn, tid);

 final:
    pthread_mutex_unlock(latch);
#ifdef BREAKDOWN
    uint64_t end = asm_rdtsc();
    uint64_t t_lock_critical_section = (end - start);
    start = end;
#endif

    // notify if there are any promoted
    if(!buffer.empty()){
	for(auto it=buffer.begin(); it!=buffer.end(); it++){
	    txn->notify(*it, tid);
	}
    }

#ifdef BREAKDOWN
    end = asm_rdtsc();
    uint64_t t_notification = (end - start);
    ADD_STAT(tid, time_lock_critical_section, t_lock_critical_section);
    ADD_STAT(tid, time_notification, t_notification);
    ADD_STAT(tid, count_lock_critical_section, 1);
#endif

    // free entry outside the critical section
    if(rc == ABORT)
	return_entry(entry);

    return rc;
}

// lock for DPU to host migration
RC woundwait_t::lock_get(int tid){
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


void woundwait_t::lock_release(txn_man_t* txn, int client_id, int tid){
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

    if(en){ // found the entry in the owner list
	if(prev)
	    prev->next = en->next;
	else{
	    if(en == owners)
		owners = en->next;
	}
	//debug::notify_info("tid %d --- %d unlocked \t(client_id %d)", tid, en->access->rid, en->access->client_id);
	temp = en;
	//return_entry(en);
	owner_cnt--;
	if(owner_cnt == 0)
	    lock_type = LOCK_NONE;
    }
    // else, it has already been removed

    if(owner_cnt == 0){
	lock_type = LOCK_NONE;
    }

    bring_next(txn, buffer, tid);
    //bring_next(txn, tid);

    pthread_mutex_unlock(latch);
#ifdef BREAKDOWN
    uint64_t end = asm_rdtsc();
    uint64_t t_unlock_critical_section = end - start;
    start = end;
#endif

    // notify promoted txns
    if(!buffer.empty()){
	for(auto it=buffer.begin(); it!=buffer.end(); it++){
	    txn->notify(*it, tid);
	}
    }

#ifdef BREAKDOWN
    end = asm_rdtsc();
    uint64_t t_notification = end - start;
    ADD_STAT(tid, time_unlock_critical_section, t_unlock_critical_section);
    ADD_STAT(tid, time_notification, t_notification);
    ADD_STAT(tid, count_unlock_critical_section, 1);
#endif

    // reclaim complete txns outside critical section
    if(temp)
	return_entry(temp);

}

void woundwait_t::bring_next(txn_man_t* txn, std::vector<Access*>& buffer, int tid){
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
 
void woundwait_t::bring_next(txn_man_t* txn, int tid){
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
 
bool woundwait_t::lock_conflict(lock_type_t l1, lock_type_t l2){
    if(l1 == LOCK_NONE || l2 == LOCK_NONE)
	return false;
    else if(l1 == LOCK_EX || l2 == LOCK_EX)
	return true;
    return false;
}

lock_entry_t* woundwait_t::get_entry(){
    lock_entry_t* entry = new lock_entry_t;
    return entry;
}

void woundwait_t::return_entry(lock_entry_t* entry){
    delete entry;
}
