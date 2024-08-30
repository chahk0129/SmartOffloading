#include "concurrency/row_lock.h"
#include "worker/txn.h"
#include "worker/transport.h"
#include "common/helper.h"

void lock_t::init(){
    reader = 0;
    writer = 0;
}

void lock_t::lock_release(lock_type_t type){
    if(type == LOCK_SH)
	ATOM_SUB(reader, 1);
    else
	release_exclusive();
}
	
void lock_t::lock_release(){
    release_exclusive();
}
	
RC lock_t::lock_get(lock_type_t type){
RETRY:
    auto _lock = *this;
    if(type == LOCK_SH){
	if(_lock.is_exclusive())
	    return ABORT;

	auto swap = _lock;
	swap.acquire_shared();
	if(!ATOM_CAS((uint64_t*)this, *(uint64_t*)&_lock, *(uint64_t*)&swap))
	    goto RETRY;
    }
    else{
	if(_lock.is_exclusive() || _lock.is_shared())
	    return ABORT;

	auto swap = _lock;
	swap.acquire_exclusive();
	if(!ATOM_CAS((uint64_t*)this, *(uint64_t*)&_lock, *(uint64_t*)&swap))
	    return ABORT;
    }
    return RCOK;
}

RC lock_t::lock_get(){
    auto _lock = *this;
    if(_lock.is_exclusive() || _lock.is_shared())
	return ABORT;

    auto swap = _lock;
    swap.acquire_exclusive();
    if(!ATOM_CAS((uint64_t*)this, *(uint64_t*)&_lock, *(uint64_t*)&swap))
	return ABORT;
    return RCOK;
}


void lock_t::lock_release(lock_type_t type, txn_man_t* txn, uint64_t addr, int tid, int pid){
    bool ret = false;
    if(type == LOCK_SH){
	while(!ret){
	    auto _lock = *this;
	    _lock.release_shared();
	    ret = txn->transport->cas((uint64_t)this, addr, *(uint64_t*)this, *(uint64_t*)&_lock, sizeof(uint64_t), tid, pid);
	}
    }
    else{
	release_exclusive();
	txn->transport->write((uint64_t)this, addr, sizeof(uint64_t), tid, pid);
    }
}

RC lock_t::lock_get(lock_type_t type, txn_man_t* txn, uint64_t addr, int tid, int pid){
RETRY:
    auto _lock = *this;
    if(type == LOCK_SH){
	if(_lock.is_exclusive())
	    return ABORT;

	_lock.acquire_shared();
        if(!txn->transport->cas((uint64_t)this, addr, *(uint64_t*)this, *(uint64_t*)&_lock, sizeof(uint64_t), tid, pid)){
            goto RETRY;
        }
    }
    else{
        if(_lock.is_exclusive() || _lock.is_shared())
            return ABORT;

        _lock.acquire_exclusive();
        if(!txn->transport->cas((uint64_t)this, addr, *(uint64_t*)this, *(uint64_t*)&_lock, sizeof(uint64_t), tid, pid)){
            return ABORT;
        }
    }
    return RCOK;
}

