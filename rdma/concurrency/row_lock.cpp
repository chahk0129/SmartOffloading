#include "concurrency/row_lock.h"
#include "client/transport.h"
#include "system/txn.h"

RC lock_t::lock_get(lock_type_t type, txn_man_t* txn, uint64_t addr, int tid, int pid){
RETRY:
    auto _lock = *this;
    if(type == LOCK_SH){
	if(_lock.is_exclusive())
	    return ABORT;

	_lock.acquire_shared();
	if(!txn->transport->cas((uint64_t)this, addr, *(uint64_t*)this, *(uint64_t*)&_lock, sizeof(uint64_t), tid, pid)){
	    if(is_exclusive())
		return ABORT;
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

void lock_t::lock_release(lock_type_t type, txn_man_t* txn, uint64_t addr, int tid, int pid){
    bool ret = false;
    if(type == LOCK_SH){
	while(!ret){
	    auto _lock = *this;
	    _lock.release_shared();
	    ret = txn->transport->cas((uint64_t)this, addr, *(uint64_t*)this, *(uint64_t*)&_lock, sizeof(uint64_t), tid, pid);
	}
    }
    else{ // LOCK_EX
	release_exclusive();
	txn->transport->write((uint64_t)this, addr, sizeof(uint64_t), tid, pid);
    }
}
