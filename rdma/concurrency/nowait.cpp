#include "concurrency/nowait.h"
#include "common/global.h"
#include "system/txn.h"
#include "client/transport.h"
#include <cassert>

bool nowait_t::lock_conflict(lock_type_t lt){
    if(lt == LOCK_EX){
	if(writer > 0 || reader > 0)
	    return true;
	return false;
    }
    else{ // LOCK_SH
	if(writer > 0)
	    return true;
	return false;
    }
    assert(false);
    return false;
}

RC nowait_t::lock_get(lock_type_t type, txn_man_t* txn, uint64_t addr, int tid, int pid){
    //txn->transport->read((uint64_t)this, addr, sizeof(uint64_t), tid, pid);
RETRY:
    bool conflict = lock_conflict(type);
    if(conflict){ // lock conflicts -- cannot acquire a lock
	return ABORT;
    }

    auto _lock = *this;
    if(type == LOCK_SH){
	_lock.reader++;
	if(!txn->transport->cas((uint64_t)this, addr, *(uint64_t*)this, *(uint64_t*)&_lock, sizeof(uint64_t), tid, pid)){
	    goto RETRY;
	}
    }
    else{
	_lock.writer++;
	if(!txn->transport->cas((uint64_t)this, addr, *(uint64_t*)this, *(uint64_t*)&_lock, sizeof(uint64_t), tid, pid)){
	    return ABORT;
	}
    }
    return RCOK;
}
    
void nowait_t::lock_release(lock_type_t type, txn_man_t* txn, uint64_t addr, int tid, int pid){
    bool ret = false;
    if(type == LOCK_SH){
	while(!ret){
	    auto _lock = *this;
	    _lock.reader--;
	    ret = txn->transport->cas((uint64_t)this, addr, *(uint64_t*)this, *(uint64_t*)&_lock, sizeof(uint64_t), tid, pid);
	}
    }
    else{ // LOCK_EX
	writer--;
	txn->transport->write((uint64_t)this, addr, sizeof(uint64_t), tid, pid);
    }
}
