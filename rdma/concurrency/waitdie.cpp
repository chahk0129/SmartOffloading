#include "concurrency/waitdie.h"
#include "common/global.h"
#include "system/txn.h"
#include "client/transport.h"
#include <cassert>

bool waitdie_t::lock_conflict(lock_type_t lt){
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

RC waitdie_t::lock_get(lock_type_t type, txn_man_t* txn, uint64_t addr, int tid, int pid){
    bool retry = false;
RETRY:
    if(retry)
	txn->transport->read((uint64_t)this, addr, sizeof(waitdie_t), tid, pid);
    uint64_t ts = txn->get_ts();
    bool conflict = lock_conflict(type);
    if(conflict){ // lock conflicts -- cannot acquire a lock
	if(ts < timestamp){ // can wait
	    retry = true;
	    goto RETRY;
	}
	return ABORT;
    }

    auto _lock = *this;
    if(type == LOCK_SH){
	_lock.reader++;
	assert(_lock.writer == 0);
	if(!txn->transport->cas((uint64_t)this, addr, *(uint64_t*)this, *(uint64_t*)&_lock, sizeof(uint64_t), tid, pid)){
	    retry = true;
	    goto RETRY;
	}
    }
    else{
	_lock.writer++;
	if(!txn->transport->cas((uint64_t)this, addr, *(uint64_t*)this, *(uint64_t*)&_lock, sizeof(uint64_t), tid, pid)){
	    return ABORT;
	}
    }

RETRY_TIMESTAMP:
    if(timestamp != 0 && ts < timestamp){
	if(!txn->transport->cas((uint64_t)this + sizeof(uint64_t), addr + sizeof(uint64_t), timestamp, ts, sizeof(uint64_t), tid, pid)){
	    txn->transport->read((uint64_t)this, addr, sizeof(waitdie_t), tid, pid);
	    goto RETRY_TIMESTAMP;
	}
    }
    else if(timestamp == 0){
	if(!txn->transport->cas((uint64_t)this + sizeof(uint64_t), addr + sizeof(uint64_t), timestamp, ts, sizeof(uint64_t), tid, pid)){
	    txn->transport->read((uint64_t)this, addr, sizeof(waitdie_t), tid, pid);
	    goto RETRY_TIMESTAMP;
	}
    }
	
    return RCOK;
}
    
void waitdie_t::lock_release(lock_type_t type, txn_man_t* txn, uint64_t addr, int tid, int pid){
    bool retry = false;
RETRY:
    if(type == LOCK_SH){
	if(retry)
	    txn->transport->read((uint64_t)this, addr, sizeof(waitdie_t), tid, pid);
	auto _lock = *this;
	_lock.reader--;

	assert(_lock.writer == 0);
	if(!txn->transport->cas((uint64_t)this, addr, *(uint64_t*)this, *(uint64_t*)&_lock, sizeof(uint64_t), tid, pid)){
	    retry = true;
	    goto RETRY;
	}

RETRY_TIMESTAMP:
	if(reader == 0 && writer == 0){
	    uint64_t ts = 0;
	    if(!txn->transport->cas((uint64_t)this + sizeof(uint64_t), addr + sizeof(uint64_t), timestamp, ts, sizeof(uint64_t), tid, pid)){
		txn->transport->read((uint64_t)this, addr, sizeof(waitdie_t), tid, pid);
		goto RETRY_TIMESTAMP;
	    }
	}
    }
    else{ // LOCK_EX
	writer--;
	timestamp = 0;
	txn->transport->write((uint64_t)this, addr, sizeof(waitdie_t), tid, pid);
    }
}
