#include "worker/txn.h"
#include "storage/row.h"
#include "worker/mr.h"
#include "worker/transport.h"
#include "worker/worker.h"
#include "worker/thread.h"
#include "index/tree.h"
#include "common/stat.h"
#include "worker/page_table.h"
#include <algorithm>

#ifdef BREAKDOWN
breakdown_t t[CLIENT_THREAD_NUM];
#endif

void txn_man_t::init(worker_t* worker){
    this->worker = worker;
    for(int i=0; i<CLIENT_THREAD_NUM; i++){
	accesses[i] = new Access*[MAX_ROW_PER_TXN];
	memset(accesses[i], 0, sizeof(Access*)*MAX_ROW_PER_TXN);
	row_cnt[i] = 0;
	#ifdef BREAKDOWN
	memset(&t[i], 0, sizeof(breakdown_t));
	#endif
    }

    this->mem = worker->mem;
    this->transport = worker->transport;
}

worker_t* txn_man_t::get_worker(){
    return worker;
}

int txn_man_t::get_row_cnt(int client_id){
    return row_cnt[client_id];
}

#if defined BATCH || defined BATCH2
RC txn_man_t::finish(RC rc, int qp_id, int client_id, int tid){
    return cleanup(rc, qp_id, client_id, tid);
}

RC txn_man_t::cleanup(RC rc, int qp_id, int client_id, int tid){
    auto access = accesses[client_id];
    RC _rc = rc;

    if(_rc == ABORT)
	prepare_abort(access[0]);
    else
	_rc = prepare_commit(access[0]);

    int _row_cnt = row_cnt[client_id];
    for(int rid=_row_cnt-1; rid>=0; rid--){
	auto type = access[rid]->type;
	if(type == WRITE && _rc == ABORT){
	    type = XP;
	}

	#ifdef LOCKTABLE
	auto entry = access[rid]->entry;
	if(_rc == ABORT){ // abort reads
	    // this lock has been already dropped by preemption
	    if(access[rid]->lock_status == lock_status_t::LOCK_DROPPED){
		continue;
	    }
	}
	entry->release(type, this, client_id, tid, access[rid]->timestamp);
	#else
	assert(_rc == RCOK); // txns cannot be wounded without the support of locktable
	auto addr = access[rid]->addr;
	auto pid = access[rid]->pid;
	auto is_remote = access[rid]->is_remote;
	row_t* row;
	if(is_remote){
	    row = mem->row_buffer_pool(tid);
	    row->return_row(type, this, get_unmasked_addr(addr), tid, pid);
	}
	else{
	    row = (row_t*)addr;
	    row->return_row(type);
	}
	#endif
    }

    flush(access[0]);
    row_cnt[client_id] = 0;

    return _rc;
}

RC txn_man_t::finish_with_write(char* _new_row, int qp_id, int client_id, int tid){
    auto access = accesses[client_id];
    int _row_cnt = row_cnt[client_id];
    RC rc = prepare_commit(access[0]);

    for(int rid=_row_cnt-1; rid>=0; rid--){
	auto _access = access[rid];
	auto type = _access->type;

	row_t* row;
	#ifdef LOCKTABLE
	auto entry = _access->entry;
	if(rc == ABORT){ // abort reads and writes
	    if(_access->lock_status == lock_status_t::LOCK_DROPPED){
		continue;
	    }
	}
	else{
	    if(type == WRITE){ // commit writes
		auto new_row = (row_t*)&_new_row[sizeof(row_t) * rid];
		if(entry->is_remote()){
		    row = mem->row_buffer_pool(tid);
		    memcpy(row, new_row, sizeof(row_t));
		    transport->write((uint64_t)row, entry->remote_addr, sizeof(row_t), tid, entry->pid);
		}
		else{
		    row = (row_t*)entry->local_addr;
		    row->copy(new_row);
		}
	    }
	}
		
	entry->release(type, this, client_id, tid, _access->timestamp);
	#else
	auto is_remote = _access->is_remote;
	assert(rc == RCOK); // txns cannot be wounded without the support of locktable
	if(type == WRITE){ // commit writes
	    auto new_row = (row_t*)&_new_row[sizeof(row_t) * rid];
	    if(is_remote){
		row = mem->row_buffer_pool(tid);
		memcpy(row, new_row, sizeof(row_t));
		transport->write((uint64_t)row, get_unmasked_addr(_access->addr), sizeof(row_t), tid, _access->pid);
	    }
	    else{
		row = (row_t*)_access->addr;
		row->copy(new_row);
	    }
	}

	if(is_remote)
	    row->return_row(type, this, get_unmasked_addr(_access->addr), tid, _access->pid);
	else
	    row->return_row(type);
	#endif // end of LOCKTABLE
    }

    flush(access[0]);
    row_cnt[client_id] = 0;
    return rc;
}

void txn_man_t::inc_row_cnt(Access* access){
    row_cnt[access->client_id]++;
}

void txn_man_t::prepare_abort(Access* access){
    auto txn_status = access->txn_status;
    auto txn_status_value = txn_status->load();
    if(txn_status_value == txn_status_t::ABORTING)
	return;
    assert(txn_status_value == txn_status_t::RUNNING);
    if(!txn_status->compare_exchange_strong(txn_status_value, txn_status_t::ABORTING)){
	assert(txn_status->load() == txn_status_t::ABORTING);
    }
}

RC txn_man_t::prepare_commit(Access* access){
    auto txn_status = access->txn_status;
    auto txn_status_value = txn_status->load();
    if(txn_status_value == txn_status_t::ABORTING){ // this txn has been wounded
        return ABORT;
    }
    assert(txn_status_value == txn_status_t::RUNNING);
    if(!txn_status->compare_exchange_strong(txn_status_value, txn_status_t::COMMITTING)){ // this txn has been wounded
        return ABORT;
    }
    return RCOK;
}

void txn_man_t::flush(Access* access){
    auto txn_status = access->txn_status;
    auto txn_status_value = txn_status->load();
    //assert(txn_status_value == txn_status_t::COMMITTING);
    txn_status->store(txn_status_t::RUNNING);
}


#ifdef LOCKTABLE
row_t* txn_man_t::get_row(RC& rc, uint32_t row_id, int qp_id, int client_id, int tid, int pid, access_t type, page_table_t* tab, table_entry_t*& entry, uint64_t timestamp){
    auto access = accesses[client_id];
    int _row_cnt = row_cnt[client_id];
    if(access[_row_cnt] == nullptr){
	auto _access = new Access;
	access[_row_cnt] = _access;
	std::atomic<txn_status_t>* txn_status = nullptr;
	if(_row_cnt == 0){
	    txn_status = new std::atomic<txn_status_t>;
	    txn_status->store(txn_status_t::RUNNING);
	}
	else{
	    txn_status = access[0]->txn_status;
	}
	access[_row_cnt]->txn_status = txn_status;
    }

    auto txn_status = access[_row_cnt]->txn_status->load();
    if(txn_status == txn_status_t::ABORTING){ // this txn has been wounded
	assert(_row_cnt > 0);
	rc = ABORT;
	cleanup(rc, qp_id, client_id, tid);
	return nullptr;
    }

    access[_row_cnt]->qp_id = qp_id;
    access[_row_cnt]->client_id = client_id;
    access[_row_cnt]->timestamp = timestamp;
    access[_row_cnt]->type = type;
    access[_row_cnt]->lock_status = lock_status_t::LOCK_READY;

    rc = tab->get(entry, row_id, type, this, access[_row_cnt], tid);
    if(rc == ABORT){
	if(_row_cnt > 0){
	    cleanup(rc, qp_id, client_id, tid);
	}
	return nullptr;
    }
    else if(rc == WAIT){
	return nullptr;
    }

    // RCOK
    row_t* row = nullptr;
    if(entry->is_remote()){
	row = mem->row_buffer_pool(tid);
    }
    else{
	row = (row_t*)entry->local_addr;
    }

    //access[_row_cnt]->entry = entry;
    row_cnt[client_id]++;
    return row;
}

bool txn_man_t::wound(Access* access){
    auto status = access->txn_status->load();
    if(status == txn_status_t::RUNNING){
	// failed to wound this txn --- this has entered commit phase
	if(!access->txn_status->compare_exchange_strong(status, txn_status_t::ABORTING)){
	    return false;
	}
	// wounded this txn
	access->lock_status = lock_status_t::LOCK_DROPPED;
	return true;
    }
    else if(status == txn_status_t::ABORTING){ // this txn is aborting
	access->lock_status = lock_status_t::LOCK_DROPPED;
	return true;
    }
    assert(status == txn_status_t::COMMITTING);
    return false;
}


void txn_man_t::notify(Access* access, int tid){
    int qp_id = access->qp_id;
    int client_id = access->client_id;
    int local_client_id = client_id % BATCH_THREAD_NUM;

    //auto ptr = mem->rpc_response_pool(tid);
    auto ptr = mem->rpc_notify_response_pool(tid);
    auto response = create_message<rpc_response_t>((void*)ptr, tid, 1);

    auto res = response->get_buffer(0);
    row_cnt[client_id]++;
    auto entry = access->entry;

    row_t* row;
    if(entry->is_remote()){
	row = mem->row_buffer_pool(tid);
	transport->read((uint64_t)row, entry->remote_addr, sizeof(row_t), tid, entry->pid);
    }
    else{
	row = (row_t*)entry->local_addr;
    }

    //rpc_notify->num = 1;
    res->update(client_id, RCOK);
    memcpy(res->data, row, sizeof(row_t));

    size_t response_size = sizeof(int) * 2 + sizeof(rpc_response_buffer_t);
    //size_t response_size = sizeof(rpc_response_t);

    //transport->send_client_((uint64_t)response, response_size, qp_id);
    transport->send_client((uint64_t)response, response_size, qp_id);
    //transport->send_client_async((uint64_t)rpc_response, response_size, qp_id);
    //TODO: only send the data for specific client_id
    //TODO: if tid == entry->tid, send in a batch
    /*
    for(int i=0; i<response->num; i++){
	auto res = response->get_buffer(i);
	debug::notify_info("tid %d --- notify  \tclient %d to qp %d", tid, res->tid, qp_id);
    }
    */
}

#else // infdef LOCKTABLE

row_t* txn_man_t::get_row(RC& rc, uint64_t row_addr, int qp_id, int client_id, int tid, int pid, access_t type, bool is_remote){
    //int idx = qp_id * BATCH_THREAD_NUM + client_id % BATCH_THREAD_NUM;
    auto access = accesses[client_id];
    int _row_cnt = row_cnt[client_id];
    if(access[_row_cnt] == nullptr){
	auto _access = new Access;
	access[_row_cnt] = _access;
    }

    row_t* row;
    if(is_remote){
	row = mem->row_buffer_pool(tid);
	rc = row->get_row(type, this, row_addr, tid, pid);
    }
    else{
	row = (row_t*)row_addr;
	rc = row->get_row(type, this);
    }

    if(rc == ABORT){
	if(_row_cnt > 0)
	    cleanup(rc, qp_id, client_id, tid);
	return nullptr;
    }

    access[_row_cnt]->type = type;
    access[_row_cnt]->is_remote = is_remote;
    access[_row_cnt]->addr = row_addr;
    access[_row_cnt]->pid = pid;
    access[_row_cnt]->qp_id = qp_id;
    access[_row_cnt]->client_id = client_id;

    row_cnt[client_id]++;
    return row;

}

void txn_man_t::notify(Access* access, int tid){
    // dummy
}
#endif // end of LOCKTABLE

#else // ifn defined BATCH || defined BATCH2ING
RC txn_man_t::finish(RC rc, int client_id, int tid){
    return cleanup(rc, client_id, tid);
}

RC txn_man_t::cleanup(RC rc, int client_id, int tid){
    auto access = accesses[client_id];
    RC _rc = rc;

    #ifdef LOCKTABLE
    if(_rc == ABORT)
	prepare_abort(access[0]);
    else
	_rc = prepare_commit(access[0]);
    #endif

    for(int rid=row_cnt[client_id]-1; rid>=0; rid--){
	auto type = access[rid]->type;
	if(type == WRITE && _rc == ABORT){
	    type = XP;
	}

	#ifdef LOCKTABLE
	auto entry = access[rid]->entry;
	if(_rc == ABORT){ // abort reads
	    // this lock has been already dropped by preemption 
	    if(access[rid]->lock_status == lock_status_t::LOCK_DROPPED){
		//debug::notify_info("tid %d --- %d unlock skipped \t(client_id %d, rid %d)", tid, access[rid]->rid, client_id, rid);
		continue;
	    }
	}
	entry->release(type, this, client_id, tid, access[rid]->timestamp);
	//debug::notify_info("tid %d --- %d unlocked \t(client_id %d, rid %d)", tid, access[rid]->rid, client_id, rid);
	#else
	auto addr = access[rid]->addr;
	auto pid = access[rid]->pid;
	auto is_remote = access[rid]->is_remote;
	row_t* row;
	if(is_remote){
	    row = mem->row_buffer_pool(tid);
	    row->return_row(type, this, get_unmasked_addr(addr), tid, pid);
	}
	else{
	    row = (row_t*)addr;
	    row->return_row(type);
	}
	#endif
    }

    #ifdef LOCKTABLE
    flush(access[0]);
    #endif
    row_cnt[client_id] = 0;

    return _rc;
}

RC txn_man_t::finish_with_write(char* _new_row, int num, int client_id, int tid){
    auto access = accesses[client_id];
    int idx = num-1;
    #ifdef LOCKTABLE
    RC rc = prepare_commit(access[0]);
    #else
    RC rc = RCOK;
    #endif

    for(int rid=row_cnt[client_id]-1; rid>=0; rid--){
	auto _access = access[rid];
	auto type = _access->type;

	row_t* row;
	#ifdef LOCKTABLE
	auto entry = _access->entry;
	if(rc == ABORT){ // abort reads and writes
	    if(_access->lock_status == lock_status_t::LOCK_DROPPED){
		//debug::notify_info("tid %d --- %d unlock skipped2 \t(client_id %d, rid %d)", tid, _access->rid, client_id, rid);
		continue;
	    }
	}
	else{
	    if(type == WRITE){ // commit writes
		auto new_row = (row_t*)&_new_row[sizeof(row_t) * idx];
		if(entry->is_remote()){
		    row = mem->row_buffer_pool(tid);
		    memcpy(row, new_row, sizeof(row_t));
		    transport->write((uint64_t)row, entry->remote_addr, sizeof(row_t), tid, entry->pid);
		}
		else{
		    row = (row_t*)entry->local_addr;
		    row->copy(new_row);
		}
		idx--;
	    }
	    // else commit reads
	}

	entry->release(type, this, client_id, tid, _access->timestamp);
	//debug::notify_info("tid %d --- %d unlocked \t(client_id %d, rid %d)", tid, _access->rid, client_id, rid);
	#else
	auto is_remote = _access->is_remote;
	assert(rc == RCOK); // txns cannot be wounded without the support of locktable
	if(type == WRITE){ // commit writes
	    auto new_row = (row_t*)&_new_row[sizeof(row_t) * idx];
	    if(is_remote){
		row = mem->row_buffer_pool(tid);
		memcpy(row, new_row, sizeof(row_t));
		transport->write((uint64_t)row, get_unmasked_addr(_access->addr), sizeof(row_t), tid, _access->pid);
	    }
	    else{
		row = (row_t*)_access->addr;
		row->copy(new_row);
	    }
	    idx--;
	}

	if(is_remote)
	    row->return_row(type, this, get_unmasked_addr(_access->addr), tid, _access->pid);
	else
	    row->return_row(type);
	#endif // end of LOCKTABLE
    }

    #ifdef LOCKTABLE
    flush(access[0]);
    #endif
    row_cnt[client_id] = 0;
    return rc;
}

void txn_man_t::inc_row_cnt(Access* access){
    row_cnt[access->client_id]++;
}

void txn_man_t::prepare_abort(Access* access){
    auto txn_status = access->txn_status;
    auto txn_status_value = txn_status->load();
    if(txn_status_value == txn_status_t::ABORTING)
	return;
    assert(txn_status_value == txn_status_t::RUNNING);
    if(!txn_status->compare_exchange_strong(txn_status_value, txn_status_t::ABORTING)){
	assert(txn_status->load() == txn_status_t::ABORTING);
    }
}

RC txn_man_t::prepare_commit(Access* access){
    auto txn_status = access->txn_status;
    auto txn_status_value = txn_status->load();
    if(txn_status_value == txn_status_t::ABORTING){ // this txn has been wounded
	return ABORT;
    }
    assert(txn_status_value == txn_status_t::RUNNING);
    if(!txn_status->compare_exchange_strong(txn_status_value, txn_status_t::COMMITTING)){ // this txn has been wounded
	return ABORT;
    }
    return RCOK;
}

void txn_man_t::flush(Access* access){
    auto txn_status = access->txn_status;
    auto txn_status_value = txn_status->load();
    //assert(txn_status_value == txn_status_t::COMMITTING);
    txn_status->store(txn_status_t::RUNNING);
}

bool txn_man_t::wound(Access* access){
    auto status = access->txn_status->load();
    if(status == txn_status_t::RUNNING){
	// failed to wound txn -- this txn has entered commit phase
	if(!access->txn_status->compare_exchange_strong(status, txn_status_t::ABORTING)){
	    return false;
	}
	// wounded the txn
	access->lock_status = lock_status_t::LOCK_DROPPED;
	return true;
    }
    else if(status == txn_status_t::ABORTING){ // this txn is aborting
	access->lock_status = lock_status_t::LOCK_DROPPED;
	return true;
    }
    assert(status == txn_status_t::COMMITTING);
    return false;
}

void txn_man_t::notify(Access* access, int tid){
    int response_size = sizeof(rpc_response_t);
    auto send_ptr = mem->rpc_response_buffer_pool(tid);
    auto response = create_message<rpc_response_t>(send_ptr, tid, RCOK);
    //auto response = create_message<rpc_response_t>(send_ptr, tid, RESUME);
    int client_id = access->client_id;
    //int rid = row_cnt[client_id];
    row_cnt[client_id]++;
    auto entry = access->entry;
    //auto entry = accesses[client_id][rid]->entry;
    row_t* row;
    if(entry->is_remote()){
	row = mem->row_buffer_pool(tid);
	transport->read((uint64_t)row, entry->remote_addr, ROW_SIZE, tid, entry->pid);
    }
    else{
	row = (row_t*)entry->local_addr;
    }
    memcpy(response->data, row, ROW_SIZE);
    transport->send_client((uint64_t)response, response_size, client_id);
    #ifdef BREAKDOWN
    t[client_id].end = asm_rdtsc();
    t[client_id].wait += (t[client_id].end - t[client_id].start);
    t[client_id].start = t[client_id].end;
    #endif
    //debug::notify_info("tid %d --- %d lock resume \t(client_id %d, rid %d)", tid, access->rid, client_id, row_cnt[client_id]-1);
}

#ifdef LOCKTABLE
row_t* txn_man_t::get_row(RC& rc, uint32_t row_id, int client_id, int tid, int pid, access_t type, page_table_t* tab, table_entry_t*& entry, uint64_t timestamp){
    auto access = accesses[client_id];
    int _row_cnt = row_cnt[client_id];
    if(access[_row_cnt] == nullptr){
	auto _access = new Access;
	access[_row_cnt] = _access;
	std::atomic<txn_status_t>* txn_status = nullptr;
	if(_row_cnt == 0){
	    txn_status = new std::atomic<txn_status_t>;
	    txn_status->store(txn_status_t::RUNNING);
	}
	else{
	    txn_status = access[0]->txn_status;
	}
	access[_row_cnt]->txn_status = txn_status;
    }

    auto txn_status = access[_row_cnt]->txn_status->load();
    if(txn_status == txn_status_t::ABORTING){ // this txn has been wounded
	assert(_row_cnt > 0);
	rc = ABORT;
	cleanup(rc, client_id, tid);
	return nullptr;
    }

    access[_row_cnt]->client_id = client_id;
    access[_row_cnt]->timestamp = timestamp;
    access[_row_cnt]->type = type;
    access[_row_cnt]->lock_status = lock_status_t::LOCK_READY;
    access[_row_cnt]->rid = row_id; // debug

    rc = tab->get(entry, row_id, type, this, access[_row_cnt], tid);
    if(rc == ABORT){
	if(_row_cnt > 0)
	    cleanup(rc, client_id, tid);
	return nullptr;
    }
    else if(rc == WAIT){
	//debug::notify_info("tid %d --- %d lock wait \t(client_id %d, rid %d)", tid, row_id, client_id, _row_cnt);
//	row_cnt[client_id]++;
	return nullptr;
	//goto final;
    }
    //debug::notify_info("tid %d --- %d locked \t(client_id %d, rid %d)", tid, row_id, client_id, _row_cnt);

    // RCOK
    row_t* row = nullptr;
    if(entry->is_remote()){
	row = mem->row_buffer_pool(tid);
    }
    else{
	row = (row_t*)entry->local_addr;
    }
    
    //access[_row_cnt]->entry = entry;
    row_cnt[client_id]++;
    return row;
}

#else
row_t* txn_man_t::get_row(RC& rc, uint64_t row_addr, int client_id, int tid, int pid, access_t type, bool is_remote){
    auto access = accesses[client_id];
    int _row_cnt = row_cnt[client_id];
    if(access[_row_cnt] == nullptr){
	auto _access = new Access;
	access[_row_cnt] = _access;
    }

    row_t* row;
    if(is_remote){
	row = mem->row_buffer_pool(tid);
	rc = row->get_row(type, this, row_addr, tid, pid);
    }
    else{
	row = (row_t*)row_addr;
	rc = row->get_row(type, this);
    }

    if(rc == ABORT){
	if(_row_cnt > 0)
	    cleanup(rc, client_id, tid);
	return nullptr;
    }

    access[_row_cnt]->type = type;
    access[_row_cnt]->is_remote = is_remote;
    access[_row_cnt]->addr = row_addr;
    access[_row_cnt]->pid = pid;
    access[_row_cnt]->client_id = client_id;

    row_cnt[client_id]++;
    return row;

}

#endif // end if LOCKTABLE
#endif // end of BATCHING


