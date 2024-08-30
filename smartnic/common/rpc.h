#pragma once
#include <cstdint>
#include "common/global.h"
#include "common/debug.h"

#define MAX_SCAN_NUM (50)

enum request_type{
    // rpc requests that workers send to servers
    IDX_ALLOC_NODE = 0,
    IDX_DEALLOC_NODE,
    IDX_UPDATE_ROOT,
    IDX_GET_ROOT_ADDR,
    TABLE_ALLOC_ROW,
    TABLE_DEALLOC_ROW,
    // requests that clients send to workers
    OP_READ,
    OP_WRITE,
    OP_SCAN,
    IDX_INIT,
    IDX_UPSERT,
    IDX_SEARCH,

};

enum response_type{
    SUCCESS = 0,
    FAIL,
};

enum buffer_type_t{
    REQUEST_BUFFER,
    COMMIT_BUFFER,
    RESPONSE_BUFFER,
    NOTIFY_BUFFER
};

enum tpcc_request_type_t{
    TPCC_WAREHOUSE,
    TPCC_DISTRICT,
    TPCC_CUSTOMER_LASTNAME,
    TPCC_CUSTOMER_ID,
    TPCC_ITEM,
    TPCC_STOCK,
    TPCC_HISTORY,
    TPCC_ORDERLINE,
};

enum valid_t{
    INVALID = 0,
    VALID
};
    
#if defined BATCH || defined BATCH2
struct base_request_t{
    buffer_type_t type;
    int qp_id;

    base_request_t(buffer_type_t type, int qp_id): type(type), qp_id(qp_id) { }
};

template <typename Key_t>
struct rpc_request_buffer_t{
    valid_t valid;
    int tid;
    access_t type;
    uint64_t timestamp;
    Key_t key;

    rpc_request_buffer_t(): valid(valid_t::INVALID) { }

    // worker thread function --- update local buffer data
    void update(int _tid, access_t _type, uint64_t _timestamp, Key_t _key){
	tid = _tid;
	type = _type;
	timestamp = _timestamp;
	key = _key;
	valid = valid_t::VALID;
    }

    // worker thread function --- update local buffer data
    void update(int _tid, access_t _type, uint64_t _timestamp){
	tid = _tid;
	type = _type;
	timestamp = _timestamp;
	valid = valid_t::VALID;
    }

    void reset(){
	valid = valid_t::INVALID;
    }

    bool validate(){
	return valid == valid_t::VALID;
    }

    void debug_print(){
	assert(valid == valid_t::VALID);
	debug::notify_info("\ttid %d --- request %d, key %lu, timestamp %lu", tid, type, key, timestamp);
    }
};

template <typename Key_t>
struct rpc_request_t: public base_request_t{
    int num;
    #ifdef BATCH2
#ifdef PER_THREAD_BUFFER
    rpc_request_buffer_t<Key_t> buffer[PER_BATCH_SIZE];
#else
    rpc_request_buffer_t<Key_t> buffer[BATCH_SIZE];
#endif
    #else // BATCH
    rpc_request_buffer_t<Key_t> buffer[BATCH_THREAD_NUM];
    #endif

    rpc_request_t(int qp_id): num(0), base_request_t(buffer_type_t::REQUEST_BUFFER, qp_id){ }
    rpc_request_t(buffer_type_t buffer_type, int qp_id): num(0), base_request_t(buffer_type, qp_id) { }
    
    bool validate(){
	#ifdef BATCH2
#ifdef PER_THREAD_BUFFER
	for(int i=0; i<PER_BATCH_SIZE; i++)
#else
	for(int i=0; i<BATCH_SIZE; i++)
#endif
	#else
	for(int i=0; i<BATCH_THREAD_NUM; i++)
	#endif
	    if(buffer[i].validate())
		return true;
	return false;
    }

    // client worker thread function
    rpc_request_buffer_t<Key_t>* get_buffer(int bid){
	return &buffer[bid];
    }

    void debug_print(){
	debug::notify_info("\t sending %d requests", num);
	for(int i=0; i<num; i++){
	    buffer[i].debug_print();
	}
    }
};

struct rpc_commit_buffer_t{
    valid_t valid;
    int tid;
    int num;
    char data[ROW_SIZE * MAX_ROW_PER_TXN];

    rpc_commit_buffer_t(): valid(valid_t::INVALID) { }

    // worker thread function --- update local buffer data
    void update(int _tid, int _num){
	tid = _tid;
	num = _num;
	valid = valid_t::VALID;
    }

    // worker thread function --- update local buffer data
    void update(int _tid, int _num, char* _data){
	tid = _tid;
	num = _num;
	memcpy(data, _data, ROW_SIZE * _num);
	valid = valid_t::VALID;
    }

    void reset(){
	valid = valid_t::INVALID;
    }

    bool validate(){
	return valid == valid_t::VALID;
    }

    void debug_print(){
	assert(valid == valid_t::VALID);
	debug::notify_info("\ttid %d COMMIT", tid);
    }
};

struct rpc_commit_t: public base_request_t{
    int num;
    #ifdef BATCH2
#ifdef PER_THREAD_BUFFER
    rpc_commit_buffer_t buffer[PER_BATCH_SIZE];
#else
    rpc_commit_buffer_t buffer[BATCH_SIZE];
#endif
    #else // BATCH
    rpc_commit_buffer_t buffer[BATCH_THREAD_NUM];
    #endif

    rpc_commit_t(int qp_id): num(0), base_request_t(buffer_type_t::COMMIT_BUFFER, qp_id) { }
    rpc_commit_t(buffer_type_t buffer_type, int qp_id): num(0), base_request_t(buffer_type, qp_id) { }

    bool validate(){
	#ifdef BATCH2
#ifdef PER_THREAD_BUFFER
        for(int i=0; i<PER_BATCH_SIZE; i++)
#else
        for(int i=0; i<BATCH_SIZE; i++)
#endif
	#else
        for(int i=0; i<BATCH_THREAD_NUM; i++)
	#endif
            if(buffer[i].validate())
                return true;
        return false;
    }

    // client worker thread function
    rpc_commit_buffer_t* get_buffer(int bid){
        return &buffer[bid];
    }

    void debug_print(){
	debug::notify_info("\t sending %d commits", num);
	for(int i=0; i<num; i++){
	    buffer[i].debug_print();
	}
    }
};


struct rpc_response_buffer_t{
    valid_t valid;
    int tid;
    RC type;
    char data[ROW_SIZE];

    rpc_response_buffer_t(): valid(valid_t::INVALID){ }
    rpc_response_buffer_t(int tid): valid(valid_t::INVALID), tid(tid){ }
    rpc_response_buffer_t(int tid, RC type): valid(valid_t::INVALID), tid(tid), type(type){ }

    void set(){
	valid = valid_t::VALID;
    }

    void reset(){
	valid = valid_t::INVALID;
    }

    void update(int _tid, RC rc){
	tid = _tid;
	type = rc;
	valid = valid_t::VALID;
    }

    bool validate(){
	return valid == valid_t::VALID;
    }

    void wait(){
	while(!validate()){
	    asm("nop");
	}
    }
};
//} __attribute__((packed)); 

struct rpc_response_t{
    int qp_id;
    int num;
    #ifdef BATCH2
#ifdef PER_THREAD_BUFFER
    rpc_response_buffer_t buffer[PER_BATCH_SIZE];
#else
    rpc_response_buffer_t buffer[BATCH_SIZE];
#endif
    #else
    rpc_response_buffer_t buffer[BATCH_THREAD_NUM];
    #endif

    rpc_response_t(int qp_id): qp_id(qp_id), num(0) { }
    rpc_response_t(int qp_id, int num): qp_id(qp_id), num(num) { }

    rpc_response_buffer_t* get_buffer(int bid){
	return &buffer[bid];
    }

    // network thread function --- wait until all the responses have been copied
    void validate(){
	#ifdef BATCH2
#ifdef PER_THREAD_BUFFEr
	for(int i=0; i<PER_BATCH_SIZE; i++){
#else
	for(int i=0; i<BATCH_SIZE; i++){
#endif
	#else
	for(int i=0; i<BATCH_THREAD_NUM; i++){
	#endif
	    while(buffer[i].validate()){
		asm("nop");
	    }
	}
    }

    // network thread function --- reset buffer num to ensure worker threads are reading correct result
    void reset(){
	#ifdef BATCH2
	for(int i=0; i<BATCH_SIZE; i++)
	#else
	for(int i=0; i<BATCH_THREAD_NUM; i++)
	#endif
	    buffer[i].reset();
    }
};
//} __attribute__((packed)); 

#else // ifndef BATCH
struct base_request_t{
    int qp_id;
    access_t type;
    uint64_t timestamp;

    base_request_t(){ }
    base_request_t(int qp_id): qp_id(qp_id){ }
    base_request_t(int qp_id, access_t type): qp_id(qp_id), type(type) { }
    base_request_t(int qp_id, access_t type, uint64_t timestamp): qp_id(qp_id), type(type), timestamp(timestamp){ }
};

template <typename Key_t, typename Value_t>
struct idx_request_t: base_request_t{
    Key_t key;
    Value_t value;

    idx_request_t(int qp_id, access_t type): base_request_t(qp_id, type) { }
    idx_request_t(int qp_id, access_t type, Key_t key): base_request_t(qp_id, type), key(key) { }
    idx_request_t(int qp_id, access_t type, Key_t key, Value_t value): base_request_t(qp_id, type), key(key), value(value) { }
};

struct base_response_t{
    int qp_id;
    RC type;

    base_response_t(){ }
    base_response_t(int qp_id): qp_id(qp_id){ }
    base_response_t(int qp_id, RC type): qp_id(qp_id), type(type){ }
};

template <typename Value_t>
struct idx_response_t: base_response_t{
    Value_t value;

    idx_response_t(int qp_id): base_response_t(qp_id) { }
    idx_response_t(int qp_id, RC type): base_response_t(qp_id, type) { }
    idx_response_t(int qp_id, RC type, Value_t value): base_response_t(qp_id, type), value(value) { }
};

template <typename Key_t>
struct rpc_request_t: base_request_t{
    union{
	int num;
	struct{
	    tpcc_request_type_t tpcc_type;
	    int pid;
	};
    };
    Key_t key;
    char data[ROW_SIZE * MAX_ROW_PER_TXN];

    rpc_request_t(int qp_id, access_t type): base_request_t(qp_id, type){ }
    rpc_request_t(int qp_id, access_t type, Key_t key): base_request_t(qp_id, type), key(key){ }
    rpc_request_t(int qp_id, access_t type, Key_t key, uint64_t timestamp): base_request_t(qp_id, type, timestamp), key(key){ }
    rpc_request_t(int qp_id, access_t type, tpcc_request_type_t tpcc_type, int pid, Key_t key, uint64_t timestamp): base_request_t(qp_id, type, timestamp), tpcc_type(tpcc_type), pid(pid), key(key){ }
    rpc_request_t(int qp_id, access_t type, int num): base_request_t(qp_id, type), num(num){ }
    rpc_request_t(int qp_id, access_t type, Key_t key, int num): base_request_t(qp_id, type), num(num), key(key){ }
    rpc_request_t(int qp_id, access_t type, Key_t key, int num, uint64_t timestamp): base_request_t(qp_id, type, timestamp), num(num), key(key){ }
};

// rpc response worker sends to client
struct rpc_response_t: base_response_t{
    char data[ROW_SIZE];

    rpc_response_t(): base_response_t() { }
    rpc_response_t(int qp_id): base_response_t(qp_id) { }
    rpc_response_t(int qp_id, RC type): base_response_t(qp_id, type) { }
    rpc_response_t(int qp_id, RC type, char* value): base_response_t(qp_id, type) {
	memcpy(this->data, data, ROW_SIZE);
    }
};
#endif

// rpc request worker sends to server
struct request_t{
    int qp_id;
    int pid;
    request_type type;
    uint64_t addr;
    union{
	uint64_t old_root;
	uint64_t row_id;
    };

    request_t() { }
    request_t(int qp_id, int pid, request_type type): qp_id(qp_id), pid(pid), type(type), addr(0) { }
    request_t(int qp_id, int pid, request_type type, uint64_t addr): qp_id(qp_id), pid(pid), type(type), addr(addr) { }
    request_t(int qp_id, int pid, request_type type, uint64_t old_root, uint64_t addr): qp_id(qp_id), pid(pid), type(type), addr(addr), old_root(old_root) { }
};

// rpc request server sends to worker
struct response_t{
    int qp_id;
    response_type type;
    uint64_t addr;

    response_t() { }
    response_t(int qp_id, response_type type): qp_id(qp_id), type(type) { }
    response_t(int qp_id, response_type type, uint64_t addr): qp_id(qp_id), type(type), addr(addr) { }
};

template <typename T, class... Args>
static T* create_message(void* buf, Args&&... args){
    return new (buf) T(args...);
}

