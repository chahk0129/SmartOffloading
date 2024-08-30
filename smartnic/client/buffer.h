#pragma once

#include "common/global.h"

template <typename Key_t>
struct txn_request_frame_t{
    std::atomic<bool> valid;
    int tid;
    access_t type;
    int num; // used for commit
    uint64_t timestamp;
    Key_t key;
    char data[ROW_SIZE * MAX_ROW_PER_TXN];

    txn_request_frame_t(): valid(false){ }

    void update(int _tid, access_t _type, uint64_t _timestamp, Key_t _key){ // regular request
	tid = _tid;
	type = _type;
	timestamp = _timestamp;
	key = _key;
	valid.store(true);
    }

    void update(int _tid, access_t _type, uint64_t _timestamp){ // commit request (read-only)
	tid = _tid;
	type = _type;
	timestamp = _timestamp;
	valid.store(true);
    }

    void update(int _tid, access_t _type, int _num, uint64_t _timestamp, char* _data){ // commit request
	tid = _tid;
	type = _type;
	num = _num;
	timestamp = _timestamp;
	memcpy(data, _data, ROW_SIZE * _num);
	valid.store(true);
    }

    bool validate(){
	return valid.load();
    }

    bool acquire(){
	auto v = valid.load();
	if(v == false)
	    return false;

	if(!valid.compare_exchange_strong(v, false)){
	    return false;
	}

	return true;
    }


    void set(){
	valid.store(true);
    }

    void reset(){
	valid.store(false);
    }

    void wait(){
	while(validate()){
	    asm("nop");
	}
    }

    void debug_print(){
	debug::notify_info("tid %d --- request %d key %lu timestamp %lu", tid, type, key, timestamp);
    }
};

template <typename Key_t>
struct txn_request_buffer_t{
    #ifdef BATCH2
#ifdef PER_THREAD_BUFFER
    txn_request_frame_t<Key_t> frames[PER_BATCH_SIZE];
#else
    txn_request_frame_t<Key_t> frames[CLIENT_THREAD_NUM];
#endif
    #else
    txn_request_frame_t<Key_t> frames[BATCH_THREAD_NUM];
    #endif

    txn_request_buffer_t() { }

    txn_request_frame_t<Key_t>* get_frame(int fid){
	return &frames[fid];
    }

    bool validate(){
	#ifdef BATCH2
#ifdef PER_THREAD_BUFFER
	for(int i=0; i<PER_BATCH_SIZE; i++){
#else
	for(int i=0; i<CLIENT_THREAD_NUM; i++){
#endif
	#else
	for(int i=0; i<BATCH_THREAD_NUM; i++){
	#endif
	    if(frames[i].validate())
		return true;
	}
	return false;
    }

};

struct txn_response_frame_t{
    std::atomic<bool> valid;
    int tid;
    RC type;
    char data[ROW_SIZE];

    txn_response_frame_t(): valid(false){ }

    void update(int _tid, RC _type){
	tid = _tid;
	type = _type;
	valid.store(true);
    }

    void update(int _tid, RC _type, char* _data){
	tid = _tid;
	type = _type;
	if(_type == RCOK)
	    memcpy(data, _data, ROW_SIZE);
	valid.store(true);
    }

    bool validate(){
	return valid.load();
    }

    void wait(){
	while(!validate()){
	    asm("nop");
	}
    }

    void reset(){
	valid.store(false);
    }
};

struct txn_response_buffer_t{
    #ifdef BATCH2
    txn_response_frame_t frames[CLIENT_THREAD_NUM];
    #else
    txn_response_frame_t frames[BATCH_THREAD_NUM];
    #endif

    txn_response_buffer_t() { }
    
    txn_response_frame_t* get_frame(int fid){
	return &frames[fid];
    }

    bool validate(){
        #ifdef BATCH2
	for(int i=0; i<CLIENT_THREAD_NUM; i++){
	#else
	for(int i=0; i<BATCH_THREAD_NUM; i++){
	#endif
	    if(frames[i].validate())
		return true;
	}
	return false;
    }
};


