#pragma once 
#include "common/global.h"
#include "common/rpc.h"
#include "client/query.h"

class ycsb_request_t{
    public:
	access_t type;
	Key key;
	uint32_t scan_len;
};

class ycsb_query_t: public base_query_t{
    public:
	void init(int tid);
	void generate_request();
	access_t generate_access(int r);
	Key to_key(Key key);

	uint64_t request_cnt;
	ycsb_request_t* requests;

    private:
};
