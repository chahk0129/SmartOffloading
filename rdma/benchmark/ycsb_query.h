#ifndef __YCSB_QUERY_H__
#define __YCSB_QUERY_H__

#include "common/global.h"
#include "system/query.h"

class ycsb_request_t{
    public:
	access_t type;
	uint64_t key;
	char value;
	uint32_t scan_len;
};

class ycsb_query_t: public base_query_t{
    public:
	void init(int tid);
	uint64_t to_key(uint64_t key);
	void generate_request();
	access_t generate_access(int r);


	uint64_t request_cnt;
	ycsb_request_t* requests;

    private:
};
#endif
