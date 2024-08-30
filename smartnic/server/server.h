#ifndef __SERVER_H__
#define __SERVER_H__

#include "net/net.h"
#include "common/global.h"

#include <atomic>
#include <vector>
#include <thread>

class request_t;
class allocator_t;
class server_transport_t;
class server_mr_t;

class server_t{
    public:
	server_t();
	~server_t(){ }

    private:
	void handle_message(int tid);
        void handle_request(request_t* request);

	bool init_resources();

	// network
	server_transport_t* transport;

	// memory 
	server_mr_t* mem[MR_PARTITION_NUM];

	// index count (up to 9 for tpcc indexes)
	int idx_cnt = 0;

	// allocator
        allocator_t* allocator[MR_PARTITION_NUM];

	// thread worker
        std::vector<std::thread> workers;


};

#endif
