#include "client/txn.h"
#include "storage/row.h"
#include "client/mr.h"
#include "client/transport.h"
#include "client/worker.h"
#include "client/thread.h"
#include "common/stat.h"

void txn_man_t::init(thread_t* thread, worker_t* worker, int tid){
    this->thread = thread;
    this->worker = worker;
    this->mem = worker->mem;
    this->transport = worker->transport;

    write_num = 0;
    memset(write_buf, 0, sizeof(int)*MAX_ROW_PER_TXN);
    //memset(write_buf, 0, sizeof(int)*REQUEST_PER_QUERY);
}

int txn_man_t::get_tid(){
    return thread->get_tid();
}
