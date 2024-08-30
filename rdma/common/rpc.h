#pragma once
#include <cstdint>
//#include "common/global.h"

enum request_type{
    IDX_ALLOC_NODE = 0,
    IDX_DEALLOC_NODE,
    IDX_UPDATE_ROOT,
    IDX_GET_ROOT_ADDR,
    TABLE_ALLOC_ROW,
    TABLE_DEALLOC_ROW,
};

enum response_type{
    SUCCESS = 0,
    FAIL,
};

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

