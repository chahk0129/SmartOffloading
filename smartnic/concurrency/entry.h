#pragma once
#include "common/global.h"

class txn_man_t;
class Acess;

struct lock_entry_t{
    lock_type_t type;
    int client_id;
    Access* access;
    lock_entry_t* next;
    lock_entry_t* prev;
};


