#pragma once
#include "common/global.h"
#include "index/node.h"

struct cache_entry_t{
    Key_t from;
    Key_t to; // [from, to)
    mutable inode_t* ptr;
}__attribute__((packed));

static_assert(sizeof(cache_entry_t) == 2 * sizeof(Key_t) + sizeof(uint64_t), "XXX");

inline std::ostream& operator<<(std::ostream& os, const cache_entry_t& obj){
    os << "[" << (int)obj.from << ", " << obj.to + 1 << ")";
    return os;
}

inline static cache_entry_t decode(const char* val){
    return *(cache_entry_t*)val;
}

struct cache_entry_comparator{
    typedef cache_entry_t DecodedType;

    static DecodedType decode_key(const char* b){
	return decode(b);
    }

    int cmp(const DecodedType a, const DecodedType b) const{
	if(a.to < b.to)
	    return -1;

	if(a.to > b.to)
	    return 1;

	if(a.from < b.from)
	    return 1;
	else if(a.from > b.from)
	    return -1;
	else
	    return 0;
    }

    int operator()(const char* a, const char* b) const{
	return cmp(decode(a), decode(b));
    }

    int operator()(const char* a, const DecodedType b) const{
	return cmp(decode(a), b);
    }
};

