#pragma once
class global_addr_t{
    public:
	uint64_t node_id;
	uint64_t addr;

	global_addr_t(): node_id(0), addr(0){ }
	global_addr_t(const global_addr_t& other): node_id(other.node_id), addr(other.addr){ }

	static global_addr_t null(){
	    return global_addr_t();
	}

	inline bool operator==(const global_addr_t& other){
	    return (node_id == other.node_id) && (addr == other.addr);
	}

	inline bool operator!=(const global_addr_t& other){
	    return !(*this == other);
	}

	inline global_addr_t& operator=(const global_addr_t& other){
	    node_id = other.node_id;
	    addr = other.addr;
	    return *this;
	}


} __attribute__((packed));
/*
inline bool operator==(const global_addr_t& lhs, const global_addr_t& rhs){
    return (lhs.node_id == rhs.node_id) && (lhs.addr == rhs.addr);
}

inline bool operator!=(const global_addr_t& lhs, const global_addr_t& rhs){
    return !(lhs == rhs);
}
*/

