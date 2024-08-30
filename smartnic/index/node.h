#pragma once
#include <atomic>
#include "common/global.h"
#include "common/debug.h"
#include "common/key.h"

#define LATCH_BIT (0b10)
#define INITIAL_BIT (0b100)

#define CAS(_p, _u, _v)  (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))

template <typename Key_t>
class node_t{
    public:
	std::atomic<uint64_t> version;
	uint32_t leftmost_ptr;
	uint32_t sibling_ptr;
	uint32_t level;
	int last_index;
	Key_t lowest;
	Key_t highest;

	node_t(): version(0), leftmost_ptr(0), sibling_ptr(0), level(0), last_index(-1), lowest(MIN_KEY<Key_t>), highest(MAX_KEY<Key_t>) { }
	node_t(uint32_t level): version(0), leftmost_ptr(0), sibling_ptr(0), level(level), last_index(-1), lowest(MIN_KEY<Key_t>), highest(MAX_KEY<Key_t>) { }
	node_t(uint32_t left, uint32_t level, int last_index=0): version(0), leftmost_ptr(left), sibling_ptr(0), level(level), last_index(last_index), lowest(MIN_KEY<Key_t>), highest(MAX_KEY<Key_t>) { }

	void print(){
	    debug::notify_info("    level (%u), version (%lu), sibling_ptr (%u), leftmost_ptr (%u), last_index (%d), lowest (%lu), highest (%lu)", level, version.load(), sibling_ptr, leftmost_ptr, last_index, lowest, highest);
	}

	bool is_locked(uint64_t v){
	    return ((v & LATCH_BIT) == LATCH_BIT);
	}

	uint64_t get_version(bool& need_restart){
	    auto v = version.load();
	    if(is_locked(v))
		need_restart = true;
	    return v;
	}

	void set_version(uint64_t v){
	    version.store(v);
	}

	bool upgrade_lock(uint64_t v){
	    auto _v = version.load();
	    if(is_locked(_v) || (v != _v))
		return false;

	    if(!version.compare_exchange_strong(_v, _v+LATCH_BIT))
		return false;

	    return true;
	}

	bool write_lock(){
	    auto v = version.load();
	    if(is_locked(v))
		return false;

	    if(!version.compare_exchange_strong(v, v+LATCH_BIT))
		return false;

	    return true;
	}

	void write_unlock(){
	    version.fetch_add(LATCH_BIT);
	}
};

template <typename Key_t, typename Value_t>
class entry_t{
    public:
        Key_t key;
        Value_t value;

        entry_t(): key(0), value() { }
} __attribute__((packed));


template <typename Key_t>
static constexpr size_t inode_cardinality = (PAGE_SIZE - sizeof(node_t<Key_t>)) / sizeof(entry_t<Key_t, uint32_t>);

template <typename Key_t, typename Value_t>
static constexpr size_t lnode_cardinality = (PAGE_SIZE - sizeof(node_t<Key_t>)) / sizeof(entry_t<Key_t, Value_t>);


template <typename Key_t, typename Value_t>
class inode_t: public node_t<Key_t>{
    public:
	inode_t(uint32_t left, const Key_t& key, uint32_t right, uint32_t level=0): node_t<Key_t>(left, level){
	    entry[0].key = key;
	    entry[0].value = right;
	}

	inode_t(uint32_t level=0): node_t<Key_t>(level){
	    entry[0].value = 0;
	}

	int lowerbound(Key_t key, bool& is_update){
	    auto cnt = this->last_index+1;
	    for(int i=0; i<cnt; i++){
		if(key < entry[i].key)
		    return i;
		else if(key == entry[i].key){
		    is_update = true;
		    return i;
		}
	    }
	    return cnt;
	}

	void print(){
	    (reinterpret_cast<node_t<Key_t>*>(this))->print();
	    for(int i=0; i<this->last_index+1; i++){
		debug::notify_info("    entry[%d] key (%lu) value (%d)", i, entry[i].key, entry[i].value);
	    }
	}

	bool insert(const Key_t& key, uint32_t& value){
	    bool is_update = false;
	    auto cnt = this->last_index + 1;
	    auto idx = lowerbound(key, is_update);
	    if(is_update){
		entry[idx].value = value;
		return false;
	    }

	    memmove(&entry[idx+1], &entry[idx], sizeof(entry_t<Key_t, uint32_t>) * (cnt - idx));
	    entry[idx].key = key;
	    entry[idx].value = value;
	    this->last_index++;
	    return this->last_index+1 == inode_cardinality<Key_t>;
	}

	inode_t<Key_t, Value_t>* split(Key_t& split_key){
	    auto sibling = new inode_t<Key_t, Value_t>(this->level);
	    auto cnt = this->last_index + 1;
	    int m = cnt / 2;
	    int num = cnt - m;
	    memcpy(sibling->entry, entry+m+1, sizeof(entry_t<Key_t, uint32_t>) * num);

	    this->last_index -= num;
	    sibling->last_index += (num - 1);
	    sibling->leftmost_ptr = entry[m].value;
	    sibling->sibling_ptr = this->sibling_ptr;
	    split_key = entry[m].key;
	    sibling->lowest = entry[m].key;
	    sibling->highest = this->highest;
	    this->highest = entry[m].key;

	    return sibling;
	}

	void split(Key_t& split_key, inode_t<Key_t, Value_t>* sibling){
	    auto cnt = this->last_index + 1;
	    int m = cnt / 2;
	    int num = cnt - m;
	    memcpy(sibling->entry, entry+m+1, sizeof(entry_t<Key_t, uint32_t>) * num);

	    this->last_index -= num;
	    sibling->last_index += (num - 1);
	    sibling->leftmost_ptr = entry[m].value;
	    split_key = entry[m].key;
	    sibling->sibling_ptr = this->sibling_ptr;
	    sibling->lowest = entry[m].key;
	    sibling->highest = this->highest;
	    this->highest = entry[m].key;
	}

	void search(const Key_t& key, result_t<Value_t>& result){
            auto cnt = this->last_index + 1;
            if(key < entry[0].key){ // smaller than the lowest key
                result.child = this->leftmost_ptr;
                return;
            }

            for(int i=1; i<cnt; i++){
                if(key < entry[i].key){
                    result.child = entry[i-1].value;
                    return;
                }
            }
            result.child = entry[cnt-1].value;
        }

    private:
	entry_t<Key_t, uint32_t> entry[inode_cardinality<Key_t>];
} __attribute__((packed));


template <typename Key_t, typename Value_t>
class lnode_t: public node_t<Key_t>{
    public:
        lnode_t(): node_t<Key_t>(0){ }

	void print(){
	    (reinterpret_cast<node_t<Key_t>*>(this))->print();
	    for(int i=0; i<this->last_index+1; i++){
		debug::notify_info("    entry[%d] key (%lu) value (%d)", i, entry[i].key, entry[i].value);
	    }
	}

        void search(const Key_t& key, result_t<Value_t>& result){
	    auto cnt = this->last_index + 1;
            for(int i=0; i<cnt; i++){
                if(key == entry[i].key){
                    result.value = entry[i].value;
                    return;
                }
            }
        }

	void scan(const Key_t& key, Value_t*& values, int num, int& cnt){
            auto c = this->last_index + 1;
            for(int i=0; i<c; i++){
                if(key <= entry[i].key){
                    values[cnt] = entry[i].value;
                    cnt++;
                    if(cnt == num)
                        return;
                }
            }
        }

	void search_next(const Key_t& key, result_t<Value_t>& result){
	    if((key >= this->highest) && (this->sibling_ptr != 0)){ // move right
		result.sibling = this->sibling_ptr;
		return;
	    }

	    auto cnt = this->last_index + 1;
	    for(int i=0; i<cnt; i++){
		if(key < entry[i].key){
		    result.value = entry[i].value;
		    return;
		}
	    }
	    
	    if(this->sibling_ptr != 0){ // move right
		result.sibling = this->sibling_ptr;
	    }
	}

        int lowerbound(Key_t key, bool& is_update){
            auto cnt = this->last_index + 1;
            for(int i=0; i<cnt; i++){
                if(key < entry[i].key)
                    return i;
                else if(key == entry[i].key){
                    is_update = true;
                    return i;
                }
            }
            return cnt;
        }

        bool insert(const Key_t& key, const Value_t& value){
            bool is_update = false;
	    auto cnt = this->last_index + 1;
            auto idx = lowerbound(key, is_update);
            if(is_update){
                entry[idx].value = value;
                return false;
            }

            memmove(&entry[idx+1], &entry[idx], sizeof(entry_t<Key_t, Value_t>) * (cnt - idx));
            entry[idx].key = key;
            entry[idx].value = value;
	    this->last_index++;
	    return this->last_index+1 == lnode_cardinality<Key_t, Value_t>;
        }

        lnode_t<Key_t, Value_t>* split(Key_t& split_key){
            auto sibling = new lnode_t<Key_t, Value_t>();
            auto cnt = this->last_index + 1;
            int m = cnt / 2;
	    int num = cnt - m;
            memcpy(sibling->entry, entry+m, sizeof(entry_t<Key_t, Value_t>) * num);

            this->last_index -= num;
            sibling->last_index += num;
	    sibling->sibling_ptr = this->sibling_ptr;
            sibling->highest = this->highest;
            sibling->lowest = entry[m].key;
            this->highest = entry[m].key;
            split_key = entry[m].key;

            return sibling;
        }

	void split(Key_t& split_key, lnode_t<Key_t, Value_t>* sibling){
            auto cnt = this->last_index + 1;
            int m = cnt / 2;
	    int num = cnt - m;
            memcpy(sibling->entry, entry+m, sizeof(entry_t<Key_t, Value_t>) * num);

            this->last_index -= num;
            sibling->last_index += num;
	    sibling->sibling_ptr = this->sibling_ptr;
            sibling->highest = this->highest;
            sibling->lowest = entry[m].key;
            this->highest = entry[m].key;
            split_key = entry[m].key;
        }

    private:
        entry_t<Key_t, Value_t> entry[lnode_cardinality<Key_t, Value_t>];
} __attribute__((packed));


