#pragma once
#include "index/cache_entry.h"
#include "common/huge_page.h"
#include "index/wrlock.h"
#include "index/third_party/inlineskiplist.h"

#include <queue>
#include <vector>

using cache_skiplist_t = InlineSkipList<cache_entry_comparator>;

class cache_t{
    private:
	uint64_t cache_size;
	std::atomic<int64_t> free_page_cnt;
	std::atomic<int64_t> skiplist_node_cnt;
	int64_t all_page_cnt;

	std::queue<std::pair<void*, uint64_t>> delay_free_list;
	wrlock_t freelock;

	cache_skiplist_t* skiplist;
	cache_entry_comparator cmp;
	Allocator alloc;


	void evict_one(){
	    uint64_t freq1, freq2;
	    auto e1 = get_a_random_entry(freq1);
	    auto e2 = get_a_random_entry(freq2);

	    if(freq1 < freq2)
		invalidate(e1);
	    else
		invalidate(e2);
	}

	const cache_entry_t* get_a_random_entry(uint64_t& freq){
	    uint32_t seed = asm_rdtsc();
	    uint64_t temp_addr;
	restart:
	    auto k = rand_r(&seed) % (1000ull * 1024 * 1024);
	    auto e = this->search_from_cache(k, &temp_addr);
	    if(!e)
		goto restart;
	    auto ptr = e->ptr;
	    if(!ptr)
		goto restart;

	    freq = ptr->cache_freq;
	    if(e->ptr != ptr)
		goto restart;
	    return e;
	}

    public:
	cache_t(int cache_size): cache_size(cache_size){
	    skiplist = new cache_skiplist_t(cmp, &alloc, 21);
	    uint64_t memory_size = cache_size * 1024 * 1024 * 1024;

	    all_page_cnt = memory_size / PAGE_SIZE;
	    free_page_cnt.store(all_page_cnt);
	    skiplist_node_cnt.store(0);
	}

	bool add_entry(const Key_t& from, const Key_t& to, inode_t* ptr){
	    auto buf = skiplist->AllocateKey(sizeof(cache_entry_t));
	    auto& e = *(cache_entry_t*)buf;
	    e.from = from;
	    e.to = to-1;
	    e.ptr = ptr;

	    return skiplist->InsertConcurrently(buf);
	}

	const cache_entry_t* find_entry(const Key_t& from, const Key_t& to){
	    cache_skiplist_t::Iterator iter(skiplist);
	    cache_entry_t e;
	    e.from = from;
	    e.to = to-1;
	    iter.Seek((char*)&e);
	    if(iter.Valid()){
		auto val = (const cache_entry_t*)iter.key();
		return val;
	    }
	    return nullptr;
	}

	const cache_entry_t* find_entry(const Key_t& key){
	    return find_entry(key, key+1);
	}

	bool add_to_cache(inode_t* page){
	    auto new_page = (inode_t*)malloc(PAGE_SIZE);
	    memcpy(new_page, page, PAGE_SIZE);
	    new_page->cache_freq = 0;

	    if(this->add_entry(page->header.lowest, page->header.highest, new_page)){
		skiplist_node_cnt.fetch_add(1);
		auto v = free_page_cnt.fetch_add(-1);
		if(v <= 0)
		    evict_one();

		return true;
	    }

	    // conflict
	    auto e = this->find_entry(page->header.lowest, page->header.highest);
	    if(e && e->from == page->header.lowest && e->to == page->header.highest-1){
		auto ptr = e->ptr;
		if(ptr == nullptr && __sync_bool_compare_and_swap(&(e->ptr), 0ull, new_page)){
		    auto v = free_page_cnt.fetch_add(-1);
		    if(v <= 0)
			evict_one();
		    return true;
		}
	    }
	    free(new_page);
	    return false;
	}

	const cache_entry_t* search_from_cache(const Key_t& key, uint64_t* addr, bool is_leader=false){
	    if(is_leader){ // T0 makes the progress
		if(!delay_free_list.empty()){ // free pages
		    auto p = delay_free_list.front();
		    if(asm_rdtsc() - p.second > 3000ull * 10){
			free(p.first);
			free_page_cnt.fetch_add(1);
			
			freelock.wlock();
			delay_free_list.pop();
			freelock.wunlock();
		    }
		}
	    }

	    auto entry = find_entry(key);
	    inode_t* page = entry ? entry->ptr : nullptr;
	    if(page && entry->from <= key && entry->to >= key){
		page->cache_freq++;
		auto cnt = page->last_index() + 1;
		if(key < page->entry[0].key)
		    *addr = page->leftmost_ptr();
		else{
		    bool find = false;
		    for(int i=1; i<cnt; i++){
			if(key < page->entry[i].key){
			    find = true;
			    *addr = page->entry[i-1].ptr;
			    break;
			}
		    }
		    if(!find)
			*addr = page->entry[cnt-1].ptr;
		}

		compiler_barrier();
		if(entry->ptr) // check if it is freed
		    return entry;
	    }
	    return nullptr;
	}

	bool invalidate(const cache_entry_t* entry){
	    auto ptr = entry->ptr;
	    if(!ptr)
		return false;

	    if(__sync_bool_compare_and_swap(&(entry->ptr), ptr, 0)){
		freelock.wlock();
		delay_free_list.push(std::make_pair(ptr, asm_rdtsc()));
		freelock.wunlock();
		return true;
	    }
	    return false;
	}

};

