#include "common/rpc.h"
#include "common/stat.h"
#include "index/node.h"
#include "index/tree.h"
#include "index/indirection.h"
#include "worker/transport.h"
#include "worker/mr.h"

static thread_local uint32_t path_stack[MAX_TREE_LEVEL];
static thread_local uint64_t t_traversal;
static thread_local uint64_t t_traversal_retry;
static thread_local uint64_t t_latch;
static thread_local uint64_t t_smo;
static thread_local uint64_t t_leaf;
static thread_local uint64_t t_buffer;
static thread_local bool retry;


static thread_local uint64_t start, end;

template <typename Key_t, typename Value_t>
tree_t<Key_t, Value_t>::tree_t(worker_mr_t* mem, worker_transport_t* transport, int pid): mem(mem), transport(transport), pid(pid){
    set_key<Key_t>();
    //free_pages.store(0);
    free_pages.store((uint64_t)(INDEX_PAGE_SIZE));
    //free_pages.store(INDEX_PAGE_SIZE);
    tab = new indirection_table_t();
    //tab = new indirection_table_t(free_pages.load());
    _root_id.store(0);

#ifdef CACHE
    cache = new tstarling::ThreadSafeScalableCache<uint32_t, uint64_t>(INDEX_CACHE_SIZE);
#endif
    initialize_root();
}

template <typename Key_t, typename Value_t>
uint64_t tree_t<Key_t, Value_t>::rpc_alloc(int tid){
    auto send_ptr = mem->request_buffer_pool(tid);
    auto request = create_message<request_t>((void*)send_ptr, tid, pid, request_type::IDX_ALLOC_NODE);
    transport->send((uint64_t)request, sizeof(request_t), tid);

    auto recv_ptr = mem->response_buffer_pool(tid);
    auto response = create_message<response_t>((void*)recv_ptr);
    transport->recv((uint64_t)response, sizeof(response_t), tid);
    if(response->type != response_type::SUCCESS)
	debug::notify_error("[INDEX] Memory allocation failed: return type %d", response->type);

    return response->addr;
}

template <typename Key_t, typename Value_t>
void tree_t<Key_t, Value_t>::rpc_dealloc(int tid, uint64_t addr){
    auto send_ptr = mem->request_buffer_pool(tid);
    auto request = create_message<request_t>((void*)send_ptr, tid, pid, request_type::IDX_DEALLOC_NODE, addr);
    transport->send((uint64_t)request, sizeof(request_t), tid);

    auto recv_ptr = mem->response_buffer_pool(tid);
    auto response = create_message<response_t>((void*)recv_ptr);
    transport->recv((uint64_t)response, sizeof(response_t), tid);
    if(response->type != response_type::SUCCESS)
	debug::notify_error("[INDEX] Memory deletion failed: return type %d", response->type);
}

template <typename Key_t, typename Value_t>
void tree_t<Key_t, Value_t>::cache_stats(){
    #ifdef CACHE
    size_t size = cache->size();
    debug::notify_info("Cache is %ld (%lf percent utilized) (%lu KB)", size, (double)size / INDEX_CACHE_SIZE * 100, size*PAGE_SIZE/1000);
    #endif
}

template <typename Key_t, typename Value_t>
void tree_t<Key_t, Value_t>::initialize_root(){
    int tid = 0;
    uint64_t root_addr = 0;
    auto root_id = tab->get_next_id();
    lnode_t<Key_t, Value_t>* root = nullptr;

#ifdef CACHE 
    auto page_buffer = mem->page_buffer_pool(tid);
    uint64_t remote_addr = rpc_alloc(tid);
    if(free_pages.load()){ // local cache
	free_pages.fetch_sub(1);
	root = new lnode_t<Key_t, Value_t>();
	root_addr = (uint64_t)root;
	bool cached = cache->insert(root_id, root_addr);
	if(!cached){ // cannot be cached, do a remote write
	    memcpy((void*)page_buffer, root, sizeof(lnode_t<Key_t, Value_t>));
	    transport->write(page_buffer, remote_addr, PAGE_SIZE, tid, pid);
	}
    }
    else{
	root = new ((void*)page_buffer) lnode_t<Key_t, Value_t>();
	transport->write(page_buffer, remote_addr, PAGE_SIZE, tid, pid);
    }
    tab->set(root_id, remote_addr);
#else // static allocation
    uint64_t remote_addr = rpc_alloc(tid);
    if(free_pages.load()){ // local alloc
	free_pages.fetch_sub(1);
	root = new lnode_t<Key_t, Value_t>();
	root_addr = (uint64_t)root;
    }
    else{ // remote alloc
	auto page_buffer = mem->page_buffer_pool(tid);
	root = new ((void*)page_buffer) lnode_t<Key_t, Value_t>();
	root_addr = rpc_alloc(tid);
	transport->write(page_buffer, root_addr, PAGE_SIZE, tid, pid);
	root_addr = set_masked_addr(root_addr);
    }
    tab->set(root_id, root_addr);
#endif
    uint32_t old_root_id = 0;
    if(!update_root(old_root_id, root_id)){
	debug::notify_error("[INDEX] Failed to initizlie root --- old root id is not zero!");
	exit(0);
    }
}

template <typename Key_t, typename Value_t>
bool tree_t<Key_t, Value_t>::update_root(uint32_t old_id, uint32_t new_id){
    return _root_id.compare_exchange_strong(old_id, new_id);
}

template <typename Key_t, typename Value_t>
bool tree_t<Key_t, Value_t>::insert_to_cache(uint32_t node_id, uint64_t node_addr, int tid){
#ifdef CACHE
    bool evict = false;

    tstarling::ThreadSafeScalableCache<uint32_t, uint64_t>::Accessor ac;
    bool cached = cache->insert_with_evict(node_id, node_addr, evict, ac);
    if(!cached){
	return false;
    }

    if(evict){ // this page is still accessible by hash table in cache, so make sure to latch it first
	auto evict_node_id = ac.m_hashAccessor->first;
	auto evict_node_addr = *ac.get();
	//auto evict_node_addr = ac.m_hashAccessor->second;
	auto evict_remote_addr = tab->get_addr(evict_node_id);
	auto local_page = (node_t<Key_t>*)evict_node_addr;
	if(!local_page->write_lock()){ 
	    assert(false);
	}
	auto page_buffer = mem->sibling_buffer_pool(tid);
	memcpy((void*)page_buffer, local_page, sizeof(inode_t<Key_t, Value_t>));
	((node_t<Key_t>*)page_buffer)->write_unlock(); // latch is only released in the copy buffer, not the actual page structure
	transport->write(page_buffer, evict_remote_addr, PAGE_SIZE, tid, pid);
	cache->remove(ac);
	delete local_page;
    }
#endif
    return true;
}

template <typename Key_t, typename Value_t>
bool tree_t<Key_t, Value_t>::update_new_root(uint32_t left, const Key_t& key, uint32_t right, uint8_t level, uint32_t old_root, int tid){
    auto new_root_id = tab->get_next_id();
    inode_t<Key_t, Value_t>* new_root = nullptr;
#ifdef CACHE 
    bool cached = false;
    auto page_buffer = mem->page_buffer_pool(tid);
    uint64_t remote_addr = rpc_alloc(tid);
    bool local = free_pages.load();
    if(local){ // cache it
	free_pages.fetch_sub(1);
	new_root = new inode_t<Key_t, Value_t>(left, key, right, level);
	cached = insert_to_cache(new_root_id, (uint64_t)new_root, tid);
	if(!cached){
	    free_pages.fetch_add(1);
	    memcpy((void*)page_buffer, new_root, sizeof(inode_t<Key_t, Value_t>));
	    delete new_root;
	    new_root = (inode_t<Key_t, Value_t>*)page_buffer;
	    transport->write(page_buffer, remote_addr, PAGE_SIZE, tid, pid);
	}
    }
    else{
	new_root = new ((void*)page_buffer) inode_t<Key_t, Value_t>(left, key, right, level);
	transport->write(page_buffer, remote_addr, PAGE_SIZE, tid, pid);
    }
    tab->set(new_root_id, remote_addr);

    if(!update_root(old_root, new_root_id)){ // failed to update root, cleanup
	tab->clear_addr(new_root_id);
	rpc_dealloc(tid, remote_addr);
	if(cached){
	    free_pages.fetch_add(1);
	    delete new_root;
	}
	return false;
    }
#else
    uint64_t addr = 0;
    if(free_pages.load()){ // local alloc
	free_pages.fetch_sub(1);
	new_root = new inode_t<Key_t, Value_t>(left, key, right, level);
	addr = (uint64_t)new_root;
	tab->set(new_root_id, addr);
	if(!update_root(old_root, new_root_id)){
	    tab->clear_addr(new_root_id);
	    delete new_root;
	    return false;
	}
    }
    else{ // remote alloc
	addr = rpc_alloc(tid);
	auto page_buffer = mem->page_buffer_pool(tid);
	new_root = new ((void*)page_buffer) inode_t<Key_t, Value_t>(left, key, right, level);
	transport->write(page_buffer, addr, PAGE_SIZE, tid, pid);

	tab->set(new_root_id, set_masked_addr(addr));
	if(!update_root(old_root, new_root_id)){
	    tab->clear_addr(new_root_id);
	    rpc_dealloc(tid, addr);
	    return false;
	}
    }
#endif
    return true;
}

template <typename Key_t, typename Value_t>
void tree_t<Key_t, Value_t>::print(){
    int tid = 0;
    auto page_id = get_root_id();
    auto page_buffer = mem->page_buffer_pool(tid);
    auto node = (node_t<Key_t>*)page_buffer;
    uint64_t page_addr = 0;

    do{
	page_addr = tab->get_addr(page_id);
	if(is_masked_addr(page_addr)) // remote
	    transport->read(page_buffer, get_unmasked_addr(page_addr), PAGE_SIZE, tid, pid);
	else // local
	    node = (node_t<Key_t>*)page_addr;

	if(node->level == 0){
	    debug::notify_info("[LEAF %lx]", page_addr);
	    lnode_t<Key_t, Value_t>* page;
	    if(is_masked_addr(page_addr))
		page = (lnode_t<Key_t, Value_t>*)page_buffer;
	    else
		page = (lnode_t<Key_t, Value_t>*)page_addr;
	    page->print();

	    auto sibling_id = page->sibling_ptr;
	    auto sibling_addr = tab->get_addr(sibling_id);
	    auto sibling_buffer = mem->sibling_buffer_pool(tid);
	    while(sibling_id != 0){
		auto sibling = (lnode_t<Key_t, Value_t>*)sibling_buffer;
		if(is_masked_addr(sibling_addr))
		    transport->read(sibling_buffer, get_unmasked_addr(sibling_addr), PAGE_SIZE, tid, pid);
		else
		    sibling = (lnode_t<Key_t, Value_t>*)sibling_addr;

		debug::notify_info("[LEAF %lx]", sibling_addr);
		sibling->print();
		sibling_id = sibling->sibling_ptr;
		sibling_addr = tab->get_addr(sibling_id);
	    }
	    page_id = 0;
	}
	else{
	    debug::notify_info("[INTERNAL %lx]", page_addr);
	    auto page = (inode_t<Key_t, Value_t>*)page_buffer;
	    if(!is_masked_addr(page_addr))
		page = (inode_t<Key_t, Value_t>*)page_addr;
	    page->print();

	    auto sibling_id = page->sibling_ptr;
	    auto sibling_addr = tab->get_addr(sibling_id);
	    auto leftmost_ptr = page->leftmost_ptr;
	    while(sibling_id != 0){
		auto sibling_buffer = mem->sibling_buffer_pool(tid);
		auto sibling = (inode_t<Key_t, Value_t>*)sibling_buffer;
		if(is_masked_addr(sibling_addr))
		    transport->read(sibling_buffer, sibling_addr, PAGE_SIZE, tid, pid);
		else
		    sibling = (inode_t<Key_t, Value_t>*)sibling_addr;

		debug::notify_info("[INTERNAL %lx]", sibling_addr);
		sibling->print();
		sibling_id = sibling->sibling_ptr;
		sibling_addr = tab->get_addr(sibling_id);
	    }
	    page_id = leftmost_ptr;
	}
    }while(page_id != 0);
}

template <typename Key_t, typename Value_t>
void tree_t<Key_t, Value_t>::insert(Key_t key, Value_t value, int tid){
#ifdef BREAKDOWN
    t_traversal = t_traversal_retry = t_latch = t_smo = t_leaf = t_buffer = 0;
    retry = false;
#endif
    memset(path_stack, 0, sizeof(uint32_t) * MAX_TREE_LEVEL);
    auto root_id = get_root_id();
    auto p = root_id;
    result_t<Value_t> result;

RETRY:
    auto prev = p;
    if(!page_search(p, key, result, tid)){
	root_id = get_root_id();
	p = root_id;
	goto RETRY;
    }

    if(!result.is_leaf){ // inode
	if(result.sibling != 0){ // move right
	    p = result.sibling;
	    goto RETRY;
	}

	p = result.child; // move down
	if(result.level != 1){
	    goto RETRY;
	}
    }

    store(p, key, value, root_id, tid);
}

template <typename Key_t, typename Value_t>
bool tree_t<Key_t, Value_t>::search(const Key_t& key, Value_t& value, int tid){
#ifdef BREAKDOWN
    t_traversal = t_traversal_retry = t_latch = t_smo = t_leaf = t_buffer = 0;
    retry = false;
#endif
    auto root_id = get_root_id();
    auto p = root_id;
    result_t<Value_t> result;

RETRY:
    auto prev = p;
    if(!page_search(p, key, result, tid)){
	root_id = get_root_id();
	p = root_id;
	goto RETRY;
    }

    if(!result.is_leaf){ // inode
	if(result.sibling != 0){ // move right
	    p = result.sibling;
	    goto RETRY;
	}

	p = result.child; // move down
	goto RETRY;
    }

    // lnode
    if(result.value != 0){
	value = result.value;
	return true;
    }

    if(result.sibling != 0){ // move right
	p = result.sibling;
	goto RETRY;
    }

    return false;
}

template <typename Key_t, typename Value_t>
bool tree_t<Key_t, Value_t>::search(const Key_t& key, Value_t& value, uint32_t& page_id, int tid){
#ifdef BREAKDOWN
    t_traversal = t_traversal_retry = t_latch = t_smo = t_leaf = t_buffer = 0;
    retry = false;
#endif
    auto root_id = get_root_id();
    auto p = root_id;
    result_t<Value_t> result;

RETRY:
    auto prev = p;
    if(!page_search(p, key, result, tid)){
	root_id = get_root_id();
	p = root_id;
	goto RETRY;
    }

    if(!result.is_leaf){ // inode
	if(result.sibling != 0){ // move right
	    p = result.sibling;
	    goto RETRY;
	}

	p = result.child; // move down
	goto RETRY;
    }

    // lnode
    if(result.value != 0){
	value = result.value;
	page_id = p;
	return true;
    }

    if(result.sibling != 0){ // move right
	p = result.sibling;
	goto RETRY;
    }

    return false;
}

template <typename Key_t, typename Value_t>
int tree_t<Key_t, Value_t>::scan(const Key_t& start_key, Value_t* values, int num, int tid){
#ifdef BREAKDOWN
    t_traversal = t_traversal_retry = t_latch = t_smo = t_leaf = t_buffer = 0;
    retry = false;
#endif
    auto root_id = get_root_id();
    auto p = root_id;
    result_t<Value_t> result;

RETRY:
    int cnt = 0;
    auto prev = p;
    if(!page_search_lastlevel(p, start_key, result, tid)){
	root_id = get_root_id();
	p = root_id;
	goto RETRY;
    }

    if(result.sibling != 0){ // move right
	p = result.sibling;
	goto RETRY;
    }

    if(result.level != 0){
	p = result.child; // move down
	goto RETRY;
    }

    int ret = scan(p, start_key, values, num, cnt, tid);
    if(!ret){ //conflict during the scan
	goto RETRY;
    }

    return cnt;
}


template <typename Key_t, typename Value_t>
bool tree_t<Key_t, Value_t>::search_next(const Key_t& key, Value_t& value, uint32_t& page_id, int tid){
    auto page_buffer = mem->page_buffer_pool(tid);
    auto page = (lnode_t<Key_t, Value_t>*)page_buffer;
    result_t<Value_t> result;

RETRY:
    bool need_restart = false;
    auto page_addr = tab->get_addr(page_id);
    uint64_t unmasked_addr = get_unmasked_addr(page_addr);
    bool is_remote = is_masked_addr(page_addr);
    memset(&result, 0, sizeof(result_t<Value_t>));
    
    if(is_remote)
	transport->read(page_buffer, unmasked_addr, sizeof(node_t<Key_t>), tid, pid);
    else
	page = (lnode_t<Key_t, Value_t>*)page_addr;

    auto v_start = page->get_version(need_restart);
    if(need_restart)
        goto RETRY;

    if(is_remote)
	transport->read(page_buffer, unmasked_addr, PAGE_SIZE, tid, pid);

    page->search_next(key, result);

    if(is_remote)
	transport->read(page_buffer, unmasked_addr, sizeof(node_t<Key_t>), tid, pid);

    auto v_end = page->get_version(need_restart);
    if(need_restart || (v_start != v_end))
        goto RETRY;

    if(result.value != 0){
        value = result.value;
        return true;
    }

    if(result.sibling != 0){ // move right
        page_id = result.sibling;
        goto RETRY;
    }

    return false;
}

template <typename Key_t, typename Value_t>
bool tree_t<Key_t, Value_t>::page_search(uint32_t page_id, const Key_t& key, result_t<Value_t>& result, int tid){
    auto page_buffer = mem->page_buffer_pool(tid);
    auto node = (node_t<Key_t>*)page_buffer;
    #ifdef BREAKDOWN
    start = asm_rdtsc();
    #endif

RETRY:
    memset(&result, 0, sizeof(result_t<Value_t>));
    bool need_restart = false;
#if CACHE
    tstarling::ThreadSafeScalableCache<uint32_t, uint64_t>::ConstAccessor ac;
    uint64_t page_addr, unmasked_addr;
    bool is_remote = false;
    bool cached = cache->find(ac, page_id);
    if(cached){
	page_addr = *ac.get();
	node = (node_t<Key_t>*)page_addr;
    }
    else{
	page_addr = tab->get_addr(page_id);
	unmasked_addr = page_addr;
	is_remote = true;
	transport->read(page_buffer, unmasked_addr, sizeof(node_t<Key_t>), tid, pid);
    }
#else
    auto page_addr = tab->get_addr(page_id);
    bool is_remote = is_masked_addr(page_addr);
    uint64_t unmasked_addr = get_unmasked_addr(page_addr);
    if(is_remote){
	transport->read(page_buffer, unmasked_addr, sizeof(node_t<Key_t>), tid, pid);
    }
    else{
	node = (node_t<Key_t>*)page_addr;
    }
#endif
    #ifdef BREAKDOWN
    end = asm_rdtsc();
    t_buffer += (end - start);
    start = end;
    #endif

    result.level = node->level;
    result.is_leaf = node->level == 0;
    path_stack[result.level] = page_id;
    auto v_start = node->get_version(need_restart);
    if(need_restart){
	#ifdef BREAKDOWN
	end = asm_rdtsc();
	if(retry) t_traversal_retry += (end - start);
	else t_traversal += (end - start);
	retry = true;
	start = end;
	#endif
	goto RETRY;
    }

    if(is_remote){
	transport->read(page_buffer, unmasked_addr, PAGE_SIZE, tid, pid);
    }

    if(result.is_leaf){ // lnode
	lnode_t<Key_t, Value_t>* page;
	if(is_remote)
	    page = (lnode_t<Key_t, Value_t>*)page_buffer;
	else
	    page = (lnode_t<Key_t, Value_t>*)page_addr;

	if((key >= page->highest) && (page->sibling_ptr != 0)){ // move right
	    if(is_remote){
		transport->read(page_buffer, unmasked_addr, sizeof(uint64_t), tid, pid);
	    }

	    auto v_end = page->get_version(need_restart);
	    if(need_restart || (v_start != v_end)){
		#ifdef BREAKDOWN
		end = asm_rdtsc();
		if(retry) t_traversal_retry += (end - start);
		else t_traversal += (end - start);
		retry = true;
		start = end;
		#endif
		goto RETRY;
	    }

	    result.sibling = page->sibling_ptr;
	    #ifdef BREAKDOWN
	    end = asm_rdtsc();
	    if(retry) t_traversal_retry += (end - start);
	    else t_traversal += (end - start);
	    #endif
	    return true;
	}

	if(key < page->lowest){ // go back to left
	    #ifdef BREAKDOWN
	    end = asm_rdtsc();
	    if(retry) t_traversal_retry += (end - start);
	    else t_traversal += (end - start);
	    retry = true;
	    #endif
	    return false;
	}

	page->search(key, result);
	#ifdef BREAKDOWN
	end = asm_rdtsc();
	t_leaf += (end - start);
	start = end;
	#endif
    }
    else{ // inode
	inode_t<Key_t, Value_t>* page;
	if(is_remote)
	    page = (inode_t<Key_t, Value_t>*)page_buffer;
	else
	    page = (inode_t<Key_t, Value_t>*)page_addr;

	if((key >= page->highest) && (page->sibling_ptr != 0)){ // move right
	    if(is_remote)
		transport->read(page_buffer, unmasked_addr, sizeof(uint64_t), tid, pid);

	    auto v_end = page->get_version(need_restart);
	    if(need_restart || (v_start != v_end)){
	        #ifdef BREAKDOWN
		end = asm_rdtsc();
		if(retry) t_traversal_retry += (end - start);
		else t_traversal += (end - start);
		retry = true;
		start = end;
	        #endif
		goto RETRY;
	    }

	    result.sibling = page->sibling_ptr;
	    #ifdef BREAKDOWN
	    end = asm_rdtsc();
	    if(retry) t_traversal_retry += (end - start);
	    else t_traversal += (end - start);
	    #endif
	    return true;
	}

	if(key < page->lowest){ // go back to left
	    #ifdef BREAKDOWN
	    end = asm_rdtsc();
	    if(retry) t_traversal_retry += (end - start);
	    else t_traversal += (end - start);
	    retry = true;
	    #endif
	    return false;
	}

	page->search(key, result);
    }

    if(is_remote)
	transport->read(page_buffer, unmasked_addr, sizeof(uint64_t), tid, pid);

    auto v_end = node->get_version(need_restart);
    if(need_restart || (v_start != v_end)){
	#ifdef BREAKDOWN
	end = asm_rdtsc();
	if(retry) t_traversal_retry += (end - start);
	else t_traversal += (end - start);
	retry = true;
	start = end;
        #endif
	goto RETRY;
    }
    #ifdef BREAKDOWN
    end = asm_rdtsc();
    if(retry) t_traversal_retry += (end - start);
    else t_traversal += (end - start);
    #endif

#ifdef CACHE
    if(is_remote){
	if(test_probability()){
	    auto new_addr = new char[PAGE_SIZE];
	    memcpy(new_addr, (char*)page_buffer, PAGE_SIZE);
	    if(!insert_to_cache(page_id, (uint64_t)new_addr, tid)){
		delete new_addr;
	    }
	}
    }
#endif

    return true;
}


template <typename Key_t, typename Value_t>
bool tree_t<Key_t, Value_t>::page_search_lastlevel(uint32_t page_id, const Key_t& key, result_t<Value_t>& result, int tid){
    auto page_buffer = mem->page_buffer_pool(tid);
    auto node = (node_t<Key_t>*)page_buffer;

    #ifdef BREAKDOWN
    start = asm_rdtsc();
    #endif
RETRY:
    memset(&result, 0, sizeof(result_t<Value_t>));
    bool need_restart = false;
#if CACHE
    tstarling::ThreadSafeScalableCache<uint32_t, uint64_t>::ConstAccessor ac;
    bool cached = cache->find(ac, page_id);
    uint64_t page_addr, unmasked_addr;
    bool is_remote = false;
    if(cached){
	page_addr = *ac.get();
	node = (node_t<Key_t>*)page_addr;
    }
    else{
	page_addr = tab->get_addr(page_id);
	unmasked_addr = page_addr;
	is_remote = true;
	transport->read(page_buffer, unmasked_addr, sizeof(node_t<Key_t>), tid, pid);
    }
#else
    auto page_addr = tab->get_addr(page_id);
    bool is_remote = is_masked_addr(page_addr);
    uint64_t unmasked_addr = get_unmasked_addr(page_addr);
    if(is_remote)
	transport->read(page_buffer, unmasked_addr, sizeof(node_t<Key_t>), tid, pid);
    else
	node = (node_t<Key_t>*)page_addr;
#endif
    #ifdef BREAKDOWN
    end = asm_rdtsc();
    t_buffer += (end - start);
    start = end;
    #endif

    result.level = node->level;
    result.is_leaf = node->level == 0;
    auto v_start = node->get_version(need_restart);
    if(need_restart){
	#ifdef BREAKDOWN
	end = asm_rdtsc();
	if(retry) t_traversal_retry += (end - start);
	else t_traversal += (end - start);
	retry = true;
	start = end;
        #endif
	goto RETRY;
    }

    if(is_remote)
	transport->read(page_buffer, unmasked_addr, PAGE_SIZE, tid, pid);

    if(result.is_leaf){ // lnode
	lnode_t<Key_t, Value_t>* page;
	if(is_remote)
	    page = (lnode_t<Key_t, Value_t>*)page_buffer;
	else
	    page = (lnode_t<Key_t, Value_t>*)page_addr;

	if((key >= page->highest) && (page->sibling_ptr != 0)){ // move right
	    if(is_remote)
		transport->read(page_buffer, unmasked_addr, sizeof(uint64_t), tid, pid);

	    auto v_end = page->get_version(need_restart);
	    if(need_restart || (v_start != v_end)){
		#ifdef BREAKDOWN
		end = asm_rdtsc();
		if(retry) t_traversal_retry += (end - start);
		else t_traversal += (end - start);
		retry = true;
		start = end;
                #endif
		goto RETRY;
	    }

	    result.sibling = page->sibling_ptr;
	    #ifdef BREAKDOWN
	    end = asm_rdtsc();
	    if(retry) t_traversal_retry += (end - start);
	    else t_traversal += (end - start);
            #endif
	    return true;
	}

	if(key < page->lowest){ // go back to left
	    #ifdef BREAKDOWN
	    end = asm_rdtsc();
	    if(retry) t_traversal_retry += (end - start);
	    else t_traversal += (end - start);
	    retry = true;
            #endif
	    return false;
	}

	if(is_remote)
	    transport->read(page_buffer, unmasked_addr, sizeof(uint64_t), tid, pid);
	auto v_end = page->get_version(need_restart);
	if(need_restart || (v_start != v_end)){
	    #ifdef BREAKDOWN
	    end = asm_rdtsc();
	    if(retry) t_traversal_retry += (end - start);
	    else t_traversal += (end - start);
	    retry = true;
	    start = end;
            #endif
	    goto RETRY;
	}

	#ifdef BREAKDOWN
	end = asm_rdtsc();
	if(retry) t_traversal_retry += (end - start);
	else t_traversal += (end - start);
	#endif
#ifdef CACHE
	if(is_remote){
	    if(test_probability()){
	        auto new_addr = new char[PAGE_SIZE];
	        memcpy(new_addr, page, PAGE_SIZE);
	        if(!insert_to_cache(page_id, (uint64_t)new_addr, tid)){
		    delete new_addr;
	        }
	    }
	}
#endif
	// proceed to scan
	return true;
    }
    else{ // inode
	inode_t<Key_t, Value_t>* page;
	if(is_remote)
	    page = (inode_t<Key_t, Value_t>*)page_buffer;
	else
	    page = (inode_t<Key_t, Value_t>*)page_addr;

	if((key >= page->highest) && (page->sibling_ptr != 0)){ // move right
	    if(is_remote)
		transport->read(page_buffer, unmasked_addr, sizeof(uint64_t), tid, pid);

	    auto v_end = page->get_version(need_restart);
	    if(need_restart || (v_start != v_end)){
		#ifdef BREAKDOWN
		end = asm_rdtsc();
		if(retry) t_traversal_retry += (end - start);
		else t_traversal += (end - start);
		retry = true;
		start = end;
        	#endif
		goto RETRY;
	    }

	    result.sibling = page->sibling_ptr;
	    #ifdef BREAKDOWN
	    end = asm_rdtsc();
	    if(retry) t_traversal_retry += (end - start);
	    else t_traversal += (end - start);
            #endif
	    return true;
	}

	if(key < page->lowest){ // go back to left
	    #ifdef BREAKDOWN
	    end = asm_rdtsc();
	    if(retry) t_traversal_retry += (end - start);
	    else t_traversal += (end - start);
	    retry = true;
	    #endif
	    return false;
	}

	page->search(key, result);
    }

    if(is_remote)
	transport->read(page_buffer, unmasked_addr, sizeof(uint64_t), tid, pid);

    auto v_end = node->get_version(need_restart);
    if(need_restart || (v_start != v_end)){
	#ifdef BREAKDOWN
	end = asm_rdtsc();
	if(retry) t_traversal_retry += (end - start);
	else t_traversal += (end - start);
	retry = true;
	start = end;
        #endif
	goto RETRY;
    }

    #ifdef BREAKDOWN
    end = asm_rdtsc();
    if(retry) t_traversal_retry += (end - start);
    else t_traversal += (end - start);
    #endif
#ifdef CACHE
    if(is_remote){
	if(test_probability()){
	    auto new_addr = new char[PAGE_SIZE];
	    memcpy(new_addr, (char*)page_buffer, PAGE_SIZE);
	    if(!insert_to_cache(page_id, (uint64_t)new_addr, tid)){
	        delete new_addr;
	    }
	}
    }
#endif
    return true;
}

template <typename Key_t, typename Value_t>
bool tree_t<Key_t, Value_t>::scan(uint32_t page_id, const Key_t& key, Value_t*& values, int num, int& cnt, int tid){
    auto page_buffer = mem->page_buffer_pool(tid);
    auto page = (lnode_t<Key_t, Value_t>*)page_buffer;

    #ifdef BRAEKDOWN
    start = asm_rdtsc();
    #endif
RETRY:
    bool need_restart = false;
#if CACHE
    tstarling::ThreadSafeScalableCache<uint32_t, uint64_t>::ConstAccessor ac;
    bool cached = cache->find(ac, page_id);
    uint64_t page_addr, unmasked_addr;
    bool is_remote = false;
    if(cached){
	page_addr = *ac.get();
	page = (lnode_t<Key_t, Value_t>*)page_addr;
    }
    else{
	page_addr = tab->get_addr(page_id);
	unmasked_addr = page_addr;
	is_remote = true;
	transport->read(page_buffer, unmasked_addr, sizeof(node_t<Key_t>), tid, pid);
    }
#else
    auto page_addr = tab->get_addr(page_id);
    bool is_remote = is_masked_addr(page_addr);
    uint64_t unmasked_addr = get_unmasked_addr(page_addr);
    if(is_remote)
	transport->read(page_buffer, unmasked_addr, sizeof(node_t<Key_t>), tid, pid);
    else
	page = (lnode_t<Key_t, Value_t>*)page_addr;
#endif
    #ifdef BREAKDOWN
    end = asm_rdtsc();
    t_buffer += (end - start);
    start = end;
    #endif

    assert(page->level == 0);
    auto v_start = page->get_version(need_restart);
    if(need_restart){
	#ifdef BREAKDOWN
        end = asm_rdtsc();
	if(retry) t_traversal_retry += (end - start);
	else t_traversal += (end - start);
        retry = true;
	start = end;
        #endif
	goto RETRY;
    }

    if((key >= page->highest) && (page->sibling_ptr != 0)){ // move right
	if(is_remote)
	    transport->read(page_buffer, unmasked_addr, sizeof(uint64_t), tid, pid);
	auto v_end = page->get_version(need_restart);
	if(need_restart || (v_start != v_end)){
	    #ifdef BREAKDOWN
	    end = asm_rdtsc();
	    if(retry) t_traversal_retry += (end - start);
	    else t_traversal += (end - start);
	    retry = true;
	    start = end;
            #endif
	    goto RETRY;
	}

	#ifdef BREAKDOWN
        end = asm_rdtsc();
	if(retry) t_traversal_retry += (end - start);
	else t_traversal += (end - start);
        #endif
	scan(page->sibling_ptr, key, values, num, cnt, tid);
	return true;
    }

    if(key < page->lowest){ // move left
	#ifdef BREAKDOWN
        end = asm_rdtsc();
	if(retry) t_traversal_retry += (end - start);
	else t_traversal += (end - start);
        retry = true;
        start = end;
        #endif
	return false;
    }

    if(is_remote)
	transport->read(page_buffer, unmasked_addr, PAGE_SIZE, tid, pid);

    int temp = cnt;
    page->scan(key, values, num, cnt);
    if(is_remote)
	transport->read(page_buffer, unmasked_addr, sizeof(uint64_t), tid, pid);
    auto v_end = page->get_version(need_restart);
    if(need_restart || (v_start != v_end)){
	#ifdef BREAKDOWN
        end = asm_rdtsc();
	t_leaf += (end - start);
        retry = true;
        start = end;
        #endif
	cnt = temp;
	goto RETRY;
    }

    #ifdef BREAKDOWN
    end = asm_rdtsc();
    t_leaf += (end - start);
    #endif

    if((num != cnt) && (page->sibling_ptr != 0)){
#ifdef CACHE
	if(is_remote){
	    auto new_addr = new char[PAGE_SIZE];
	    memcpy(new_addr, (char*)page_buffer, PAGE_SIZE);
	    if(!insert_to_cache(page_id, (uint64_t)new_addr, tid)){
		delete new_addr;
	    }
	}
#endif
	scan(page->sibling_ptr, key, values, num, cnt, tid);
    }
#ifdef CACHE
    if(is_remote){
	auto new_addr = new char[PAGE_SIZE];
	memcpy(new_addr, (char*)page_buffer, PAGE_SIZE);
	if(!insert_to_cache(page_id, (uint64_t)new_addr, tid)){
	    delete new_addr;
	}
    }
#endif
    return true;
}

template <typename Key_t, typename Value_t>
bool tree_t<Key_t, Value_t>::store(uint32_t page_id, const Key_t& key, const Value_t& value, uint32_t root_id, int tid){
    lnode_t<Key_t, Value_t>* page;
    auto page_buffer = mem->page_buffer_pool(tid);
    #ifdef BREAKDOWN
    start = asm_rdtsc();
    #endif
RETRY:
    bool need_restart = false;
#ifdef CACHE
    tstarling::ThreadSafeScalableCache<uint32_t, uint64_t>::ConstAccessor ac;
    bool cached = cache->find(ac, page_id);
    uint64_t page_addr, unmasked_addr;
    bool is_remote = false;
    if(cached){
	page_addr = *ac.get();
	page = (lnode_t<Key_t, Value_t>*)page_addr;
    }
    else{
	page_addr = tab->get_addr(page_id);
	unmasked_addr = page_addr;
	is_remote = true;
    }
#else
    auto page_addr = tab->get_addr(page_id);
    bool is_remote = is_masked_addr(page_addr);
    auto unmasked_addr = get_unmasked_addr(page_addr);
#endif
    #ifdef BREAKDOWN
    end = asm_rdtsc();
    t_buffer += (end - start);
    start = end;
    #endif

    if(is_remote){
	page = (lnode_t<Key_t, Value_t>*)page_buffer;
	transport->read(page_buffer, unmasked_addr, sizeof(uint64_t), tid, pid);
    }
    else{
	page = (lnode_t<Key_t, Value_t>*)page_addr;
    }

    auto version = page->get_version(need_restart);
    if(need_restart){
	#ifdef BREAKDOWN
        end = asm_rdtsc();
	if(retry) t_traversal_retry += (end - start);
	else t_traversal += (end - start);
        retry = true;
        start = end;
        #endif
	goto RETRY;
    }

    bool ret = false;
    if(is_remote)
	ret = transport->cas(page_buffer, unmasked_addr, version, version+LATCH_BIT, sizeof(uint64_t), tid, pid);
    else
	ret = page->upgrade_lock(version);

    #ifdef BREAKDOWN
    end = asm_rdtsc();
    t_latch += (end - start);
    retry = true;
    start = end;
    #endif

    if(!ret){
	goto RETRY;
    }

    if(is_remote)
	transport->read(page_buffer, unmasked_addr, PAGE_SIZE, tid, pid);

    if((key >= page->highest) && (page->sibling_ptr != 0)){ // move right
	auto sibling_id = page->sibling_ptr;
	if(is_remote){
	    page->set_version(version + INITIAL_BIT);
	    transport->write(page_buffer, unmasked_addr, sizeof(uint64_t), tid, pid);
	}
	else
	    page->write_unlock();

	#ifdef BREAKDOWN
        end = asm_rdtsc();
	t_traversal += (end - start);
        #endif
	store(sibling_id, key, value, root_id, tid);
	return true;
    }

    bool need_split = page->insert(key, value);
    if(!need_split){ // normal insertion
	//page->write_unlock();
	if(is_remote){
	    page->set_version(version + INITIAL_BIT);
	    transport->write(page_buffer, unmasked_addr, PAGE_SIZE, tid, pid);
	}
	else{
	    page->write_unlock();
	}

	#ifdef BREAKDOWN
        end = asm_rdtsc();
	t_leaf += (end - start);
        #endif
	return true;
    }

    // split
    Key_t split_key;
    auto sibling_id = tab->get_next_id();
    uint64_t sibling_addr;
    lnode_t<Key_t, Value_t>* sibling;

#ifdef CACHE
    auto sibling_buffer = mem->sibling_buffer_pool(tid);
    uint64_t remote_addr = rpc_alloc(tid);
    if(free_pages.load()){ // local cache
	free_pages.fetch_sub(1);
	sibling = page->split(split_key);
	sibling_addr = remote_addr;
	bool cached = insert_to_cache(sibling_id, (uint64_t)sibling, tid);
	if(!cached){
	    free_pages.fetch_add(1);
	    memcpy((void*)sibling_buffer, sibling, sizeof(lnode_t<Key_t, Value_t>));
	    delete sibling;
	    transport->write(sibling_buffer, remote_addr, PAGE_SIZE, tid, pid);
	}
    }
    else{
	sibling = new ((void*)sibling_buffer) lnode_t<Key_t, Value_t>();
	page->split(split_key, sibling);
	sibling_addr = remote_addr;
	transport->write(sibling_buffer, remote_addr, PAGE_SIZE, tid, pid);
    }
#else
    if(free_pages.load()){ // local alloc
	free_pages.fetch_sub(1);
	sibling = page->split(split_key);
	sibling_addr = (uint64_t)sibling;
    }
    else{ // remote alloc
	auto sibling_buffer = mem->sibling_buffer_pool(tid);
	sibling = new ((void*)sibling_buffer) lnode_t<Key_t, Value_t>();
	sibling_addr = rpc_alloc(tid);
	page->split(split_key, sibling);

	transport->write(sibling_buffer, sibling_addr, PAGE_SIZE, tid, pid);
	sibling_addr = set_masked_addr(sibling_addr);
    }
#endif
    page->sibling_ptr = sibling_id;
    tab->set(sibling_id, sibling_addr);

    if(is_remote){
	page->set_version(version + INITIAL_BIT);
	transport->write(page_buffer, unmasked_addr, PAGE_SIZE, tid, pid);
    }
    else{
	page->write_unlock();
    }

    if(page_id == root_id){ // update new root
	if(update_new_root(page_id, split_key, sibling_id, 1, root_id, tid)){
	    #ifdef BREAKDOWN
	    end = asm_rdtsc();
	    t_smo += (end - start);
	    #endif
	    return true;
	}
    }

    #ifdef BREAKDOWN
    end = asm_rdtsc();
    t_smo += (end - start);
    #endif

    auto upper = path_stack[1];
    if(upper != 0) // traverse from upper level
	internal_store(upper, split_key, sibling_id, root_id, 1, tid);
    else // traverse from the root
	internal_insert(split_key, sibling_id, 1, tid);
    return true;
}

template <typename Key_t, typename Value_t>
void tree_t<Key_t, Value_t>::internal_insert(const Key_t& key, uint32_t value, uint8_t level, int tid){
    auto root_id = get_root_id();
    auto p = root_id;
    result_t<Value_t> result;

RETRY:
    if(!page_search(p, key, result, tid)){
	root_id = get_root_id();
	p = root_id;
	#ifdef BREAKDOWN
	retry = true;
	#endif
	goto RETRY;
    }

    if(result.sibling != 0){
	p = result.sibling;
	goto RETRY;
    }

    if(result.level == level)
	internal_store(p, key, value, root_id, level, tid);
    else{ // traverse down
	p = result.child;
	if(result.level != level+1) // passed the level, restart
	    goto RETRY;

	internal_store(p, key, value, root_id, level, tid);
    }
}

template <typename Key_t, typename Value_t>
void tree_t<Key_t, Value_t>::internal_store(uint32_t page_id, const Key_t& key, uint32_t value, uint32_t root_id, uint8_t level, int tid){
    auto page_buffer = mem->page_buffer_pool(tid);
    #ifdef BREAKDOWN
    start = asm_rdtsc();
    #endif
RETRY:
    bool need_restart = false;
    inode_t<Key_t, Value_t>* page;
#if CACHE
    tstarling::ThreadSafeScalableCache<uint32_t, uint64_t>::ConstAccessor ac;
    uint64_t page_addr, unmasked_addr;
    bool is_remote = false;
    bool cached = cache->find(ac, page_id);
    if(cached){
        page_addr = *ac.get();
        page = (inode_t<Key_t, Value_t>*)page_addr;
    }
    else{
        page_addr = tab->get_addr(page_id);
        unmasked_addr = page_addr;
        is_remote = true;
    }
#else
    auto page_addr = tab->get_addr(page_id);
    bool is_remote = is_masked_addr(page_addr);
    uint64_t unmasked_addr = get_unmasked_addr(page_addr);
#endif
    #ifdef BREAKDOWN
    end = asm_rdtsc();
    t_buffer += (end - start);
    start = end;
    #endif

    if(is_remote){
	transport->read(page_buffer, unmasked_addr, sizeof(uint64_t), tid, pid);
	page = (inode_t<Key_t, Value_t>*)page_buffer;
    }
    else{
	page = (inode_t<Key_t, Value_t>*)page_addr;
    }

    auto version = page->get_version(need_restart);
    if(need_restart){
        #ifdef BREAKDOWN
	retry = true;
	end = asm_rdtsc();
	t_smo += (end - start);
	start = end;
	#endif
	goto RETRY;
    }

    bool ret = false;
    if(is_remote)
	ret = transport->cas(page_buffer, unmasked_addr, version, version+LATCH_BIT, sizeof(uint64_t), tid, pid);
    else
	ret = page->upgrade_lock(version);
    #ifdef BREAKDOWN
    retry = true;
    end = asm_rdtsc();
    t_latch += (end - start);
    start = end;
    #endif

    if(!ret){
	goto RETRY;
    }

    if(is_remote)
	transport->read(page_buffer, unmasked_addr, PAGE_SIZE, tid, pid);

    if((key >= page->highest) && (page->sibling_ptr != 0)){ // move right
	auto sibling_id = page->sibling_ptr;
	if(is_remote){
	    page->set_version(version + INITIAL_BIT);
	    transport->write(page_buffer, unmasked_addr, sizeof(uint64_t), tid, pid);
	}
	else
	    page->write_unlock();

	#ifdef BREAKDOWN
	end = asm_rdtsc();
	t_smo += (end - start);
	#endif
	internal_store(sibling_id, key, value, root_id, level, tid);
	return;
    }

    bool need_split = page->insert(key, value);
    if(!need_split){ // normal insertion
	if(is_remote){
	    page->set_version(version + INITIAL_BIT);
	    transport->write(page_buffer, unmasked_addr, PAGE_SIZE, tid, pid);
	}
	else{
	    page->write_unlock();
	}
	#ifdef BREAKDOWN
	end = asm_rdtsc();
	t_smo += (end - start);
	#endif
	return;
    }

    // split
    Key_t split_key;
    auto sibling_id = tab->get_next_id();
    uint64_t sibling_addr = 0;
    inode_t<Key_t, Value_t>* sibling;

#ifdef CACHE
    auto sibling_buffer = mem->sibling_buffer_pool(tid);
    uint64_t remote_addr = rpc_alloc(tid);
    if(free_pages.load()){ // local cache
        free_pages.fetch_sub(1);
        sibling = page->split(split_key);
        sibling_addr = remote_addr;
        bool cached = insert_to_cache(sibling_id, (uint64_t)sibling, tid);
	if(!cached){
	    free_pages.fetch_add(1);
	    memcpy((void*)sibling_buffer, sibling, sizeof(inode_t<Key_t, Value_t>));
	    delete sibling;
	    transport->write(sibling_buffer, remote_addr, PAGE_SIZE, tid, pid);
	    transport->write(sibling_buffer, remote_addr, PAGE_SIZE, tid, pid);
        }
    }
    else{
        sibling = new ((void*)sibling_buffer) inode_t<Key_t, Value_t>(level);
        page->split(split_key, sibling);
        sibling_addr = remote_addr;
	transport->write(sibling_buffer, remote_addr, PAGE_SIZE, tid, pid);
    }
#else
    if(free_pages.load()){ // local alloc
	free_pages.fetch_sub(1);
	sibling = page->split(split_key);
	sibling_addr = (uint64_t)sibling;
    }
    else{ // remote alloc
	auto sibling_buffer = mem->sibling_buffer_pool(tid);
	sibling_addr = rpc_alloc(tid);
	sibling = new ((void*)sibling_buffer) inode_t<Key_t, Value_t>(level);
	page->split(split_key, sibling);
	transport->write(sibling_buffer, sibling_addr, PAGE_SIZE, tid, pid);
	sibling_addr = set_masked_addr(sibling_addr);
    }
#endif
    tab->set(sibling_id, sibling_addr);
    page->sibling_ptr = sibling_id;

    if(is_remote){
	page->set_version(version + INITIAL_BIT);
	transport->write(page_buffer, unmasked_addr, PAGE_SIZE, tid, pid);
    }
    else{
	page->write_unlock();
    }

    if(page_id == root_id){ // update new root
	if(update_new_root(page_id, split_key, sibling_id, level+1, root_id, tid)){
	    #ifdef BREAKDOWN
	    end = asm_rdtsc();
	    t_smo += (end - start);
	    #endif
	    return;
	}
    }

    #ifdef BREAKDOWN
    end = asm_rdtsc();
    t_smo += (end - start);
    #endif
    auto upper = path_stack[level+1];
    if(upper != 0) // traverse from upper level
	internal_store(upper, split_key, sibling_id, root_id, level+1, tid);
    else // traverse from the root
	internal_insert(split_key, sibling_id, level+1, tid);
}

template class tree_t<Key, Value>;
