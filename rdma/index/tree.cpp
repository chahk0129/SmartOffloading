#include "common/rpc.h"
#include "index/node.h"
#include "index/tree.h"
#include "client/transport.h"
#include "client/mr.h"

static thread_local uint64_t path_stack[MAX_TREE_LEVEL];

template <typename Key_t, typename Value_t>
tree_t<Key_t, Value_t>::tree_t(client_mr_t* mem, client_transport_t* transport, int pid): mem(mem), transport(transport), pid(pid){
    set_key<Key_t>();
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
void tree_t<Key_t, Value_t>::initialize_root(){
    int tid = 0;
    auto page_buffer = mem->page_buffer_pool(tid);
    auto root = new ((void*)page_buffer) lnode_t<Key_t, Value_t>();
    auto addr = rpc_alloc(tid);
    transport->write(page_buffer, addr, PAGE_SIZE, tid, pid);

    auto send_ptr = mem->request_buffer_pool(tid);
    auto request = create_message<request_t>((void*)send_ptr, tid, pid, request_type::IDX_UPDATE_ROOT, addr);
    transport->send((uint64_t)request, sizeof(request_t), tid);

    auto recv_ptr = mem->response_buffer_pool(tid);
    auto response = create_message<response_t>((void*)recv_ptr);
    transport->recv((uint64_t)response, sizeof(response_t), tid);
    if(response->type != response_type::SUCCESS){
	debug::notify_error("[INDEX] RPC root update failed: return type %d", response->type);
	exit(0);
    }
    _root_addr = response->addr;
}

template <typename Key_t, typename Value_t>
bool tree_t<Key_t, Value_t>::update_new_root(uint64_t left, const Key_t& key, uint64_t right, uint8_t level, uint64_t old_root, int tid){
    auto addr = rpc_alloc(tid);
    auto page_buffer = mem->page_buffer_pool(tid);
    auto new_root = new ((void*)page_buffer) inode_t<Key_t, Value_t>(left, key, right, level);
    transport->write(page_buffer, addr, PAGE_SIZE, tid, pid);

    auto cas_buffer = mem->cas_buffer_pool(tid);
    uint64_t* cmp = (uint64_t*)cas_buffer;
    *cmp = old_root;
    bool ret = transport->cas(cas_buffer, _root_addr, old_root, addr, sizeof(uint64_t), tid, pid);
    if(!ret){
	rpc_dealloc(tid, addr);
	return false;
    }
    return true;
}

template <typename Key_t, typename Value_t>
uint64_t tree_t<Key_t, Value_t>::get_root_addr(int tid){
    auto root_buffer = mem->root_buffer_pool(tid);
    transport->read(root_buffer, _root_addr, sizeof(uint64_t), tid, pid);
    return *(uint64_t*)root_buffer;
}

template <typename Key_t, typename Value_t>
void tree_t<Key_t, Value_t>::print(){
    int tid = 0;
    auto root_addr = get_root_addr(tid);
    auto p = root_addr;

    auto page_buffer = mem->page_buffer_pool(tid);
    auto node = (node_t<Key_t>*)page_buffer;
    do{
	transport->read(page_buffer, p, PAGE_SIZE, tid, pid);
	if(node->level == 0){
	    debug::notify_info("[LEAF %lx]", p);
	    auto page = (lnode_t<Key_t, Value_t>*)page_buffer;
	    auto sibling_addr = page->sibling_ptr;
	    page->print();
	    while(sibling_addr != 0){
		auto sibling_buffer = mem->sibling_buffer_pool(tid);
		auto sibling = (lnode_t<Key_t, Value_t>*)sibling_buffer;
		transport->read(sibling_buffer, sibling_addr, PAGE_SIZE, tid, pid);
		debug::notify_info("[LEAF %lx]", sibling_addr);
		sibling->print();
		sibling_addr = sibling->sibling_ptr;
	    }
	}
	else{
	    debug::notify_info("[INTERNAL %lx]", p);
	    auto page = (inode_t<Key_t, Value_t>*)page_buffer;
	    auto sibling_addr = page->sibling_ptr;
	    auto leftmost_ptr = page->leftmost_ptr;
	    page->print();
	    while(sibling_addr != 0){
		auto sibling_buffer = mem->sibling_buffer_pool(tid);
		auto sibling = (inode_t<Key_t, Value_t>*)sibling_buffer;
		transport->read(sibling_buffer, sibling_addr, PAGE_SIZE, tid, pid);
		debug::notify_info("[INTERNAL %lx]", sibling_addr);
		sibling->print();
		sibling_addr = sibling->sibling_ptr;
	    }
	    p = leftmost_ptr;
	}
    }while(node->level != 0);
}

template <typename Key_t, typename Value_t>
void tree_t<Key_t, Value_t>::insert(Key_t key, Value_t value, int tid){
    memset(path_stack, 0, sizeof(uint64_t) * MAX_TREE_LEVEL);
    auto root_addr = get_root_addr(tid);
    auto p = root_addr;
    result_t<Value_t> result;

RETRY:
    auto prev = p;
    if(!page_search(p, key, result, tid)){
	root_addr = get_root_addr(tid);
	p = root_addr;
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

    store(p, key, value, root_addr, tid);
}

template <typename Key_t, typename Value_t>
bool tree_t<Key_t, Value_t>::search(const Key_t& key, Value_t& value, int tid){
    auto root_addr = get_root_addr(tid);
    auto p = root_addr;
    result_t<Value_t> result;

RETRY:
    auto prev = p;
    if(!page_search(p, key, result, tid)){
	root_addr = get_root_addr(tid);
	p = root_addr;
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
bool tree_t<Key_t, Value_t>::search(const Key_t& key, Value_t& value, uint64_t& page_addr, int tid){
    auto root_addr = get_root_addr(tid);
    auto p = root_addr;
    result_t<Value_t> result;

RETRY:
    auto prev = p;
    if(!page_search(p, key, result, tid)){
	root_addr = get_root_addr(tid);
	p = root_addr;
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
	page_addr = p;
	return true;
    }

    if(result.sibling != 0){ // move right
	p = result.sibling;
	goto RETRY;
    }

    return false;
}

template <typename Key_t, typename Value_t>
bool tree_t<Key_t, Value_t>::search_next(const Key_t& key, Value_t& value, uint64_t& page_addr, int tid){
    auto page_buffer = mem->page_buffer_pool(tid);
    auto page = (lnode_t<Key_t, Value_t>*)page_buffer;
    result_t<Value_t> result;

RETRY:
    bool need_restart = false;
    memset(&result, 0, sizeof(result_t<Value_t>));
    transport->read(page_buffer, page_addr, sizeof(node_t<Key_t>), tid, pid);
    auto v_start = page->get_version(need_restart);
    if(need_restart)
	goto RETRY;

    transport->read(page_buffer, page_addr, PAGE_SIZE, tid, pid);
    page->search_next(key, result);
    transport->read(page_buffer, page_addr, sizeof(node_t<Key_t>), tid, pid);
    auto v_end = page->get_version(need_restart);
    if(need_restart || (v_start != v_end))
	goto RETRY;

    if(result.value != 0){
	value = result.value;
	return true;
    }

    if(result.sibling != 0){ // move right
	page_addr = result.sibling;
	goto RETRY;
    }

    return false;
}

template <typename Key_t, typename Value_t>
int tree_t<Key_t, Value_t>::scan(const Key_t& start_key, Value_t*& values, int num, int tid){
    auto root_addr = get_root_addr(tid);
    auto p = root_addr;
    result_t<Value_t> result;
RETRY:
    int cnt = 0;
    auto prev = p;
    if(!page_search_lastlevel(p, start_key, result, tid)){
        root_addr = get_root_addr(tid);
        p = root_addr;
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
bool tree_t<Key_t, Value_t>::page_search_lastlevel(uint64_t page_addr, const Key_t& key, result_t<Value_t>& result, int tid){
    auto page_buffer = mem->page_buffer_pool(tid);
    auto node = (node_t<Key_t>*)page_buffer;

RETRY:
    memset(&result, 0, sizeof(result_t<Value_t>));
    bool need_restart = false;
    transport->read(page_buffer, page_addr, sizeof(node_t<Key_t>), tid, pid);

    result.level = node->level;
    result.is_leaf = node->level == 0;
    auto v_start = node->get_version(need_restart);
    if(need_restart){
        goto RETRY;
    }

    transport->read(page_buffer, page_addr, PAGE_SIZE, tid, pid);
    if(result.is_leaf){ // lnode
        auto page = (lnode_t<Key_t, Value_t>*)page_buffer;
        if((key >= page->highest) && (page->sibling_ptr != 0)){ // move right
            transport->read(page_buffer, page_addr, sizeof(uint64_t), tid, pid);
            auto v_end = page->get_version(need_restart);
            if(need_restart || (v_start != v_end)){
                goto RETRY;
	    }

	    result.sibling = page->sibling_ptr;
            return true;
        }

        if(key < page->lowest){ // go back to left
            return false;
        }

        transport->read(page_buffer, page_addr, sizeof(uint64_t), tid, pid);
        auto v_end = page->get_version(need_restart);
        if(need_restart || (v_start != v_end)){
            goto RETRY;
        }

        // proceed to scan
        return true;
    }
    else{ // inode
	auto page = (inode_t<Key_t, Value_t>*)page_buffer;
        if((key >= page->highest) && (page->sibling_ptr != 0)){ // move right
            transport->read(page_buffer, page_addr, sizeof(uint64_t), tid, pid);
            auto v_end = page->get_version(need_restart);
            if(need_restart || (v_start != v_end)){
                goto RETRY;
            }

            result.sibling = page->sibling_ptr;
            return true;
        }

        if(key < page->lowest){ // go back to left
            return false;
        }

        page->search(key, result);
    }

    transport->read(page_buffer, page_addr, sizeof(uint64_t), tid, pid);
    auto v_end = node->get_version(need_restart);
    if(need_restart || (v_start != v_end)){
        goto RETRY;
    }

    return true;
}


template <typename Key_t, typename Value_t>
bool tree_t<Key_t, Value_t>::page_search(uint64_t page_addr, const Key_t& key, result_t<Value_t>& result, int tid){
    auto page_buffer = mem->page_buffer_pool(tid);
    auto node = (node_t<Key_t>*)page_buffer;

RETRY:
    memset(&result, 0, sizeof(result_t<Value_t>));
    bool need_restart = false;
    transport->read(page_buffer, page_addr, sizeof(node_t<Key_t>), tid, pid);

    result.level = node->level;
    result.is_leaf = node->level == 0;
    path_stack[result.level] = page_addr;
    auto v_start = node->get_version(need_restart);
    if(need_restart)
	goto RETRY;

    transport->read(page_buffer, page_addr, PAGE_SIZE, tid, pid);
    if(result.is_leaf){ // lnode
	auto page = (lnode_t<Key_t, Value_t>*)page_buffer;
	if((key >= page->highest) && (page->sibling_ptr != 0)){ // move right
	    transport->read(page_buffer, page_addr, sizeof(uint64_t), tid, pid);
	    auto v_end = page->get_version(need_restart);
	    if(need_restart || (v_start != v_end))
		goto RETRY;

	    result.sibling = page->sibling_ptr;
	    return true;
	}

	if(key < page->lowest){ // go back to left
	    return false;
	}

	page->search(key, result);
    }
    else{ // inode
	auto page = (inode_t<Key_t, Value_t>*)page_buffer;
	if((key >= page->highest) && (page->sibling_ptr != 0)){ // move right
	    transport->read(page_buffer, page_addr, sizeof(uint64_t), tid, pid);
	    auto v_end = page->get_version(need_restart);
	    if(need_restart || (v_start != v_end))
		goto RETRY;

	    result.sibling = page->sibling_ptr;
	    return true;
	}

	if(key < page->lowest){ // go back to left
	    return false;
	}

	page->search(key, result);
    }

    transport->read(page_buffer, page_addr, sizeof(uint64_t), tid, pid);
    auto v_end = node->get_version(need_restart);
    if(need_restart || (v_start != v_end))
	goto RETRY;

    return true;
}

template <typename Key_t, typename Value_t>
bool tree_t<Key_t, Value_t>::scan(uint64_t page_addr, const Key_t& key, Value_t*& values, int num, int& cnt, int tid){
    auto page_buffer = mem->page_buffer_pool(tid);
    auto page = (lnode_t<Key_t, Value_t>*)page_buffer;

RETRY:
    bool need_restart = false;
    transport->read(page_buffer, page_addr, sizeof(node_t<Key_t>), tid, pid);

    assert(page->level == 0);
    auto v_start = page->get_version(need_restart);
    if(need_restart){
        goto RETRY;
    }

    if((key >= page->highest) && (page->sibling_ptr != 0)){ // move right
        transport->read(page_buffer, page_addr, sizeof(uint64_t), tid, pid);
        auto v_end = page->get_version(need_restart);
        if(need_restart || (v_start != v_end)){
            goto RETRY;
        }

        scan(page->sibling_ptr, key, values, num, cnt, tid);
	return true;
    }

    if(key < page->lowest){ // move left
        return false;
    }

    int temp = cnt;
    page->scan(key, values, num, cnt);

    transport->read(page_buffer, page_addr, sizeof(uint64_t), tid, pid);
    auto v_end = page->get_version(need_restart);
    if(need_restart || (v_start != v_end)){
        cnt = temp;
        goto RETRY;
    }

    if((num != cnt) && (page->sibling_ptr != 0)){
        scan(page->sibling_ptr, key, values, num, cnt, tid);
    }
    return true;
}

template <typename Key_t, typename Value_t>
bool tree_t<Key_t, Value_t>::store(uint64_t page_addr, const Key_t& key, const Value_t& value, uint64_t root_addr, int tid){
    lnode_t<Key_t, Value_t>* page;
    auto page_buffer = mem->page_buffer_pool(tid);
RETRY:
    bool need_restart = false;
    transport->read(page_buffer, page_addr, sizeof(uint64_t), tid, pid);
    page = (lnode_t<Key_t, Value_t>*)page_buffer;
    auto version = page->get_version(need_restart);
    if(need_restart)
	goto RETRY;

    auto ret = transport->cas(page_buffer, page_addr, version, version+LATCH_BIT, sizeof(uint64_t), tid, pid);
    if(!ret){
	goto RETRY;
    }
    
    transport->read(page_buffer, page_addr, PAGE_SIZE, tid, pid);
    if((key >= page->highest) && (page->sibling_ptr != 0)){ // move right
	auto sibling_addr = page->sibling_ptr;
	page->set_version(version + INITIAL_BIT);
	transport->write(page_buffer, page_addr, sizeof(uint64_t), tid, pid);
	store(sibling_addr, key, value, root_addr, tid);
	return true;
    }

    bool need_split = page->insert(key, value);
    if(!need_split){ // normal insertion
	//page->write_unlock();
	page->set_version(version + INITIAL_BIT);
	transport->write(page_buffer, page_addr, PAGE_SIZE, tid, pid);
	return true;
    }

    // split
    Key_t split_key;
    auto sibling_addr = rpc_alloc(tid);

    auto sibling_buffer = mem->sibling_buffer_pool(tid);
    auto sibling = new ((void*)sibling_buffer) lnode_t<Key_t, Value_t>();

    page->split(split_key, sibling);
    transport->write(sibling_buffer, sibling_addr, PAGE_SIZE, tid, pid);
    page->sibling_ptr = sibling_addr;

    //page->write_unlock();
    page->set_version(version + INITIAL_BIT);
    transport->write(page_buffer, page_addr, PAGE_SIZE, tid, pid);

    if(page_addr == root_addr){ // update new root
	if(update_new_root(page_addr, split_key, sibling_addr, 1, root_addr, tid))
	    return true;
    }

    auto upper = path_stack[1];
    if(upper != 0) // traverse from upper level
	internal_store(upper, split_key, sibling_addr, root_addr, 1, tid);
    else // traverse from the root
	internal_insert(split_key, sibling_addr, 1, tid);
    return true;
}

template <typename Key_t, typename Value_t>
void tree_t<Key_t, Value_t>::internal_insert(const Key_t& key, uint64_t value, uint8_t level, int tid){
    auto root_addr = get_root_addr(tid);
    auto p = root_addr;
    result_t<Value_t> result;

RETRY:
    if(!page_search(p, key, result, tid)){
	root_addr = get_root_addr(tid);
	p = root_addr;
	goto RETRY;
    }

    if(result.sibling != 0){
	p = result.sibling;
	goto RETRY;
    }

    if(result.level == level)
	internal_store(p, key, value, root_addr, level, tid);
    else{ // traverse down
	p = result.child;
	if(result.level != level+1) // passed the level, restart
	    goto RETRY;

	internal_store(p, key, value, root_addr, level, tid);
    }
}

template <typename Key_t, typename Value_t>
void tree_t<Key_t, Value_t>::internal_store(uint64_t page_addr, const Key_t& key, uint64_t value, uint64_t root_addr, uint8_t level, int tid){
    auto page_buffer = mem->page_buffer_pool(tid);
RETRY:
    bool need_restart = false;
    transport->read(page_buffer, page_addr, sizeof(uint64_t), tid, pid);
    auto page = (inode_t<Key_t, Value_t>*)page_buffer;
    auto version = page->get_version(need_restart);
    if(need_restart)
	goto RETRY;

    auto ret = transport->cas(page_buffer, page_addr, version, version+LATCH_BIT, sizeof(uint64_t), tid, pid);
    if(!ret)
	goto RETRY;

    transport->read(page_buffer, page_addr, PAGE_SIZE, tid, pid);
    if((key >= page->highest) && (page->sibling_ptr != 0)){ // move right
	auto sibling_addr = page->sibling_ptr;
	//page->write_unlock();
	page->set_version(version + INITIAL_BIT);
	transport->write(page_buffer, page_addr, sizeof(uint64_t), tid, pid);
	internal_store(sibling_addr, key, value, root_addr, level, tid);
	return;
    }

    bool need_split = page->insert(key, value);
    if(!need_split){ // normal insertion
	page->set_version(version + INITIAL_BIT);
	//page->write_unlock();
	transport->write(page_buffer, page_addr, PAGE_SIZE, tid, pid);
	return;
    }

    // split
    Key_t split_key;
    auto sibling_addr = rpc_alloc(tid);

    auto sibling_buffer = mem->sibling_buffer_pool(tid);
    auto sibling = new ((void*)sibling_buffer) inode_t<Key_t, Value_t>(level);
    page->split(split_key, sibling);
    transport->write(sibling_buffer, sibling_addr, PAGE_SIZE, tid, pid);
    page->sibling_ptr = sibling_addr;

    //page->write_unlock();
    page->set_version(version + INITIAL_BIT);
    transport->write(page_buffer, page_addr, PAGE_SIZE, tid, pid);
    
    if(page_addr == root_addr){ // update new root
	if(update_new_root(page_addr, split_key, sibling_addr, level+1, root_addr, tid))
	    return;
    }

    auto upper = path_stack[level+1];
    if(upper != 0) // traverse from upper level
	internal_store(upper, split_key, sibling_addr, root_addr, level+1, tid);
    else // traverse from the root
	internal_insert(split_key, sibling_addr, level+1, tid);
}

template class tree_t<uint64_t, uint64_t>;
