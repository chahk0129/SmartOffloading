#include "common/global.h"
#include "common/helper.h"
#include "common/hash.h"
#include "system/workload.h"
#include "system/thread.h"
#include "system/query.h"
#include "system/txn.h"
#include "storage/table.h"
#include "storage/row.h"
#include "index/tree.h"
#include "benchmark/tpcc.h"
#include "benchmark/tpcc_helper.h"
#include "benchmark/tpcc_const.h"
#include "client/transport.h"
#include "client/mr.h"

#include <vector>
#include <thread>

RC tpcc_workload_t::init(config_t* conf){
    workload_t::init(conf);
    std::string path = "../benchmark/";
    #if TPCC_SMALL
    path += "TPCC_short_schema.txt";
    #else
    path += "TPCC_full_schema.txt";
    #endif
    std::cout << "Reading schema file: " << path << std::endl;
    init_schema(path);
    std::cout << "TPCC schema initialized" << std::endl;
    init_table();
    return RCOK;
}

RC tpcc_workload_t::init_schema(std::string path){
    workload_t::init_schema(path);
    t_warehouse = tables["WAREHOUSE"];
    t_district = tables["DISTRICT"];
    t_customer = tables["CUSTOMER"];
    t_history = tables["HISTORY"];
    t_neworder = tables["NEW-ORDER"];
    t_order = tables["ORDER"];
    t_orderline = tables["ORDER-LINE"];
    t_item = tables["ITEM"];
    t_stock = tables["STOCK"];

    i_item = indexes["ITEM_IDX"];
    i_warehouse = indexes["WAREHOUSE_IDX"];
    i_district = indexes["DISTRICT_IDX"];
    i_customer_id = indexes["CUSTOMER_ID_IDX"];
    i_customer_last = indexes["CUSTOMER_LAST_IDX"];
    i_stock = indexes["STOCK_IDX"];
    return RCOK;
}

RC tpcc_workload_t::init_table(){
    num_wh = g_num_wh;
    /******** fill in data ************/
    // data filling process:
    //- item
    //- wh
    //      - stock
    //      - dist
    //      - cust
    //              - hist
    //              - order
    //              - new order
    //              - order line
    /**********************************/
    tpcc_buffer = new drand48_data*[g_num_wh];
    std::vector<std::thread> thd;
    for(int i=0; i<g_num_wh; i++)
	thd.push_back(std::thread(thread_init_warehouse, this, i));
    for(auto& t: thd) t.join();
    printf("TPCC Data Initialization Complete!\n");
    return RCOK;
}

RC tpcc_workload_t::get_txn_man(txn_man_t*& txn_manager, thread_t* thd){
    txn_manager = new tpcc_txn_man_t;
    txn_manager->init(thd, this, thd->get_tid());
    return RCOK;
}

void* tpcc_workload_t::thread_init_warehouse(void* This, int tid){
    auto wl = (tpcc_workload_t*)This;
    uint32_t wid = tid + 1;
    tpcc_buffer[tid] = (drand48_data *) _mm_malloc(sizeof(drand48_data), 64);
    assert((uint64_t)tid < g_num_wh);
    srand48_r(wid, tpcc_buffer[tid]);

    if (tid == 0)
	wl->init_tab_item(tid);
    wl->init_tab_wh(wid, tid);
    wl->init_tab_dist(wid, tid);
    wl->init_tab_stock(wid, tid );
    for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
	wl->init_tab_cust(did, wid, tid);
	wl->init_tab_order(did, wid, tid);
	for (uint64_t cid = 1; cid <= g_cust_per_dist; cid++)
	    wl->init_tab_hist(cid, did, wid, tid);
    }
    return NULL;
}

uint64_t tpcc_workload_t::rpc_alloc_row(int pid, int tid){
    if(pid >= MR_PARTITION_NUM){
	debug::notify_error("tid %d ---- pid %d is larger than MR_PARTITON_NUM %d", tid, pid, MR_PARTITION_NUM);
    }
    assert(pid < MR_PARTITION_NUM);
    auto send_ptr = mem->request_buffer_pool(tid);
    auto request = create_message<request_t>((void*)send_ptr, tid, pid, request_type::TABLE_ALLOC_ROW);
    transport->send((uint64_t)request, sizeof(request_t), tid);

    auto recv_ptr = mem->response_buffer_pool(tid);
    auto response = create_message<response_t>((void*)recv_ptr);
    transport->recv((uint64_t)response, sizeof(response_t), tid);
    if(response->type != response_type::SUCCESS)
	debug::notify_error("New row allocation failed (%d)", response->type);

    return response->addr;
}

void tpcc_workload_t::init_tab_item(int tid) {
    catalog_t* schema = t_item->get_schema();
    for (uint32_t i = 1; i <= g_max_items; i++) {
	int pid = key_to_part(i);
	uint64_t row_addr = rpc_alloc_row(pid, tid);
	uint64_t primary_key = i;
	uint64_t row_id;

	auto new_row = mem->row_buffer_pool(tid, 0);
	memset(new_row, 0, sizeof(row_t));
	new_row->init(t_item, pid, row_id);
	new_row->set_primary_key(primary_key);
	new_row->set_value(schema, I_ID, i);
	new_row->set_value(schema, I_IM_ID, URand(1L, 10000L, 0));
	char name[24];
	MakeAlphaString(14, 24, name, 0);
	new_row->set_value(schema, I_NAME, name);
	new_row->set_value(schema, I_PRICE, URand(1, 100, 0));
	char data[50];
	MakeAlphaString(26, 50, data, 0);
	// TODO in TPCC, "original" should start at a random position
	if (RAND(10, 0) == 0)
	    strcpy(data, "original");
	new_row->set_value(schema, I_DATA, data);
	transport->write((uint64_t)new_row, row_addr, sizeof(row_t), tid, pid);

	uint64_t idx_key = primary_key;
	uint64_t idx_value = row_addr;
	i_item->insert(idx_key, idx_value, tid);
    }
}

void tpcc_workload_t::init_tab_wh(uint32_t wid, int tid) {
    assert(wid >= 1 && wid <= g_num_wh);
    int pid = key_to_part(wid);
    uint64_t row_addr = rpc_alloc_row(pid, tid);
    uint64_t primary_key = wid;

    catalog_t* schema = t_warehouse->get_schema();
    auto row = mem->row_buffer_pool(tid, 0);
    uint64_t row_id;
    row->init(t_warehouse, pid, row_id);
    row->set_primary_key(primary_key);
    row->set_value(schema, W_ID, wid);
    char name[10];
    MakeAlphaString(6, 10, name, wid-1);
    row->set_value(schema, W_NAME, name);
    char street[20];
    MakeAlphaString(10, 20, street, wid-1);
    row->set_value(schema, W_STREET_1, street);
    MakeAlphaString(10, 20, street, wid-1);
    row->set_value(schema, W_STREET_2, street);
    MakeAlphaString(10, 20, street, wid-1);
    row->set_value(schema, W_CITY, street);
    char state[2];
    MakeAlphaString(2, 2, state, wid-1); /* State */
    row->set_value(schema, W_STATE, state);
    char zip[9];
    MakeNumberString(9, 9, zip, wid-1); /* Zip */
    row->set_value(schema, W_ZIP, zip);
    double tax = (double)URand(0L,200L,wid-1)/1000.0;
    double w_ytd=300000.00;
    row->set_value(schema, W_TAX, tax);
    row->set_value(schema, W_YTD, w_ytd);

    transport->write((uint64_t)row, row_addr, sizeof(row_t), tid, pid);
    i_warehouse->insert(primary_key, row_addr, tid);
    return;
}

void tpcc_workload_t::init_tab_dist(uint64_t wid, int tid) {
    catalog_t* schema = t_district->get_schema();
    for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
	Key key = distKey(did, wid);
	int pid = key_to_part(key);
	//int pid = key_to_part(did);
	uint64_t row_addr = rpc_alloc_row(pid, tid);

	auto row = mem->row_buffer_pool(tid, 0);
	uint64_t row_id;
	row->init(t_district, pid, row_id);
	row->set_primary_key(did);

	row->set_value(schema, D_ID, did);
	row->set_value(schema, D_W_ID, wid);
	char name[10];
	MakeAlphaString(6, 10, name, wid-1);
	row->set_value(schema, D_NAME, name);
	char street[20];
	MakeAlphaString(10, 20, street, wid-1);
	row->set_value(schema, D_STREET_1, street);
	MakeAlphaString(10, 20, street, wid-1);
	row->set_value(schema, D_STREET_2, street);
	MakeAlphaString(10, 20, street, wid-1);
	row->set_value(schema, D_CITY, street);
	char state[2];
	MakeAlphaString(2, 2, state, wid-1); /* State */
	row->set_value(schema, D_STATE, state);
	char zip[9];
	MakeNumberString(9, 9, zip, wid-1); /* Zip */
	row->set_value(schema, D_ZIP, zip);
	double tax = (double)URand(0L,200L,wid-1)/1000.0;
	double w_ytd=30000.00;
	row->set_value(schema, D_TAX, tax);
	row->set_value(schema, D_YTD, w_ytd);
	row->set_value(schema, D_NEXT_O_ID, 3001);

	transport->write((uint64_t)row, row_addr, sizeof(row_t), tid, pid);
	i_district->insert(key, row_addr, tid);
    }
}

void tpcc_workload_t::init_tab_stock(uint64_t wid, int tid) {
    catalog_t* schema = t_stock->get_schema();
    for (uint32_t sid = 1; sid <= g_max_items; sid++) {
	Key key = stockKey(sid, wid);
	int pid = key_to_part(key);
	//int pid = key_to_part(sid);
	uint64_t row_addr = rpc_alloc_row(pid, tid);
	auto row = mem->row_buffer_pool(tid, 0);

	uint64_t row_id;
	row->init(t_stock, pid, row_id);
	row->set_primary_key(sid);
	row->set_value(schema, S_I_ID, sid);
	row->set_value(schema, S_W_ID, wid);
	row->set_value(schema, S_QUANTITY, URand(10, 100, wid-1));
	row->set_value(schema, S_REMOTE_CNT, 0);
#if !TPCC_SMALL
	char s_dist[25];
	char row_name[10] = "S_DIST_";
	for (int i = 1; i <= 10; i++) {
	    if (i < 10) {
		row_name[7] = '0';
		row_name[8] = i + '0';
	    } 
	    else {
		row_name[7] = '1';
		row_name[8] = '0';
	    }
	    row_name[9] = '\0';
	    MakeAlphaString(24, 24, s_dist, wid-1);
	    row->set_value(schema, row_name, s_dist);
	}
	row->set_value(schema, S_YTD, 0);
	row->set_value(schema, S_ORDER_CNT, 0);
	char s_data[50];
	int len = MakeAlphaString(26, 50, s_data, wid-1);
	if (rand() % 100 < 10) {
	    int idx = URand(0, len - 8, wid-1);
	    strcpy(&s_data[idx], "original");
	}
	row->set_value(schema, S_DATA, s_data);
#endif
	transport->write((uint64_t)row, row_addr, sizeof(row_t), tid, pid);
	i_stock->insert(key, row_addr, tid);
    }
}

void tpcc_workload_t::init_tab_cust(uint64_t did, uint64_t wid, int tid) {
    assert(g_cust_per_dist >= 1000);
    catalog_t* schema = t_customer->get_schema();
    for (uint32_t cid = 1; cid <= g_cust_per_dist; cid++) {
	Key key = distKey(did, wid);
	int pid = key_to_part(key);
	//Key key;
	//int pid = key_to_part(did);
	uint64_t row_addr = rpc_alloc_row(pid, tid);
	auto row = mem->row_buffer_pool(tid, 0);

	uint64_t row_id;
	row->init(t_customer, pid, row_id);
	row->set_primary_key(cid);
	row->set_value(schema, C_ID, cid);
	row->set_value(schema, C_D_ID, did);
	row->set_value(schema, C_W_ID, wid);
	char c_last[LASTNAME_LEN];
	if (cid <= 1000)
	    Lastname(cid - 1, c_last);
	else
	    Lastname(NURand(255,0,999,wid-1), c_last);
	row->set_value(schema, C_LAST, c_last);
#if !TPCC_SMALL
	char tmp[3] = "OE";
	row->set_value(schema, C_MIDDLE, tmp);
	char c_first[FIRSTNAME_LEN];
	MakeAlphaString(FIRSTNAME_MIN_LEN, sizeof(c_first), c_first, wid-1);
	row->set_value(schema, C_FIRST, c_first);
	char street[20];
	MakeAlphaString(10, 20, street, wid-1);
	row->set_value(schema, C_STREET_1, street);
	MakeAlphaString(10, 20, street, wid-1);
	row->set_value(schema, C_STREET_2, street);
	MakeAlphaString(10, 20, street, wid-1);
	row->set_value(schema, C_CITY, street);
	char state[2];
	MakeAlphaString(2, 2, state, wid-1); /* State */
	row->set_value(schema, C_STATE, state);
	char zip[9];
	MakeNumberString(9, 9, zip, wid-1); /* Zip */
	row->set_value(schema, C_ZIP, zip);
	char phone[16];
	MakeNumberString(16, 16, phone, wid-1); /* Zip */
	row->set_value(schema, C_PHONE, phone);
	row->set_value(schema, C_SINCE, 0);
	row->set_value(schema, C_CREDIT_LIM, 50000);
	row->set_value(schema, C_DELIVERY_CNT, 0);
	char c_data[500];
	MakeAlphaString(300, 500, c_data, wid-1);
	row->set_value(schema, C_DATA, c_data);
#endif
	if (RAND(10, wid-1) == 0) {
	    char tmp[] = "GC";
	    row->set_value(schema, C_CREDIT, tmp);
	} else {
	    char tmp[] = "BC";
	    row->set_value(schema, C_CREDIT, tmp);
	}
	row->set_value(schema, C_DISCOUNT, (double)RAND(5000,wid-1) / 10000);
	row->set_value(schema, C_BALANCE, -10.0);
	row->set_value(schema, C_YTD_PAYMENT, 10.0);
	row->set_value(schema, C_PAYMENT_CNT, 1);

	transport->write((uint64_t)row, row_addr, sizeof(row_t), tid, pid);

	key = custNPKey(c_last, did, wid);
	i_customer_last->insert(key, row_addr, tid);

	key = custKey(cid, did, wid);
	i_customer_id->insert(key, row_addr, tid);
    }
}

void tpcc_workload_t::init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id, int tid) {
	/*
    Key key = custKey(c_id, d_id, w_id);
    //Key key = 0;
    int pid = key_to_part(key);
    uint64_t row_addr = rpc_alloc_row(pid, tid);
    auto row = mem->row_buffer_pool(tid, 0);
    uint64_t row_id;
    auto schema = t_history->get_schema();
    row->init(t_history, pid, row_id);
    row->set_primary_key(key);
    row->set_value(schema, H_C_ID, c_id);
    row->set_value(schema, H_C_D_ID, d_id);
    row->set_value(schema, H_D_ID, d_id);
    row->set_value(schema, H_C_W_ID, w_id);
    row->set_value(schema, H_W_ID, w_id);
    row->set_value(schema, H_DATE, 0);
    row->set_value(schema, H_AMOUNT, 10.0);
#if !TPCC_SMALL
    char h_data[24];
    MakeAlphaString(12, 24, h_data, w_id-1);
    row->set_value(schema, H_DATA, h_data);
#endif

    transport->write((uint64_t)row, row_addr, sizeof(row_t), tid, pid);
    */
}

void tpcc_workload_t::init_tab_order(uint64_t did, uint64_t wid, int tid) {
    catalog_t* schema;
    uint64_t perm[g_cust_per_dist];
    init_permutation(perm, wid); /* initialize permutation of customer numbers */
    for (uint32_t oid = 1; oid <= g_cust_per_dist; oid++) {
	int pid = key_to_part(orderPrimaryKey(wid, did, oid));
	//int pid = key_to_part(oid);
	uint64_t row_addr = rpc_alloc_row(pid, tid);
	auto row = mem->row_buffer_pool(tid, 0);
	uint64_t row_id;
	row->init(t_order, pid, row_id);
	schema = t_order->get_schema();
	row->set_primary_key(oid);
	uint64_t o_ol_cnt = 1;
	uint64_t cid = perm[oid - 1]; //get_permutation();
	row->set_value(schema, O_ID, oid);
	row->set_value(schema, O_C_ID, cid);
	row->set_value(schema, O_D_ID, did);
	row->set_value(schema, O_W_ID, wid);
	uint64_t o_entry = 2013;
	row->set_value(schema, O_ENTRY_D, o_entry);
	if (oid < 2101)
	    row->set_value(schema, O_CARRIER_ID, URand(1, 10, wid-1));
	else
	    row->set_value(schema, O_CARRIER_ID, 0);
	o_ol_cnt = URand(5, 15, wid-1);
	row->set_value(schema, O_OL_CNT, o_ol_cnt);
	row->set_value(schema, O_ALL_LOCAL, 1);

	transport->write((uint64_t)row, row_addr, sizeof(row_t), tid, pid);
	// ORDER-LINE
#if !TPCC_SMALL
	for (uint32_t ol = 1; ol <= o_ol_cnt; ol++) {
	    pid = key_to_part(orderlineKey(wid, did, ol));
	    //pid = key_to_part(ol);
	    row_addr = rpc_alloc_row(pid, tid);

	    schema = t_orderline->get_schema();
	    row = mem->row_buffer_pool(tid, 1);
	    row->init(t_orderline, pid, row_id);
	    row->set_value(schema, OL_O_ID, oid);
	    row->set_value(schema, OL_D_ID, did);
	    row->set_value(schema, OL_W_ID, wid);
	    row->set_value(schema, OL_NUMBER, ol);
	    row->set_value(schema, OL_I_ID, URand(1, 100000, wid-1));
	    row->set_value(schema, OL_SUPPLY_W_ID, wid);
	    if (oid < 2101) {
		row->set_value(schema, OL_DELIVERY_D, o_entry);
		row->set_value(schema, OL_AMOUNT, 0);
	    } else {
		row->set_value(schema, OL_DELIVERY_D, 0);
		row->set_value(schema, OL_AMOUNT, (double)URand(1, 999999, wid-1)/100);
	    }
	    row->set_value(schema, OL_QUANTITY, 5);
	    char ol_dist_info[24];
	    MakeAlphaString(24, 24, ol_dist_info, wid-1);
	    row->set_value(schema, OL_DIST_INFO, ol_dist_info);

	    transport->write((uint64_t)row, row_addr, sizeof(row_t), tid, pid);
	}
#endif
	// NEW ORDER
	if (oid > 2100) {
	    pid = key_to_part(orderPrimaryKey(wid, did, oid));
	    //pid = key_to_part(oid);
	    row_addr = rpc_alloc_row(pid, tid);

	    schema = t_neworder->get_schema();
	    row = mem->row_buffer_pool(tid, 2);
	    row->init(t_neworder, pid, row_id);
	    row->set_value(schema, NO_O_ID, oid);
	    row->set_value(schema, NO_D_ID, did);
	    row->set_value(schema, NO_W_ID, wid);

	    transport->write((uint64_t)row, row_addr, sizeof(row_t), tid, pid);
	}
    }
}

void tpcc_workload_t::init_permutation(uint64_t* perm_c_id, uint64_t wid) {
    uint32_t i;
    // Init with consecutive values
    for(i = 0; i < g_cust_per_dist; i++)
	perm_c_id[i] = i+1;

    // shuffle
    for(i=0; i < g_cust_per_dist-1; i++) {
	uint64_t j = URand(i+1, g_cust_per_dist-1, wid-1);
	uint64_t tmp = perm_c_id[i];
	perm_c_id[i] = perm_c_id[j];
	perm_c_id[j] = tmp;
    }
}

int tpcc_workload_t::key_to_part(Key key){
    return h(&key, sizeof(key), HASH_FUNC) % MR_PARTITION_NUM;
}
