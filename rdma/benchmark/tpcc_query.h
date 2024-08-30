#pragma once

#include "common/global.h"
#include "system/query.h"
#include "common/helper.h"

// items of new order transaction
struct item_no_t{
    uint64_t ol_i_id;
    uint64_t ol_supply_w_id;
    uint64_t ol_quantity;
};

class tpcc_query_t: public base_query_t{
    public:
	void init(int tid);

	tpcc_txn_type_t type;

	/**********************************************/
	// common txn input for both payment & new-order
	/**********************************************/
	uint64_t w_id;
	uint64_t d_id;
	uint64_t c_id;

	/**********************************************/
	// txn input for payment
	/**********************************************/
	uint64_t d_w_id;
	uint64_t c_w_id;
	uint64_t c_d_id;
	char c_last[LASTNAME_LEN];
	double h_amount;
	bool by_last_name;

	/**********************************************/
	// txn input for new-order
	/**********************************************/
	item_no_t* items;
	uint64_t rbk;
	bool remote;
	uint64_t ol_cnt;
	uint64_t o_entry_d;
	// Input for delivery
	uint64_t o_carrier_id;
	uint64_t ol_delivery_d;
	// for order-status


    private:
	// warehouse id to partition id mapping
	//      uint64_t wh_to_part(uint64_t wid);
	void gen_payment(int tid);
	void gen_neworder(int tid);
	void gen_orderstatus(int tid);
	void gen_delivery(int tid);
	void gen_stocklevel(int tid);
};

