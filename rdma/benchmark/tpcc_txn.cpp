#include "benchmark/tpcc.h"
#include "benchmark/tpcc_query.h"
#include "benchmark/tpcc_helper.h"
#include "benchmark/tpcc_const.h"
#include "system/query.h"
#include "system/workload.h"
#include "system/thread.h"
#include "storage/table.h"
#include "storage/row.h"
#include "index/tree.h"
#include "client/mr.h"
#include "client/transport.h"
#include "common/stat.h"

void tpcc_txn_man_t::init(thread_t* thd, workload_t* workload, int tid){
    txn_man_t::init(thd, workload, tid);
    this->workload = (tpcc_workload_t*)workload;
    #ifdef BREAKDOWN
    t_index = t_commit = t_abort = t_wait = t_backoff = t_total = 0;
    #endif
}

RC tpcc_txn_man_t::run_txn(base_query_t* query){
    auto m_query = (tpcc_query_t*) query;
    switch (m_query->type) {
	case TPCC_PAYMENT :
	    return run_payment(m_query); break;
	case TPCC_NEW_ORDER :
	    return run_neworder(m_query); break;
	case TPCC_ORDER_STATUS :
	    return run_orderstatus(m_query); break;
	case TPCC_DELIVERY :
	    return run_delivery(m_query); break;
	case TPCC_STOCK_LEVEL :
	    return run_stocklevel(m_query); break;
	default:
	    assert(false); return ABORT;
    }
}

RC tpcc_txn_man_t::run_payment(tpcc_query_t* query){
    // declare all variables
    RC rc = RCOK;
    int tid = thread->get_tid();
    auto m_wl = workload;
    uint64_t key;
    int cnt;
    int pid;
    bool exists = false;
    uint64_t row_addr;

    double tmp_value;
    char w_name[11];
    char * tmp_str;
    char d_name[11];
    double c_balance;
    double c_ytd_payment;
    double c_payment_cnt;
    char * c_credit;
    catalog_t* schema;
    set_ts(query->timestamp);

    #ifdef BREAKDOWN
    if(start == 0){ // tx begins
        start = asm_rdtsc();
    }
    else{
	assert(end == 0); // backoff end
	end = asm_rdtsc();
	t_backoff += (end - start);
	start = end;
    }

    #endif

#ifdef INTERACTIVE
    int base = 100;
    int delay = 100;
    int interactive_delay = (rand() % delay) + base;
    usleep(interactive_delay);
#endif
    uint64_t w_id = query->w_id;
    uint64_t c_w_id = query->c_w_id;
    /*====================================================+
      EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
      WHERE w_id=:w_id;
      +====================================================*/
    /*===================================================================+
      EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
	INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
	FROM warehouse
	WHERE w_id=:w_id;
    +===================================================================*/

    // TODO: for variable length variable (string). Should store the size of
    //  the variable.

    //BEGIN: [WAREHOUSE] RW
    //use index to retrieve that warehouse

    key = query->w_id;
    pid = m_wl->key_to_part(key);
    exists = m_wl->i_warehouse->search(key, row_addr, tid);
    if(!exists){
	debug::notify_error("Item for key %lu does not exist in the warehouse", key);
	exit(0);
    }

    #ifdef BREAKADOWN
    end = asm_rdtsc();
    t_index += (end - start);
    start = end;
    #endif
    row_t* r_wh;
    if(g_wh_update)
	r_wh = get_row(row_addr, tid, pid, WRITE);
    else
	r_wh = get_row(row_addr, tid, pid, READ);

    #ifdef BREAKDOWN
    #ifdef WAITDIE
    end = asm_rdtsc();
    t_wait += (end - start);
    start = end;
    #endif
    #endif
    if(r_wh == nullptr){
	rc = ABORT;
	#ifdef BREAKDOWN
	rc = finish(rc ,tid);
	end = asm_rdtsc();
	t_abort += (end - start);
	t_total += t_abort + t_index + t_commit + t_wait + t_backoff;
	ADD_STAT(tid, time_abort, t_total);
	t_abort = t_index = t_commit = t_wait = t_total = t_backoff = end = 0;
	start = asm_rdtsc(); // backoff begin
	return rc;
	#else
	return finish(rc, tid);
	#endif
    }
#ifdef INTERACTIVE
    usleep(interactive_delay);
#endif

    transport->read((uint64_t)r_wh, row_addr, ROW_SIZE, tid, pid);
    schema = m_wl->t_warehouse->get_schema();

    double w_ytd;
    r_wh->get_value(schema, W_YTD, &w_ytd);
    if(g_wh_update){
	r_wh->set_value(schema, W_YTD, w_ytd + query->h_amount);
	memcpy(write_buffer[row_cnt-1].data, r_wh, ROW_SIZE);
    }

    tmp_str = r_wh->get_value(schema, W_NAME);
    memcpy(w_name, tmp_str, 10);
    w_name[10] = '\0';

#ifdef INTERACTIVE
    usleep(interactive_delay);
#endif
    /*=====================================================+
      EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
      WHERE d_w_id=:w_id AND d_id=:d_id;
      +=====================================================*/
    key = distKey(query->d_id, query->d_w_id);
    pid = m_wl->key_to_part(key);

    #ifdef BREAKDOWN
    end = asm_rdtsc();
    t_commit += (end - start);
    start = end; 
    #endif
    exists = m_wl->i_district->search(key, row_addr, tid);
    if(!exists){
	debug::notify_error("Item for key %lu does not exist in the district", key);
	exit(0);
    }
    #ifdef BREAKDOWN
    end = asm_rdtsc();
    t_index += (end - start);
    start = end; 
    #endif

    row_t* r_dist = get_row(row_addr, tid, pid, WRITE);

    #ifdef BREAKDOWN
    #ifdef WAITDIE
    end = asm_rdtsc();
    t_wait += (end - start);
    start = end; 
    #endif
    #endif
    if(r_dist == nullptr){
	rc = ABORT;
	#ifdef BREAKDOWN
        rc = finish(rc ,tid);
        end = asm_rdtsc();
        t_abort += (end - start);
        t_total += t_abort + t_index + t_commit + t_wait + t_backoff;
        ADD_STAT(tid, time_abort, t_total);
        t_abort = t_index = t_commit = t_wait = t_total = t_backoff = end = 0;
	start = asm_rdtsc();
        return rc;
        #else
	return finish(rc, tid);
	#endif
    }

#ifdef INTERACTIVE
    usleep(interactive_delay);
#endif
    transport->read((uint64_t)r_dist, row_addr, ROW_SIZE, tid, pid);
    schema = m_wl->t_district->get_schema();


    double d_ytd;
    r_dist->get_value(schema, D_YTD, &d_ytd);
    r_dist->set_value(schema, D_YTD, d_ytd + query->h_amount);
    tmp_str = r_dist->get_value(schema, D_NAME);
    memcpy(d_name, tmp_str, 10);
    d_name[10] = '\0';
    memcpy(write_buffer[row_cnt-1].data, r_dist, ROW_SIZE);

    /*====================================================================+
      EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
      INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
      FROM district
      WHERE d_w_id=:w_id AND d_id=:d_id;
    +====================================================================*/

    #ifdef BREAKDOWN
    end = asm_rdtsc();
    t_commit += (end - start);
    start = end;
    #endif

#ifdef INTERACTIVE
     usleep(interactive_delay);
#endif

    if (query->by_last_name) {
	/*==========================================================+
	  EXEC SQL SELECT count(c_id) INTO :namecnt
	  FROM customer
	  WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;
	  +==========================================================*/
	/*==========================================================================+
	  EXEC SQL DECLARE c_byname CURSOR FOR
	  SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state,
	  c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since
	  FROM customer
	  WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
	  ORDER BY c_first;
	  EXEC SQL OPEN c_byname;
	  +===========================================================================*/

	key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
	// XXX: the list is not sorted. But let's assume it's sorted...
	// The performance won't be much different.
	exists = m_wl->i_customer_last->search(key, row_addr, tid);
	if(!exists){
	    debug::notify_error("Item for key %lu does not exist in customer (last name)", key);
	    exit(0);
	}
    }
    else { // search customers by cust_id
	/*=====================================================================+
	  EXEC SQL SELECT c_first, c_middle, c_last, c_street_1, c_street_2,
	  c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim,
	  c_discount, c_balance, c_since
	  INTO :c_first, :c_middle, :c_last, :c_street_1, :c_street_2,
	  :c_city, :c_state, :c_zip, :c_phone, :c_credit, :c_credit_lim,
	  :c_discount, :c_balance, :c_since
	  FROM customer
	  WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
	+======================================================================*/
	key = custKey(query->c_id, query->c_d_id, query->c_w_id);
	exists = m_wl->i_customer_id->search(key, row_addr, tid);
	if(!exists){
	    debug::notify_error("Item for key %lu does not exist in customer (id)", key);
	    exit(0);
	}
    }

    #ifdef BREAKDOWN
    end = asm_rdtsc();
    t_index += (end - start);
    start = end;
    #endif
    //assert(distKey(query->c_d_id, query->c_w_id) == distKey(query->d_id, query->w_id));
    //pid = m_wl->key_to_part(query->c_w_id);
    pid = m_wl->key_to_part(distKey(query->c_d_id, query->c_w_id));
    //pid = m_wl->key_to_part(query->c_d_id);
    /*======================================================================+
      EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
      WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
      +======================================================================*/
    row_t* r_cust = get_row(row_addr, tid, pid, WRITE);

    #ifdef BREAKDOWN
    #ifdef WAITDIE
    end = asm_rdtsc();
    t_wait += (end - start);
    start = end;
    #endif
    #endif
    if (r_cust == nullptr) {
	rc = ABORT;
	#ifdef BREAKDOWN
        rc = finish(rc ,tid);
        end = asm_rdtsc();
        t_abort += (end - start);
        t_total += t_abort + t_index + t_commit + t_wait + t_backoff;
        ADD_STAT(tid, time_abort, t_total);
        t_abort = t_index = t_commit = t_wait = t_total = t_backoff = end = 0;
	start = asm_rdtsc(); // backoff start
        return rc;
        #else
	return finish(rc, tid);
	#endif
    }

#ifdef INTERACTIVE
    usleep(interactive_delay);
#endif

    transport->read((uint64_t)r_cust, row_addr, ROW_SIZE, tid, pid);
    schema = m_wl->t_customer->get_schema();

    r_cust->get_value(schema, C_BALANCE, &c_balance);
    r_cust->set_value(schema, C_BALANCE, c_balance - query->h_amount);
    r_cust->get_value(schema, C_YTD_PAYMENT, &c_ytd_payment);
    r_cust->set_value(schema, C_YTD_PAYMENT, c_ytd_payment + query->h_amount);
    r_cust->get_value(schema, C_PAYMENT_CNT, &c_payment_cnt);
    r_cust->set_value(schema, C_PAYMENT_CNT, c_payment_cnt + 1);

    c_credit = r_cust->get_value(schema, C_CREDIT);
    if ( strstr(c_credit, "BC") && !TPCC_SMALL ) {
	/*=====================================================+
	  EXEC SQL SELECT c_data
	  INTO :c_data
	  FROM customer
	  WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
	+=====================================================*/
	char c_new_data[501];
	sprintf(c_new_data,"| %zu %zu %zu %zu %zu $%7.2f %d",
		query->c_id, query->c_d_id, c_w_id, query->d_id, w_id, query->h_amount,'\0');
	//char * c_data = r_cust->get_value("C_DATA");
	//need to fix this line
	//strncat(c_new_data, c_data, 500 - strlen(c_new_data));
	//c_new_data[500]='\0';
	r_cust->set_value(schema, "C_DATA", c_new_data);
    }
    memcpy(write_buffer[row_cnt-1].data, r_cust, ROW_SIZE);

    //update h_data according to spec
    char h_data[25];
    strncpy(h_data, w_name, 10);
    int length = strlen(h_data);
    if (length > 10) length = 10;
    strcpy(&h_data[length], "    ");
    strncpy(&h_data[length + 4], d_name, 10);
    h_data[length+14] = '\0';
    /*=============================================================================+
      EXEC SQL INSERT INTO
      history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
      VALUES (:c_d_id, :c_w_id, :c_id, :d_id, :w_id, :datetime, :h_amount, :h_data);
      +=============================================================================*/
    //not causing the buffer overflow
    /*
    pid = m_wl->key_to_part(0);
    row_addr = m_wl->rpc_alloc_row(pid, tid);
    auto r_hist = get_new_row(row_addr, tid, pid);
    uint64_t row_id;
    r_hist->init(m_wl->t_history, pid, row_id);
    schema = m_wl->t_history->get_schema();
    r_hist->set_value(schema, H_C_ID, query->c_id);
    r_hist->set_value(schema, H_C_D_ID, query->c_d_id);
    r_hist->set_value(schema, H_C_W_ID, c_w_id);
    r_hist->set_value(schema, H_D_ID, query->d_id);
    r_hist->set_value(schema, H_W_ID, w_id);
    int64_t date = 2013;
    r_hist->set_value(schema, H_DATE, date);
    r_hist->set_value(schema, H_AMOUNT, query->h_amount);
#if !TPCC_SMALL
    r_hist->set_value(schema, H_DATA, h_data);
#endif
    memcpy(write_buffer[row_cnt-1].data, r_hist, ROW_SIZE);
    */
    // XXX(zhihan): no index maintained for history table.
    //END: [HISTORY] - WR
    assert( rc == RCOK );
    #ifdef BREAKDOWN
    rc = finish(rc ,tid);
    end = asm_rdtsc();
    t_commit += (end - start);
    ADD_STAT(tid, time_commit, t_commit);
    ADD_STAT(tid, time_index, t_index);
    ADD_STAT(tid, time_wait, t_wait);
    ADD_STAT(tid, time_backoff, t_backoff);
    t_abort = t_index = t_commit = t_wait = t_total = t_backoff = start = end = 0;
    return rc;
    #else
    return finish(rc, tid);
    #endif
}

RC tpcc_txn_man_t::run_neworder(tpcc_query_t* query){
    RC rc = RCOK;
    uint64_t key;
    int pid;
    int tid = thread->get_tid();
    bool remote = query->remote;
    bool exists = false;
    uint64_t row_addr;
    uint64_t w_id = query->w_id;
    uint64_t d_id = query->d_id;
    uint64_t c_id = query->c_id;
    uint64_t ol_cnt = query->ol_cnt;
    auto m_wl = workload;
    catalog_t* schema;

    set_ts(query->timestamp);
    #ifdef BREAKDOWN
    if(start == 0){ // tx begins
        start = asm_rdtsc();
    }
    else{
        assert(end == 0); // backoff end
        end = asm_rdtsc();
        t_backoff += (end - start);
        start = end;
    }
    #endif

#ifdef INTERACTIVE
    int base = 100;
    int delay = 100;
    int interactive_delay = (rand() % delay) + base;
    usleep(interactive_delay);
#endif

    /*=======================================================================+
      EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
      INTO :c_discount, :c_last, :c_credit, :w_tax
      FROM customer, warehouse
      WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
    +========================================================================*/
    key = w_id;
    pid = m_wl->key_to_part(key);
    exists = m_wl->i_warehouse->search(key, row_addr, tid);
    if(!exists){
	debug::notify_error("Item for key %lu does not exist in the warehouse", key);
	exit(0);
    }
    #ifdef BREAKDOWN
    end = asm_rdtsc();
    t_index += (end - start);
    start = end;
    #endif

    auto r_wh = get_row(row_addr, tid, pid, READ);
    #ifdef BREAKDOWN
    #ifdef WAITDIE
    end = asm_rdtsc();
    t_wait += (end - start);
    start = end;
    #endif
    #endif
    if(r_wh == nullptr){
	rc = ABORT;
	#ifdef BREAKDOWN
        rc = finish(rc ,tid);
        end = asm_rdtsc();
        t_abort += (end - start);
        t_total += t_abort + t_index + t_commit + t_wait + t_backoff;
        ADD_STAT(tid, time_abort, t_total);
        t_abort = t_index = t_commit = t_wait = t_total = t_backoff = end  = 0;
	start = asm_rdtsc(); // backoff start
        return rc;
        #else
	return finish(rc, tid);
	#endif
    }

    transport->read((uint64_t)r_wh, row_addr, ROW_SIZE, tid, pid);
    schema = m_wl->t_warehouse->get_schema();
#ifdef INTERACTIVE
    usleep(interactive_delay);
#endif

    //retrieve the tax of warehouse
    double w_tax;
    r_wh->get_value(schema, W_TAX, &w_tax);

#ifdef INTERACTIVE
    usleep(interactive_delay);
#endif
    #ifdef BREAKDOWN
    end = asm_rdtsc();
    t_commit += (end - start);
    start = end;
    #endif
    /*==================================================+
      EXEC SQL SELECT d_next_o_id, d_tax
      INTO :d_next_o_id, :d_tax
      FROM district WHERE d_id = :d_id AND d_w_id = :w_id;
      EXEC SQL UPDATE d istrict SET d _next_o_id = :d _next_o_id + 1
      WH ERE d _id = :d_id AN D d _w _id = :w _id ;
    +===================================================*/
    key = distKey(d_id, w_id);
    pid = m_wl->key_to_part(key);
    exists = m_wl->i_district->search(key, row_addr, tid);
    if(!exists){
	debug::notify_error("Item for key %lu does not exist in the district", key);
	exit(0);
    }
    #ifdef BREAKDOWN
    end = asm_rdtsc();
    t_index += (end - start);
    start = end;
    #endif

    auto r_dist = get_row(row_addr, tid, pid, WRITE);

    #ifdef BREAKDOWN
    #ifdef WAITDIE
    end = asm_rdtsc();
    t_wait += (end - start);
    start = end;
    #endif
    #endif
    if(r_dist == nullptr){
	rc = ABORT;
	#ifdef BREAKDOWN
        rc = finish(rc ,tid);
        end = asm_rdtsc();
        t_abort += (end - start);
        t_total += t_abort + t_index + t_commit + t_wait + t_backoff;
        ADD_STAT(tid, time_abort, t_total);
        t_abort = t_index = t_commit = t_wait = t_total = t_backoff = end = 0;
	start = asm_rdtsc(); // backoff start
        return rc;
        #else
	return finish(rc, tid);
	#endif
    }

    transport->read((uint64_t)r_dist, row_addr, ROW_SIZE, tid, pid);
    schema = m_wl->t_district->get_schema();
#ifdef INTERACTIVE
    usleep(interactive_delay);
#endif
    int64_t o_id;
    //d_tax = *(double *) r_dist_local->get_value(D_TAX);
    o_id = *(int64_t *)r_dist->get_value(schema, D_NEXT_O_ID);
    o_id++;
    r_dist->set_value(schema, D_NEXT_O_ID, o_id);
    memcpy(write_buffer[row_cnt-1].data, r_dist, ROW_SIZE);

    #ifdef BREAKDOWN
    end = asm_rdtsc();
    t_commit += (end - start);
    start = end;
    #endif
#ifdef INTERACTIVE
    usleep(interactive_delay);
#endif

    //select customer
    key = custKey(c_id, d_id, w_id);
    //pid = m_wl->key_to_part(d_id);
    pid = m_wl->key_to_part(distKey(d_id, w_id));
    //pid = m_wl->key_to_part(w_id);
    exists = m_wl->i_customer_id->search(key, row_addr, tid);
    if(!exists){
	debug::notify_error("Item for key %lu does not exist in the customer (id)", key);
	exit(0);
    }

    #ifdef BREAKDOWN
    end = asm_rdtsc();
    t_index += (end - start);
    start = end;
    #endif
    auto r_cust = get_row(row_addr, tid, pid, READ);
    #ifdef BREAKDOWN
    #ifdef WAIT
    end = asm_rdtsc();
    t_wait += (end - start);
    start = end;
    #endif
    #endif

    if(r_cust == nullptr){
	rc = ABORT;
	#ifdef BREAKDOWN
        rc = finish(rc ,tid);
        end = asm_rdtsc();
        t_abort += (end - start);
        t_total += t_abort + t_index + t_commit + t_wait + t_backoff;
        ADD_STAT(tid, time_abort, t_total);
        t_abort = t_index = t_commit = t_wait = t_total = t_backoff = end = 0;
	start = asm_rdtsc(); // backoff start
        return rc;
        #else
	return finish(rc, tid);
	#endif
    }
    transport->read((uint64_t)r_cust, row_addr, ROW_SIZE, tid, pid);
    schema = m_wl->t_customer->get_schema();

#ifdef INTERACTIVE
    usleep(interactive_delay);
#endif

    //retrieve data
    uint64_t c_discount;
#if !TPCC_SMALL
    r_cust->get_value(schema, C_LAST);
    r_cust->get_value(schema, C_CREDIT);
#endif
    r_cust->get_value(schema, C_DISCOUNT, &c_discount);

    /*=======================================================+
      EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
      VALUES (:o_id, :d_id, :w_id);
    +=======================================================*/
    /*
    pid = 0;
    row_addr = m_wl->rpc_alloc_row(pid, tid);
    auto r_no = get_new_row(row_addr, tid, pid);
    r_no->init(t_neworder, pid, row_id);
    r_no->set_value(NO_O_ID, o_id);
    r_no->set_value(NO_D_ID, d_id);
    r_no->set_value(NO_W_ID, w_id);
    //insert_row(r_no, _wl->t_neworder);
     */

    /*========================================================================================+
      EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
      VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
    +========================================================================================*/
    /*
    pid = 0;
    row_addr = m_wl->rpc_alloc_row(pid, tid);
    auto r_order = get_new_row(row_addr, tid, pid);
    r_order->init(t_order, pid, row_id);
    r_order->set_value(O_ID, o_id);
    r_order->set_value(O_C_ID, c_id);
    r_order->set_value(O_D_ID, d_id);
    r_order->set_value(O_W_ID, w_id);
    r_order->set_value(O_ENTRY_D, query->o_entry_d);
    r_order->set_value(O_OL_CNT, ol_cnt);
    //o_d_id=*(int64_t *) r_order->get_value(O_D_ID);
    o_d_id=d_id;
    all_local = (remote? 0 : 1);
    r_order->set_value(O_ALL_LOCAL, all_local);
    //insert_row(r_order, _wl->t_order);
    //may need to set o_ol_cnt=ol_cnt;
     */
    //o_d_id=d_id;

    for(uint32_t ol_number=0; ol_number<ol_cnt; ol_number++){
	uint64_t ol_i_id = query->items[ol_number].ol_i_id;
	uint64_t ol_supply_w_id = query->items[ol_number].ol_supply_w_id;
	uint64_t ol_quantity = query->items[ol_number].ol_quantity;
	/*===========================================+
	  EXEC SQL SELECT i_price, i_name , i_data
	  	INTO :i_price, :i_name, :i_data
		FROM item
		WHERE i_id = :ol_i_id;
	+===========================================*/
	#ifdef BREAKDOWN
	end = asm_rdtsc();
	t_commit += (end - start);
	start = end;
	#endif
#ifdef INTERACTIVE
	usleep(interactive_delay);
#endif
	key = ol_i_id;
	pid = m_wl->key_to_part(key);
	exists = m_wl->i_item->search(key, row_addr, tid);
	if(!exists){
	    debug::notify_error("Item for key %lu does not exist in the item", key);
	    exit(0);
	}

	#ifdef BREAKDOWN
	end = asm_rdtsc();
	t_index += (end - start);
	start = end;
	#endif
	auto r_item = get_row(row_addr, tid, pid, READ);
	#ifdef BREAKDOWN
	#ifdef WAITDIE
	end = asm_rdtsc();
	t_wait += (end - start);
	start = end;
	#endif
	#endif
	if(r_item == nullptr){
	    rc = ABORT;
	    #ifdef BREAKDOWN
	    rc = finish(rc ,tid);
	    end = asm_rdtsc();
	    t_abort += (end - start);
	    t_total += t_abort + t_index + t_commit + t_wait + t_backoff;
	    ADD_STAT(tid, time_abort, t_total);
	    t_abort = t_index = t_commit = t_wait = t_total = t_backoff = end = 0;
	    start = asm_rdtsc(); // backoff start
	    return rc;
            #else
	    return finish(rc, tid);
	    #endif
	}
#ifdef INTERACTIVE
        usleep(interactive_delay);
#endif
	transport->read((uint64_t)r_item, row_addr, ROW_SIZE, tid, pid);
	schema = m_wl->t_item->get_schema();

	int64_t i_price;
	r_item->get_value(schema, I_PRICE, &i_price);
	r_item->get_value(schema, I_NAME);
	r_item->get_value(schema, I_DATA);

	#ifdef BREAKDOWN
	end = asm_rdtsc();
	t_commit += (end - start);
	start = end;
	#endif
#ifdef INTERACTIVE
        usleep(interactive_delay);
#endif
	/*===================================================================+
	  EXEC SQL SELECT s_quantity, s_data,
	  	s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
	  	s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
	  INTO :s_quantity, :s_data,
		:s_dist_01, :s_dist_02, :s_dist_03, :s_dist_04, :s_dist_05,
		:s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
	  FROM stock
	  WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;

	  EXEC SQL UPDATE stock SET s_quantity = :s_quantity
	  WHERE s_i_id = :ol_i_id
	  AND s_w_id = :ol_supply_w_id;
	+===============================================*/
	key = stockKey(ol_i_id, ol_supply_w_id);
	pid = m_wl->key_to_part(key);
	exists = m_wl->i_stock->search(key, row_addr, tid);
	if(!exists){
	    debug::notify_error("Item for key %lu does not exist in the stock", key);
	    exit(0);
	}
	
	#ifdef BREAKDOWN
	end = asm_rdtsc();
	t_index += (end - start);
	start = end;
        #endif
	auto r_stock = get_row(row_addr, tid, pid, WRITE);
	#ifdef BREAKDOWN
        #ifdef WAITDIE
	end = asm_rdtsc();
	t_wait += (end - start);
	start = end;
        #endif
        #endif
	if(r_stock == nullptr){
	    rc = ABORT;
	    #ifdef BREAKDOWN
            rc = finish(rc ,tid);
            end = asm_rdtsc();
            t_abort += (end - start);
            t_total += t_abort + t_index + t_commit + t_wait + t_backoff;
            ADD_STAT(tid, time_abort, t_total);
            t_abort = t_index = t_commit = t_wait = t_total = t_backoff = end = 0;
	    start = asm_rdtsc(); // backoff start
            return rc;
            #else
	    return finish(rc, tid);
	    #endif
	}
#ifdef INTERACTIVE
        usleep(interactive_delay);
#endif
	transport->read((uint64_t)r_stock, row_addr, ROW_SIZE, tid, pid);
	schema = m_wl->t_stock->get_schema();

	// XXX s_dist_xx are not retrieved.
	int64_t s_remote_cnt;
	int64_t s_quantity = *(int64_t*)r_stock->get_value(schema, S_QUANTITY);
#if !TPCC_SMALL
	/*
        auto s_dist_01=(char *)r_stock->get_value(S_DIST_01);
        auto s_dist_02=(char *)r_stock->get_value(S_DIST_02);
        auto s_dist_03=(char *)r_stock->get_value(S_DIST_03);
        auto s_dist_04=(char *)r_stock->get_value(S_DIST_04);
        auto s_dist_05=(char *)r_stock->get_value(S_DIST_05);
        auto s_dist_06=(char *)r_stock->get_value(S_DIST_06);
        auto s_dist_07=(char *)r_stock->get_value(S_DIST_07);
        auto s_dist_08=(char *)r_stock->get_value(S_DIST_08);
        auto s_dist_09=(char *)r_stock->get_value(S_DIST_09);
        auto s_dist_10=(char *)r_stock->get_value(S_DIST_10);
        */
	int64_t s_ytd;
	int64_t s_order_cnt;
	//char * s_data = "test";
	r_stock->get_value(schema, S_YTD, &s_ytd);
	r_stock->set_value(schema, S_YTD, s_ytd + ol_quantity);
	r_stock->get_value(schema, S_ORDER_CNT, &s_order_cnt);
	r_stock->set_value(schema, S_ORDER_CNT, s_order_cnt + 1);
	//s_data = r_stock->get_value(S_DATA);
#endif
	if(remote){
	    s_remote_cnt = *(int64_t*)r_stock->get_value(schema, S_REMOTE_CNT);
	    s_remote_cnt++;
	    r_stock->set_value(schema, S_REMOTE_CNT, &s_remote_cnt);
	}

	uint64_t quantity;
	if(s_quantity > ol_quantity + 10){
	    quantity = s_quantity - ol_quantity;
	} 
	else{
	    quantity = s_quantity - ol_quantity + 91;
	}

	r_stock->set_value(schema, S_QUANTITY, &quantity);
	memcpy(write_buffer[row_cnt-1].data, r_stock, ROW_SIZE);

	/*====================================================+
	  EXEC SQL INSERT
	  	INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number,
	  	ol_i_id, ol_supply_w_id,
	  	ol_quantity, ol_amount, ol_dist_info)
	  	VALUES(:o_id, :d_id, :w_id, :ol_number,
	  	:ol_i_id, :ol_supply_w_id,
	  	:ol_quantity, :ol_amount, :ol_dist_info);
	+====================================================*/
	// XXX district info is not inserted.
	/*
        pid = 0;
	row_addr = m_wl->rpc_alloc_row(pid, tid);
	auto r_ol = get_new_row(row_addr, tid, pid);
	r_ol->init(t_order, pid, row_id);
	r_ol->set_value(OL_O_ID, &o_id);
	r_ol->set_value(OL_D_ID, &d_id);
	r_ol->set_value(OL_W_ID, &w_id);
	r_ol->set_value(OL_NUMBER, &ol_number);
	r_ol->set_value(OL_I_ID, &ol_i_id);
	//deal with district
#if !TPCC_SMALL
	if(o_d_id==1){
	    r_ol->set_value(OL_DIST_INFO, &s_dist_01);
	}
	else if(o_d_id==2){
	    r_ol->set_value(OL_DIST_INFO, &s_dist_02);
	}
	else if(o_d_id==3){
	    r_ol->set_value(OL_DIST_INFO, &s_dist_03);
	}
	else if(o_d_id==4){
	    r_ol->set_value(OL_DIST_INFO, &s_dist_04);
	}
	else if(o_d_id==5){
	    r_ol->set_value(OL_DIST_INFO, &s_dist_05);
	}
	else if(o_d_id==6){
	    r_ol->set_value(OL_DIST_INFO, &s_dist_06);
	}
	else if(o_d_id==7){
	    r_ol->set_value(OL_DIST_INFO, &s_dist_07);
	}
	else if(o_d_id==8){
	    r_ol->set_value(OL_DIST_INFO, &s_dist_08);
	}
	else if(o_d_id==9){
	    r_ol->set_value(OL_DIST_INFO, &s_dist_09);
	}
	else if(o_d_id==10){
	    r_ol->set_value(OL_DIST_INFO, &s_dist_10);
	}
	ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);
	r_ol->set_value(OL_SUPPLY_W_ID, &ol_supply_w_id);
	r_ol->set_value(OL_QUANTITY, &ol_quantity);
	r_ol->set_value(OL_AMOUNT, &ol_amount);
#endif
	 */
        //insert_row(r_ol, _wl->t_orderline);
    }

    assert(rc == RCOK);
    #ifdef BREAKDOWN
    rc = finish(rc, tid);
    end = asm_rdtsc();
    t_commit += (end - start);
    ADD_STAT(tid, time_commit, t_commit);
    ADD_STAT(tid, time_index, t_index);
    ADD_STAT(tid, time_wait, t_wait);
    ADD_STAT(tid, time_backoff, t_backoff);
    assert(t_abort == 0);
    t_commit = t_index = t_wait = t_abort = t_total = t_backoff = start = end = 0;
    return rc;
    #else
    return finish(rc, tid);
    #endif
}

RC tpcc_txn_man_t::run_orderstatus(tpcc_query_t* query){
    return RCOK;
}

RC tpcc_txn_man_t::run_delivery(tpcc_query_t* query){
    return RCOK;
}

RC tpcc_txn_man_t::run_stocklevel(tpcc_query_t* query){
    return RCOK;
}
