#include "storage/catalog.h"
#include "storage/row.h"
#include "storage/table.h"

#include "benchmark/tpcc_helper.h"
#include "benchmark/tpcc_const.h"

#include "client/worker.h"
#include "client/tpcc.h"
#include "client/tpcc_query.h"
#include "client/query.h"
#include "client/thread.h"
#include "client/txn.h"
#include "client/mr.h"
#include "client/transport.h"

#include "common/rpc.h"
#include "common/stat.h"

void tpcc_txn_man_t::init(thread_t* thd, worker_t* worker, int tid){
    txn_man_t::init(thd, worker, tid);
    this->worker = (tpcc_worker_t*)worker;
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
	    debug::notify_info("unsupported workload type %d", m_query->type);
            assert(false); return ABORT;
    }
}

RC tpcc_txn_man_t::run_payment(tpcc_query_t* query){
#if defined BATCH || defined BATCH2
    return RCOK;
#else
    // declare all variables
    RC rc = RCOK;
    int tid = thread->get_tid();
    auto m_wl = worker;
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
    int step = 0;
    write_num = 0;

    uint64_t timestamp = query->timestamp;
    auto send_ptr = mem->rpc_request_buffer_pool(tid);
    rpc_request_t<Key>* request;
    rpc_response_t* response;

    size_t request_size = sizeof(base_request_t) + sizeof(int)*2 + sizeof(Key);
    size_t response_size = sizeof(rpc_response_t);

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
    if(g_wh_update)
	request = create_message<rpc_request_t<Key>>((void*)send_ptr, tid, WRITE, TPCC_WAREHOUSE, pid, key, timestamp);
    else
	request = create_message<rpc_request_t<Key>>((void*)send_ptr, tid, READ, TPCC_WAREHOUSE, pid, key, timestamp);
    transport->send((uint64_t)request, request_size, tid);

    auto recv_ptr = mem->rpc_response_buffer_pool(tid, step);
    response = create_message<rpc_response_t>((void*)recv_ptr);
    transport->recv((uint64_t)response, response_size, tid);

    if(response->type == ABORT){
	write_num = 0;
	return ABORT;
    }
    else if(response->type == ERROR){
	debug::notify_error("tid %d -- TX %d \t ERROR for warehouse access", tid, step);
	exit(0);
    }

#ifdef INTERACTIVE
    usleep(interactive_delay);
#endif
    double w_ytd;
    schema = m_wl->t_warehouse->get_schema();
    auto r_wh = (row_t*)response->data;
    r_wh->get_value(schema, W_YTD, &w_ytd);
    if(g_wh_update){
        r_wh->set_value(schema, W_YTD, w_ytd + query->h_amount);
	write_buf[write_num] = step;
	write_num++;
    }
    step++;

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
    request = create_message<rpc_request_t<Key>>((void*)send_ptr, tid, WRITE, TPCC_DISTRICT, pid, key, timestamp);
    transport->send((uint64_t)request, request_size, tid);

    recv_ptr = mem->rpc_response_buffer_pool(tid, step);
    response = create_message<rpc_response_t>((void*)recv_ptr);
    transport->recv((uint64_t)response, response_size, tid);

    if(response->type == ABORT){
        write_num = 0;
        return ABORT;
    }
    else if(response->type == ERROR){
        debug::notify_error("tid %d -- TX %d \t ERROR for district access", tid, step);
        exit(0);
    }

#ifdef INTERACTIVE
    usleep(interactive_delay);
#endif
    double d_ytd;
    schema = m_wl->t_district->get_schema();
    auto r_dist = (row_t*)response->data;
    r_dist->get_value(schema, D_YTD, &d_ytd);
    r_dist->set_value(schema, D_YTD, d_ytd + query->h_amount);
    tmp_str = r_dist->get_value(schema, D_NAME);
    memcpy(d_name, tmp_str, 10);
    d_name[10] = '\0';
    write_buf[write_num] = step;
    write_num++;
    step++;

    /*====================================================================+
      EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
      INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
      FROM district
      WHERE d_w_id=:w_id AND d_id=:d_id;
    +====================================================================*/

#ifdef INTERACTIVE
     usleep(interactive_delay);
#endif

    pid = m_wl->key_to_part(distKey(query->c_d_id, query->c_w_id));
    //pid = m_wl->key_to_part(query->c_d_id);
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
	request = create_message<rpc_request_t<Key>>((void*)send_ptr, tid, WRITE, TPCC_CUSTOMER_LASTNAME, pid, key, timestamp);
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
	request = create_message<rpc_request_t<Key>>((void*)send_ptr, tid, WRITE, TPCC_CUSTOMER_ID, pid, key, timestamp);
    }
    transport->send((uint64_t)request, request_size, tid);

    recv_ptr = mem->rpc_response_buffer_pool(tid, step);
    response = create_message<rpc_response_t>((void*)recv_ptr);
    transport->recv((uint64_t)response, response_size, tid);

    if(response->type == ABORT){
        write_num = 0;
        return ABORT;
    }
    else if(response->type == ERROR){
        debug::notify_error("tid %d -- TX %d \t ERROR for customer access", tid, step);
        exit(0);
    }

#ifdef INTERACTIVE
    usleep(interactive_delay);
#endif
    /*======================================================================+
      EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
      WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
      +======================================================================*/
    auto r_cust = (row_t*)response->data;
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
    write_buf[write_num] = step;
    write_num++;
    step++;


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
    memcpy(write_buffer[row_cnt-1].data, r_hist, sizeof(row_t));
    // XXX(zhihan): no index maintained for history table.
    //END: [HISTORY] - WR
    */

    request = create_message<rpc_request_t<Key>>((void*)send_ptr, tid, COMMIT_DATA);
    request->num = write_num;
    for(int i=0; i<write_num; i++){
	memcpy(&request->data[sizeof(row_t)*i], mem->rpc_response_buffer_pool(tid, write_buf[i])->data, sizeof(row_t));
    }
    size_t commit_size = request_size + sizeof(row_t) * write_num;
    transport->send((uint64_t)request, commit_size, tid);

    recv_ptr = mem->rpc_response_buffer_pool(tid, 0);
    response = create_message<rpc_response_t>((void*)recv_ptr);
    transport->recv((uint64_t)response, response_size, tid);
    rc = response->type;
    write_num = 0;

    return rc;
#endif
}

RC tpcc_txn_man_t::run_neworder(tpcc_query_t* query){
#if defined BATCH || defined BATCH2
    return RCOK;
#else
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
    auto m_wl = worker;
    catalog_t* schema;

    int step = 0;
    write_num = 0;

    uint64_t timestamp = query->timestamp;
    auto send_ptr = mem->rpc_request_buffer_pool(tid);
    rpc_request_t<Key>* request;
    rpc_response_t* response;


    size_t request_size = sizeof(base_request_t) + sizeof(int)*2 + sizeof(Key);
    size_t response_size = sizeof(rpc_response_t);

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
    request = create_message<rpc_request_t<Key>>((void*)send_ptr, tid, READ, TPCC_WAREHOUSE, pid, key, timestamp);
    transport->send((uint64_t)request, request_size, tid);

    auto recv_ptr = mem->rpc_response_buffer_pool(tid, step);
    response = create_message<rpc_response_t>((void*)recv_ptr);
    transport->recv((uint64_t)response, response_size, tid);

    if(response->type == ABORT){
	write_num = 0;
	return ABORT;
    }
    else if(response->type == ERROR){
        debug::notify_error("tid %d -- TX %d \t ERROR for warehouse access", tid, step);
        exit(0);
    }

#ifdef INTERACTIVE
    usleep(interactive_delay);
#endif
    schema = m_wl->t_warehouse->get_schema();
    auto r_wh = (row_t*)response->data;

    //retrieve the tax of warehouse
    double w_tax;
    r_wh->get_value(schema, W_TAX, &w_tax);
    step++;

#ifdef INTERACTIVE
    usleep(interactive_delay);
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
    request = create_message<rpc_request_t<Key>>((void*)send_ptr, tid, WRITE, TPCC_DISTRICT, pid, key, timestamp);
    transport->send((uint64_t)request, request_size, tid);

    recv_ptr = mem->rpc_response_buffer_pool(tid, step);
    response = create_message<rpc_response_t>((void*)recv_ptr);
    transport->recv((uint64_t)response, response_size, tid);

    if(response->type == ABORT){
        write_num = 0;
        return ABORT;
    }
    else if(response->type == ERROR){
        debug::notify_error("tid %d -- TX %d \t ERROR for district access", tid, step);
        exit(0);
    }

#ifdef INTERACTIVE
    usleep(interactive_delay);
#endif
    schema = m_wl->t_district->get_schema();
    auto r_dist = (row_t*)response->data;

    int64_t o_id;
    //d_tax = *(double *) r_dist_local->get_value(D_TAX);
    o_id = *(int64_t *)r_dist->get_value(schema, D_NEXT_O_ID);
    o_id++;
    r_dist->set_value(schema, D_NEXT_O_ID, o_id);

    write_buf[write_num] = step;
    write_num++;
    step++;

#ifdef INTERACTIVE
    usleep(interactive_delay);
#endif
    //select customer
    key = custKey(c_id, d_id, w_id);
    pid = m_wl->key_to_part(distKey(d_id, w_id));
    //pid = m_wl->key_to_part(d_id);

    request = create_message<rpc_request_t<Key>>((void*)send_ptr, tid, READ, TPCC_CUSTOMER_ID, pid, key, timestamp);
    transport->send((uint64_t)request, request_size, tid);

    recv_ptr = mem->rpc_response_buffer_pool(tid, step);
    response = create_message<rpc_response_t>((void*)recv_ptr);
    transport->recv((uint64_t)response, response_size, tid);

    if(response->type == ABORT){
        write_num = 0;
        return ABORT;
    }
    else if(response->type == ERROR){
        debug::notify_error("tid %d -- TX %d \t ERROR for customer access (id)", tid, step);
        exit(0);
    }

#ifdef INTERACTIVE
    usleep(interactive_delay);
#endif
    schema = m_wl->t_customer->get_schema();
    auto r_cust = (row_t*)response->data;

    //retrieve data
    uint64_t c_discount;
#if !TPCC_SMALL
    r_cust->get_value(schema, C_LAST);
    r_cust->get_value(schema, C_CREDIT);
#endif
    r_cust->get_value(schema, C_DISCOUNT, &c_discount);
    step++;

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
      EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt,           o_all_local)
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
#ifdef INTERACTIVE
	usleep(interactive_delay);
#endif

        key = ol_i_id;
        pid = m_wl->key_to_part(key);

	request = create_message<rpc_request_t<Key>>((void*)send_ptr, tid, READ, TPCC_ITEM, pid, key, timestamp);
	transport->send((uint64_t)request, request_size, tid);

	recv_ptr = mem->rpc_response_buffer_pool(tid, step);
	response = create_message<rpc_response_t>((void*)recv_ptr);
	transport->recv((uint64_t)response, response_size, tid);

	if(response->type == ABORT){
	    write_num = 0;
	    return ABORT;
	}
	else if(response->type == ERROR){
	    debug::notify_error("tid %d -- TX %d \t ERROR for item access", tid, step);
	    exit(0);
	}

#ifdef INTERACTIVE
	usleep(interactive_delay);
#endif
	auto r_item = (row_t*)response->data;
        schema = m_wl->t_item->get_schema();

        int64_t i_price;
        r_item->get_value(schema, I_PRICE, &i_price);
        r_item->get_value(schema, I_NAME);
        r_item->get_value(schema, I_DATA);
	step++;

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
#ifdef INTERACTIVE
	usleep(interactive_delay);
#endif

        key = stockKey(ol_i_id, ol_supply_w_id);
        pid = m_wl->key_to_part(key);

	request = create_message<rpc_request_t<Key>>((void*)send_ptr, tid, WRITE, TPCC_STOCK, pid, key, timestamp);
	transport->send((uint64_t)request, request_size, tid);

	recv_ptr = mem->rpc_response_buffer_pool(tid, step);
	response = create_message<rpc_response_t>((void*)recv_ptr);
	transport->recv((uint64_t)response, response_size, tid);

	if(response->type == ABORT){
	    write_num = 0;
	    return ABORT;
	}
	else if(response->type == ERROR){
	    debug::notify_error("tid %d -- TX %d \t ERROR for stock access", tid, step);
	    exit(0);
	}

#ifdef INTERACTIVE
	usleep(interactive_delay);
#endif
	auto r_stock = (row_t*)response->data;
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
	write_buf[write_num] = step;
	write_num++;
	step++;

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

    request = create_message<rpc_request_t<Key>>((void*)send_ptr, tid, COMMIT_DATA);
    request->num = write_num;
    for(int i=0; i<write_num; i++){
	memcpy(&request->data[sizeof(row_t)*i], mem->rpc_response_buffer_pool(tid, write_buf[i])->data, sizeof(row_t));
    }
    size_t commit_size = request_size + sizeof(row_t) * write_num;
    transport->send((uint64_t)request, commit_size, tid);

    recv_ptr = mem->rpc_response_buffer_pool(tid, 0);
    response = create_message<rpc_response_t>((void*)recv_ptr);
    transport->recv((uint64_t)response, response_size, tid);
    rc = response->type;
    write_num = 0;

    return rc;
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

#ifdef BATCH
void tpcc_txn_man_t::run_network_thread(){
}
#elif defined BATCH2
void tpcc_txn_man_t::run_send(int tid){
}
void tpcc_txn_man_t::run_recv(int tid){
}
void tpcc_txn_man_t::run_network_thread(){
}
#endif
