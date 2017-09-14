/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef _TPCC_H_
#define _TPCC_H_

#include <quecc_thread.h>
#include <lads.h>
#include "wl.h"
#include "txn.h"
#include "query.h"
#include "row.h"
#include "tpcc_helper.h"
#include "tpcc_const.h"
#include "table.h"

#if WORKLOAD == TPCC
class TPCCQuery;
class TPCCQueryMessage;
struct Item_no;

class table_t;
class INDEX;
class TPCCQuery;
enum TPCCRemTxnType {
    TPCC_PAYMENT_S=0,
    TPCC_PAYMENT0,
    TPCC_PAYMENT1,
    TPCC_PAYMENT2,
    TPCC_PAYMENT3,
    TPCC_PAYMENT4,
    TPCC_PAYMENT5,
    TPCC_NEWORDER_S,
    TPCC_NEWORDER0,
    TPCC_NEWORDER1,
    TPCC_NEWORDER2,
    TPCC_NEWORDER3,
    TPCC_NEWORDER4,
    TPCC_NEWORDER5,
    TPCC_NEWORDER6,
    TPCC_NEWORDER7,
    TPCC_NEWORDER8,
    TPCC_NEWORDER9,
    TPCC_FIN,
    TPCC_RDONE};


class TPCCWorkload : public Workload {
public:
    RC init();
    RC init_table();
    RC init_schema(const char * schema_file);
    RC get_txn_man(TxnManager *& txn_manager);
    table_t * 		t_warehouse;
    table_t * 		t_district;
    table_t * 		t_customer;
    table_t *		t_history;
    table_t *		t_neworder;
    table_t *		t_order;
    table_t *		t_orderline;
    table_t *		t_item;
    table_t *		t_stock;

    INDEX * 	i_item;
    INDEX * 	i_warehouse;
    INDEX * 	i_district;
    INDEX * 	i_customer_id;
    INDEX * 	i_customer_last;
    INDEX * 	i_stock;
    INDEX * 	i_order; // key = (w_id, d_id, o_id)
//	INDEX * 	i_order_wdo; // key = (w_id, d_id, o_id)
//	INDEX * 	i_order_wdc; // key = (w_id, d_id, c_id)
    INDEX * 	i_orderline; // key = (w_id, d_id, o_id)
    INDEX * 	i_orderline_wd; // key = (w_id, d_id).

    // XXX HACK
    // For delivary. Only one txn can be delivering a warehouse at a time.
    // *_delivering[warehouse_id] -> the warehouse is delivering.
    bool ** delivering;
//	bool volatile ** delivering;


private:
    uint64_t num_wh;
    void init_tab_item(int id);
    void init_tab_wh();
    void init_tab_dist(uint64_t w_id);
    void init_tab_stock(int id,uint64_t w_id);
    // init_tab_cust initializes both tab_cust and tab_hist.
    void init_tab_cust(int id, uint64_t d_id, uint64_t w_id);
    void init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id);
    void init_tab_order(int id,uint64_t d_id, uint64_t w_id);

    UInt32 perm_count;
    uint64_t * perm_c_id;
    void init_permutation();
    uint64_t get_permutation();

    static void * threadInitItem(void * This);
    static void * threadInitWh(void * This);
    static void * threadInitDist(void * This);
    static void * threadInitStock(void * This);
    static void * threadInitCust(void * This);
    static void * threadInitHist(void * This);
    static void * threadInitOrder(void * This);
};

struct thr_args{
    TPCCWorkload * wl;
    UInt32 id;
    UInt32 tot;
};

class TPCCTxnManager : public TxnManager
{
public:
    void init(uint64_t thd_id, Workload * h_wl);
    void reset();
    RC acquire_locks();
    RC run_txn();
    RC run_txn_post_wait();
    RC run_calvin_txn();
    RC run_tpcc_phase2();
    RC run_tpcc_phase5();
    TPCCRemTxnType state;
    void copy_remote_items(TPCCQueryMessage * msg);


#if CC_ALG == QUECC
    RC run_quecc_txn(exec_queue_entry * exec_qe);

    inline RC payment_lookup_w(uint64_t w_id, row_t *& r_wh_local){
        uint64_t key;
        itemid_t * item;
        key = w_id;
        INDEX * index = _wl->i_warehouse;
        item = index_read(index, key, wh_to_part(w_id));

        assert(item != NULL);
        r_wh_local = ((row_t *)item->location);
        assert(r_wh_local);
        return RCOK;
    };

    inline RC plan_payment_update_w(double h_amount, row_t * r_wh_local, exec_queue_entry *& entry){
        assert(r_wh_local != NULL);

        entry->rid = r_wh_local->get_row_id();
        entry->row = r_wh_local;
        entry->txn_ctx->h_amount = h_amount;
        entry->type = TPCC_PAYMENT_UPDATE_W;

        return RCOK;
    };
    inline RC run_payment_update_w(exec_queue_entry * entry){
        double w_ytd;
        row_t * r_wh_local = entry->row;
        r_wh_local->get_value(W_YTD, w_ytd);
        if (g_wh_update) {
            r_wh_local->set_value(W_YTD, w_ytd + entry->txn_ctx->h_amount);
        }
        return RCOK;
    };
    inline RC payment_lookup_d(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, row_t *& r_dist_local){
        uint64_t key;
        itemid_t * item;
        key = distKey(d_id, d_w_id);
        item = index_read(_wl->i_district, key, wh_to_part(w_id)); // TQ: should this be wh_to_part(d_w_id)??
        assert(item != NULL);
        r_dist_local = ((row_t *)item->location);
        // TODO(tq): handle deleted records
        return RCOK;
    };
    inline RC plan_payment_update_d(double h_amount, row_t * r_dist_local, exec_queue_entry *& entry){
        assert(r_dist_local != NULL);
        entry->row = r_dist_local;
        entry->rid = r_dist_local->get_row_id();
        entry->txn_ctx->h_amount = h_amount;
        entry->type = TPCC_PAYMENT_UPDATE_D;

        return RCOK;
    };
    inline RC run_payment_update_d(exec_queue_entry * entry){
        row_t * r_dist_local = entry->row;
        assert(r_dist_local != NULL);

        double d_ytd;
        r_dist_local->get_value(D_YTD, d_ytd);
        r_dist_local->set_value(D_YTD, d_ytd + entry->txn_ctx->h_amount);

        return RCOK;
    };
    inline RC payment_lookup_c(uint64_t c_id,uint64_t c_w_id, uint64_t c_d_id, char * c_last, bool by_last_name, row_t *& r_cust_local){

        itemid_t * item;
        uint64_t key;
        if (by_last_name) {

            key = custNPKey(c_last, c_d_id, c_w_id);
            // XXX: the list is not sorted. But let's assume it's sorted...
            // The performance won't be much different.
            INDEX * index = _wl->i_customer_last;
            item = index_read(index, key, wh_to_part(c_w_id));
            assert(item != NULL);

            int cnt = 0;
            itemid_t * it = item;
            itemid_t * mid = item;
            while (it != NULL) {
                cnt ++;
                it = it->next;
                if (cnt % 2 == 0)
                    mid = mid->next;
            }
            r_cust_local = ((row_t *)mid->location);
        }
        else { // search customers by cust_id
            key = custKey(c_id, c_d_id, c_w_id);
            INDEX * index = _wl->i_customer_id;
            item = index_read(index, key, wh_to_part(c_w_id));
            assert(item != NULL);
            r_cust_local = (row_t *) item->location;
        }

        return RCOK;
    };
    inline RC plan_payment_update_c(double h_amount, row_t * r_cust_local, exec_queue_entry *& entry){
        entry->row = r_cust_local;
        entry->txn_ctx->h_amount = h_amount;
        entry->rid = r_cust_local->get_row_id();
        entry->type = TPCC_PAYMENT_UPDATE_C;
        return RCOK;
    };
    inline RC run_payment_update_c(exec_queue_entry * entry){
        row_t * r_cust_local = entry->row;

        assert(r_cust_local != NULL);
        double c_balance;
        double c_ytd_payment;
        double c_payment_cnt;

        r_cust_local->get_value(C_BALANCE, c_balance);
        r_cust_local->set_value(C_BALANCE, c_balance - entry->txn_ctx->h_amount);
        r_cust_local->get_value(C_YTD_PAYMENT, c_ytd_payment);
        r_cust_local->set_value(C_YTD_PAYMENT, c_ytd_payment + entry->txn_ctx->h_amount);
        r_cust_local->get_value(C_PAYMENT_CNT, c_payment_cnt);
        r_cust_local->set_value(C_PAYMENT_CNT, c_payment_cnt + 1);
        return RCOK;
    };
    inline RC plan_payment_insert_h(uint64_t w_id, uint64_t d_id,uint64_t c_id,uint64_t c_w_id, uint64_t c_d_id, double h_amount, exec_queue_entry *& entry){

        uint64_t row_id = rid_man.next_rid(this->h_thd->_thd_id);

        entry->rid = row_id;
        entry->txn_ctx->w_id = w_id;
        entry->txn_ctx->d_id = d_id;
        entry->txn_ctx->c_id = c_id;
        entry->txn_ctx->c_w_id = c_w_id;
        entry->txn_ctx->c_d_id = c_d_id;
        entry->txn_ctx->h_amount = h_amount;
        entry->type = TPCC_PAYMENT_INSERT_H;

        return RCOK;
    };
    inline RC run_payment_insert_h(exec_queue_entry * entry){

        row_t * r_hist;
        RC rc = _wl->t_history->get_new_row(r_hist, wh_to_part(entry->txn_ctx->c_w_id), entry->rid);
        assert(rc == RCOK);
        r_hist->set_value(H_C_ID, entry->txn_ctx->c_id);
        r_hist->set_value(H_C_D_ID, entry->txn_ctx->c_d_id);
        r_hist->set_value(H_C_W_ID, entry->txn_ctx->c_w_id);
        r_hist->set_value(H_D_ID, entry->txn_ctx->d_id);
        r_hist->set_value(H_W_ID, entry->txn_ctx->w_id);
        int64_t date = 2017;
        r_hist->set_value(H_DATE, date);
        r_hist->set_value(H_AMOUNT, entry->txn_ctx->h_amount);
        // TQ: the following just records the insert into the transaction manager. The actual insert is done above by get_new_row
        // Since this is a MainMemory-DB, allocating the row is basically an insert.
//        insert_row(r_hist, _wl->t_history);
        return RCOK;
    };


    inline RC neworder_lookup_w(uint64_t w_id, row_t *& r_wh_local){
        uint64_t key;
        itemid_t * item;
        key = w_id;
        INDEX * index = _wl->i_warehouse;
        item = index_read(index, key, wh_to_part(w_id));
        assert(item != NULL);
        r_wh_local = ((row_t *)item->location);
        return RCOK;
    };
    inline RC plan_neworder_read_w(row_t *& r_wh_local, exec_queue_entry * entry){
        entry->row = r_wh_local;
        entry->rid = r_wh_local->get_row_id();
        entry->type = TPCC_NEWORDER_READ_W;
        return RCOK;
    };

    inline RC run_neworder_read_w(exec_queue_entry * entry){
        assert(entry != NULL);
        assert(entry->row != NULL);
        double w_tax;
        entry->row->get_value(W_TAX, w_tax);
        return RCOK;
    };

    inline RC neworder_lookup_c(uint64_t w_id, uint64_t d_id, uint64_t c_id, row_t *& r_cust_local){
        uint64_t key;
        itemid_t * item;
        key = custKey(c_id, d_id, w_id);
        INDEX * index = _wl->i_customer_id;
        item = index_read(index, key, wh_to_part(w_id));
        assert(item != NULL);
        r_cust_local = (row_t *) item->location;
        return RCOK;
    };
    inline RC plan_neworder_read_c(row_t *& r_cust_local, exec_queue_entry * entry){
        entry->row = r_cust_local;
        entry->rid = r_cust_local->get_row_id();
        entry->type = TPCC_NEWORDER_READ_C;
        return RCOK;
    };
    inline RC run_neworder_read_c(exec_queue_entry * entry){
        assert(entry != NULL);
        assert(entry->row != NULL);
        uint64_t c_discount;
        //char * c_last;
        //char * c_credit;
        entry->row->get_value(C_DISCOUNT, c_discount);
        //c_last = entry->row->get_value(C_LAST);
        //c_credit = entry->row->get_value(C_CREDIT);
        return RCOK;
    };

    inline RC neworder_lookup_d(uint64_t w_id, uint64_t d_id, row_t *& r_dist_local){
        uint64_t key;
        itemid_t * item;
        key = distKey(d_id, w_id);
        item = index_read(_wl->i_district, key, wh_to_part(w_id));
        assert(item != NULL);
        r_dist_local = ((row_t *)item->location);
        return RCOK;
    };
    inline RC plan_neworder_update_d(row_t *& r_dist_local, exec_queue_entry * entry){
        entry->row = r_dist_local;
        entry->rid = r_dist_local->get_row_id();
        entry->type = TPCC_NEWORDER_UPDATE_D;
        return RCOK;
    };
    inline RC run_neworder_update_d(exec_queue_entry * entry){
        assert(entry != NULL);
        assert(entry->row != NULL);
        //double * d_tax;
        int64_t * o_id;
        //d_tax = (double *) entry->row->get_value(D_TAX);
        o_id = (int64_t *) entry->row->get_value(D_NEXT_O_ID);
        (*o_id) ++;
        entry->row->set_value(D_NEXT_O_ID, *o_id);
        int64_t e = 0;
        int64_t d = *o_id;

        if(!entry->txn_ctx->o_id.compare_exchange_strong(e,d)){
            M_ASSERT_V(false, "we should have 0 but found %ld\n", entry->txn_ctx->o_id.load());
        }

        return RCOK;
    };

    inline RC plan_neworder_insert_o(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, exec_queue_entry * entry){
        uint64_t row_id = rid_man.next_rid(this->h_thd->_thd_id);
                entry->rid = row_id;
        entry->txn_ctx->w_id = w_id;
        entry->txn_ctx->d_id = d_id;
        entry->txn_ctx->c_id = c_id;
        entry->txn_ctx->o_entry_d = o_entry_d;
        entry->txn_ctx->ol_cnt = ol_cnt;
        entry->txn_ctx->remote = remote;
        entry->type = TPCC_NEWORDER_INSERT_O;

        return RCOK;

    };
    inline RC run_neworder_insert_o(exec_queue_entry * entry){
        row_t * r_order;
        // insert by allocating memory here
        RC rc = _wl->t_order->get_new_row(r_order, wh_to_part(entry->txn_ctx->w_id), entry->rid);

        r_order->set_value(O_C_ID, entry->txn_ctx->c_id);
        r_order->set_value(O_D_ID, entry->txn_ctx->d_id);
        r_order->set_value(O_W_ID, entry->txn_ctx->w_id);
        r_order->set_value(O_ENTRY_D, entry->txn_ctx->o_entry_d);
        r_order->set_value(O_OL_CNT, entry->txn_ctx->ol_cnt);
        int64_t all_local = (entry->txn_ctx->remote? 0 : 1);
        r_order->set_value(O_ALL_LOCAL, all_local);

        // may get stuck here when simulation is done
        while (entry->txn_ctx->o_id.load() == 0){
            if (simulation->is_done()) break;
        } // spin here until order id for this txn is set
        r_order->set_value(O_ID, entry->txn_ctx->o_id.load());

//        _wl->index_insert(_wl->i_order, orderPrimaryKey(entry->w_id,entry->d_id,entry->txn_ctx->o_id), r_order, wh_to_part(entry->w_id));
        return rc;
    };

    inline RC plan_neworder_insert_no(uint64_t w_id, uint64_t d_id, uint64_t c_id, exec_queue_entry * entry){
        uint64_t row_id = rid_man.next_rid(this->h_thd->_thd_id);

        entry->txn_ctx->w_id = w_id;
        entry->txn_ctx->d_id = d_id;
        entry->rid = row_id;
        entry->type = TPCC_NEWORDER_INSERT_NO;

        return RCOK;
    };
    inline RC run_neworder_insert_no(exec_queue_entry * entry){
        row_t * r_no;

        RC rc = _wl->t_neworder->get_new_row(r_no, wh_to_part(entry->txn_ctx->w_id), entry->rid);

        r_no->set_value(NO_D_ID, entry->txn_ctx->d_id);
        r_no->set_value(NO_W_ID, entry->txn_ctx->w_id);

        while (entry->txn_ctx->o_id.load() == 0){
            if (simulation->is_done()) break;
        } // spin here until order id for this txn is set
        r_no->set_value(NO_O_ID, entry->txn_ctx->o_id.load());

        //TQ: do we need to have an index for NewOrder table??

        return rc;
    };

    inline RC neworder_lookup_i(uint64_t ol_i_id, row_t *& r_item_local){
        uint64_t key;
        itemid_t * item;
        key = ol_i_id;
        item = index_read(_wl->i_item, key, 0);
        assert(item != NULL);
        r_item_local = ((row_t *)item->location);
        return RCOK;
    };
    inline RC plan_neworder_read_i(row_t *& r_item_local, exec_queue_entry * entry){
        entry->row = r_item_local;
        entry->rid = r_item_local->get_row_id();
        entry->type = TPCC_NEWORDER_READ_I;
        return RCOK;
    };
    inline RC run_neworder_read_i(exec_queue_entry * entry){
        assert(entry != NULL);
        assert(entry->row != NULL);
        int64_t i_price;
        //char * i_name;
        //char * i_data;
        //TODO(tq): unify method of attribute access
        entry->row->get_value(I_PRICE, i_price);

        //i_name = entry->row->get_value(I_NAME);
        //i_data = entry->row->get_value(I_DATA);
        return RCOK;
    };

    inline RC neworder_lookup_s(uint64_t ol_i_id, uint64_t ol_supply_w_id, row_t *& r_stock_local){
        uint64_t key;
        itemid_t * item;
        key = stockKey(ol_i_id, ol_supply_w_id);
        INDEX * index = _wl->i_stock;
        item = index_read(index, key, wh_to_part(ol_supply_w_id));
        assert(item != NULL);
        r_stock_local = ((row_t *)item->location);
        return RCOK;
    };
    inline RC plan_neworder_update_s(uint64_t ol_quantity, bool remote, row_t *& r_stock_local, exec_queue_entry * entry){
        entry->row = r_stock_local;
        entry->rid = r_stock_local->get_row_id();
        entry->txn_ctx->ol_quantity = ol_quantity;
        entry->txn_ctx->remote = remote;
        entry->type = TPCC_NEWORDER_UPDATE_S;
        return RCOK;
    };

    inline RC run_neworder_update_s(exec_queue_entry * entry){
        assert(entry != NULL);
        assert(entry->row != NULL);
        row_t * r_stock_local = entry->row;

        // XXX s_dist_xx are not retrieved.
        UInt64 s_quantity;
        int64_t s_remote_cnt;
        s_quantity = *(int64_t *)r_stock_local->get_value(S_QUANTITY);
#if !TPCC_SMALL
        int64_t s_ytd;
        int64_t s_order_cnt;
        char * s_data __attribute__ ((unused));
        r_stock_local->get_value(S_YTD, s_ytd);
        r_stock_local->set_value(S_YTD, s_ytd + entry->txn_ctx->ol_quantity);
        // In Coordination Avoidance, this record must be protected!
        r_stock_local->get_value(S_ORDER_CNT, s_order_cnt);
        r_stock_local->set_value(S_ORDER_CNT, s_order_cnt + 1);
        s_data = r_stock_local->get_value(S_DATA);
#endif
        if (entry->txn_ctx->remote) {
            s_remote_cnt = *(int64_t*)r_stock_local->get_value(S_REMOTE_CNT);
            s_remote_cnt ++;
            r_stock_local->set_value(S_REMOTE_CNT, &s_remote_cnt);
        }
        uint64_t quantity;
        if (s_quantity > entry->txn_ctx->ol_quantity + 10) {
            quantity = s_quantity - entry->txn_ctx->ol_quantity;
        } else {
            quantity = s_quantity - entry->txn_ctx->ol_quantity + 91;
        }
        r_stock_local->set_value(S_QUANTITY, &quantity);
        return RCOK;
    };

    inline RC plan_neworder_insert_ol(uint64_t ol_i_id, uint64_t ol_supply_w_id, uint64_t ol_quantity,uint64_t  ol_number, row_t *& r_ol_local, exec_queue_entry * entry){

        uint64_t row_id = rid_man.next_rid(this->h_thd->_thd_id);

        entry->rid = row_id;
        entry->txn_ctx->ol_supply_w_id = ol_supply_w_id;
        entry->txn_ctx->ol_i_id = ol_i_id;
        entry->txn_ctx->ol_quantity = ol_quantity;
        entry->txn_ctx->ol_number = ol_number;
        entry->type = TPCC_NEWORDER_INSERT_OL;

        return RCOK;
    };
    inline RC run_neworder_insert_ol(exec_queue_entry * entry){
        /*====================================================+
		EXEC SQL INSERT
			INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number,
				ol_i_id, ol_supply_w_id,
				ol_quantity, ol_amount, ol_dist_info)
			VALUES(:o_id, :d_id, :w_id, :ol_number,
				:ol_i_id, :ol_supply_w_id,
				:ol_quantity, :ol_amount, :ol_dist_info);
		+====================================================*/
        row_t * r_ol;
        _wl->t_orderline->get_new_row(r_ol, wh_to_part(entry->txn_ctx->ol_supply_w_id), entry->rid);

        r_ol->set_value(OL_D_ID, &entry->txn_ctx->d_id);
        r_ol->set_value(OL_W_ID, &entry->txn_ctx->w_id);
        r_ol->set_value(OL_NUMBER, &entry->txn_ctx->ol_number);
        r_ol->set_value(OL_I_ID, &entry->txn_ctx->ol_i_id);
#if !TPCC_SMALL
        r_ol->set_value(OL_SUPPLY_W_ID, &entry->txn_ctx->ol_supply_w_id);
        r_ol->set_value(OL_QUANTITY, &entry->txn_ctx->ol_quantity);
        uint64_t ol_amount = URand(1, 10); // TODO(tq): Fix this. Compute OL_AMOUNT proparly
//        ol_amount = ol_quantity * i_price * (1+w_tax+d_tax) * (1-c_discount);
//        amt[ol_number-1]=ol_amount;
//        total += ol_amount;
        r_ol->set_value(OL_AMOUNT, ol_amount);
#endif
        while (entry->txn_ctx->o_id.load() == 0){
            if (simulation->is_done()) break;
        } // spin here until order id for this txn is set
        r_ol->set_value(OL_O_ID, entry->txn_ctx->o_id.load());

        return RCOK;
    };

#endif // End of CC_ALG == QUECC

private:
    TPCCWorkload * _wl;
    volatile RC _rc;
    row_t * row;

    // For QueCC

    RC run_hstore_txn();
    // For LADS
    RC execute_lads_action(gdgcc::Action * action, int eid);
    RC resolve_txn_dependencies(Message* msg);
    uint64_t next_item_id;

    void next_tpcc_state();
    RC run_txn_state();
    bool is_done();
    bool is_local_item(uint64_t idx);
    RC send_remote_request();


    RC run_payment_0(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, row_t *& r_wh_local);
    RC run_payment_1(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, row_t * r_wh_local);
    RC run_payment_2(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, row_t *& r_dist_local);
    RC run_payment_3(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, row_t * r_dist_local);
    RC run_payment_4(uint64_t w_id, uint64_t d_id,uint64_t c_id,uint64_t c_w_id, uint64_t c_d_id, char * c_last, double h_amount, bool by_last_name, row_t *& r_cust_local);
    RC run_payment_5(uint64_t w_id, uint64_t d_id,uint64_t c_id,uint64_t c_w_id, uint64_t c_d_id, char * c_last, double h_amount, bool by_last_name, row_t * r_cust_local);

    RC new_order_0(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t *& r_wh_local);
    RC new_order_1(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t * r_wh_local);
    RC new_order_2(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t *& r_cust_local);
    RC new_order_3(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t * r_cust_local);
    RC new_order_4(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t *& r_dist_local);
    RC new_order_5(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t * r_dist_local);
    RC new_order_6(uint64_t ol_i_id, row_t *& r_item_local);
    RC new_order_7(uint64_t ol_i_id, row_t * r_item_local);
    RC new_order_8(uint64_t w_id,uint64_t  d_id,bool remote, uint64_t ol_i_id, uint64_t ol_supply_w_id, uint64_t ol_quantity,uint64_t  ol_number,uint64_t  o_id, row_t *& r_stock_local);
    RC new_order_9(uint64_t w_id,uint64_t  d_id,bool remote, uint64_t ol_i_id, uint64_t ol_supply_w_id, uint64_t ol_quantity,uint64_t  ol_number,uint64_t ol_amount, uint64_t  o_id, row_t * r_stock_local);
    RC run_order_status(TPCCQuery * query);
    RC run_delivery(TPCCQuery * query);
    RC run_stock_level(TPCCQuery * query);
};

#endif //WORKLOAD == TPCC

#endif
