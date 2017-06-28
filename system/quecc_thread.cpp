//
// Created by Thamir Qadah on 4/20/17.
//


#include "global.h"
#include "manager.h"
#include "thread.h"
#include "calvin_thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "math.h"
#include "helper.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "sequencer.h"
#include "logger.h"
#include "message.h"
#include "work_queue.h"
#include "quecc_thread.h"
//TQ: we will use standard lib version for now. We can use optimized implementation later
#include <unordered_map>


inline void exec_queue_get_or_create(Array<exec_queue_entry> *&exec_q, uint64_t planner_id, uint64_t exec_queue_capacity) {
    if (!work_queue.exec_queue_free_list[planner_id]->pop(exec_q)){
        exec_q = (Array<exec_queue_entry> *) mem_allocator.alloc(sizeof(Array<exec_queue_entry>));
        exec_q->init(exec_queue_capacity);
//        DEBUG_Q("Allocating exec_q\n");
    }
    else {
        exec_q->clear();
//        DEBUG_Q("Reusing exec_q\n");
    }
}

void exec_queue_release(Array<exec_queue_entry> *&exec_q, uint64_t planner_id) {
    exec_q->clear();
    while(!work_queue.exec_queue_free_list[planner_id]->push(exec_q)){};
}

void assign_entry_clear(assign_entry * &a_entry){
    a_entry->exec_qs = NULL;
    a_entry->exec_thd_id = 0;
    a_entry->curr_sum = 0;
}


void assign_entry_add(assign_entry * a_entry, Array<exec_queue_entry> * exec_q) {
    a_entry->curr_sum += exec_q->size();
    a_entry->exec_qs->add(exec_q);
}

void assign_entry_init(assign_entry * &a_entry, uint64_t thd_id){
    if (!a_entry->exec_qs){
        if (!work_queue.exec_qs_free_list[thd_id]->pop(a_entry->exec_qs)){
//            DEBUG_Q("Allocating a_entry->exec_qs\n");
            a_entry->exec_qs = new Array<Array<exec_queue_entry> *>();
            a_entry->exec_qs->init(32);
        }
//        else {
//            DEBUG_Q("Reusing a_entry->exec_qs\n");
//        }
    }
    else{
        a_entry->exec_qs->clear();
    }
}

inline void assign_entry_get_or_create(assign_entry *&a_entry, boost::lockfree::spsc_queue<assign_entry *> * assign_entry_free_list){
    if (!assign_entry_free_list->pop(a_entry)){
        a_entry = (assign_entry *) mem_allocator.alloc(sizeof(assign_entry));
//        DEBUG_Q("Allocating a_entry\n");
    }
//    else {
//        DEBUG_Q("Reusing a_entry\n");
//    }
    memset(a_entry, 0, sizeof(assign_entry));
}

inline void split_entry_get_or_create(split_entry *&s_entry,
                                      boost::lockfree::spsc_queue<split_entry *> * split_entry_free_list) {
    if (!split_entry_free_list->pop(s_entry)){
        s_entry = (split_entry *) mem_allocator.alloc(sizeof(split_entry));
//        DEBUG_Q("Allocating s_entry\n");
    }
//    else {
//        DEBUG_Q("Reusing s_entry\n");
//    }
}

void split_entry_print(split_entry * ptr, uint64_t planner_id) {
    DEBUG_Q("Planner_%ld: exec_q size = %ld, from %ld to %ld, range_size = %ld\n",
            planner_id, ptr->exec_q->size(), ptr->range_start,
            ptr->range_end, ptr->range_size);
}

inline void txn_ctxs_get_or_create(transaction_context * &txn_ctxs, uint64_t length, uint64_t planner_id){
    if (!work_queue.txn_ctxs_free_list[planner_id]->pop(txn_ctxs)){
//        DEBUG_Q("Allocating txn_ctxs\n");
        txn_ctxs = (transaction_context *) mem_allocator.alloc(sizeof(transaction_context)*length);
    }
//    else {
//        DEBUG_Q("Reusing txn_ctxs\n");
//    }
    memset(txn_ctxs, 0,sizeof(transaction_context)*length);
}

void txn_ctxs_release(transaction_context * &txn_ctxs, uint64_t planner_id){
    while(!work_queue.txn_ctxs_free_list[planner_id]->push(txn_ctxs));
}

inline void pg_get_or_create(priority_group * &pg, uint64_t planner_id){
    if (!work_queue.pg_free_list[planner_id]->pop(pg)){
        pg = (priority_group *) mem_allocator.alloc(sizeof(priority_group));
        DEBUG_Q("Allocating priority group\n");
    }
    memset(pg, 0,sizeof(priority_group));
}

void pg_release(priority_group * &pg, uint64_t planner_id){
    while(!work_queue.pg_free_list[planner_id]->push(pg)){};
}

void CommitThread::setup() {
}

RC CommitThread::run() {
    tsetup();

    //TODO(tq): collect stats, idle time, commit cnt, abort_cnt

//    uint64_t idle_starttime = 0;
    uint64_t batch_slot = 0;
    uint64_t batch_id = 0;
    uint64_t cplanner_id = 0; // planner_id
    transaction_context * txn_ctxs = NULL;
    uint64_t tmp_p = 0;
    priority_group * planner_pg = NULL;

    uint64_t expected = 0;
    uint64_t desired = 0;

    uint8_t expected8 = 0;
    uint8_t desired8 = 0;

    while(!simulation->is_done()) {
        // wait for a priority group to complete
        // start committing transactions in a priority group
        //
//        heartbeat();
//        DEBUG_Q("CT_%ld : Going to spin waiting for priority group %ld to be COMPLETED , batch_id = %ld, batch_slot = %ld\n",
//                _thd_id, cplanner_id, batch_id, batch_slot);
        if (work_queue.batch_map_comp_cnts[batch_slot][cplanner_id].load() != g_thread_cnt){
            continue;
        }

//        DEBUG_Q("CT_%ld : Ready to process priority group %ld for batch_id = %ld, batch_slot = %ld\n",
//                _thd_id, cplanner_id, batch_id, batch_slot);

        // get the pointer to the transaction contexts of the current priority group
        tmp_p = work_queue.batch_pg_map[batch_slot][cplanner_id].load();
        // spin here if necessary
//        while (tmp_p == 0  && !simulation->is_done()){
//            tmp_p = work_queue.batch_pg_map[batch_slot][cplanner_id].load();
//        }
        M_ASSERT_V(tmp_p != 0, "CT_%ld : planner_pg is not inisitalized, batch_slot = %ld, batch_id = %ld, cplanner_id = %ld, tmp_p = %ld\n",
                   _thd_id, batch_slot, batch_id, cplanner_id, tmp_p);

        planner_pg = ((priority_group *) tmp_p);

        M_ASSERT_V(batch_id == planner_pg->batch_id, "CT_%ld : batch_id mismatch, batch_slot = %ld, batch_id = %ld, pg_batch_id = %ld\n",
                   _thd_id, batch_slot, batch_id, planner_pg->batch_id);

        M_ASSERT_V(cplanner_id == planner_pg->planner_id, "CT_%ld : planner_pg is not inisitalized, batch_slot = %ld, batch_id = %ld, cplanner_id = %ld, pg_planner_id = %ld\n",
                   _thd_id, batch_slot, batch_id, cplanner_id, planner_pg->planner_id);

        M_ASSERT_V(planner_pg->planner_id == cplanner_id, "CT_%ld : mismatch of planner id pg_plan_id(%ld) == cplan_id(%ld)\n",
                   _thd_id, planner_pg->planner_id, cplanner_id);
        M_ASSERT_V(planner_pg->batch_id == batch_id, "CT_%ld : mismatch of batch ids pg_batch_id(%ld) == local batch_id(%ld)\n",
                   _thd_id, planner_pg->batch_id, batch_id);

        txn_ctxs   = planner_pg->txn_ctxs;
        bool canCommit = true;
        //loop over all transactions in the priority group
        for (uint64_t i = 0; i < planner_pg->batch_txn_cnt; i++){
            // hardcod for YCSB workload for now
            // TODO(tq): fix this to support other workloads

            if (txn_ctxs[i].txn_state == TXN_READY_TO_COMMIT){
#if BUILD_TXN_DEPS
//            if (txn_ctxs[i].completion_cnt.load() == REQ_PER_QUERY){
                // We are ready to commit, now we need to check if we need to abort due to dependent aborted transactions
                // to check if we need to abort, we lookup transaction dependency graph
                auto search = planner_pg->txn_dep_graph->find(txn_ctxs[i].txn_id);
                if (search != planner_pg->txn_dep_graph->end()){
                    // print dependenent transactions for now.
                    if (search->second->size() > 0){
                        // there are dependen transactions
//                        DEBUG_Q("CT_%ld : txn_id = %ld depends on %ld other transactions\n", _thd_id, txn_ctxs[i].txn_id, search->second->size());
//                        for(std::vector<uint64_t>::iterator it = search->second->begin(); it != search->second->end(); ++it) {
//                            DEBUG_Q("CT_%ld : txn_id = %ld depends on txn_id = %ld\n", _thd_id, txn_ctxs[i].txn_id, (uint64_t) *it);
//                        }
                        uint64_t d_txn_id = search->second->back();
                        uint64_t d_txn_ctx_idx = d_txn_id-planner_pg->batch_starting_txn_id+1;
//                        transaction_context * dep_txn = txn_ctxs[];
                        M_ASSERT_V(txn_ctxs[d_txn_ctx_idx].txn_id == d_txn_id,
                                   "Txn_id mismatch for d_ctx_txn_id %ld == tdg_d_txn_id %ld , d_txn_ctx_idx = %ld,"
                                           "c_txn_id = %ld, batch_starting_txn_id = %ld\n",
                                   txn_ctxs[d_txn_ctx_idx].txn_id,
                                   d_txn_id, d_txn_ctx_idx, txn_ctxs[i].txn_id, planner_pg->batch_starting_txn_id
                        );
                        if (txn_ctxs[d_txn_ctx_idx].txn_state != TXN_READY_TO_COMMIT){
                            // abort
//                            DEBUG_Q("CT_%ld : going to abort txn_id = %ld due to dependencies\n", _thd_id, txn_ctxs[i].txn_id);
                            canCommit = false;
                        }
                    }
//                    else{
                        // no dependent transactions, we should be able to commit
//                        DEBUG_Q("CT_%ld :no dependent transactions for txn_id = %ld\n", _thd_id, txn_ctxs[i].txn_id);
//                    }
                }
#endif
                if (canCommit){
                    // Committing
                    // Sending response to client a
#if !SERVER_GENERATE_QUERIES
                    Message * rsp_msg = Message::create_message(CL_RSP);
                    rsp_msg->txn_id = txn_ctxs[i].txn_id;
                    rsp_msg->batch_id = batch_id; // using batch_id from local, we can also use the one in the context
                    ((ClientResponseMessage *) rsp_msg)->client_startts = txn_ctxs[i].client_startts;
                    rsp_msg->lat_work_queue_time = 0;
                    rsp_msg->lat_msg_queue_time = 0;
                    rsp_msg->lat_cc_block_time = 0;
                    rsp_msg->lat_cc_time = 0;
                    rsp_msg->lat_process_time = 0;
                    rsp_msg->lat_network_time = 0;
                    rsp_msg->lat_other_time = 0;

                    msg_queue.enqueue(_thd_id, rsp_msg, txn_ctxs[i].return_node_id);
#endif
                    INC_STATS(get_thd_id(), txn_cnt, 1);

                }
                else {
                    INC_STATS(get_thd_id(), total_txn_abort_cnt, 1);
                }
            }
            else{
                // abort case
                //TODO(tq): handle abort case, we now just increment a counter, however, shouldn't we retry?
                // Exectution layer decided to abort, which can be a logic-induced abort
                INC_STATS(get_thd_id(), total_txn_abort_cnt, 1);
            }
        }

        // reset priority group counter to allow ETs to proceed
//        DEBUG_Q("CT_%ld : For batch %ld : failing to RESET batch_map_comp_cnts slot [%ld][%ld],"
//                    " current value = %d, expected = %d\n",
//                       _thd_id, batch_id, batch_slot, cplanner_id,
//                       work_queue.batch_map_comp_cnts[batch_slot][cplanner_id].load(), expected8);
        desired8 = 0;
        expected8 = (uint8_t) g_thread_cnt;
        while(!work_queue.batch_map_comp_cnts[batch_slot][cplanner_id].compare_exchange_strong(
                expected8, desired8)){
            // this should not happen
            M_ASSERT_V(false, "CT_%ld : For batch %ld : failing to RESET batch_map_comp_cnts slot [%ld][%ld],"
                    " current value = %d, expected = %d\n",
                       _thd_id, batch_id, batch_slot, cplanner_id,
                       work_queue.batch_map_comp_cnts[batch_slot][cplanner_id].load(), expected8);

        }
        // return txn_ctxs to the pool
//        while(!work_queue.txn_ctxs_free_list[cplanner_id]->push(txn_ctxs)){}
        txn_ctxs_release(txn_ctxs, cplanner_id);

#if BUILD_TXN_DEPS
        // Clean up and clear txn_graph
        for (auto it = planner_pg->txn_dep_graph->begin(); it != planner_pg->txn_dep_graph->end(); ++it){
            delete it->second;
        }
        delete planner_pg->txn_dep_graph;
#endif

        expected = (uint64_t) planner_pg;
        desired = 0;
//        DEBUG_Q("CT_%ld :going to reset pg(%ld) for batch_%ld at b_slot = %ld\n", _thd_id, cplanner_id, batch_id, batch_slot);
        while(!work_queue.batch_pg_map[batch_slot][cplanner_id].compare_exchange_strong(
                expected, desired)){
            // this should not happen after spinning
//            M_ASSERT_V(false, "CT_%ld : For batch %ld : failing to RESET batch_pg_map slot [%ld][%ld], current_value = %ld, expected = %ld\n",
//                       _thd_id, batch_id, batch_slot, cplanner_id,
//                       work_queue.batch_pg_map[batch_slot][cplanner_id].load(), expected
//            );
        }
//        DEBUG_Q("CT_%ld : Done processing priority group %ld for batch_id = %ld\n", _thd_id, cplanner_id, batch_id);
//        mem_allocator.free(planner_pg, sizeof(priority_group));
        pg_release(planner_pg, cplanner_id);
        cplanner_id++;

        if (cplanner_id == g_plan_thread_cnt){
            // proceed to the next batch
            batch_id++;
            batch_slot = batch_id % g_batch_map_length;
            cplanner_id = 0;
        }
    }

    // cleanup
    desired8 = 0;
    desired = 0;
    for (uint64_t i = 0; i < g_batch_map_length; ++i){
        for (uint64_t j = 0; j < g_plan_thread_cnt; ++j){
            // we need to signal all ETs that are spinning
            work_queue.batch_map_comp_cnts[i][j].store(desired8);
            // Signal all spinning PTs
            work_queue.batch_pg_map[i][j].store(desired);
        }
    }

    printf("FINISH CT %ld:%ld\n",_node_id,_thd_id);
    fflush(stdout);
    return FINISH;

}

void PlannerThread::setup() {
}

uint32_t PlannerThread::get_bucket(uint64_t key) {
    uint64_t no_buckets = g_thread_cnt;
    for (uint64_t i = 0; i < no_buckets; i++){
        if (key >= (i*bucket_size) && key < ((i+1)*bucket_size)){
            return i;
        }
    }
    return 0;
}

uint32_t PlannerThread::get_split(uint64_t key, uint32_t range_cnt, uint64_t range_start, uint64_t range_end) {
    uint64_t range_size = (range_end - range_start)/range_cnt;
    for (uint64_t i = 0; i < range_cnt; i++){
        if (key >= ((i*range_size)+range_start) && key < (((i+1)*range_size)+range_start)){
            return i;
        }
    }
    return 0;
}

uint32_t PlannerThread::get_split(uint64_t key, Array<uint64_t> * ranges) {
    for (uint64_t i = 0; i < ranges->size(); i++){
        if (key < ranges->get(i)){
            return i;
        }
    }
    M_ASSERT_V(false, "could not assign to range key = %ld\n", key);
}

RC PlannerThread::run() {
    tsetup();

    Message * msg;
    uint64_t idle_starttime = 0;
//    uint64_t prof_starttime = 0;
    uint64_t batch_cnt = 0;

    uint64_t txn_prefix_base = 0x0010000000000000;
    uint64_t txn_prefix_planner_base = (_planner_id * txn_prefix_base);

    assert(UINT64_MAX > (txn_prefix_planner_base+txn_prefix_base));

    DEBUG_Q("Planner_%ld thread started, txn_ids start at %ld \n", _planner_id, txn_prefix_planner_base);
    uint64_t planner_batch_size = g_batch_size/g_plan_thread_cnt;
#if SPLIT_MERGE_ENABLED
    uint64_t exec_queue_limit = planner_batch_size/g_thread_cnt;
#endif
    // max capcity, assume YCSB workload
//    uint64_t exec_queue_capacity = planner_batch_size*10;
    uint64_t exec_queue_capacity = EQ_INIT_CAP;
#if BATCHING_MODE == SIZE_BASED
    exec_queue_capacity = planner_batch_size * REQ_PER_QUERY;
#endif


#if SPLIT_MERGE_ENABLED
    split_max_heap_t pq_test;
    split_min_heap_t range_sorted;
    assign_ptr_min_heap_t assignment;
    uint64_t * f_assign = (uint64_t *) mem_allocator.alloc(sizeof(uint64_t)*g_thread_cnt);

    boost::lockfree::spsc_queue<split_entry *> * split_entry_free_list =
            new boost::lockfree::spsc_queue<split_entry *>(FREE_LIST_INITIAL_SIZE*10);

    boost::lockfree::spsc_queue<assign_entry *> * assign_entry_free_list =
            new boost::lockfree::spsc_queue<assign_entry *>(FREE_LIST_INITIAL_SIZE);

    split_entry ** nsp_entries = (split_entry **) mem_allocator.alloc(sizeof(split_entry*)*2);

    exec_queue_limit = (planner_batch_size/g_thread_cnt) * REQ_PER_QUERY * EXECQ_CAP_FACTOR;

    volatile bool ranges_stored = false;

#endif

    // Array to track ranges
    Array<uint64_t> * exec_qs_ranges = new Array<uint64_t>();
    exec_qs_ranges->init(g_thread_cnt*10);
    for (uint64_t i =0; i<g_thread_cnt; ++i){

        if (i == 0){
            exec_qs_ranges->add(bucket_size);
            continue;
        }

        exec_qs_ranges->add(exec_qs_ranges->get(i-1)+bucket_size);
    }

    // create and and pre-allocate execution queues
    // For each mrange which will be assigned to an execution thread
    // there will be an array pointer.
    // When the batch is complete we will CAS the exec_q array to allow
    // execution threads to be

// implementtion using array class
    Array<Array<exec_queue_entry> *> * exec_queues = new Array<Array<exec_queue_entry> *>();
//#if SPLIT_MERGE_ENABLED
    exec_queues->init(g_thread_cnt*10);
//#else
//    exec_queues->init(g_thread_cnt);
//#endif
    for (uint64_t i = 0; i < g_thread_cnt; i++) {
//        Array<exec_queue_entry> * exec_q = new Array<exec_queue_entry>();
        Array<exec_queue_entry> * exec_q;
        exec_queue_get_or_create(exec_q, _planner_id, exec_queue_capacity);
//        exec_q->init(exec_queue_capacity);
        exec_queues->add(exec_q);
    }

    // Pre-allocate execution queues to be used by planners and executors
    // this should eliminate the overhead of memory allocation
    uint64_t  exec_queue_pool_size = FREE_LIST_INITIAL_SIZE;
    boost::random::mt19937 plan_rng;
    boost::random::uniform_int_distribution<> eq_idx_rand(0, g_thread_cnt-1);

//    for (uint64_t i = 0; i < g_thread_cnt; i++) {
        for (uint64_t j = 0; j < exec_queue_pool_size; j++){
//            Array<exec_queue_entry> * exec_q = new Array<exec_queue_entry>();
            Array<exec_queue_entry> * exec_q;
//#if SPLIT_MERGE_ENABLED
//            exec_q->init(planner_batch_size/g_thread_cnt);
//#else
//            exec_q->init(exec_queue_capacity);
//#endif
            exec_queue_get_or_create(exec_q, _planner_id, exec_queue_capacity);
            exec_queue_release(exec_q, _planner_id);
//            while(!work_queue.exec_queue_free_list[_planner_id]->push(exec_q)){}
        }
//    }

    // Pre-allocate planner's transaction contexts
    uint64_t  txn_ctxs_pool_size = FREE_LIST_INITIAL_SIZE;
    for (uint64_t j = 0; j < txn_ctxs_pool_size; j++){
        transaction_context * txn_ctxs_tmp;
//        transaction_context * txn_ctxs_tmp = (transaction_context *) mem_allocator.alloc(
//                sizeof(transaction_context)*planner_batch_size);
//        memset(txn_ctxs_tmp, 0,sizeof(transaction_context)*planner_batch_size );
//        while(!work_queue.txn_ctxs_free_list[_planner_id]->push(txn_ctxs_tmp)){}
        txn_ctxs_get_or_create(txn_ctxs_tmp, planner_batch_size, _planner_id);
        txn_ctxs_release(txn_ctxs_tmp, _planner_id);
    }

    transaction_context * txn_ctxs = NULL;
    txn_ctxs_get_or_create(txn_ctxs, planner_batch_size, _planner_id);
//    if (work_queue.txn_ctxs_free_list[_planner_id]->pop(txn_ctxs)){
//        memset(txn_ctxs, 0,sizeof(transaction_context)*planner_batch_size );
//    }
//    else{
//        txn_ctxs = (transaction_context *) mem_allocator.alloc(sizeof(transaction_context)*planner_batch_size);
//    }

//    priority_group * planner_pg = (priority_group *) mem_allocator.alloc(sizeof(priority_group));
    priority_group * planner_pg;
    pg_get_or_create(planner_pg, _planner_id);
    planner_pg->planner_id = _planner_id;
    planner_pg->txn_ctxs = txn_ctxs;

#if DEBUG_QUECC
//    Array<uint64_t> mrange_cnts;
//    mrange_cnts.init(g_thread_cnt);
//
//    for (uint64_t i = 0; i < g_thread_cnt; i++) {
//        mrange_cnts.add(0);
//    }
    uint64_t total_msg_processed_cnt = 0;
    uint64_t total_access_cnt = 0;
#endif
    planner_txn_id = txn_prefix_planner_base;

#if BUILD_TXN_DEPS
    txn_dep_graph = new hash_table_t();
#endif
    uint64_t batch_starting_txn_id = planner_txn_id;

    exec_queue_entry *entry = (exec_queue_entry *) mem_allocator.alloc(sizeof(exec_queue_entry));

    while(!simulation->is_done()) {
        if (plan_starttime == 0 && simulation->is_warmup_done()){
            plan_starttime = get_sys_clock();
        }

        // dequeue for repective input_queue: there is an input queue for each planner
        // entries in the input queue is placed by the I/O thread
        // for now just dequeue and print notification
//        DEBUG_Q("Planner_%d is dequeuing\n", _planner_id);
        prof_starttime = get_sys_clock();
        msg = work_queue.plan_dequeue(_thd_id, _planner_id);
        INC_STATS(_thd_id, plan_queue_dequeue_time[_planner_id], get_sys_clock()-prof_starttime);

        if(!msg) {
            if(idle_starttime == 0){
                idle_starttime = get_sys_clock();
            }
            idle_cnt++;
//            double idle_for =  ( (double) get_sys_clock() - idle_starttime);
//            double batch_start_since = ( (double) get_sys_clock() - batch_start_time);
//            if (idle_starttime > 0 && idle_cnt % (10 * MILLION) == 0){
//                DEBUG_Q("Planner_%ld : we should force batch delivery with batch_cnt = %ld, total_idle_time = %f, idle_for=%f, batch_started_since=%f\n",
//                        _planner_id,
//                        batch_cnt,
//                        stats._stats[_thd_id]->plan_idle_time[_planner_id]/BILLION,
//                        idle_for/BILLION,
//                        batch_start_since/BILLION)
//            }
            // we have not recieved a transaction
                continue;
        }
        INC_STATS(_thd_id,plan_queue_deq_cnt[_planner_id],1);

        if (idle_starttime != 0){
            // plan_idle_time includes dequeue time, which should be subtracted from it
            INC_STATS(_thd_id,plan_idle_time[_planner_id],get_sys_clock() - idle_starttime);
            idle_starttime = 0;
        }

#if BATCHING_MODE == SIZE_BASED
        force_batch_delivery = (batch_cnt == planner_batch_size);
#else
    // Default to TIME_BASED
        force_batch_delivery = ((get_sys_clock() - batch_start_time) >= BATCH_COMP_TIMEOUT && batch_cnt > 0);
#endif

        if (force_batch_delivery) {
            slot_num = (batch_id % g_batch_map_length);

//            DEBUG_Q("Ranges:\n");
//            for (uint64_t i = 0; i < exec_qs_ranges->size(); i++){
//                DEBUG_Q("PL_%ld : [%ld] %ld\n", _planner_id, i, exec_qs_ranges->get(i));
//            }

            if (BATCHING_MODE == TIME_BASED) {
                INC_STATS(_thd_id, plan_time_batch_cnts[_planner_id], 1)
                force_batch_delivery = false;
            } else {
                INC_STATS(_thd_id, plan_size_batch_cnts[_planner_id], 1)
            }

//            DEBUG_Q("Batch complete\n")
            // a batch is ready to be delivered to the the execution threads.
            // we will have a batch partition prepared by for each of the planners and they are scanned in known order
            // by execution threads
            // We also have a batch queue for each of the executor that is mapped to bucket
            // for now, we will assign a bucket to each executor
            // All we need to do is to automically CAS the pointer to the address of the accumulated batch
            // and the execution threads who are spinning can start execution
            // Here major ranges have one-to-one mapping to worker threads
            // Split execution queues if needed

            batch_partition *batch_part = NULL;

            prof_starttime = get_sys_clock();
#if SPLIT_MERGE_ENABLED
#if BATCHING_MODE != SIZE_BASED
            exec_queue_limit = (batch_cnt/g_thread_cnt) * REQ_PER_QUERY * EXECQ_CAP_FACTOR;
#endif


            // Splitting phase
            if (ranges_stored) {


                for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
                    assign_entry *a_tmp;
                    if (i < g_thread_cnt){

                        assign_entry_get_or_create(a_tmp, assign_entry_free_list);

//                        if (!assign_entry_free_list->pop(a_tmp)) {
//                            a_tmp = (assign_entry *) mem_allocator.alloc(sizeof(assign_entry));
//                            memset(a_tmp, 0, sizeof(assign_entry));
//                        }

                        assign_entry_init(a_tmp, _planner_id);
                        a_tmp->exec_thd_id = i;
                        assign_entry_add(a_tmp, exec_queues->get(i));
                        assignment.push((uint64_t) a_tmp);
                    }
                    else {
                        a_tmp = (assign_entry *) assignment.top();
                        assign_entry_add(a_tmp, exec_queues->get(i));
                        assignment.pop();
                        assignment.push((uint64_t) a_tmp);
                    }
                }

            } else {

                for (uint64_t i = 0; i < exec_queues->size(); i++) {
    //                hasSplit = false;
                    Array<exec_queue_entry> *exec_q = exec_queues->get(i);

                    split_entry *root;
                    split_entry_get_or_create(root, split_entry_free_list);

                    root->exec_q = exec_q;
                    root->range_start = (i * bucket_size);
                    root->range_end = ((i + 1) * bucket_size);
                    root->range_size = bucket_size;

                    // Check if we need to split
                    if (exec_queue_limit < exec_q->size()) {
                        DEBUG_Q("Planner_%ld : We need to split current size = %ld, limit = %ld,"
                                        " batch_cnt = %ld, planner_batch_size = %ld, batch_id = %ld"
                                        "\n",
                                _planner_id, exec_q->size(), exec_queue_limit, batch_cnt, planner_batch_size, batch_id);

                        uint64_t split_rounds = 0;

                        split_entry *top_entry = root;

                        while (true) {

                            // Allocate memory for new exec_qs
                            Array<exec_queue_entry> **nexec_qs = (Array<exec_queue_entry> **) mem_allocator.alloc(
                                    sizeof(Array<exec_queue_entry> *) * 2);

                            if (work_queue.exec_queue_free_list[_planner_id]->pop(nexec_qs[0])) {
                                nexec_qs[0]->clear();
                            } else {
                                nexec_qs[0] = new Array<exec_queue_entry>();
                                nexec_qs[0]->init(exec_queue_capacity);
                            }

                            if (work_queue.exec_queue_free_list[_planner_id]->pop(nexec_qs[1])) {
                                nexec_qs[1]->clear();
                            } else {
                                nexec_qs[1] = new Array<exec_queue_entry>();
                                nexec_qs[1]->init(exec_queue_capacity);
                            }

                            //split current exec_q
                            // split current queue's range in half
                            for (uint64_t j = 0; j < top_entry->exec_q->size(); j++) {
                                exec_queue_entry exec_qe = top_entry->exec_q->get(j);
                                ycsb_request *req = (ycsb_request *) &exec_qe.req_buffer;
                                uint32_t newrange = get_split(req->key, 2, top_entry->range_start, top_entry->range_end);
                                nexec_qs[newrange]->add(exec_qe);
                            }

                            // Allocate memory for split_entry if none are available
                            split_entry_get_or_create(nsp_entries[0], split_entry_free_list);

                            uint64_t range_size = ((top_entry->range_end - top_entry->range_start) / 2);
                            nsp_entries[0]->exec_q = nexec_qs[0];
                            nsp_entries[0]->range_start = top_entry->range_start;
                            nsp_entries[0]->range_end = range_size + top_entry->range_start;
                            nsp_entries[0]->range_size = range_size;
    //                        DEBUG_Q("PL_%ld : pushed split : mrange = %ld, range_start = %ld, range_end = %ld, range_size = %ld\n",
    //                                _planner_id , i, nsp_entries[0]->range_start, nsp_entries[0]->range_end, nsp_entries[0]->range_size
    //                        );
                            pq_test.push((uint64_t) nsp_entries[0]);

                            split_entry_get_or_create(nsp_entries[1], split_entry_free_list);
                            nsp_entries[1]->exec_q = nexec_qs[1];
                            nsp_entries[1]->range_start = range_size + top_entry->range_start;
                            nsp_entries[1]->range_end = top_entry->range_end;
                            nsp_entries[1]->range_size = range_size;
    //                        DEBUG_Q("PL_%ld : pushed split : mrange = %ld,  range_start = %ld, range_end = %ld, range_size = %ld\n",
    //                                _planner_id , i, nsp_entries[1]->range_start, nsp_entries[1]->range_end, nsp_entries[1]->range_size
    //                        );
                            pq_test.push((uint64_t) nsp_entries[1]);

    //                        DEBUG_Q("Planner_%ld : We need to split for range %ld, current size = %ld, range_start = %ld, range_end = %ld"
    //                                        ", into two: "
    //                                        "[0] exec_q_size = %ld, range_start = %ld, range_end=%ld, range_size = %ld, "
    //                                        "[1] exec_q_size = %ld, range_start = %ld, range_end=%ld, range_size = %ld\n",
    //                                _planner_id, i, top_entry->exec_q->size(), top_entry->range_start, top_entry->range_end,
    //                                nsp_entries[0]->exec_q->size(), nsp_entries[0]->range_start, nsp_entries[0]->range_end, nsp_entries[0]->range_size,
    //                                nsp_entries[1]->exec_q->size(), nsp_entries[1]->range_start, nsp_entries[1]->range_end, nsp_entries[1]->range_size
    //                        );


                            // Recycle execution queue
                            while (!work_queue.exec_queue_free_list[_planner_id]->push(top_entry->exec_q)) {};
    //                        top_entry->exec_q = NULL;
    //                        mem_allocator.free(top_entry, 0);
                            if (!split_entry_free_list->push(top_entry)) {
                                M_ASSERT_V(false,
                                           "FREE_LIST_INITIAL_SIZE is not enough for holding split entries, increase the size")
                            };

                            top_entry = (split_entry *) pq_test.top();

                            if (top_entry->exec_q->size() <= exec_queue_limit) {

                                break;
                            } else {

                                split_rounds++;
    //                            top_entry = new SplitEntry();
    //                            top_entry->exec_q = pq_test.top().exec_q;
    //                            top_entry->range_start = pq_test.top().range_start;
    //                            top_entry->range_end = pq_test.top().range_end;
    //                            DEBUG_Q("PL_%ld : poped : mrange = %ld, range_start = %ld, range_end = %ld, range_size = %ld\n",
    //                                    _planner_id, i, top_entry->range_start, top_entry->range_end, top_entry->range_size
    //                            );
                                pq_test.pop();
                            }

                        }
                    } else {
                        split_entry *tmp;
                        split_entry_get_or_create(tmp, split_entry_free_list);

                        tmp->exec_q = exec_queues->get(i);
                        tmp->range_start = (i * bucket_size);
                        tmp->range_end = ((i + 1) * bucket_size);
                        tmp->range_size = bucket_size;
    //                    DEBUG_Q("PL_%ld : pushed original : mrange = %ld, range_start = %ld, range_end = %ld, range_size = %ld\n",
    //                            _planner_id, i, tmp->range_start, tmp->range_end, tmp->range_size
    //                    );
                        pq_test.push((uint64_t) tmp);
                    }
                }

                uint64_t pq_test_size UNUSED = pq_test.size();

                if (!ranges_stored && pq_test.size() != g_thread_cnt) {
                    // we have splits, we need to update ranges
                    ranges_stored = true;
                    uint64_t range_cnt = pq_test.size();

                    exec_qs_ranges->release();
                    exec_qs_ranges->init(range_cnt);

                    while (!pq_test.empty()) {
    //                    split_entry * s_tmp = (split_entry *)pq_test.top();
                        range_sorted.push(pq_test.top());
                        pq_test.pop();
                    }

                    M_ASSERT_V(pq_test_size == range_sorted.size(),
                               "PL_%ld : pg_test_size = %ld, range_sorted_size = %ld\n",
                               _planner_id, pq_test_size, range_sorted.size()
                    );

//                    uint64_t ranges_total_cnt UNUSED = 0;
                    for (uint64_t i = 0; !range_sorted.empty(); ++i) {

                        split_entry *s_tmp = (split_entry *) range_sorted.top();
//                        DEBUG_Q("PL_%ld : pq_test: range_start = %ld, range_end = %ld, range_size = %ld, exec_q_size = %ld\n",
//                                _planner_id, s_tmp->range_start, s_tmp->range_end, s_tmp->range_size, s_tmp->exec_q->size()
//                        );
//                        ranges_total_cnt = s_tmp->range_end;
                        exec_qs_ranges->add(s_tmp->range_end);
                        pq_test.push(range_sorted.top());
                        range_sorted.pop();
                    }

                    exec_qs_ranges->set(exec_qs_ranges->size()-1, g_synth_table_size);

                    M_ASSERT_V(exec_qs_ranges->get(exec_qs_ranges->size()-1) == g_synth_table_size, "PL_%ld: Mismatch table size and range total cnt, "
                            "ranges_total_cnt = %ld, g_synth_table = %ld\n",
                               _planner_id, exec_qs_ranges->get(exec_qs_ranges->size()-1), g_synth_table_size
                    );
                }

            //TODO(tq): This can be optimized
            // Merge and ET assignment phase
            // We utilize two heap, a max-heap and a min-heap
            // The max-heap is used to maintain the list of all EQs to be assigned
            // The min-heap is used to maintain the list of assignment of EQs to ETs
            // We use the min-heap to track the top candidate with the lowest number of operations
            // in its assigned exec_queues, and assign the next largest EQ to it
            // We do this until all EQs are assigned.
            // This gives roughly a balanced assignment to all ETs but we probably need a formal proof of this.
            // It is also useful to have a formal analysis of the time-complexity of this.

            // Final list of EQs are maintained them in a max-heap sorted by their sizes


            for (uint64_t i = 0; i < g_thread_cnt; ++i) {
                assign_entry *a_tmp;
                split_entry *s_tmp = ((split_entry *) pq_test.top());

                assign_entry_get_or_create(a_tmp, assign_entry_free_list);
//                if (!assign_entry_free_list->pop(a_tmp)) {
//                    a_tmp = (assign_entry *) mem_allocator.alloc(sizeof(assign_entry));
//                    memset(a_tmp, 0, sizeof(assign_entry));
//                }

                assign_entry_init(a_tmp, _planner_id);
                a_tmp->exec_thd_id = i;
                assign_entry_add(a_tmp, s_tmp->exec_q);

//                split_entry_print(s_tmp, _planner_id);
                if (!split_entry_free_list->push(s_tmp)) {
                    M_ASSERT_V(false,
                               "FREE_LIST_INITIAL_SIZE is not enough for holding split entries, increase the size")
                };
                pq_test.pop();
                assignment.push((uint64_t) a_tmp);
            }

            while (!pq_test.empty()) {
                assign_entry *a_tmp = (assign_entry *) assignment.top();
                split_entry *s_tmp = ((split_entry *) pq_test.top());
//                split_entry_print(s_tmp, _planner_id);
//                a_tmp->exec_thd_id = assignment.top()->exec_thd_id;
//                for (uint64_t i=0; i < assignment.top()->exec_qs->size(); i++){
//                    a_tmp->add(assignment.top()->exec_qs->get(i));
//                }
                assign_entry_add(a_tmp, s_tmp->exec_q);
                assignment.pop();
                assignment.push((uint64_t) a_tmp);

                if (!split_entry_free_list->push(s_tmp)) {
                    M_ASSERT_V(false,
                               "FREE_LIST_INITIAL_SIZE is not enough for holding split entries, increase the size")
                };
                pq_test.pop();
            }
            M_ASSERT_V(pq_test.size() == 0, "PT_%ld: We have not assigned all splitted EQs", _planner_id);
        }

            // To produce the final assignment we do a heap-sort based on ET's id, this can be optimized further later
            memset(f_assign, 0, sizeof(uint64_t)*g_thread_cnt);

            while (!assignment.empty()){
                assign_entry * te = (assign_entry *) assignment.top();
                f_assign[te->exec_thd_id] = (uint64_t) te->exec_qs;
                assignment.pop();
                assign_entry_clear(te);
                while(!assign_entry_free_list->push(te)){}
            }
//            M_ASSERT_V(final_assignment.size() == g_thread_cnt, "Planner_%ld : Assignment list size = %ld should equal to the thread_cnt = %d\n",
//                    _planner_id, final_assignment.size(), g_thread_cnt
//            );
            M_ASSERT_V(assignment.size() == 0, "PT_%ld: We have not used all assignments in the final assignments", _planner_id);

//            for (uint64_t i = 0; i < g_thread_cnt; i++){
//                uint64_t eq_sum = 0;
//                Array<Array<exec_queue_entry> *> * tmp_qs = (Array<Array<exec_queue_entry> *> *) f_assign[i];
//                for (uint64_t j=0; j < tmp_qs->size(); ++j){
//                    eq_sum += tmp_qs->get(j)->size();
//                }
//                DEBUG_Q("Planner_%ld : Assigned to ET_%ld, %ld exec_qs with total size = %ld\n",
//                        _planner_id, i, tmp_qs->size(),
//                        eq_sum
//                );
//            }

//            DEBUG_Q("Planner_%ld : Going to assign the following EQs to ETs\n",
//                    _planner_id);

#endif // end of if SPLIT_MERGE_ENABLED

            planner_pg->batch_txn_cnt = batch_cnt;
            planner_pg->batch_id = batch_id;
#if BUILD_TXN_DEPS
            planner_pg->txn_dep_graph = txn_dep_graph;
#endif
            planner_pg->batch_starting_txn_id = batch_starting_txn_id;

            // deilvery  phase
            for (uint64_t i = 0; i < g_thread_cnt; i++){

                // create a new batch_partition
                batch_part = (batch_partition *) mem_allocator.alloc(sizeof(batch_partition));
                batch_part->planner_id = _planner_id;
                batch_part->batch_id = batch_id;
//                batch_part->exec_q = exec_queues->get(i);
                batch_part->single_q = true;
                batch_part->batch_part_status.store(0);
                batch_part->exec_q_status.store(0);

#if SPLIT_MERGE_ENABLED
                Array<Array<exec_queue_entry> *> * fa_execqs = ((Array<Array<exec_queue_entry> *> *)f_assign[i]);
                if (fa_execqs->size() > 1){

                    batch_part->single_q = false;
                    // Allocate memory for exec_qs
                    batch_part->sub_exec_qs_cnt = fa_execqs->size();
//                    batch_part->exec_qs = (Array<exec_queue_entry> **) mem_allocator.alloc(
//                            sizeof(Array<exec_queue_entry> *)*batch_part->sub_exec_qs_cnt);
//                    memset(batch_part->exec_qs, 0, sizeof(Array<exec_queue_entry> *)*batch_part->sub_exec_qs_cnt);
                    batch_part->exec_qs = fa_execqs;

                    batch_part->exec_qs_status = (atomic<uint64_t> *) mem_allocator.alloc(
                            sizeof(uint64_t)*batch_part->sub_exec_qs_cnt);
                    memset(batch_part->exec_qs_status, 0, sizeof(uint64_t)*batch_part->sub_exec_qs_cnt);

//                    for (uint64_t j = 0; j < batch_part->sub_exec_qs_cnt; j++){
//                        batch_part->exec_qs[j] = fa_execqs->get(j);
//                        batch_part->exec_qs_status[j].store(AVAILABLE);
//                    }
                }
                else{
                    batch_part->exec_q = fa_execqs->get(0);
                    // recycle fa_exec_q
                    fa_execqs->clear();
                    while(!work_queue.exec_qs_free_list[_planner_id]->push(fa_execqs)){
                        M_ASSERT_V(false, "Should not happen");
                    };
                }

//                fa_execqs->clear();
//                while(!exec_qs_free_list->push(fa_execqs)){}
#else
                batch_part->exec_q = exec_queues->get(i);
#endif
                batch_part->planner_pg = planner_pg;
//                DEBUG_Q("Planner_%ld : Assigned to ET_%ld, %ld exec_qs with total size = %ld\n",
//                        _planner_id, final_assignment.top().exec_thd_id, final_assignment.top().exec_qs.size(),
//                        final_assignment.top().curr_sum
//                );


                // In case batch slot is not ready, spin!
                expected = 0;
                desired = (uint64_t) batch_part;
                while(work_queue.batch_map[slot_num][i][_planner_id].load() != 0) {
                    if(idle_starttime == 0){
                        idle_starttime = get_sys_clock();
                    }
//                    DEBUG_Q("PT_%ld: SPIN!!! for batch %ld  Completed a lap up to map slot [%ld][%ld][%ld]\n", _planner_id, batch_id, slot_num, i, _planner_id);
                }

                // include idle time from spinning above
                if (idle_starttime != 0){
                    INC_STATS(_thd_id,plan_idle_time[_planner_id],get_sys_clock() - idle_starttime);
                    idle_starttime = 0;
                }

                // Deliver batch partition to the repective ET
                while(!work_queue.batch_map[slot_num][i][_planner_id].compare_exchange_strong(
                        expected, desired)){
                    // this should not happen after spinning but can happen if simulation is done
//                    M_ASSERT_V(false, "For batch %ld : failing to SET map slot [%ld][%ld][%ld]\n", batch_id, slot_num, i, _planner_id);
                }
//                DEBUG_Q("Planner_%ld :Batch_%ld for range_%ld ready! b_slot = %ld\n", _planner_id, batch_id, i, slot_num);

            }

            // Spin here if PG map slot is not available
            while(work_queue.batch_pg_map[slot_num][_planner_id].load() != 0) {
                if(idle_starttime == 0){
                    idle_starttime = get_sys_clock();
                }
//                DEBUG_Q("Planner_%ld : spinning for batch_%ld at b_slot = %ld\n", _planner_id, batch_id, slot_num);
            }

            // include idle time from spinning above
            if (idle_starttime != 0){
                INC_STATS(_thd_id,plan_idle_time[_planner_id],get_sys_clock() - idle_starttime);
                idle_starttime = 0;
            }

            // Set priority group pointer in the pg_map
            expected = 0;
            desired = (uint64_t) planner_pg;
//            std::atomic_thread_fence(std::memory_order_seq_cst);
//            DEBUG_Q("Planner_%ld :going to set pg(%ld) for batch_%ld at b_slot = %ld\n", _planner_id, desired, batch_id, slot_num);

            // This CAS operation can fail if CT did not reset the value
            // this means planners nneds to spin
            while(!work_queue.batch_pg_map[slot_num][_planner_id].compare_exchange_strong(
                    expected, desired)){
                // this should not happen after spinning
//                M_ASSERT_V(false, "For batch %ld : failing to SET batch_pg_map slot [%ld][%ld], current value = %ld, \n",
//                           batch_id, slot_num, _planner_id, work_queue.batch_pg_map[slot_num][_planner_id].load());
            }


            txn_ctxs_get_or_create(txn_ctxs, planner_batch_size, _planner_id);
//            std::atomic_thread_fence(std::memory_order_seq_cst);
            // TODO(tq): reuse from a free list instead of allocating new mem. block
//            txn_ctxs = (transaction_context *) mem_allocator.alloc(sizeof(transaction_context)*planner_batch_size);
//            if (work_queue.txn_ctxs_free_list[_planner_id]->pop(txn_ctxs)){
//                memset(txn_ctxs, 0,sizeof(transaction_context)*planner_batch_size );
//            }
//            else{
//                txn_ctxs = (transaction_context *) mem_allocator.alloc(sizeof(transaction_context)*planner_batch_size);
//            }

//            planner_pg = (priority_group *) mem_allocator.alloc(sizeof(priority_group));
            pg_get_or_create(planner_pg, _planner_id);
            planner_pg->planner_id = _planner_id;
            planner_pg->txn_ctxs = txn_ctxs;

            // reset data structures and execution queues for the new batch
            prof_starttime = get_sys_clock();
            exec_queues->clear();
            for (uint64_t i = 0; i < exec_qs_ranges->size(); i++) {
                Array<exec_queue_entry> * exec_q;
//                if (work_queue.exec_queue_free_list[_planner_id]->pop(exec_q)){
//                    exec_q->clear();
////                    DEBUG_Q("Planner_%ld : Reusing exec_q\n", _planner_id);
//                    INC_STATS(_thd_id, plan_reuse_exec_queue_cnt[_planner_id], 1);
//                }
//                else{
////                    exec_q = new Array<exec_queue_entry>();
////                    exec_q->init(exec_queue_capacity);
////                    DEBUG_Q("Planner_%ld: Allocating new exec_q\n", _planner_id);
//                    INC_STATS(_thd_id, plan_alloc_exec_queue_cnt[_planner_id], 1);
//                }
                exec_queue_get_or_create(exec_q, _planner_id, exec_queue_capacity);
                exec_queues->add(exec_q);
            }

            INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock()-prof_starttime);
            INC_STATS(_thd_id, plan_batch_cnts[_planner_id], 1);
            INC_STATS(_thd_id, plan_batch_process_time[_planner_id], get_sys_clock() - batch_start_time);
#if BUILD_TXN_DEPS
            for (auto it = access_table.begin(); it != access_table.end(); ++it){
                delete it->second;
            }
            access_table.clear();

            txn_dep_graph = new hash_table_t();
            M_ASSERT_V(access_table.size() == 0, "Access table is not empty!!\n");
            M_ASSERT_V(txn_dep_graph->size() == 0, "TDG table is not empty!!\n");
#endif
            batch_starting_txn_id = planner_txn_id+1;
            batch_id++;
            batch_cnt = 0;
            batch_start_time = 0;
            batch_starting_txn_id = planner_txn_id;
            INC_STATS(_thd_id, plan_batch_delivery_time[_planner_id], get_sys_clock()-prof_starttime);
        }

        // we have got a message, which is a transaction
        batch_cnt++;
        if (batch_start_time == 0){
            batch_start_time = get_sys_clock();
        }

        switch (msg->get_rtype()) {
            case CL_QRY: {
                // Query from client
//                DEBUG_Q("Planner_%d planning txn %ld\n", _planner_id,msg->txn_id);


#if WORKLOAD == YCSB
                txn_prof_starttime = get_sys_clock();
                // create transaction context
                // TODO(tq): here also we are dynamically allocting memory, we should use a pool recycle
//                prof_starttime = get_sys_clock();

                // Reset tctx memblock for reuse
//                memset(tctx, 0, sizeof(transaction_context));
                transaction_context *tctx = &txn_ctxs[batch_cnt];

//                INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock() - prof_starttime);
                tctx->txn_id = planner_txn_id;
                tctx->txn_state = TXN_INITIALIZED;
                tctx->completion_cnt.store(0);
                tctx->client_startts = ((ClientQueryMessage *) msg)->client_startts;
                tctx->batch_id = batch_id;


                // Analyze read-write set
                /* We need to determine the ranges needed for each key
                 * We group keys that fall in the same range to be processed together
                 * TODO(tq): add repartitioning
                 */
                YCSBClientQueryMessage *ycsb_msg = ((YCSBClientQueryMessage *) msg);
                for (uint64_t j = 0; j < ycsb_msg->requests.size(); j++) {
                    memset(entry, 0, sizeof(exec_queue_entry));
                    ycsb_request *ycsb_req = ycsb_msg->requests.get(j);
                    uint64_t key = ycsb_req->key;
//                    DEBUG_Q("Planner_%d looking up bucket for key %ld\n", _planner_id, key);

                    // TODO(tq): use get split with dynamic ranges
                    uint64_t idx = get_split(key, exec_qs_ranges);
//                    uint64_t idx = get_bucket(key);

//                    DEBUG_Q("Planner_%d using bucket %ld for key %ld\n", _planner_id,idx, key);

                    // TODO(tq): move to statitic module
#if DEBUG_QUECC
//                    uint64_t nval = mrange_cnts.get(idx) + 1;
//                    mrange_cnts.set(idx, nval);
                    total_access_cnt++;
#endif

                    // create execution entry, for now it will contain only one request
                    // we dont need to allocate memory here
                    prof_starttime = get_sys_clock();
                    INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock() - prof_starttime);
                    Array<exec_queue_entry> *mrange = exec_queues->get(idx);
                    // increment of batch mrange to use the next entry slot
                    entry->txn_id = planner_txn_id;
                    entry->txn_ctx = tctx;
                    entry->batch_id = batch_id;
                    assert(tctx->batch_id == batch_id);
#if !SERVER_GENERATE_QUERIES
                    assert(msg->return_node_id != g_node_id);
#endif
                    entry->return_node_id = msg->return_node_id;

                    // Dirty code
                    ycsb_request * req_buff = (ycsb_request *) &entry->req_buffer;
                    req_buff->acctype = ycsb_req->acctype;
                    req_buff->key = ycsb_req->key;
                    req_buff->value = ycsb_req->value;

                    // add entry into range/bucket queue
                    // entry is a sturct, need to double check if this works
                    mrange->add(*entry);
                    prof_starttime = get_sys_clock();

                    // TODO(tq): optimize this by reusing a single memory block
//                    mem_allocator.free(entry, sizeof(exec_queue_entry));
                    INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock() - prof_starttime);

#if BUILD_TXN_DEPS
                    // add to dependency graph if needed
                    // lookup key in the access_table
                    // if key is not found:
                    //      if access type is write:
                    //          allocate a vector and append txnid and insert (key,vector) into access_table
                    //      if access type is read: do nothing
                    // if key is found:
                    //      if access type is write:
                    //          append to existing vector in access_table
                    //      if access type is read:
                    //          get the last txnd id from vector and insert into tdg



                    auto search = access_table.find(key);
                    if (search != access_table.end()){
                        // found
                        if (ycsb_req->acctype == WR){
                            search->second->push_back(planner_txn_id);
                            // this is a write-write conflict,
                            // but since this is a single record operation, no need for dependencies
                        }
                        else{
                            M_ASSERT_V(ycsb_req->acctype == RD, "only RD access type is supported");
                            M_ASSERT_V(search->second->back() >= batch_starting_txn_id,
                                       "invalid txn_id in access table!! last_txn_id = %ld, batch_starting_txn_id = %ld\n",
                                       search->second->back(), batch_starting_txn_id
                            );
                            auto search_txn = txn_dep_graph->find(planner_txn_id);
                            if (search_txn != txn_dep_graph->end()){
                                search_txn->second->push_back(search->second->back());
                            }
                            else{
                                // first operation for this txn_id
                                std::vector<uint64_t> * txn_list = new std::vector<uint64_t>();

                                txn_list->push_back(search->second->back());
                                txn_dep_graph->insert({planner_txn_id, txn_list});
                            }
//                            DEBUG_Q("PT_%ld : txn_id = %ld depends on txn_id = %ld\n", _thd_id, planner_txn_id, (uint64_t) search->second->back());
                        }
                    }
                    else{
                        // not found
                        if (ycsb_req->acctype == WR){
                            std::vector<uint64_t> * txn_list = new std::vector<uint64_t>();
                            txn_list->push_back(planner_txn_id);
                            access_table.insert({ycsb_req->key, txn_list});
                        }
                    }
#endif
                }

#if DEBUG_QUECC
                total_msg_processed_cnt++;
#endif
                INC_STATS(_thd_id,plan_txn_process_time[_planner_id], get_sys_clock() - txn_prof_starttime);
                // Free message, as there is no need for it anymore
                msg->release();
                // increment for next ransaction
                planner_txn_id++;
#endif
                break;
            }
            default:
                assert(false);
        }

    }

    mem_allocator.free(entry, sizeof(exec_queue_entry));
    INC_STATS(_thd_id, plan_total_time[_planner_id], get_sys_clock()-plan_starttime);
#if DEBUG_QUECC
    DEBUG_Q("Planner_%ld: Total access cnt = %ld, and processed %ld msgs\n", _planner_id, total_access_cnt, total_msg_processed_cnt);
//    for (uint64_t i = 0; i < g_thread_cnt; i++) {
//        DEBUG_Q("Planner_%ld: Access cnt for bucket_%ld = %ld\n",_planner_id, i, mrange_cnts.get(i));
//    }
    uint64_t txn_free_list_cnt = 0;
    transaction_context * txn_ctxs_tmp = NULL;
    while(work_queue.txn_ctxs_free_list[_planner_id]->pop(txn_ctxs_tmp)){
        txn_free_list_cnt++;
    }

    DEBUG_Q("Planner_%ld: created %ld txn_ctxs\n",_planner_id, txn_free_list_cnt);


//    uint64_t exec_q_free_list_cnt = 0;
//    while(work_queue.exec_queue_free_list[wplanner_id]->pop(exec_q)){
//        exec_q_free_list_cnt++;
//    }
//    DEBUG_Q("PT_%ld: free %ld exec_q\n",_planner_id, exec_q_free_list_cnt);
#endif


    printf("FINISH PT %ld:%ld\n",_node_id,_thd_id);
    fflush(stdout);
    return FINISH;

}