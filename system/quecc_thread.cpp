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
#include <ycsb.h>
#include <tpcc.h>
#include <tpcc_helper.h>
#include "index_hash.h"
#include <catalog.h>

#if CC_ALG == QUECC
void assign_entry_clear(assign_entry * &a_entry){
    a_entry->exec_qs = NULL;
    a_entry->exec_thd_id = 0;
    a_entry->curr_sum = 0;
}

void assign_entry_add(assign_entry * a_entry, Array<exec_queue_entry> * exec_q) {
    a_entry->curr_sum += exec_q->size();
    a_entry->exec_qs->add(exec_q);
}

void assign_entry_init(assign_entry * &a_entry, uint64_t pt_thd_id){
    if (!a_entry->exec_qs){
        quecc_pool.exec_qs_get_or_create(a_entry->exec_qs, pt_thd_id);
    }
    else{
        a_entry->exec_qs->clear();
    }
}

void assign_entry_get_or_create(assign_entry *&a_entry, boost::lockfree::spsc_queue<assign_entry *> * assign_entry_free_list){
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

#if CT_ENABLED
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
#if COMMIT_BEHAVIOR == AFTER_PG_COMP
        if (work_queue.batch_map_comp_cnts[batch_slot][cplanner_id].load() != g_thread_cnt){
            continue;
        }
#elif COMMIT_BEHAVIOR == AFTER_BATCH_COMP
        if (work_queue.batch_map_comp_cnts[batch_slot].load() != g_thread_cnt){
            continue;
        }
#endif
//        DEBUG_Q("CT_%ld : Ready to process priority group %ld for batch_id = %ld, batch_slot = %ld\n",
//                _thd_id, cplanner_id, batch_id, batch_slot);

        // get the pointer to the transaction contexts of the current priority group
#if BATCHING_MODE == TIME_BASED
        tmp_p = work_queue.batch_pg_map[batch_slot][cplanner_id].load();

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

#else
        tmp_p = work_queue.batch_pg_map[batch_slot][cplanner_id].status.load();
        M_ASSERT_V(tmp_p == PG_READY, "CT_%ld : planner_pg is not ready, batch_slot = %ld, batch_id = %ld, cplanner_id = %ld, tmp_p = %ld\n",
                   _thd_id, batch_slot, batch_id, cplanner_id, tmp_p);
        planner_pg = &work_queue.batch_pg_map[batch_slot][cplanner_id];

#endif
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

#if COMMIT_BEHAVIOR == AFTER_PG_COMP
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
#endif

#if BATCHING_MODE == TIME_BASED
        // return txn_ctxs to the pool
        txn_ctxs_release(txn_ctxs, cplanner_id);
#endif

#if BUILD_TXN_DEPS
        // Clean up and clear txn_graph
        for (auto it = planner_pg->txn_dep_graph->begin(); it != planner_pg->txn_dep_graph->end(); ++it){
            delete it->second;
        }
        delete planner_pg->txn_dep_graph;
#endif

#if BATCHING_MODE == TIME_BASED
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
#else
        // Allow PTs to proceed
        expected = PG_READY;
        desired = PG_AVAILABLE;
//        DEBUG_Q("CT_%ld :going to reset pg(%ld) for batch_%ld at b_slot = %ld\n", _thd_id, cplanner_id, batch_id, batch_slot);
        while(!work_queue.batch_pg_map[batch_slot][cplanner_id].status.compare_exchange_strong(
                expected, desired)){
            // this should not happen after spinning
            M_ASSERT_V(false, "CT_%ld : For batch %ld : failing to RESET batch_pg_map slot [%ld][%ld], current_value = %ld, expected = %ld\n",
                       _thd_id, batch_id, batch_slot, cplanner_id,
                       work_queue.batch_pg_map[batch_slot][cplanner_id].status.load(), expected
            );
        }
#endif
        cplanner_id++;

        if (cplanner_id == g_plan_thread_cnt){
#if CT_ENABLED && COMMIT_BEHAVIOR == AFTER_BATCH_COMP
            desired8 = 0;
            expected8 = (uint8_t) g_thread_cnt;
            // Allow ETs to proceed
            while(!work_queue.batch_map_comp_cnts[batch_slot].compare_exchange_strong(
                expected8, desired8)){
            // this should not happen
            M_ASSERT_V(false, "CT_%ld : For batch %ld : failing to RESET batch_map_comp_cnts slot [%ld][%ld],"
                    " current value = %d, expected = %d\n",
                       _thd_id, batch_id, batch_slot, cplanner_id,
                       work_queue.batch_map_comp_cnts[batch_slot].load(), expected8);

        }
#endif

            // proceed to the next batch
            batch_id++;
            batch_slot = batch_id % g_batch_map_length;
            cplanner_id = 0;
        }
    }

    // cleanup
    desired8 = 0;
    desired = PG_AVAILABLE;
    for (uint64_t i = 0; i < g_batch_map_length; ++i){
#if CT_ENABLED && COMMIT_BEHAVIOR == AFTER_BATCH_COMP
        work_queue.batch_map_comp_cnts[i].store(desired8);
#endif
        for (uint64_t j = 0; j < g_plan_thread_cnt; ++j){
            // we need to signal all ETs that are spinning
#if CT_ENABLED && COMMIT_BEHAVIOR == AFTER_PG_COMP
            work_queue.batch_map_comp_cnts[i][j].store(desired8);
#endif
            // Signal all spinning PTs
#if BATCHING_MODE == SIZE_BASED
            work_queue.batch_pg_map[i][j].status.store(desired);
#else
            work_queue.batch_pg_map[i][j].store(desired);
#endif
        }
    }

    printf("FINISH CT %ld:%ld\n",_node_id,_thd_id);
    fflush(stdout);
    return FINISH;

}

#endif //CT_ENABLED

void PlannerThread::setup() {
}

//uint32_t PlannerThread::get_bucket(uint64_t key) {
//    uint64_t no_buckets = g_thread_cnt;
//#if WORKLOAD == YCSB
//    for (uint64_t i = 0; i < no_buckets; i++){
//        if (key >= (i*bucket_size) && key < ((i+1)*bucket_size)){
//            return i;
//        }
//    }
//#endif
//    return 0;
//}

uint32_t PlannerThread::get_split(uint64_t key, uint32_t range_cnt, uint64_t range_start, uint64_t range_end) {
    uint64_t range_size = (range_end - range_start)/range_cnt;
    for (uint64_t i = 0; i < range_cnt; i++){
        if (key >= ((i*range_size)+range_start) && key < (((i+1)*range_size)+range_start)){
            return i;
        }
    }
    return 0;
}


RC PlannerThread::run() {

#if MODE == NORMAL_MODE
    return run_normal_mode();
#elif MODE == FIXED_MODE
    return run_fixed_mode();
#else
    M_ASSERT(false, "Selected mode is not supported\n");
    return FINISH;
#endif
}
#if MODE == FIXED_MODE
RC PlannerThread::run_fixed_mode() {
    tsetup();

    Message * msg;

    uint64_t idle_starttime = 0;
    uint64_t prof_starttime = 0;
    uint64_t batch_cnt = 0;

    uint64_t txn_prefix_base = 0x0010000000000000;
    uint64_t txn_prefix_planner_base = (_planner_id * txn_prefix_base);

    assert(UINT64_MAX > (txn_prefix_planner_base+txn_prefix_base));

    DEBUG_Q("Planner_%ld thread started, txn_ids start at %ld \n", _planner_id, txn_prefix_planner_base);
    uint64_t planner_batch_size = g_batch_size/g_plan_thread_cnt;
#if SPLIT_MERGE_ENABLED && SPLIT_STRATEGY == LAZY_SPLIT
//    uint64_t exec_queue_limit = planner_batch_size/g_thread_cnt;
    uint64_t exec_queue_limit = quecc_pool.exec_queue_capacity;
#endif

#if SPLIT_MERGE_ENABLED

#if SPLIT_STRATEGY == LAZY_SPLIT
    split_max_heap_t pq_test;
    split_min_heap_t range_sorted;
    exec_queue_limit = (planner_batch_size/g_thread_cnt) * REQ_PER_QUERY * EXECQ_CAP_FACTOR;
    boost::lockfree::spsc_queue<split_entry *> * split_entry_free_list =
            new boost::lockfree::spsc_queue<split_entry *>(FREE_LIST_INITIAL_SIZE*10);
    split_entry ** nsp_entries = (split_entry **) mem_allocator.alloc(sizeof(split_entry*)*2);
    volatile bool ranges_stored = false;
     Array<exec_queue_entry> **nexec_qs = (Array<exec_queue_entry> **) mem_allocator.alloc(
            sizeof(Array<exec_queue_entry> *) * 2);

#elif SPLIT_STRATEGY == EAGER_SPLIT
    Array<uint64_t> * exec_qs_ranges_tmp = new Array<uint64_t>();
    exec_qs_ranges_tmp->init(g_exec_qs_max_size);

    Array<Array<exec_queue_entry> *> * exec_queues_tmp;
    exec_queues_tmp = new Array<Array<exec_queue_entry> *>();
    //FIXME(tq): remove this condition
#if WORKLOAD == YCSB
    Array<uint64_t> * exec_qs_ranges_tmp_tmp = new Array<uint64_t>();
    Array<Array<exec_queue_entry> *> * exec_queues_tmp_tmp;
#endif
    exec_queues_tmp->init(g_exec_qs_max_size);

#endif

    assign_ptr_min_heap_t assignment;
    uint64_t * f_assign = (uint64_t *) mem_allocator.alloc(sizeof(uint64_t)*g_thread_cnt);
    boost::lockfree::spsc_queue<assign_entry *> * assign_entry_free_list =
            new boost::lockfree::spsc_queue<assign_entry *>(FREE_LIST_INITIAL_SIZE);

#endif

    // Array to track ranges
    Array<uint64_t> * exec_qs_ranges = new Array<uint64_t>();
    exec_qs_ranges->init(g_exec_qs_max_size);
#if WORKLOAD == YCSB
    for (uint64_t i =0; i<g_thread_cnt; ++i){

        if (i == 0){
            exec_qs_ranges->add(bucket_size);
            continue;
        }

        exec_qs_ranges->add(exec_qs_ranges->get(i-1)+bucket_size);
    }
#endif
    // create and and pre-allocate execution queues
    // For each mrange which will be assigned to an execution thread
    // there will be an array pointer.
    // When the batch is complete we will CAS the exec_q array to allow
    // execution threads to be

// implementtion using array class
    Array<Array<exec_queue_entry> *> * exec_queues;
    exec_queues = new Array<Array<exec_queue_entry> *>();

    exec_queues->init(g_exec_qs_max_size);

    uint64_t et_id = 0;
    for (uint64_t i = 0; i < g_thread_cnt; i++) {
        Array<exec_queue_entry> * exec_q;
        quecc_pool.exec_queue_get_or_create(exec_q, _planner_id, i);
        exec_queues->add(exec_q);
    }

    boost::random::mt19937 plan_rng;
    boost::random::uniform_int_distribution<> eq_idx_rand(0, g_thread_cnt-1);

    transaction_context * txn_ctxs UNUSED = NULL;
    priority_group * planner_pg;
    batch_id = 0;

    slot_num = (batch_id % g_batch_map_length);

    // Implement for SIZE_BASED
    M_ASSERT_V(BATCHING_MODE == SIZE_BASED, "Only size-based batching is supported for this mode\n");

#if DEBUG_QUECC
    uint64_t total_msg_processed_cnt = 0;
    uint64_t total_access_cnt = 0;
#endif
    planner_txn_id = txn_prefix_planner_base;

#if BUILD_TXN_DEPS
//    txn_dep_graph = new hash_table_t();
    txn_dep_graph = new hash_table_tctx_t();
#endif
    uint64_t batch_starting_txn_id = planner_txn_id;
    exec_queue_entry *entry = (exec_queue_entry *) mem_allocator.align_alloc(sizeof(exec_queue_entry));

    for (uint64_t slot_num = 0; slot_num < g_batch_map_length; ++slot_num){
//        DEBUG_Q("PL_%ld: Starting to work on planner_pg with slot_num =%ld\n", _planner_id, slot_num);
        planner_pg = &work_queue.batch_pg_map[slot_num][_planner_id];
        txn_ctxs = planner_pg->txn_ctxs;
//        planner_pg->planner_id = _planner_id;

        while (true){
            msg = work_queue.plan_dequeue(_thd_id, _planner_id);
            M_ASSERT_V(msg, "We should have a message here\n");

            force_batch_delivery = (batch_cnt == planner_batch_size);
            if (force_batch_delivery) {

                batch_partition *batch_part = NULL;

                prof_starttime = get_sys_clock();
#if SPLIT_MERGE_ENABLED

#if SPLIT_STRATEGY == EAGER_SPLIT
                // we just need compute assignment since all EQs satisfy the limit

                //Logically merge EQs.
                // We group EQs and assign each group to an ET
                // Size-balance merging
                for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
                    assign_entry *a_tmp;
                    if (i < g_thread_cnt){
                        // create an assignment entry for each ET and assign it a single EQ
                        assign_entry_get_or_create(a_tmp, assign_entry_free_list);
                        assign_entry_init(a_tmp, _planner_id);
                        a_tmp->exec_thd_id = i;

                        assign_entry_add(a_tmp, exec_queues->get(i));
                        assignment.push((uint64_t) a_tmp);
                    }
                    else {
                        // After the initial assignment is done (i.e. each group has a single EQ assigned to it)
                        // We group based on their sizes
                        a_tmp = (assign_entry *) assignment.top();
                        assign_entry_add(a_tmp, exec_queues->get(i));
                        assignment.pop();
                        assignment.push((uint64_t) a_tmp);
                    }
                }

//                 Given that each the current EQs are ordered based on their ranges.
//                DEBUG_Q("PT_%ld: the plan for batch_id=%ld\n",
//                        _planner_id, batch_id);
//                for (uint64_t i = 0; i < exec_qs_ranges->size(); ++i){
//                    uint64_t r_start = 0;
//                    if (i > 0){
//                        r_start = exec_qs_ranges->get(i-1);
//                    }
//                    uint64_t r_end = exec_qs_ranges->get(i);
//                    DEBUG_Q("PT_%ld: EQ[%ld] has %ld entries range_start = %ld, range_end = %ld\n",
//                            _planner_id,i, exec_queues->get(i)->size(),r_start, r_end);
//
//                }

                // Check if we need to merge

                // Spatial locality perseving merge
//                uint64_t ae_limit = quecc_pool.exec_queue_capacity;
//                DEBUG_Q("PT_%ld: ae_limit=%ld\n", _planner_id, ae_limit);
//                uint64_t eq_idx = 0;
//                for (uint64_t i = 0; i < g_thread_cnt; ++i){
//                    assign_entry *a_tmp = NULL;
//                    uint64_t ae_count = 0;
//                    uint64_t eq_count = 0;
//                    while (true){
//                        // create an AE for this
//                        if (a_tmp == NULL){
//                            assign_entry_get_or_create(a_tmp, assign_entry_free_list);
//                            assign_entry_init(a_tmp, _planner_id);
//                            a_tmp->exec_thd_id = i;
//                        }
//
//                        if (i == g_thread_cnt-1){
//                            // assign remaining entries regardless of size
//                            while (eq_idx < exec_queues->size()){
//                                assign_entry_add(a_tmp, exec_queues->get(eq_idx));
//                                eq_count++;
//                                ae_count += exec_queues->get(eq_idx)->size();
//                                eq_idx++;
//                            }
//                            assignment.push((uint64_t) a_tmp);
//                            break;
//                        }
//                        else if (eq_idx < exec_queues->size()){
//                            assign_entry_add(a_tmp, exec_queues->get(eq_idx));
//                            ae_count += exec_queues->get(eq_idx)->size();
//                            eq_count++;
//                            if (ae_count >= ae_limit){
//                                assignment.push((uint64_t) a_tmp);
//                                eq_idx++; // increament before breaking out of while (true)
////                                DEBUG_Q("PT_%ld: Assigned %ld EQs to ET_%ld, remaining EQ cnt = %ld, total EQE= %ld, limit = %ld\n",
////                                        _planner_id, eq_count, i,
////                                        exec_queues->size()-eq_idx, ae_count, ae_limit);
//                                break;
//                            }
//                        }
//                        else {
//                            // push assignment with empty EQs
//                            assignment.push((uint64_t) a_tmp);
//                            break;
//                        }
//                        eq_idx++;
//                    }
//
//                    DEBUG_Q("PT_%ld: Assigned %ld EQs to ET_%ld, remaining EQ cnt = %ld, total EQE= %ld, limit = %ld\n",
//                            _planner_id, eq_count, i,
//                            exec_queues->size()-eq_idx, ae_count, ae_limit);
//                }
#else
                // Splitting phase (and merge if there is no need to split)
                if (ranges_stored) {

                    for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
                        assign_entry *a_tmp;
                        if (i < g_thread_cnt){

                            assign_entry_get_or_create(a_tmp, assign_entry_free_list);

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

                            // We keep splitting until the largest EQ satisfies the limit
                            while (true) {

                                // Allocate memory for new exec_qs

                                et_id = eq_idx_rand(plan_rng);
                                quecc_pool.exec_queue_get_or_create(nexec_qs[0], _planner_id, et_id);
                                et_id = eq_idx_rand(plan_rng);
                                quecc_pool.exec_queue_get_or_create(nexec_qs[1], _planner_id, et_id);

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
                                et_id = eq_idx_rand(plan_rng);
                                quecc_pool.exec_queue_release(top_entry->exec_q, _planner_id, et_id);

                                if (!split_entry_free_list->push(top_entry)) {
                                    M_ASSERT_V(false,
                                               "FREE_LIST_INITIAL_SIZE is not enough for holding split entries, increase the size")
                                };

                                top_entry = (split_entry *) pq_test.top();

                                if (top_entry->exec_q->size() <= exec_queue_limit) {

                                    break;
                                } else {

                                    split_rounds++;
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
                            range_sorted.push(pq_test.top());
                            pq_test.pop();
                        }

                        M_ASSERT_V(pq_test_size == range_sorted.size(),
                                   "PL_%ld : pg_test_size = %ld, range_sorted_size = %ld\n",
                                   _planner_id, pq_test_size, range_sorted.size()
                        );


                        for (uint64_t i = 0; !range_sorted.empty(); ++i) {

                            split_entry *s_tmp = (split_entry *) range_sorted.top();
//                        DEBUG_Q("PL_%ld : pq_test: range_start = %ld, range_end = %ld, range_size = %ld, exec_q_size = %ld\n",
//                                _planner_id, s_tmp->range_start, s_tmp->range_end, s_tmp->range_size, s_tmp->exec_q->size()
//                        );
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

#endif

                // To produce the final assignment we do a heap-sort based on ET's id, this can be optimized further later
                memset(f_assign, 0, sizeof(uint64_t)*g_thread_cnt);

                while (!assignment.empty()){
                    assign_entry * te = (assign_entry *) assignment.top();
                    f_assign[te->exec_thd_id] = (uint64_t) te->exec_qs;
                    assignment.pop();
                    assign_entry_clear(te);
                    while(!assign_entry_free_list->push(te)){}
                }
                M_ASSERT_V(assignment.size() == 0, "PT_%ld: We have not used all assignments in the final assignments", _planner_id);

//            DEBUG_Q("Planner_%ld : Going to assign the following EQs to ETs\n",
//                    _planner_id);

#endif // end of if SPLIT_MERGE_ENABLED

//                planner_pg->batch_txn_cnt = batch_cnt;
//                planner_pg->batch_id = batch_id;
#if BUILD_TXN_DEPS
                planner_pg->txn_dep_graph = txn_dep_graph;
#endif
                planner_pg->batch_starting_txn_id = batch_starting_txn_id;

                // deilvery  phase
                for (uint64_t i = 0; i < g_thread_cnt; i++){
                    // create a new batch_partition
                    quecc_pool.batch_part_get_or_create(batch_part, _planner_id, i);
//                    batch_part->planner_id = _planner_id;
//                    batch_part->batch_id = batch_id;
                    batch_part->single_q = true;
//                    batch_part->status.store(0);
                    batch_part->exec_q_status.store(0);

#if SPLIT_MERGE_ENABLED
                    Array<Array<exec_queue_entry> *> * fa_execqs = ((Array<Array<exec_queue_entry> *> *)f_assign[i]);
                    if (fa_execqs->size() > 1){

                        batch_part->single_q = false;
                        // Allocate memory for exec_qs
//                        batch_part->sub_exec_qs_cnt = fa_execqs->size();
                        batch_part->exec_qs = fa_execqs;
//                        quecc_pool.exec_qs_status_get_or_create(batch_part->exec_qs_status, _planner_id, i);

                    }
                    else{
                        batch_part->exec_q = fa_execqs->get(0);
                        // recycle fa_exec_q
                        fa_execqs->clear();
                        quecc_pool.exec_qs_release(fa_execqs, _planner_id);

                    }

#else
                    batch_part->exec_q = exec_queues->get(i);
#endif
                    batch_part->planner_pg = planner_pg;

                    // In case batch slot is not ready, spin!
                    expected = 0;
                    desired = (uint64_t) batch_part;
#if BATCH_MAP_ORDER == BATCH_ET_PT
                    while(work_queue.batch_map[slot_num][i][_planner_id].fetch_add(0) != 0) {
                        if(idle_starttime == 0){
                            idle_starttime = get_sys_clock();
                            DEBUG_Q("PT_%ld: SPINNING!!! for batch %ld  Completed a lap up to map slot [%ld][%ld][%ld]\n", _planner_id, batch_id, slot_num, i, _planner_id);
                        }
//                        M_ASSERT_V(false, "PT_%ld : this should not happen! for batch_%ld : [%ld][%ld][%ld]\n",
//                                   _planner_id, batch_id, slot_num, i, _planner_id)
                    }
#else
                    while(work_queue.batch_map[slot_num][_planner_id][i].fetch_add(0) != 0) {
                        if(idle_starttime == 0){
                            idle_starttime = get_sys_clock();
                            DEBUG_Q("PT_%ld: SPINNING!!! for batch %ld  Completed a lap up to map slot [%ld][%ld][%ld]\n", _planner_id, batch_id, slot_num, i, _planner_id);
                        }
//                        M_ASSERT_V(false, "PT_%ld : this should not happen! for batch_%ld : [%ld][%ld][%ld]\n",
//                                   _planner_id, batch_id, slot_num, i, _planner_id)
                    }
#endif

                    // include idle time from spinning above
                    if (idle_starttime != 0){
                        INC_STATS(_thd_id,plan_idle_time[_planner_id],get_sys_clock() - idle_starttime);
                        idle_starttime = 0;
                    }

                    // Deliver batch partition to the repective ET
#if BATCH_MAP_ORDER == BATCH_ET_PT
                    while(!work_queue.batch_map[slot_num][i][_planner_id].compare_exchange_strong(expected, desired)){
                        M_ASSERT_V(false, "For batch %ld : failing to SET map slot [%ld][%ld][%ld]\n", batch_id, slot_num, i, _planner_id);
                    }
#else
                    while(!work_queue.batch_map[slot_num][_planner_id][i].compare_exchange_strong(expected, desired)){
                        M_ASSERT_V(false, "For batch %ld : failing to SET map slot [%ld][%ld][%ld]\n", batch_id, slot_num, i, _planner_id);
                    }
#endif

//                DEBUG_Q("Planner_%ld :Batch_%ld for range_%ld ready! b_slot = %ld\n", _planner_id, batch_id, i, slot_num);

                }
#if ATOMIC_PG_STATUS
                expected8 = PG_AVAILABLE;
                desired8 = PG_READY;
                if (!planner_pg->status.compare_exchange_strong(expected8, desired8)){
                    M_ASSERT_V(false, "For batch %ld : failed to SET status for planner_pg [%ld][%ld], value = %d\n",
                               batch_id, slot_num, _planner_id, planner_pg->status.load());
                }
#endif

                // reset data structures and execution queues for the new batch
                prof_starttime = get_sys_clock();
                exec_queues->clear();
                for (uint64_t i = 0; i < exec_qs_ranges->size(); i++) {
                    Array<exec_queue_entry> * exec_q;
                    if (i >= g_thread_cnt){
                        et_id = eq_idx_rand.operator()(plan_rng);
                    }
                    else{
                        et_id = i;
                    }
                    quecc_pool.exec_queue_get_or_create(exec_q, _planner_id, et_id);
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

//            txn_dep_graph = new hash_table_t();
            txn_dep_graph = new hash_table_tctx_t();
            M_ASSERT_V(access_table.size() == 0, "Access table is not empty!!\n");
            M_ASSERT_V(txn_dep_graph->size() == 0, "TDG table is not empty!!\n");
#endif
                batch_id++;
                batch_cnt = 0;
                batch_start_time = 0;
                batch_starting_txn_id = planner_txn_id;

//            DEBUG_Q("PL_%ld: Starting to work on planner_pg with batch_id = %ld, with slot_num =%ld\n",
//                    _planner_id, batch_id, slot_num);
//                planner_pg = &work_queue.batch_pg_map[slot_num][_planner_id];
                // maybe we don't need this if we are going to overwrite all fields
//                txn_ctxs = planner_pg->txn_ctxs;

//                planner_pg->planner_id = _planner_id;

                if (batch_start_time == 0){
//                    batch_start_time = get_sys_clock();
                }

                // exit loop to start the next batch
                break;
            } // end of force-delivery

            // we have got a message, which is a transaction
            batch_cnt++;

            switch (msg->get_rtype()) {
                case CL_QRY: {
                    // Query from client
//                DEBUG_Q("Planner_%d planning txn %ld\n", _planner_id,msg->txn_id);


#if WORKLOAD == YCSB
                    txn_prof_starttime = get_sys_clock();
                    transaction_context *tctx = &txn_ctxs[batch_cnt-1];

                    tctx->txn_id = planner_txn_id;
                    tctx->txn_state = TXN_INITIALIZED;
//                    tctx->completion_cnt.store(0);
                    tctx->completion_cnt = 0;
#if !SERVER_GENERATE_QUERIES
                    tctx->client_startts = ((ClientQueryMessage *) msg)->client_startts;
#endif
//                    tctx->batch_id = batch_id;


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

//                    DEBUG_Q("Planner_%d using bucket %ld for key %ld\n", _planner_id,idx, key);

#if DEBUG_QUECC
                    total_access_cnt++;
#endif

                        Array<exec_queue_entry> *mrange;
                        mrange = exec_queues->get(idx);
#if SPLIT_MERGE_ENABLED && SPLIT_STRATEGY == EAGER_SPLIT
                        if (mrange->is_full()){
                            // we need to split
//                            uint64_t mrange_size;
                            uint64_t c_range_start;
                            uint64_t c_range_end;

                            if (idx == 0){
                                c_range_start = 0;
                                c_range_end = exec_qs_ranges->get(idx);
                            }
                            else{
                                c_range_start = exec_qs_ranges->get(idx-1);;
                                c_range_end = exec_qs_ranges->get(idx);
                            }

                            DEBUG_Q("Planner_%ld : Eagerly we need to split current size = %ld,"
                                            " batch_cnt = %ld, planner_batch_size = %ld, batch_id = %ld, c_range_start = %ld, c_range_end = %ld"
                                            "\n",
                                    _planner_id, mrange->size(), batch_cnt, planner_batch_size, batch_id, c_range_start, c_range_end);

                            uint64_t split_point = (c_range_end-c_range_start)/2;

                            // compute new ranges
                            exec_qs_ranges_tmp->clear();
                            exec_queues_tmp->clear();

                            for (uint64_t r=0; r < exec_qs_ranges->size(); ++r){
                                if (r == idx){
                                    // insert split
                                    exec_qs_ranges_tmp->add(split_point+c_range_start);
                                    // add two new and empty exec_queues
                                    Array<exec_queue_entry> * nexec_q;
                                    Array<exec_queue_entry> * oexec_q;
                                    et_id = eq_idx_rand(plan_rng);
                                    quecc_pool.exec_queue_get_or_create(oexec_q, _planner_id, et_id);
                                    quecc_pool.exec_queue_get_or_create(nexec_q, _planner_id, et_id);
                                    exec_queues_tmp->add(oexec_q);
                                    exec_queues_tmp->add(nexec_q);
                                }
                                else{
                                    exec_queues_tmp->add(exec_queues->get(r));
                                }
                                exec_qs_ranges_tmp->add(exec_qs_ranges->get(r));
                            }

                            DEBUG_Q("Planner_%ld : New ranges size = %ld, old ranges size = %ld"
                                            "\n",
                                    _planner_id, exec_qs_ranges_tmp->size(), exec_qs_ranges->size());

                            uint64_t nidx;
                            // use new ranges to split current execq
                            for (uint64_t r =0; r < mrange->size(); ++r){
                                ycsb_request *ycsb_req_tmp = (ycsb_request *) mrange->get(r).req_buffer;
                                nidx = get_split(ycsb_req_tmp->key, exec_qs_ranges_tmp);
                                assert(nidx == idx || nidx == (idx+1));
                                exec_queues_tmp->get(nidx)->add(mrange->get(r));
                            }

                            // swap data structures
                            exec_queues_tmp_tmp = exec_queues;
                            exec_qs_ranges_tmp_tmp = exec_qs_ranges;

                            exec_queues = exec_queues_tmp;
                            exec_qs_ranges = exec_qs_ranges_tmp;

                            exec_queues_tmp = exec_queues_tmp_tmp;
                            exec_qs_ranges_tmp = exec_qs_ranges_tmp_tmp;

                            DEBUG_Q("Planner_%ld : After swapping New ranges size = %ld, old ranges size = %ld"
                                            "\n",
                                    _planner_id, exec_qs_ranges->size(), exec_qs_ranges_tmp->size());


                            // use the new ranges to assign the new execution entry
                            idx = get_split(key, exec_qs_ranges);
                            mrange = exec_queues->get(idx);
                        }
#endif
                        // increment of batch mrange to use the next entry slot
                        entry->txn_id = planner_txn_id;
                        entry->txn_ctx = tctx;
//                        entry->batch_id = batch_id;
//                        assert(tctx->batch_id == batch_id);
#if !SERVER_GENERATE_QUERIES
                        assert(msg->return_node_id != g_node_id);
                         entry->return_node_id = msg->return_node_id;
#endif

                        // Dirty code
                        ycsb_request * req_buff = (ycsb_request *) &entry->req_buffer;
                        req_buff->acctype = ycsb_req->acctype;
                        req_buff->key = ycsb_req->key;
                        req_buff->value = ycsb_req->value;

                        // add entry into range/bucket queue
                        // entry is a sturct, need to double check if this works
                        mrange->add(*entry);

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

#if TDG_ENTRY_TYPE == VECTOR_ENTRY
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
                                std::vector<uint64_t> * txn_list;
                                quecc_pool.txn_list_get_or_create(txn_list, _planner_id);

                                txn_list->push_back(search->second->back());
                                txn_dep_graph->insert({planner_txn_id, txn_list});
                            }
//                            DEBUG_Q("PT_%ld : txn_id = %ld depends on txn_id = %ld\n", _thd_id, planner_txn_id, (uint64_t) search->second->back());
                        }
                    }
                    else{
                        // not found
                        if (ycsb_req->acctype == WR){
                            std::vector<uint64_t> * txn_list;
                            quecc_pool.txn_list_get_or_create(txn_list, _planner_id);
                            txn_list->push_back(planner_txn_id);
                            access_table.insert({ycsb_req->key, txn_list});
                        }
                    }
#elif TDG_ENTRY_TYPE == ARRAY_ENTRY
                        auto search = access_table.find(key);
                        if (search != access_table.end()){
                            // found
                            if (ycsb_req->acctype == WR){
                                search->second->add(planner_txn_id);
                                // this is a write-write conflict,
                                // but since this is a single record operation, no need for dependencies
                            }
                            else{
                                M_ASSERT_V(search->second->last() >= batch_starting_txn_id,
                                           "invalid txn_id in access table!! last_txn_id = %ld, batch_starting_txn_id = %ld\n",
                                           search->second->last(), batch_starting_txn_id
                                );
                                auto search_txn = txn_dep_graph->find(planner_txn_id);
                                uint64_t d_txnid;
                                uint64_t d_txn_ctx_idx;
                                transaction_context * d_tctx;
                                d_txnid = search->second->last();
                                d_txn_ctx_idx = d_txnid-planner_pg->batch_starting_txn_id;
                                d_tctx = &planner_pg->txn_ctxs[d_txn_ctx_idx];
                                if (search_txn != txn_dep_graph->end()){
                                    search_txn->second->add(d_tctx);
                                }
                                else{
                                    // first operation for this txn_id
                                    Array<transaction_context *> * txn_list;
                                    quecc_pool.txn_ctx_list_get_or_create(txn_list, _planner_id);
                                    txn_list->add(d_tctx);
                                    txn_dep_graph->insert({planner_txn_id, txn_list});
                                }
//                            DEBUG_Q("PT_%ld : txn_id = %ld depends on txn_id = %ld\n", _thd_id, planner_txn_id, (uint64_t) search->second->back());
                            }
                        }
                        else{
                            // not found
                            if (ycsb_req->acctype == WR){
                                Array<uint64_t> * txn_list;
                                quecc_pool.txn_id_list_get_or_create(txn_list, _planner_id);
                                txn_list->add(planner_txn_id);
                                access_table.insert({ycsb_req->key, txn_list});
                            }
                        }
#endif // -#if TDG_ENTRY_TYPE == VECTOR_TYPE
#endif // -#if BUILD_TXN_DEPS
                    }

#if DEBUG_QUECC
                    total_msg_processed_cnt++;
#endif
                    INC_STATS(_thd_id,plan_txn_process_time[_planner_id], get_sys_clock() - txn_prof_starttime);
#if !INIT_QUERY_MSGS
                    // Free message, as there is no need for it anymore
                    Message::release_message(msg);
#endif
                    // increment for next ransaction
                    planner_txn_id++;
#endif
                    break;
                }
                default:
                    assert(false);
            } // end of switch statement

        } // end of while true

    } // end of for loop

    mem_allocator.free(entry, sizeof(exec_queue_entry));
    // release all exec_queues
    for (uint64_t i=0; i < exec_queues->size(); ++i){
        Array<exec_queue_entry> * exec_q = exec_queues->get(i);
        if (i >= g_thread_cnt){
            et_id = eq_idx_rand(plan_rng);
        }
        else{
            et_id = i;
        }
        quecc_pool.exec_queue_release(exec_q, _planner_id, et_id);
    }
#if SPLIT_MERGE_ENABLED
    // clean up all thread-local memory
    // free all split entries
#if SPLIT_STRATEGY == LAZY_SPLIT
    while(!split_entry_free_list->empty()){
        split_entry * s_entry_tmp;
        split_entry_free_list->pop(s_entry_tmp);
        mem_allocator.free(s_entry_tmp, sizeof(split_entry));
    }
#endif

    // free all assignment entries
    while(!assign_entry_free_list->empty()){
        assign_entry * a_entry_tmp = NULL;
        assign_entry_free_list->pop(a_entry_tmp);
        mem_allocator.free(a_entry_tmp, sizeof(assign_entry));
    }
#endif

    INC_STATS(_thd_id, plan_total_time[_planner_id], get_sys_clock()-plan_starttime);
#if DEBUG_QUECC
    DEBUG_Q("Planner_%ld: Total access cnt = %ld, and processed %ld msgs\n", _planner_id, total_access_cnt, total_msg_processed_cnt);
#endif


    printf("FINISH PT %ld:%ld\n",_node_id,_thd_id);
    fflush(stdout);
    return FINISH;
}
#endif
RC PlannerThread::run_normal_mode() {
    tsetup();
    printf("Running PlannerThread %ld\n", _thd_id);
    _wl->get_txn_man(my_txn_man);
    my_txn_man->init(_thd_id, _wl);
    my_txn_man->register_thread(this);

    Message * msg;
    txn_prefix_planner_base = (_planner_id * txn_prefix_base);
    if (_planner_id == 0){
        //start first transaction with 1
        txn_prefix_planner_base = 1;
    }
    assert(UINT64_MAX > (txn_prefix_planner_base+txn_prefix_base));

    DEBUG_Q("Planner_%ld thread started, txn_ids start at %ld \n", _planner_id, txn_prefix_planner_base);

#if SPLIT_MERGE_ENABLED && SPLIT_STRATEGY == LAZY_SPLIT
    uint64_t exec_queue_limit = quecc_pool.exec_queue_capacity;
#endif

#if SPLIT_MERGE_ENABLED

#if SPLIT_STRATEGY == EAGER_SPLIT
    exec_qs_ranges_tmp->init(g_exec_qs_max_size);
    exec_queues_tmp = new Array<Array<exec_queue_entry> *>();
    exec_queues_tmp->init(g_exec_qs_max_size);
#endif
#endif

    // Array to track ranges
    exec_qs_ranges->init(g_exec_qs_max_size);
#if WORKLOAD == YCSB
    for (uint64_t i =0; i<g_thread_cnt; ++i){

        if (i == 0){
            exec_qs_ranges->add(bucket_size);
            continue;
        }
        else if (i == (g_thread_cnt-1)){
            exec_qs_ranges->add(g_synth_table_size);
            continue;
        }

        exec_qs_ranges->add(exec_qs_ranges->get(i-1)+bucket_size);
    }
#elif WORKLOAD == TPCC
    for (uint64_t i =0; i<g_num_wh; ++i){

        if (i == 0){
            exec_qs_ranges->add(bucket_size);
            continue;
        }
        else if (i == (g_thread_cnt-1)){
            exec_qs_ranges->add(UINT64_MAX);
            continue;
        }

        exec_qs_ranges->add(exec_qs_ranges->get(i-1)+bucket_size);
    }
#else
    M_ASSERT(false,"only YCSB and TPCC are currently supported\n")
#endif

#if SPLIT_MERGE_ENABLED
    exec_queues->init(g_exec_qs_max_size);
#else
    exec_queues->init(g_thread_cnt);
#endif

#if WORKLOAD == TPCC
    for (uint64_t i = 0; i < g_num_wh; i++) {
        Array<exec_queue_entry> * exec_q;

        quecc_pool.exec_queue_get_or_create(exec_q, _planner_id, i % g_thread_cnt);
        exec_queues->add(exec_q);
    }
#else
    for (uint64_t i = 0; i < g_thread_cnt; i++) {
        Array<exec_queue_entry> * exec_q;
        quecc_pool.exec_queue_get_or_create(exec_q, _planner_id, i);
        exec_queues->add(exec_q);
    }
#endif
    transaction_context * txn_ctxs = NULL;
    priority_group * planner_pg;
    slot_num = (batch_id % g_batch_map_length);

#if BATCHING_MODE == TIME_BASED
    quecc_pool.txn_ctxs_get_or_create(txn_ctxs, planner_batch_size, _planner_id);
    quecc_pool.pg_get_or_create(planner_pg, _planner_id);
    planner_pg->txn_ctxs = txn_ctxs;
#else
    planner_pg = &work_queue.batch_pg_map[slot_num][_planner_id];
//    DEBUG_Q("PL_%ld: Starting to work on planner_pg @%ld with batch_id = %ld, with slot_num =%ld\n",
//            _planner_id, (uint64_t)planner_pg, batch_id, slot_num);
    // maybe we don't need this if we are going to overwrite all fields
//    memset(&planner_pg->txn_ctxs,0,BATCH_SIZE/PLAN_THREAD_CNT);
    txn_ctxs = planner_pg->txn_ctxs;
#endif

    // Initialize access list
//    for (uint64_t b =0; b < BATCH_MAP_LENGTH; ++b){
//        for (uint64_t j= 0; j < g_plan_thread_cnt; ++j){
//            for (uint64_t i=0; i < planner_batch_size; ++i){
//                priority_group * planner_pg = &work_queue.batch_pg_map[b][j];
//                planner_pg->txn_ctxs[i].accesses = new Array<Access*>();
//                planner_pg->txn_ctxs[i].accesses->init(MAX_ROW_PER_TXN);
//            }
//        }
//    }

    planner_txn_id = txn_prefix_planner_base;

#if BUILD_TXN_DEPS
//    txn_dep_graph = new hash_table_t();
    txn_dep_graph = new hash_table_tctx_t();
#endif

    batch_starting_txn_id = planner_txn_id;

    uint64_t query_cnt = 0;

//    M_ASSERT_V(g_part_cnt >= g_plan_thread_cnt,
//               "PT_%ld: Number of paritions must be geq to number of planners."
//                       " g_part_cnt = %d, g_plan_thread_cnt = %d\n", _planner_id, g_part_cnt, g_plan_thread_cnt);

    uint64_t next_part = 0;
//#if DEBUG_QUECC
//    plan_active[_planner_id]->store(0);
//#endif
    while(!simulation->is_done()) {
        if (plan_starttime == 0 && simulation->is_warmup_done()){
            plan_starttime = get_sys_clock();
        }

        // dequeue for repective input_queue: there is an input queue for each planner
        // entries in the input queue is placed by the I/O thread
        // for now just dequeue and print notification
//        DEBUG_Q("Planner_%d is dequeuing\n", _planner_id);
        prof_starttime = get_sys_clock();
#if RANDOM_PLAN_DEQ
        next_part = RAND(g_part_cnt);
#else
//        next_part = (_planner_id + (g_plan_thread_cnt * query_cnt)) % g_part_cnt;
        next_part = _planner_id;
#endif
        query_cnt++;
//        SAMPLED_DEBUG_Q("PT_%ld: going to get a query with home partition = %ld\n", _planner_id, next_part);
        msg = work_queue.plan_dequeue(_planner_id, next_part);
        INC_STATS(_thd_id, plan_queue_dequeue_time[_planner_id], get_sys_clock()-prof_starttime);

        if(!msg) {
//            SAMPLED_DEBUG_Q("PL_%ld: no message??\n", _planner_id);
            if(idle_starttime == 0){
                idle_starttime = get_sys_clock();
            }
            // we have not recieved a transaction
                continue;
        }

//        DEBUG_Q("PL_%ld: got a query message, query_cnt = %ld\n", _planner_id, query_cnt);
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

        if (do_batch_delivery(force_batch_delivery, planner_pg, txn_ctxs) == BREAK){
            continue;
        }

        // we have got a message, which is a transaction
        if (batch_start_time == 0){
            batch_start_time = get_sys_clock();
        }

        switch (msg->get_rtype()) {
            case CL_QRY: {

                if (!simulation->is_done()){
                    plan_client_msg(msg, planner_pg);
                    batch_cnt++;
                }
                break;
            }
            default:
                assert(false);
        }

    }

    mem_allocator.free(entry, sizeof(exec_queue_entry));
    INC_STATS(_thd_id, plan_total_time[_planner_id], get_sys_clock()-plan_starttime);

    printf("FINISH PT %ld:%ld\n",_node_id,_thd_id);
    fflush(stdout);
    return FINISH;

}

inline SRC PlannerThread::do_batch_delivery(bool force_batch_delivery, priority_group * &planner_pg, transaction_context * &txn_ctxs){

    if (force_batch_delivery) {
//#if DEBUG_QUECC
//        print_eqs_ranges_after_swap();
//#endif
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

#if SPLIT_STRATEGY == EAGER_SPLIT
        // we just need compute assignment since all EQs satisfy the limit splitting is done message is processed
        Array<Array<exec_queue_entry> *> *exec_qs_tmp UNUSED = NULL;
        Array<exec_queue_entry> *exec_q_tmp = NULL;
        assign_entry *a_tmp UNUSED = NULL;
        for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
            exec_q_tmp = exec_queues->get(i);
            if (i < g_thread_cnt){
#if MERGE_STRATEGY == BALANCE_EQ_SIZE
                assign_entry_get_or_create(a_tmp, assign_entry_free_list);
                assign_entry_init(a_tmp, _planner_id);
                a_tmp->exec_thd_id = i;
                assign_entry_add(a_tmp, exec_q_tmp);
                assignment.push((uint64_t) a_tmp);

#elif MERGE_STRATEGY == RR
                quecc_pool.exec_qs_get_or_create(exec_qs_tmp, _planner_id);
                exec_qs_tmp->add(exec_q_tmp);
                f_assign[i] = (uint64_t) exec_qs_tmp;
#else
                M_ASSERT_V("Selected merge strategy is not supported\n");
#endif
            }
            else {
                if (exec_q_tmp->size() > 0){

#if MERGE_STRATEGY == BALANCE_EQ_SIZE
                    // Try to assign balanced workload to each
                    a_tmp = (assign_entry *) assignment.top();
                    assign_entry_add(a_tmp, exec_q_tmp);
                    assignment.pop();
                    assignment.push((uint64_t) a_tmp);

#elif MERGE_STRATEGY == RR
                    ((Array<Array<exec_queue_entry> *> *) f_assign[i % g_thread_cnt])->add(exec_q_tmp);

#else
                M_ASSERT_V(false,"Selected merge strategy is not supported\n");
#endif
//                    DEBUG_Q("PT_%ld: adding excess EQs to ET_%ld\n", _planner_id, a_tmp->exec_thd_id);
                }
                else{
                    quecc_pool.exec_queue_release(exec_q_tmp,_planner_id, i % g_thread_cnt);
                }
            }
        }
//        M_ASSERT_V(assignment.size() == g_thread_cnt, "PL_%ld: size mismatch of assignments to threads, assignment size = %ld, thread-cnt = %d\n",
//                   _planner_id, assignment.size(), g_thread_cnt);
        INC_STATS(_thd_id, plan_merge_time[_planner_id], get_sys_clock()-prof_starttime);
#else // LAZY SPLIT
        #if BATCHING_MODE != SIZE_BASED
            exec_queue_limit = (batch_cnt/g_thread_cnt) * REQ_PER_QUERY * EXECQ_CAP_FACTOR;
#endif

            // Splitting phase
            if (ranges_stored) {

                for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
                    assign_entry *a_tmp;
                    if (i < g_thread_cnt){

                        assign_entry_get_or_create(a_tmp, assign_entry_free_list);
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

                prof_starttime = get_sys_clock();
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

                            et_id = eq_idx_rand(plan_rng);
                            quecc_pool.exec_queue_get_or_create(nexec_qs[0], _planner_id, et_id);
                            et_id = eq_idx_rand(plan_rng);
                            quecc_pool.exec_queue_get_or_create(nexec_qs[1], _planner_id, et_id);

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
                            et_id = eq_idx_rand(plan_rng);
                            quecc_pool.exec_queue_release(top_entry->exec_q, _planner_id, et_id);

                            if (!split_entry_free_list->push(top_entry)) {
                                M_ASSERT_V(false,
                                           "FREE_LIST_INITIAL_SIZE is not enough for holding split entries, increase the size")
                            };

                            top_entry = (split_entry *) pq_test.top();

                            if (top_entry->exec_q->size() <= exec_queue_limit) {

                                break;
                            } else {

                                split_rounds++;
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

                INC_STATS(_thd_id, plan_split_time[_planner_id], get_sys_clock()-prof_starttime);

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
            prof_starttime = get_sys_clock();
            for (uint64_t i = 0; i < g_thread_cnt; ++i) {
                assign_entry *a_tmp;
                split_entry *s_tmp = ((split_entry *) pq_test.top());

                assign_entry_get_or_create(a_tmp, assign_entry_free_list);

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

            INC_STATS(_thd_id, plan_merge_time[_planner_id], get_sys_clock()-prof_starttime);
        }


#endif

#if MERGE_STRATEGY == BALANCE_EQ_SIZE
        // Populate the final assignemnt
        memset(f_assign, 0, sizeof(uint64_t)*g_thread_cnt);
        // balance-workload assignment
        while (!assignment.empty()){
            assign_entry * te = (assign_entry *) assignment.top();
            f_assign[te->exec_thd_id] = (uint64_t) te->exec_qs;
            assignment.pop();
            assign_entry_clear(te);
            while(!assign_entry_free_list->push(te)){}
        }
        M_ASSERT_V(assignment.size() == 0, "PT_%ld: We have not used all assignments in the final assignments", _planner_id);
#elif MERGE_STRATEGY == RR
        // nothing to do altready assigned
#else
                M_ASSERT_V("Selected merge strategy is not supported\n");
#endif
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

//            planner_pg->batch_txn_cnt = batch_cnt;
//            planner_pg->batch_id = batch_id;
#if BUILD_TXN_DEPS
        planner_pg->txn_dep_graph = txn_dep_graph;
#endif
        planner_pg->batch_starting_txn_id = batch_starting_txn_id;

        // deilvery  phase
        for (uint64_t i = 0; i < g_thread_cnt; i++){

            // create a new batch_partition
            quecc_pool.batch_part_get_or_create(batch_part, _planner_id, i);
#if SPLIT_MERGE_ENABLED
            Array<Array<exec_queue_entry> *> * fa_execqs = ((Array<Array<exec_queue_entry> *> *)f_assign[i]);

            if (fa_execqs){
                batch_part->empty = false;
                if (fa_execqs->size() > 1){
                    batch_part->single_q = false;
                    // Allocate memory for exec_qs
//                    batch_part->sub_exec_qs_cnt = fa_execqs->size();
                    batch_part->exec_qs = fa_execqs;

                }
                else if (fa_execqs->size() == 1) {
                    batch_part->exec_q = fa_execqs->get(0);
                    // recycle fa_exec_q
                    fa_execqs->clear();
                    quecc_pool.exec_qs_release(fa_execqs, _planner_id);
                }
                else{
                    batch_part->empty = true;
                }
            }
            else{
                // assign and empty EQ
                batch_part->empty = true;
            }

#else
            batch_part->exec_q = exec_queues->get(i);
#endif
            batch_part->planner_pg = planner_pg;
//                DEBUG_Q("Planner_%ld : Assigned to ET_%ld, %ld exec_qs with total size = %ld\n",
//                        _planner_id, final_assignment.top().exec_thd_id, final_assignment.top().exec_qs.size(),
//                        final_assignment.top().curr_sum
//                );


#if BATCH_MAP_ORDER == BATCH_ET_PT
            while(work_queue.batch_map[slot_num][i][_planner_id].fetch_add(0) != 0) {
                if(idle_starttime == 0){
                    idle_starttime = get_sys_clock();
//                    SAMPLED_DEBUG_Q("PT_%ld: SPINNING!!! for batch %ld  Completed a lap up to map slot [%ld][%ld][%ld]\n", _planner_id, batch_id, slot_num, i, _planner_id);
                }

                if (simulation->is_done()){
                    if (idle_starttime > 0){
                        INC_STATS(_thd_id,plan_idle_time[_planner_id],get_sys_clock() - idle_starttime);
                        idle_starttime = 0;
                    }
                    return BREAK;
                }
            }
#else
            while(work_queue.batch_map[slot_num][_planner_id][i].load() != 0) {
#if DEBUG_QUECC
//                if (_planner_id == 0){
//                    print_threads_status();
//                    M_ASSERT_V(false,"PT_%ld: non-zero batch map slot\n",_planner_id);
//                }
#endif
                if(idle_starttime == 0){
                    idle_starttime = get_sys_clock();
//                    SAMPLED_DEBUG_Q("PT_%ld: SPINNING!!! for batch %ld  Completed a lap up to map slot [%ld][%ld][%ld]\n", _planner_id, batch_id, slot_num, i, _planner_id);
                }

                if (simulation->is_done()){
                    if (idle_starttime > 0){
                        INC_STATS(_thd_id,plan_idle_time[_planner_id],get_sys_clock() - idle_starttime);
                        idle_starttime = 0;
                    }
                    return BREAK;
                }
            }
#endif
            // include idle time from spinning above
            if (idle_starttime > 0){
                INC_STATS(_thd_id,plan_idle_time[_planner_id],get_sys_clock() - idle_starttime);
                idle_starttime = 0;
            }

            // Deliver batch partition to the repective ET
            // In case batch slot is not ready, spin!
            expected = 0;
            desired = (uint64_t) batch_part;
#if BATCH_MAP_ORDER == BATCH_ET_PT
            while(!work_queue.batch_map[slot_num][i][_planner_id].compare_exchange_strong(expected, desired)){
                    // this should not happen after spinning but can happen if simulation is done
//                    M_ASSERT_V(false, "For batch %ld : failing to SET map slot [%ld][%ld][%ld]\n", batch_id, slot_num, i, _planner_id);
                }
#else
            if(!work_queue.batch_map[slot_num][_planner_id][i].compare_exchange_strong(expected, desired)){
                // this should not happen after spinning but can happen if simulation is done
                    M_ASSERT_V(false, "For batch %ld : failing to SET batch map slot [%ld][%ld][%ld]\n", batch_id, slot_num, i, _planner_id);
//                SAMPLED_DEBUG_Q("PT_%ld: for batch %ld : failing to SET map slot [%ld][%ld][%ld]\n", _planner_id, batch_id, slot_num, i, _planner_id)
            }

#endif
//                DEBUG_Q("PT_%ld :Batch_%ld for ET_%ld ready! b_slot = %ld\n", _planner_id, batch_id, i, slot_num);
        }

        // Set priority group pointer in the pg_map
#if BATCHING_MODE == TIME_BASED
        expected = 0;
            desired = (uint64_t) planner_pg;
//            DEBUG_Q("Planner_%ld :going to set pg(%ld) for batch_%ld at b_slot = %ld\n", _planner_id, desired, batch_id, slot_num);

            // This CAS operation can fail if CT did not reset the value
            // this means planners nneds to spin
            while(!work_queue.batch_pg_map[slot_num][_planner_id].compare_exchange_strong(
                    expected, desired)){
                // this should not happen after spinning
//                M_ASSERT_V(false, "For batch %ld : failing to SET batch_pg_map slot [%ld][%ld], current value = %ld, \n",
//                           batch_id, slot_num, _planner_id, work_queue.batch_pg_map[slot_num][_planner_id].load());
            }
#endif // BATCHING_MODE == TIME_BASED
        // reset data structures and execution queues for the new batch
        prof_starttime = get_sys_clock();
        exec_queues->clear();
        for (uint64_t i = 0; i < exec_qs_ranges->size(); i++) {
            Array<exec_queue_entry> * exec_q;
#if MERGE_STRATEGY == RR
            et_id = eq_idx_rand->operator()(plan_rng);
#else
            et_id = i % g_thread_cnt;
#endif
            quecc_pool.exec_queue_get_or_create(exec_q, _planner_id, et_id);
            exec_queues->add(exec_q);
        }

        INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock()-prof_starttime);
        INC_STATS(_thd_id, plan_batch_cnts[_planner_id], 1);
        INC_STATS(_thd_id, plan_batch_process_time[_planner_id], get_sys_clock() - batch_start_time);
#if BUILD_TXN_DEPS
        prof_starttime = get_sys_clock();
        for (auto it = access_table.begin(); it != access_table.end(); ++it){
#if TDG_ENTRY_TYPE == VECTOR_ENTRY
            std::vector<uint64_t> * txn_list_tmp = it->second;
#elif TDG_ENTRY_TYPE == ARRAY_ENTRY
            Array<uint64_t> * txn_list_tmp = it->second;
#endif
            quecc_pool.txn_id_list_release(txn_list_tmp,_planner_id);
        }
        access_table.clear();

        // TODO(tq): do we need to allocate a new hashtable???
        txn_dep_graph = new hash_table_tctx_t();
        M_ASSERT_V(access_table.size() == 0, "Access table is not empty!!\n");
        M_ASSERT_V(txn_dep_graph->size() == 0, "TDG table is not empty!!\n");
//        INC_STATS(_thd_id, plan_tdep_time[_planner_id], get_sys_clock()-prof_starttime);
#endif
        // batch delivered
        batch_id++;
        batch_cnt = 0;
        batch_start_time = 0;
        batch_starting_txn_id = planner_txn_id;

        slot_num = (batch_id % g_batch_map_length);
        // Spin here if PG map slot is not available, this is eager spnning
        // TODO(tq): see if we can delay this for later and do some usefl works

        // TQ: waiting for PG here, we should wait on the batch_part
#if BATCHING_MODE == TIME_BASED
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

            quecc_pool.txn_ctxs_get_or_create(txn_ctxs, planner_batch_size, _planner_id);
            quecc_pool.pg_get_or_create(planner_pg, _planner_id);
//            planner_pg->planner_id = _planner_id;
            planner_pg->txn_ctxs = txn_ctxs;
//#if DEBUG_QUECC
//            if (plan_active[_planner_id]->fetch_add(0) >= 0){
////                DEBUG_Q("PT_%ld: will wait for its batch map slot %ld, batch_id=%ld\n",_planner_id, slot_num, batch_id);
//                plan_active[_planner_id]->store(-1);
//            }
//#endif
            if(idle_starttime == 0){
                idle_starttime = get_sys_clock();
//                DEBUG_Q("PL_%ld : PG map slot for batch_%ld at b_slot = %ld is not available\n", _planner_id, batch_id, slot_num);
            }
//                SAMPLED_DEBUG_Q("Planner_%ld : spinning for batch_%ld at b_slot = %ld\n", _planner_id, batch_id, slot_num);
            if (simulation->is_done()){
                if (idle_starttime > 0){
                    INC_STATS(_thd_id,plan_idle_time[_planner_id],get_sys_clock() - idle_starttime);
                    idle_starttime = 0;
                }
                return BREAK;
            }
        }
#if !ATOMIC_PG_STATUS
        work_queue.batch_pg_map[slot_num][_planner_id].ready = 0;
        atomic_thread_fence(memory_order_release);
#endif
//        DEBUG_Q("PL_%ld : PG map slot for batch_%ld at b_slot = %ld is available now!! retarting planning again\n", _planner_id, batch_id, slot_num);
        // include idle time from spinning above
        if (idle_starttime > 0){
            INC_STATS(_thd_id,plan_idle_time[_planner_id],get_sys_clock() - idle_starttime);
            idle_starttime = 0;
        }

#if DEBUG_QUECC
//        if (_planner_id == 0){
//            print_threads_status();
//        }
        // back to planning again
//            DEBUG_Q("PT_%ld: resuming planning with slot %ld, batch_id=%ld\n",_planner_id, slot_num, batch_id);
//        plan_active[_planner_id]->store(batch_id);
#endif
        // move to the next planner PG
        planner_pg = &work_queue.batch_pg_map[slot_num][_planner_id];
//        DEBUG_Q("PL_%ld: Starting to work on planner_pg @%ld with batch_id = %ld, with slot_num =%ld\n",
//                _planner_id, (uint64_t) planner_pg, batch_id, slot_num);
        txn_ctxs = planner_pg->txn_ctxs;
#endif

    }
    return SUCCESS;
}

inline void PlannerThread::plan_client_msg(Message *msg, priority_group * planner_pg) {

// Query from client
//    DEBUG_Q("PT_%ld: planning txn %ld\n", _planner_id,planner_txn_id);
    txn_prof_starttime = get_sys_clock();
    transaction_context * txn_ctxs = planner_pg->txn_ctxs;
    // create transaction context
    // TODO(tq): here also we are dynamically allocting memory, we should use a pool recycle
//                prof_starttime = get_sys_clock();

    // Reset tctx memblock for reuse
    transaction_context *tctx = &txn_ctxs[batch_cnt];

//                INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock() - prof_starttime);

    // reset transaction context

    tctx->txn_id = planner_txn_id;
    tctx->txn_state.store(TXN_INITIALIZED,memory_order_acq_rel);
    tctx->completion_cnt.store(0,memory_order_acq_rel);
//    tctx->completion_cnt =0;
    tctx->txn_comp_cnt.store(0,memory_order_acq_rel);
    tctx->starttime = get_sys_clock(); // record start time of transaction


    //TODO(tq): move to repective benchmark transaction manager implementation
#if WORKLOAD == TPCC
    tctx->o_id.store(-1);
#endif

#if !SERVER_GENERATE_QUERIES
    tctx->client_startts = ((ClientQueryMessage *) msg)->client_startts;
#endif
//                tctx->batch_id = batch_id;

    // create execution entry, for now it will contain only one request
    // we need to reset the mutable values of tctx
    entry->txn_id = planner_txn_id;
    entry->txn_ctx = tctx;
#if ROW_ACCESS_IN_CTX
//    M_ASSERT_V(false, "undo buffer in txn context is ot currently supported for pipelined Quecc\n");
#if WORKLOAD == YCSB
    M_ASSERT_V(tctx->undo_buffer_inialized, "Txn context is not initialized\n");
#else
    M_ASSERT_V(false, "undo buffer in txn ctx is not supported for TPCC or others\n");
#endif
#else
    // initialize access_lock if it is not intinialized
    if (tctx->access_lock == NULL){
        tctx->access_lock = new spinlock();
    }

#if ROLL_BACK
    if (tctx->accesses->isInitilized()){
        // need to clear on commit phase
//        DEBUG_Q("reusing tctx accesses\n");
        tctx->accesses->clear();
    }
    else{
//        DEBUG_Q("initializing tctx accesses\n");
        tctx->accesses->init(MAX_ROW_PER_TXN);
    }
#endif
#endif
#if !SERVER_GENERATE_QUERIES
    assert(msg->return_node_id != g_node_id);
        entry->return_node_id = msg->return_node_id;
#endif

#if WORKLOAD == YCSB
    // Analyze read-write set
    /* We need to determine the ranges needed for each key
     * We group keys that fall in the same range to be processed together
     * TODO(tq): add repartitioning
     */
//    uint8_t e8 = TXN_INITIALIZED;
//    uint8_t d8 = TXN_STARTED;

    uint64_t e8 = TXN_INITIALIZED;
    uint64_t d8 = TXN_STARTED;
    if(!entry->txn_ctx->txn_state.compare_exchange_strong(e8,d8,memory_order_acq_rel)){
        assert(false);
    }
    YCSBClientQueryMessage *ycsb_msg = ((YCSBClientQueryMessage *) msg);
    for (uint64_t j = 0; j < ycsb_msg->requests.size(); j++) {
//        memset(entry, 0, sizeof(exec_queue_entry));
        ycsb_request *ycsb_req = ycsb_msg->requests.get(j);
        uint64_t key = ycsb_req->key;
//                    DEBUG_Q("Planner_%d looking up bucket for key %ld\n", _planner_id, key);

        // TODO(tq): use get split with dynamic ranges
        uint64_t idx = get_split(key, exec_qs_ranges);
        prof_starttime = get_sys_clock();
        Array<exec_queue_entry> *mrange = exec_queues->get(idx);

#if SPLIT_MERGE_ENABLED && SPLIT_STRATEGY == EAGER_SPLIT
//        et_id = eq_idx_rand->operator()(plan_rng);
        et_id = _thd_id;
        checkMRange(mrange, key, et_id);
#endif

        // Dirty code
        ycsb_request * req_buff = (ycsb_request *) &entry->req_buffer;
        req_buff->acctype = ycsb_req->acctype;
        req_buff->key = ycsb_req->key;
        req_buff->value = ycsb_req->value;

        // add entry into range/bucket queue
        // entry is a sturct, need to double check if this works
        // this actually performs a full memcopy when adding entries
        tctx->txn_comp_cnt.fetch_add(1);
        mrange->add(*entry);
        prof_starttime = get_sys_clock();

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


        uint64_t _prof_starttime = get_sys_clock();
        auto search = access_table.find(key);
        if (search != access_table.end()){
            // found
            if (ycsb_req->acctype == WR){
#if TDG_ENTRY_TYPE == VECTOR_ENTRY
                search->second->push_back(planner_txn_id);
#elif TDG_ENTRY_TYPE == ARRAY_ENTRY
                search->second->add(planner_txn_id);
#endif
                // this is a write-write conflict,
                // but since this is a single record operation, no need for dependencies
            }
            else{
                M_ASSERT_V(ycsb_req->acctype == RD, "only RD access type is supported");
#if TDG_ENTRY_TYPE == VECTOR_ENTRY
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
                    std::vector<uint64_t> * txn_list;
                    quecc_pool.txn_list_get_or_create(txn_list,_planner_id);

                    txn_list->push_back(search->second->back());
                    txn_dep_graph->insert({planner_txn_id, txn_list});
                }
#elif TDG_ENTRY_TYPE == ARRAY_ENTRY
                M_ASSERT_V(search->second->last() >= batch_starting_txn_id,
                           "invalid txn_id in access table!! last_txn_id = %ld, batch_starting_txn_id = %ld\n",
                           search->second->last(), batch_starting_txn_id
                );
                auto search_txn = txn_dep_graph->find(planner_txn_id);
                uint64_t d_txnid;
                uint64_t d_txn_ctx_idx;
                transaction_context * d_tctx;
                d_txnid = search->second->last();
                d_txn_ctx_idx = d_txnid-planner_pg->batch_starting_txn_id;
                d_tctx = &planner_pg->txn_ctxs[d_txn_ctx_idx];
                if (search_txn != txn_dep_graph->end()){
                    search_txn->second->add(d_tctx);
                }
                else{
                    // first operation for this txn_id
                    Array<transaction_context *> * txn_list;
                    quecc_pool.txn_ctx_list_get_or_create(txn_list,_planner_id);

                    txn_list->add(d_tctx);
                    txn_dep_graph->insert({planner_txn_id, txn_list});
                }
#endif

//                            DEBUG_Q("PT_%ld : txn_id = %ld depends on txn_id = %ld\n", _thd_id, planner_txn_id, (uint64_t) search->second->back());
            }
        }
        else{
            // not found
            if (ycsb_req->acctype == WR){
#if TDG_ENTRY_TYPE == VECTOR_ENTRY
                std::vector<uint64_t> * txn_list;
                quecc_pool.txn_list_get_or_create(txn_list,_planner_id);
                txn_list->push_back(planner_txn_id);
#elif TDG_ENTRY_TYPE == ARRAY_ENTRY
                Array<uint64_t> * txn_list;
                quecc_pool.txn_id_list_get_or_create(txn_list,_planner_id);
                txn_list->add(planner_txn_id);
#endif
                access_table.insert({ycsb_req->key, txn_list});
            }
        }

        INC_STATS(_thd_id, plan_tdep_time[_planner_id], get_sys_clock()-_prof_starttime);
#endif
        // will always return RCOK, should we convert it to void??
    }

#elif WORKLOAD == TPCC

    // TPCC
    TPCCClientQueryMessage *tpcc_msg = ((TPCCClientQueryMessage *) msg);
    TPCCTxnManager * tpcc_txn_man = (TPCCTxnManager *)my_txn_man;
    row_t * r_local;
    Array<exec_queue_entry> *mrange;
//    uint64_t idx;
    uint64_t rid;

    uint64_t e8 = TXN_INITIALIZED;
    uint64_t d8 = TXN_STARTED;

    switch (tpcc_msg->txn_type) {
        case TPCC_PAYMENT:
            prof_starttime = get_sys_clock();

            if(!entry->txn_ctx->txn_state.compare_exchange_strong(e8,d8)){
                assert(false);
            }
            // index look up for warehouse record
            tpcc_txn_man->payment_lookup_w(tpcc_msg->w_id, r_local);
            rid = r_local->get_row_id();
//            idx = get_split(rid, exec_qs_ranges);
//            mrange = exec_queues->get(idx);
            et_id = eq_idx_rand->operator()(plan_rng);
            // check range for warehouse and split if needed
            checkMRange(mrange, rid, et_id);

            // create exec_qe for updating  warehouse record
            tpcc_txn_man->plan_payment_update_w(tpcc_msg->h_amount,r_local, entry);
            mrange->add(*entry);

            // plan read/update district record
            tpcc_txn_man->payment_lookup_d(tpcc_msg->w_id,tpcc_msg->d_id,tpcc_msg->d_w_id,r_local);
            rid = r_local->get_row_id();
//            idx = get_split(rid, exec_qs_ranges);
//            mrange = exec_queues->get(idx);
            checkMRange(mrange, rid, et_id);
            tpcc_txn_man->plan_payment_update_d(tpcc_msg->h_amount, r_local,entry);
            mrange->add(*entry);

            // plan read/update customer record
            tpcc_txn_man->payment_lookup_c(tpcc_msg->c_id, tpcc_msg->c_w_id, tpcc_msg->c_d_id, tpcc_msg->c_last,
                                           tpcc_msg->by_last_name, r_local);

            rid = r_local->get_row_id();
//            idx = get_split(rid, exec_qs_ranges);
//            mrange = exec_queues->get(idx);
            checkMRange(mrange, rid, et_id);
            tpcc_txn_man->plan_payment_update_c(tpcc_msg->h_amount, r_local, entry);
            mrange->add(*entry);

            // plan insert into history
            tpcc_txn_man->plan_payment_insert_h(tpcc_msg->w_id, tpcc_msg->d_id, tpcc_msg->c_id, tpcc_msg->c_w_id, tpcc_msg->d_w_id, tpcc_msg->h_amount, entry);
            rid = entry->rid;
//            idx = get_split(rid, exec_qs_ranges);
//            mrange = exec_queues->get(idx);
            checkMRange(mrange, rid, et_id);
            mrange->add(*entry);

            break;
        case TPCC_NEW_ORDER:
            // plan read on warehouse record

            if(!entry->txn_ctx->txn_state.compare_exchange_strong(e8,d8)){
                assert(false);
            }
            tpcc_txn_man->neworder_lookup_w(tpcc_msg->w_id,r_local);
            rid = r_local->get_row_id();
//            idx = get_split(rid, exec_qs_ranges);
//            mrange = exec_queues->get(idx);
            et_id = eq_idx_rand->operator()(plan_rng);
            // check range for warehouse and split if needed
            checkMRange(mrange, rid, et_id);
            tpcc_txn_man->plan_neworder_read_w(r_local,entry);
            mrange->add(*entry);

            // plan read on cust. record
            tpcc_txn_man->neworder_lookup_c(tpcc_msg->w_id,tpcc_msg->d_id,tpcc_msg->c_id, r_local);
            rid = r_local->get_row_id();
//            idx = get_split(rid, exec_qs_ranges);
//            mrange = exec_queues->get(idx);
            et_id = eq_idx_rand->operator()(plan_rng);
            checkMRange(mrange, rid, et_id);
            tpcc_txn_man->plan_neworder_read_c(r_local, entry);
            mrange->add(*entry);


            //plan update on district table
            tpcc_txn_man->neworder_lookup_d(tpcc_msg->w_id, tpcc_msg->d_id, r_local);
            rid = r_local->get_row_id();
//            idx = get_split(rid, exec_qs_ranges);
//            mrange = exec_queues->get(idx);
            et_id = eq_idx_rand->operator()(plan_rng);
            checkMRange(mrange, rid, et_id);
            tpcc_txn_man->plan_neworder_update_d(r_local,entry);
            mrange->add(*entry);

            // plan insert into orders
            tpcc_txn_man->plan_neworder_insert_o(tpcc_msg->w_id, tpcc_msg->d_id,tpcc_msg->c_id,tpcc_msg->remote,tpcc_msg->ol_cnt,tpcc_msg->o_entry_d,entry);
            rid = entry->rid;
//            idx = get_split(rid, exec_qs_ranges);
//            mrange = exec_queues->get(idx);
            et_id = eq_idx_rand->operator()(plan_rng);
            checkMRange(mrange, rid, et_id);
            mrange->add(*entry);

            // plan insert into new order
            tpcc_txn_man->plan_neworder_insert_no(tpcc_msg->w_id,tpcc_msg->d_id, tpcc_msg->c_id, entry);
            rid = entry->rid;
//            idx = get_split(rid, exec_qs_ranges);
//            mrange = exec_queues->get(idx);
            et_id = eq_idx_rand->operator()(plan_rng);
            checkMRange(mrange, rid, et_id);
            mrange->add(*entry);

            for (uint64_t i =0; i < tpcc_msg->ol_cnt; ++i){

                uint64_t ol_number = i;
                uint64_t ol_i_id = tpcc_msg->items[ol_number]->ol_i_id;
                uint64_t ol_supply_w_id = tpcc_msg->items[ol_number]->ol_supply_w_id;
                uint64_t ol_quantity = tpcc_msg->items[ol_number]->ol_quantity;

                // plan read an item from items
                tpcc_txn_man->neworder_lookup_i(ol_i_id,r_local);
                rid = r_local->get_row_id();
//                idx = get_split(rid, exec_qs_ranges);
//                mrange = exec_queues->get(idx);
                et_id = eq_idx_rand->operator()(plan_rng);
                checkMRange(mrange, rid, et_id);
                tpcc_txn_man->plan_neworder_read_i(r_local,entry);
                mrange->add(*entry);

                // plan update to a item's stock record
                tpcc_txn_man->neworder_lookup_s(ol_i_id,ol_supply_w_id,r_local);
                rid = r_local->get_row_id();
//                idx = get_split(rid, exec_qs_ranges);
//                mrange = exec_queues->get(idx);
                et_id = eq_idx_rand->operator()(plan_rng);
                checkMRange(mrange, rid, et_id);
                tpcc_txn_man->plan_neworder_update_s(ol_quantity, tpcc_msg->remote, r_local,entry);
                mrange->add(*entry);

                // plan insert into order_line
                tpcc_txn_man->plan_neworder_insert_ol(ol_i_id,ol_supply_w_id,ol_quantity, ol_number, r_local, entry);
                rid = entry->rid;
//                idx = get_split(rid, exec_qs_ranges);
//                mrange = exec_queues->get(idx);
                et_id = eq_idx_rand->operator()(plan_rng);
                checkMRange(mrange, rid, et_id);
                mrange->add(*entry);
            }

            break;
        default:
            M_ASSERT_V(false, "Only Payment(%d) and NewOrder(%d) transactions are supported, found (%ld)\n", TPCC_PAYMENT, TPCC_NEW_ORDER, tpcc_msg->txn_type);
    }


#endif

    INC_STATS(_thd_id,plan_txn_process_time[_planner_id], get_sys_clock() - txn_prof_starttime);

    // increment for next ransaction
    planner_txn_id++;

#if !INIT_QUERY_MSGS
    // Free message, as there is no need for it anymore
    Message::release_message(msg);
#endif

}

void PlannerThread::checkMRange(Array<exec_queue_entry> *& mrange, uint64_t key, uint64_t et_id){
#if SPLIT_MERGE_ENABLED && SPLIT_STRATEGY == EAGER_SPLIT

    int max_tries = 64;
    int trial =0;

    uint64_t c_range_start;
    uint64_t c_range_end;
    uint64_t idx;
    uint64_t nidx;
    uint64_t _prof_starttime;

    idx = get_split(key, exec_qs_ranges);
    _prof_starttime = get_sys_clock();
    mrange = exec_queues->get(idx);

    while (mrange->is_full()){
        trial++;
        if (trial == max_tries){
            M_ASSERT_V(false, "Execeded max split tries\n");
        }

//        if (exec_qs_ranges->is_full()){
//            // merge zero queues
//            DEBUG_Q("PL_%ld: Going to collapse empty EQs\n", _planner_id);
//
//            assert(exec_qs_ranges->size() == exec_queues->size());
//            exec_qs_ranges_tmp->clear();
//            exec_queues_tmp->clear();
//            // collapse empty EQs into one
//            for (uint64_t i =0; i < exec_qs_ranges->size(); i++){
//                // add non-empty ranges
//                if (exec_queues->get(i)->size() != 0){
//                    exec_qs_ranges_tmp->add(exec_qs_ranges->get(i));
//                    exec_queues_tmp->add(exec_queues->get(i));
//                }
//                else{
//                    // add empty EQs if it is the last one or if the next one is non-empty
//                    if (i == exec_qs_ranges->size()-1){
//                        exec_qs_ranges_tmp->add(exec_qs_ranges->get(i));
//                        exec_queues_tmp->add(exec_queues->get(i));
//                    }
//                    else if (exec_queues->get(i+1)->size() != 0){
//                        exec_qs_ranges_tmp->add(exec_qs_ranges->get(i));
//                        exec_queues_tmp->add(exec_queues->get(i));
//                    }
//                }
//            }
//
//
//            // print EQ PARITIONING contents
//            uint64_t total_old = 0;
//            uint64_t total_new = 0;
//
//            for (uint64_t i =0; i < exec_queues->size(); ++i){
//                total_old += exec_queues->get(i)->size();
//            }
//
//            for (uint64_t i =0; i < exec_queues_tmp->size(); ++i){
//                total_new += exec_queues_tmp->get(i)->size();
//            }
//            M_ASSERT_V(total_old ==  total_new, "PL_%ld: totals mismatch, totals_old = %lu, totals_new = %lu \n",_planner_id, total_old, total_new);
//
//            // Swap data structures
//            exec_queues_tmp_tmp = exec_queues;
//            exec_qs_ranges_tmp_tmp = exec_qs_ranges;
//
//            exec_queues = exec_queues_tmp;
//            exec_qs_ranges = exec_qs_ranges_tmp;
//
//            exec_queues_tmp = exec_queues_tmp_tmp;
//            exec_qs_ranges_tmp = exec_qs_ranges_tmp_tmp;
//
////            print_eqs_ranges_after_swap();
////            assert(false);
//
//            // recycle zero-sized EQs
//            for (uint64_t i =0; i < exec_queues_tmp->size(); ++i){
//                Array<exec_queue_entry> *exec_q = exec_queues_tmp->get(i);
//                if (exec_q->size() == 0){
//                    quecc_pool.exec_queue_release(exec_q,_planner_id,eq_idx_rand->operator()(plan_rng));
//                }
//            }
//
//            // update mrange
//            idx = get_split(key, exec_qs_ranges);
//            mrange = exec_queues->get(idx);
//            continue;
//        }


        // we need to split

//        M_ASSERT_V(idx == pidx, "idx mismatch after removal of empty queues; idx=%ld , pidx=%ld\n", idx, pidx);
//        idx = get_split(key, exec_qs_ranges);
//        mrange = exec_queues->get(idx);

        if (idx == 0){
            c_range_start = 0;
        }
        else{
            c_range_start = exec_qs_ranges->get(idx-1);
        }
        c_range_end = exec_qs_ranges->get(idx);

        uint64_t split_point = (c_range_end-c_range_start)/2;

//        DEBUG_Q("Planner_%ld : Eagerly we need to split mrange ptr = %lu, key = %lu, current size = %ld,"
//                        " batch_id = %ld, c_range_start = %lu, c_range_end = %lu, split_point = %lu, trial=%d"
//                        "\n",
//                _planner_id, (uint64_t) mrange, key, mrange->size(), batch_id, c_range_start, c_range_end, split_point, trial);

        M_ASSERT_V(split_point, "PL_%ld: We are at a single record, and we cannot split anymore!, range_size = %ld, eq_size = %ld\n",
                   _planner_id, c_range_end-c_range_start, mrange->size());

        // compute new ranges
        exec_qs_ranges_tmp->clear();
        exec_queues_tmp->clear();
        M_ASSERT_V(exec_queues->size() == exec_qs_ranges->size(), "PL_%ld: Size mismatch : EQS(%lu) Ranges (%lu)\n",
                   _planner_id, exec_queues->size(), exec_qs_ranges->size());
        for (uint64_t r=0; r < exec_qs_ranges->size(); ++r){
            if (r == idx){
                // insert split
                M_ASSERT_V(exec_qs_ranges->get(r) != split_point+c_range_start,
                           "PL_%ld: old range = %lu, new range = %lu",
                           _planner_id,exec_qs_ranges->get(r), split_point+c_range_start);
                exec_qs_ranges_tmp->add(split_point+c_range_start);

                // add two new and empty exec_queues
                Array<exec_queue_entry> * nexec_q = NULL;
                Array<exec_queue_entry> * oexec_q = NULL;
#if MERGE_STRATEGY == RR
                quecc_pool.exec_queue_get_or_create(oexec_q, _planner_id, r % g_thread_cnt);
                quecc_pool.exec_queue_get_or_create(nexec_q, _planner_id, (r+1) % g_thread_cnt);
#else
                quecc_pool.exec_queue_get_or_create(oexec_q, _planner_id, et_id);
                quecc_pool.exec_queue_get_or_create(nexec_q, _planner_id, et_id);
#endif

//                M_ASSERT_V(oexec_q != mrange, "PL_%ld: oexec_q=%lu, nexec_q=%lu, mrange=%lu, trial=%d\n",
//                           _planner_id, (uint64_t) oexec_q, (uint64_t) nexec_q, (uint64_t) mrange, trial);

//                M_ASSERT_V(nexec_q != mrange, "PL_%ld: oexec_q=%lu, nexec_q=%lu, mrange=%lu, trial=%d\n",
//                           _planner_id, (uint64_t) oexec_q, (uint64_t) nexec_q, (uint64_t) mrange, trial);
//                assert(oexec_q->size() == 0);
//                assert(nexec_q->size() == 0);
                exec_queues_tmp->add(oexec_q);
                exec_queues_tmp->add(nexec_q);

            }
            else{
                exec_queues_tmp->add(exec_queues->get(r));
            }
            exec_qs_ranges_tmp->add(exec_qs_ranges->get(r));
        }

        // use new ranges to split current execq
        for (uint64_t r =0; r < mrange->size(); ++r){
            // TODO(tq): refactor this to respective benchmark implementation
#if WORKLOAD == YCSB
            ycsb_request *ycsb_req_tmp = (ycsb_request *) mrange->get_ptr(r)->req_buffer;
            nidx = get_split(ycsb_req_tmp->key, exec_qs_ranges_tmp);
#else
            nidx = get_split(mrange->get(r).rid, exec_qs_ranges_tmp);
#endif
            // all entries must fall into one of the splits
            M_ASSERT_V(nidx == idx || nidx == (idx+1), "nidx=%ld, idx = %ld\n", nidx, idx);

            exec_queues_tmp->get(nidx)->add(mrange->get(r));
        }

        // swap data structures
        exec_queues_tmp_tmp = exec_queues;
        exec_qs_ranges_tmp_tmp = exec_qs_ranges;

        exec_queues = exec_queues_tmp;
        exec_qs_ranges = exec_qs_ranges_tmp;

        exec_queues_tmp = exec_queues_tmp_tmp;
        exec_qs_ranges_tmp = exec_qs_ranges_tmp_tmp;

//        DEBUG_Q("Planner_%ld : After swapping New ranges size = %ld, old ranges size = %ld"
//                        "\n",
//                _planner_id, exec_qs_ranges->size(), exec_qs_ranges_tmp->size());

        // release current mrange
        quecc_pool.exec_queue_release(mrange,_planner_id,RAND(g_thread_cnt));
//        DEBUG_Q("PL_%ld: key =%lu, nidx=%ld, idx=%ld, trial=%d\n", _planner_id, key, nidx, idx, trial);

        // use the new ranges to assign the new execution entry
        idx = get_split(key, exec_qs_ranges);
        mrange = exec_queues->get(idx);
//#if DEBUG_QUECC
//        print_eqs_ranges_after_swap();
//#endif
    }
    INC_STATS(_thd_id, plan_split_time[_planner_id], get_sys_clock()-_prof_starttime);

#else
    M_ASSERT(false, "LAZY_SPLIT not supported in TPCC")
#endif
}

void PlannerThread::print_eqs_ranges_after_swap() const {//        // print contents after split
#if SPLIT_MERGE_ENABLED
    for (uint64_t i =0; i < exec_qs_ranges_tmp->size(); ++i){
        DEBUG_Q("PL_%ld: old exec_qs_ranges[%lu] = %lu\n", _planner_id, i, exec_qs_ranges_tmp->get(i));
    }

    for (uint64_t i =0; i < exec_queues_tmp->size(); ++i){
        DEBUG_Q("PL_%ld: old exec_queues[%lu] size = %lu, ptr = %lu, range= %lu\n",
                _planner_id, i, exec_queues_tmp->get(i)->size(), (uint64_t) exec_queues_tmp->get(i), exec_qs_ranges_tmp->get(i));
    }
#endif
    for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
        DEBUG_Q("PL_%ld: new exec_qs_ranges[%lu] = %lu\n", _planner_id, i, exec_qs_ranges->get(i));
    }
    for (uint64_t i =0; i < exec_queues->size(); ++i){
        DEBUG_Q("PL_%ld: new exec_queues[%lu] size = %lu, ptr = %lu, range=%lu\n",
                _planner_id, i, exec_queues->get(i)->size(), (uint64_t) exec_queues->get(i), exec_qs_ranges->get(i));
    }
}

void PlannerThread::print_eqs_ranges_before_swap() const {//        // print contents after split
#if SPLIT_MERGE_ENABLED
    for (uint64_t i =0; i < exec_qs_ranges_tmp->size(); ++i){
        DEBUG_Q("PL_%ld: new exec_qs_ranges[%lu] = %lu\n", _planner_id, i, exec_qs_ranges_tmp->get(i));
    }

    for (uint64_t i =0; i < exec_queues_tmp->size(); ++i){
        DEBUG_Q("PL_%ld: new exec_queues[%lu] size = %lu, ptr = %lu, range=%lu\n",
                _planner_id, i, exec_queues_tmp->get(i)->size(), (uint64_t) exec_queues_tmp->get(i), exec_qs_ranges_tmp->get(i));
    }
#endif
    for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
        DEBUG_Q("PL_%ld: old exec_qs_ranges[%lu] = %lu\n", _planner_id, i, exec_qs_ranges->get(i));
    }
    for (uint64_t i =0; i < exec_queues->size(); ++i){
        DEBUG_Q("PL_%ld: old exec_queues[%lu] size = %lu, ptr = %lu, range=%lu\n",
                _planner_id, i, exec_queues->get(i)->size(), (uint64_t) exec_queues->get(i), exec_qs_ranges->get(i));
    }
}

// QueCCPool methods implementation

void QueCCPool::init(Workload * wl, uint64_t size){

    planner_batch_size = g_batch_size/g_plan_thread_cnt;
    row_data_pool_lock = new spinlock();
//#if NUMA_ENABLED
//    numa_set_preferred(0);
//#endif
#if WORKLOAD == YCSB
#if EXPANDABLE_EQS
    exec_queue_capacity = std::ceil((double)planner_batch_size/g_thread_cnt) * REQ_PER_QUERY;
#else
    exec_queue_capacity = std::ceil((double)planner_batch_size/g_thread_cnt) * REQ_PER_QUERY * EXECQ_CAP_FACTOR;
#endif
    if (exec_queue_capacity < MIN_EXECQ_SIZE){
        exec_queue_capacity = MIN_EXECQ_SIZE;
    }
    uint64_t tuple_size = wl->tables["MAIN_TABLE"]->get_schema()->get_tuple_size();
#if ROW_ACCESS_IN_CTX
// intialize for QueCC undo_buffer int contexts here
    for (int i = 0; i < BATCH_MAP_LENGTH; ++i) {
        for (uint32_t j = 0; j < g_plan_thread_cnt; ++j) {
            for (uint64_t k = 0; k < planner_batch_size; ++k) {
                work_queue.batch_pg_map[i][j].txn_ctxs[k].undo_buffer_inialized = true;
                work_queue.batch_pg_map[i][j].txn_ctxs[k].undo_buffer_data = (char *) malloc(tuple_size*REQ_PER_QUERY);
            }
        }
    }
#endif

    boost::lockfree::queue<char *> * pool = new boost::lockfree::queue<char *> (FREE_LIST_INITIAL_SIZE);
    /// Preallocate may mess up NUMA locality. Since, we have a warmup phase, it may not incure overhead during
    // measure phase
    for (uint64_t i = 0; i < g_thread_cnt; ++i){
        row_data_pool[i].insert({size,pool});
        for (uint64_t j=0; j < (g_batch_size)/g_thread_cnt; ++j){
////#if NUMA_ENABLED
////            char * tmp = (char *) mem_allocator.align_alloc_onnode(sizeof(char)*tuple_size, i);
////#else
            char * tmp = (char *) mem_allocator.alloc(sizeof(char)*tuple_size);
////#endif
            pool->push(tmp);
        }
    }


#elif WORKLOAD == TPCC
    exec_queue_capacity = (planner_batch_size)*(EXECQ_CAP_FACTOR);
#else
    assert(false);
#endif
    M_ASSERT_V(exec_queue_capacity > 0, "EQ size is zero??\n")
#if DEBUG_QUECC
    printf("\nEQ Max size = %ld\n",exec_queue_capacity);
    fflush(stdout);
//    assert(false);
#endif
//    exec_queue_free_list = new boost::lockfree::queue<Array<exec_queue_entry> *> * [THREAD_CNT];
//    batch_part_free_list = new boost::lockfree::queue<batch_partition *> * [THREAD_CNT];

    // populate with g_thead_cnt+1 pools. The pool at g_thread_cnt will be used when there is no perference to specific
    // thread id
    // FIXME(tq): do we need this for batch partitions
    for (uint64_t i=0; i < g_thread_cnt; ++i){
        for (uint64_t j=0; j < g_plan_thread_cnt; ++j){
            exec_queue_free_list[i][j]     = new boost::lockfree::queue<Array<exec_queue_entry> *> (FREE_LIST_INITIAL_SIZE);
            batch_part_free_list[i][j]     = new boost::lockfree::queue<batch_partition *> (FREE_LIST_INITIAL_SIZE);
            exec_qs_status_free_list[i][j] = new boost::lockfree::queue<atomic<uint8_t> *>(FREE_LIST_INITIAL_SIZE);


#if DEBUG_QUECC
            exec_q_alloc_cnts[i][j].store(0);
            exec_q_rel_cnts[i][j].store(0);
            exec_q_reuse_cnts[i][j].store(0);
            batch_part_rel_cnts[i][j].store(0);
            batch_part_alloc_cnts[i][j].store(0);
            batch_part_reuse_cnts[i][j].store(0);
#endif
        }

//        for (uint64_t j=0; j < FREE_LIST_INITIAL_SIZE+10; ++j){
//            Array<exec_queue_entry> * exec_q_tmp = (Array<exec_queue_entry> *) mem_allocator.alloc(sizeof(Array<exec_queue_entry>));
//            exec_q_tmp->init(exec_queue_capacity);
//            while(!exec_queue_free_list[i]->push(exec_q_tmp)){};
//
//            // Pre-allocate batch paritions and to free_list for future use
//            batch_partition * batch_part_tmp = (batch_partition *) mem_allocator.alloc(sizeof(batch_partition));
//            while(!batch_part_free_list[i]->push(batch_part_tmp));
//        }
    }

    exec_qs_free_list = new boost::lockfree::queue<Array<Array<exec_queue_entry> *> *> * [g_plan_thread_cnt];
//    exec_qs_status_free_list = new boost::lockfree::queue<atomic<uint8_t> *> * [g_plan_thread_cnt];
    txn_ctxs_free_list = new boost::lockfree::queue<transaction_context *> * [g_plan_thread_cnt];
    pg_free_list = new boost::lockfree::queue<priority_group *> * [g_plan_thread_cnt];
#if EXEC_BUILD_TXN_DEPS && TDG_ENTRY_TYPE == ARRAY_ENTRY
    for (UInt32 i = 0; i < g_thread_cnt; ++i) {
        tctx_ptr_free_list[i] = new boost::lockfree::queue<Array<transaction_context *> *>(FREE_LIST_INITIAL_SIZE);
    }
#endif
    for ( uint64_t i = 0; i < g_plan_thread_cnt; i++) {
#if TDG_ENTRY_TYPE == VECTOR_ENTRY
        vector_free_list[i] = new boost::lockfree::queue<std::vector<uint64_t> *>(FREE_LIST_INITIAL_SIZE);
#elif TDG_ENTRY_TYPE == ARRAY_ENTRY
        vector_free_list[i] = new boost::lockfree::queue<Array<uint64_t> *>(FREE_LIST_INITIAL_SIZE);
#endif

        exec_qs_free_list[i] = new boost::lockfree::queue<Array<Array<exec_queue_entry> *> *>(FREE_LIST_INITIAL_SIZE);
//        exec_qs_status_free_list[i] = new boost::lockfree::queue<atomic<uint8_t> *>(FREE_LIST_INITIAL_SIZE);
        txn_ctxs_free_list[i] = new boost::lockfree::queue<transaction_context *> (FREE_LIST_INITIAL_SIZE);
        pg_free_list[i] = new boost::lockfree::queue<priority_group *> (FREE_LIST_INITIAL_SIZE);

#if DEBUG_QUECC
        exec_qs_alloc_cnts[i].store(0);
        exec_qs_rel_cnts[i].store(0);
        exec_qs_reuse_cnts[i].store(0);

        txn_ctxs_alloc_cnts[i].store(0);
        txn_ctxs_rel_cnts[i].store(0);
        txn_ctxs_reuse_cnts[i].store(0);

        pg_alloc_cnts[i].store(0);
        pg_rel_cnts[i].store(0);
        pg_reuse_cnts[i].store(0);
#endif

    }


}

void QueCCPool::free_all() {

}

void QueCCPool::exec_queue_get_or_create(Array<exec_queue_entry> *&exec_q, uint64_t planner_id, uint64_t et_id){
    if (!exec_queue_free_list[et_id][planner_id]->pop(exec_q)){
//#if NUMA_ENABLED
        // Allocating this memory on execution thread's node will make it faster for executors to read queues
        // However, since assignment to ETs can change, it may be better allocate on planner's
        // We should study which approach is better
//        exec_q = (Array<exec_queue_entry> *) mem_allocator.align_alloc_onnode(sizeof(Array<exec_queue_entry>), planner_id);
//        exec_q->init_numa(exec_queue_capacity, planner_id);
//#else
        exec_q = (Array<exec_queue_entry> *) mem_allocator.alloc(sizeof(Array<exec_queue_entry>));
#if EXPANDABLE_EQS
        exec_q->init_expandable(exec_queue_capacity, EXECQ_CAP_FACTOR);
#else
        exec_q->init(exec_queue_capacity);
#endif
//#endif
        exec_q->set_et_id(et_id);
        exec_q->set_pt_id(planner_id);
#if DEBUG_QUECC
        exec_q_alloc_cnts[et_id][planner_id].fetch_add(1);
//        SAMPLED_DEBUG_Q("Allocating exec_q, pt_id=%ld, et_id=%ld\n", planner_id, et_id);
#endif
    }
    else{
        M_ASSERT_V(exec_q, "Invalid exec_q. PT_%ld, ET_%ld\n", planner_id, et_id);
        exec_q->clear();
#if DEBUG_QUECC
        exec_q_reuse_cnts[et_id][planner_id].fetch_add(1);
//        SAMPLED_DEBUG_Q("Reusing exec_q, pt_id=%ld, et_id=%ld\n", planner_id, et_id);
#endif
    }
}

void QueCCPool::exec_queue_release(Array<exec_queue_entry> *&exec_q, uint64_t planner_id, uint64_t et_id){
    M_ASSERT_V(exec_q, "Invalid exec_q. PT_%ld, ET_%ld\n", planner_id, et_id);
    exec_q->clear();
    int64_t qet_id = exec_q->et_id();
    int64_t qpt_id = exec_q->pt_id();
//    while(!exec_queue_free_list[et_id][planner_id]->push(exec_q)){};
    while(!exec_queue_free_list[qet_id][qpt_id]->push(exec_q)){};
#if DEBUG_QUECC
//    SAMPLED_DEBUG_Q("PL_%ld, ET_%ld: relaseing exec_q ptr = %lu\n", planner_id, et_id, (uint64_t) exec_q);
    exec_q_rel_cnts[et_id][planner_id].fetch_add(1);
#endif
}


void QueCCPool::batch_part_get_or_create(batch_partition *&batch_part, uint64_t planner_id, uint64_t et_id){
    if (!batch_part_free_list[et_id][planner_id]->pop(batch_part)){
//#if NUMA_ENABLED
        // We allocate on Execution thread's node but that should not matter a lot.
//        batch_part = (batch_partition *) mem_allocator.align_alloc_onnode(sizeof(batch_partition), et_id);
//#else
        batch_part = (batch_partition *) mem_allocator.alloc(sizeof(batch_partition));
//#endif
#if DEBUG_QUECC
//        DEBUG_Q("Allocating batch_p, pt_id=%ld, et_id=%ld\n", planner_id, et_id);
        batch_part_alloc_cnts[et_id][planner_id].fetch_add(1);

    }
    else {
//        DEBUG_Q("Reusing batch_p, pt_id=%ld, et_id=%ld\n", planner_id, et_id);
        batch_part_reuse_cnts[et_id][planner_id].fetch_add(1);
#endif
    }
    batch_part->single_q = true;
    batch_part->empty = false;
//    batch_part->status.store(0);
    batch_part->exec_q_status.store(0);
}
void QueCCPool::batch_part_release(batch_partition *&batch_p, uint64_t planner_id, uint64_t et_id){
    while(!batch_part_free_list[et_id][planner_id]->push(batch_p));
#if DEBUG_QUECC
    batch_part_rel_cnts[et_id][planner_id].fetch_add(1);
#endif
}


void QueCCPool::exec_qs_get_or_create(Array<Array<exec_queue_entry> *> *&exec_qs, uint64_t planner_id){
    if (!exec_qs_free_list[planner_id]->pop(exec_qs)){
        exec_qs = new Array<Array<exec_queue_entry> *>();
        exec_qs->init(g_exec_qs_max_size);
#if DEBUG_QUECC
//        DEBUG_Q("Allocating new exec_qs, pt_id=%ld\n", planner_id);
        exec_qs_alloc_cnts[planner_id].fetch_add(1);
    }
    else{
//        DEBUG_Q("Reusing exec_qs, pt_id=%ld\n", planner_id);
        exec_qs_reuse_cnts[planner_id].fetch_add(1);
#endif
    }
}

void QueCCPool::exec_qs_release(Array<Array<exec_queue_entry> *> *&exec_qs, uint64_t planner_id){
    while(!exec_qs_free_list[planner_id]->push(exec_qs)){}
#if DEBUG_QUECC
    exec_qs_rel_cnts[planner_id].fetch_add(1);
#endif
}


void QueCCPool::exec_qs_status_get_or_create(atomic<uint8_t> *&execqs_status, uint64_t planner_id, uint64_t et_id){
    if (!exec_qs_status_free_list[et_id][planner_id]->pop(execqs_status)){
//#if NUMA_ENABLED
//        execqs_status = (atomic<uint8_t> *) mem_allocator.align_alloc_onnode(
//                sizeof(uint64_t)*g_exec_qs_max_size, planner_id);
//#else
        execqs_status = (atomic<uint8_t> *) mem_allocator.alloc(
                sizeof(uint64_t)*g_exec_qs_max_size);
//#endif

    }
#if DEBUG_QUECC
    else{
//        DEBUG_Q("Reusing exec_qs\n");
    }
#endif
    memset(execqs_status, 0, sizeof(uint64_t)*g_exec_qs_max_size);
}
void QueCCPool::exec_qs_status_release(atomic<uint8_t> *&execqs_status, uint64_t planner_id, uint64_t et_id){
    while(!exec_qs_status_free_list[et_id][planner_id]->push(execqs_status)){}
}

// Txn Ctxs
void QueCCPool::txn_ctxs_get_or_create(transaction_context * &txn_ctxs, uint64_t planner_id){
    if (!txn_ctxs_free_list[planner_id]->pop(txn_ctxs)){
//        DEBUG_Q("Allocating txn_ctxs\n");
//#if NUMA_ENABLED
//        txn_ctxs = (transaction_context *) mem_allocator.align_alloc_onnode(sizeof(transaction_context)*planner_batch_size, planner_id);
//#else
        txn_ctxs = (transaction_context *) mem_allocator.alloc(sizeof(transaction_context)*planner_batch_size);
//#endif
        // FIXME: intialize accesses when this is used
#if DEBUG_QUECC
        txn_ctxs_alloc_cnts[planner_id].fetch_add(1);
    }
    else {
//        DEBUG_Q("Reusing txn_ctxs\n");
        txn_ctxs_reuse_cnts[planner_id].fetch_add(1);
#endif
    }
    memset(txn_ctxs, 0,sizeof(transaction_context)*planner_batch_size);
}
void QueCCPool::txn_ctxs_release(transaction_context * &txn_ctxs, uint64_t planner_id){
    while(!txn_ctxs_free_list[planner_id]->push(txn_ctxs));
#if DEBUG_QUECC
    txn_ctxs_rel_cnts[planner_id].fetch_add(1);
#endif
}

// PGs

void QueCCPool::pg_get_or_create(priority_group * &pg, uint64_t planner_id){
    if (!pg_free_list[planner_id]->pop(pg)){
//#if NUMA_ENABLED
//        pg = (priority_group *) mem_allocator.align_alloc_onnode(sizeof(priority_group), planner_id);
//#else
        pg = (priority_group *) mem_allocator.alloc(sizeof(priority_group));
//#endif

//        DEBUG_Q("Allocating priority group\n");
#if DEBUG_QUECC
        pg_alloc_cnts[planner_id].fetch_add(1);
    }
    else{
//        DEBUG_Q("Reuseing priority group\n");
        pg_reuse_cnts[planner_id].fetch_add(1);
#endif
    }
    memset(pg, 0,sizeof(priority_group));
}

void QueCCPool::pg_release(priority_group * &pg, uint64_t planner_id){
    while(!pg_free_list[planner_id]->push(pg)){};
#if DEBUG_QUECC
    pg_rel_cnts[planner_id].fetch_add(1);
#endif
}

// Data buffer pools
void QueCCPool::databuff_get_or_create(char * &buf, uint64_t size, uint64_t thd_id){
    auto search = row_data_pool[thd_id].find(size);
    if (search == row_data_pool[thd_id].end()){
        // size is not found
        // create a queue for that size
        search = row_data_pool[thd_id].find(size);
        if (search == row_data_pool[thd_id].end()){
            boost::lockfree::queue<char *> * pool = new boost::lockfree::queue<char *> (FREE_LIST_INITIAL_SIZE);
            row_data_pool[thd_id].insert({size,pool});
        }
//#if NUMA_ENABLED
//        buf = (char *) mem_allocator.align_alloc_onnode(sizeof(char)*size, thd_id);
//#else
        buf = (char *) mem_allocator.align_alloc(sizeof(char)*size);
//#endif

    }
    else{
        if (!search->second->pop(buf)){
//#if NUMA_ENABLED
//            buf = (char *) mem_allocator.align_alloc_onnode(sizeof(char)*size, thd_id);
//#else
            buf = (char *) mem_allocator.align_alloc(sizeof(char)*size);
//#endif
//            DEBUG_Q("Allocatated row data\n");
        }
    }
}
void QueCCPool::databuff_release(char * &buf, uint64_t size, uint64_t thd_id){
    auto search = row_data_pool[thd_id].find(size);
    if (search == row_data_pool[thd_id].end()){
        M_ASSERT_V(false, "Could not find pool for given size = %ld\n", size);
    }
    else{
        while(!search->second->push(buf)){};
    }
}

void QueCCPool::print_stats(uint64_t batch_id) {
    printf("QueCC pool stats ... batch_id = %ld\n", batch_id);
    fflush(stdout);
    uint64_t total_exec_q_alloc_cnt = 0;
    uint64_t total_exec_q_reuse_cnt = 0;
    uint64_t total_exec_q_rel_cnt = 0;

    uint64_t total_bp_alloc_cnt = 0;
    uint64_t total_bp_reuse_cnt = 0;
    uint64_t total_bp_rel_cnt = 0;

    uint64_t total_exec_qs_alloc_cnt = 0;
    uint64_t total_exec_qs_reuse_cnt = 0;
    uint64_t total_exec_qs_rel_cnt = 0;

    uint64_t total_txn_ctxs_alloc_cnt = 0;
    uint64_t total_txn_ctxs_reuse_cnt = 0;
    uint64_t total_txn_ctxs_rel_cnt = 0;

    uint64_t total_pg_alloc_cnt = 0;
    uint64_t total_pg_reuse_cnt = 0;
    uint64_t total_pg_rel_cnt = 0;


#if DEBUG_QUECC
    for (uint64_t i=0; i < g_thread_cnt; ++i){
        for (uint64_t j=0; j < g_plan_thread_cnt; ++j){
//            printf("exec_q alloc cnt for ET_%ld, PT_%ld = %ld, batch_id = %ld\n", i,j, exec_q_alloc_cnts[i][j].fetch_add(0), batch_id);
//            printf("exec_q reuse cnt for ET_%ld, PT_%ld = %ld, batch_id = %ld\n", i,j, exec_q_reuse_cnts[i][j].fetch_add(0), batch_id);
//            printf("exec_q release cnt for ET_%ld, PT_%ld = %ld, batch_id = %ld\n", i,j, exec_q_rel_cnts[i][j].fetch_add(0), batch_id);

//            printf("batch_part alloc for ET_%ld, PT_%ld = %ld\n", i,j, batch_part_alloc_cnts[i][j].load());
//            printf("batch_part reuse for ET_%ld, PT_%ld = %ld\n", i,j, batch_part_reuse_cnts[i][j].load());
//            printf("batch_part release for ET_%ld, PT_%ld = %ld\n", i,j, batch_part_rel_cnts[i][j].load());
//            fflush(stdout);

            total_exec_q_alloc_cnt += exec_q_alloc_cnts[i][j].fetch_add(0);
            total_exec_q_reuse_cnt += exec_q_reuse_cnts[i][j].fetch_add(0);
            total_exec_q_rel_cnt += exec_q_rel_cnts[i][j].fetch_add(0);

            total_bp_alloc_cnt += batch_part_alloc_cnts[i][j].fetch_add(0);
            total_bp_reuse_cnt += batch_part_reuse_cnts[i][j].fetch_add(0);
            total_bp_rel_cnt += batch_part_rel_cnts[i][j].fetch_add(0);

            exec_q_alloc_cnts[i][j].store(0);
            exec_q_reuse_cnts[i][j].store(0);
            exec_q_rel_cnts[i][j].store(0);

            batch_part_alloc_cnts[i][j].store(0);
            batch_part_reuse_cnts[i][j].store(0);
            batch_part_rel_cnts[i][j].store(0);
        }
    }
#endif

    for ( uint64_t i = 0; i < g_plan_thread_cnt; i++) {
//        printf("exec_qs alloc for ET_%ld = %ld\n", i, exec_qs_alloc_cnts[i].load());
//        printf("exec_qs reuse for ET_%ld = %ld\n", i, exec_qs_reuse_cnts[i].load());
//        printf("exec_qs release for ET_%ld = %ld\n", i, exec_qs_rel_cnts[i].load());
//        fflush(stdout);

#if DEBUG_QUECC
        total_exec_qs_alloc_cnt += exec_qs_alloc_cnts[i].load();
        total_exec_qs_reuse_cnt += exec_qs_reuse_cnts[i].load();
        total_exec_qs_rel_cnt += exec_qs_rel_cnts[i].load();

        total_txn_ctxs_alloc_cnt += txn_ctxs_alloc_cnts[i].load();
        total_txn_ctxs_reuse_cnt += txn_ctxs_reuse_cnts[i].load();
        total_txn_ctxs_rel_cnt += txn_ctxs_rel_cnts[i].load();

        total_pg_alloc_cnt += pg_alloc_cnts[i].load();
        total_pg_reuse_cnt += pg_reuse_cnts[i].load();
        total_pg_rel_cnt += pg_rel_cnts[i].load();
#endif
    }

    printf("total exec_q alloc = %ld\n", total_exec_q_alloc_cnt);
    printf("total exec_q reuse = %ld\n", total_exec_q_reuse_cnt);
    printf("total exec_q release = %ld\n", total_exec_q_rel_cnt);
    printf("diff exec_q cnts = %ld\n", (total_exec_q_alloc_cnt+total_exec_q_reuse_cnt)-total_exec_q_rel_cnt);

    printf("total batch_part alloc = %ld\n", total_bp_alloc_cnt);
    printf("total batch_part reuse = %ld\n", total_bp_reuse_cnt);
    printf("total batch_part release = %ld\n", total_bp_rel_cnt);
    printf("diff batch_part cnts = %ld\n", (total_bp_alloc_cnt+total_bp_reuse_cnt)-total_bp_rel_cnt);

    printf("total exec_qs alloc = %ld\n", total_exec_qs_alloc_cnt);
    printf("total exec_qs reuse = %ld\n", total_exec_qs_reuse_cnt);
    printf("total exec_qs release = %ld\n", total_exec_qs_rel_cnt);
    printf("diff exec_qs cnts = %ld\n", (total_exec_qs_alloc_cnt+total_exec_qs_reuse_cnt)-total_exec_qs_rel_cnt);


    printf("total txn_ctxs alloc = %ld\n", total_txn_ctxs_alloc_cnt);
    printf("total txn_ctxs reuse = %ld\n", total_txn_ctxs_reuse_cnt);
    printf("total txn_ctxs release = %ld\n", total_txn_ctxs_rel_cnt);
    printf("diff txn_ctxs cnts = %ld\n", (total_txn_ctxs_alloc_cnt+total_txn_ctxs_reuse_cnt)-total_txn_ctxs_rel_cnt);

    printf("total pg alloc = %ld\n", total_pg_alloc_cnt);
    printf("total pg reuse = %ld\n", total_pg_reuse_cnt);
    printf("total pg release = %ld\n", total_pg_rel_cnt);
    printf("diff pg cnts = %ld\n", (total_pg_alloc_cnt+total_pg_reuse_cnt)-total_pg_rel_cnt);

    printf("size of exec_queue_entry = %ld\n", sizeof(exec_queue_entry));
    printf("size of Array<exec_queue_entry> = %ld\n", sizeof(Array<exec_queue_entry>));
    printf("exec_queue_capacity = %ld\n", exec_queue_capacity);
    uint64_t exec_q_sum = total_exec_q_alloc_cnt*((exec_queue_capacity*sizeof(exec_queue_entry))+sizeof(Array<exec_queue_entry>));
    printf("total allocated size for exec_q = %ld\n",
           exec_q_sum/1024);
    uint64_t exec_qs_sum = total_exec_qs_alloc_cnt*((sizeof(Array<exec_queue_entry> *)*32)+sizeof(Array<Array<exec_queue_entry> *>));
    printf("total allocated size for exec_qs = %ld\n",
           exec_qs_sum/1024);
    printf("total allocated size for all = %ld\n",
           (exec_qs_sum+exec_q_sum)/1024);

    printf("size of access type = %ld\n",
           sizeof(access_t));
    fflush(stdout);

}
#endif