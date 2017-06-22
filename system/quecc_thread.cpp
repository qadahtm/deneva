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
#include <boost/heap/priority_queue.hpp>
//TQ: we will use standard lib version for now. We can use optimized implementation later
#include <unordered_map>

void SplitEntry::printEntries(uint64_t i, uint64_t planner_id) const{
//    if (children[0] != NULL){
//        children[0]->printEntries(i+1, planner_id);
//    }
//    else if (children[0] != NULL){
//        children[1]->printEntries(i+2, planner_id);
//    }
//    else {
    DEBUG_Q("Planner_%ld: mrange = %ld, exec_q size = %ld, from %ld to %ld\n", planner_id, i, exec_q->size(), range_start, range_end);
//    }
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
        M_ASSERT_V(tmp_p != 0, "CT_%ld : txn_ctxs is not inisitalized, batch_slot = %ld, batch_id = %ld, cplanner_id = %ld, tmp_p = %ld\n",
                   _thd_id, batch_slot, batch_id, cplanner_id, tmp_p);

        planner_pg = ((priority_group *) tmp_p);

        M_ASSERT_V(batch_id == planner_pg->batch_id, "CT_%ld : batch_id mismatch, batch_slot = %ld, batch_id = %ld, pg_batch_id = %ld\n",
                   _thd_id, batch_slot, batch_id, planner_pg->batch_id);

        M_ASSERT_V(cplanner_id == planner_pg->planner_id, "CT_%ld : txn_ctxs is not inisitalized, batch_slot = %ld, batch_id = %ld, cplanner_id = %ld, pg_planner_id = %ld\n",
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
        while(!work_queue.txn_ctxs_free_list[cplanner_id]->push(txn_ctxs)){}


        // Clean up and clear txn_graph
        for (auto it = planner_pg->txn_dep_graph->begin(); it != planner_pg->txn_dep_graph->end(); ++it){
            delete it->second;
        }
        delete planner_pg->txn_dep_graph;

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

        cplanner_id++;
//        if (idle_starttime == 0){
//            idle_starttime = get_sys_clock();
//        }

        if (cplanner_id == g_plan_thread_cnt){
            // proceed to the next batch
            batch_id++;
            batch_slot = batch_id % g_batch_map_length;
            cplanner_id = 0;
        }
    }
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
    uint64_t exec_queue_limit = planner_batch_size/g_thread_cnt;
    // max capcity, assume YCSB workload
//    uint64_t exec_queue_capacity = planner_batch_size*10;
    uint64_t exec_queue_capacity = 1024 * 32;
#if BATCHING_MODE == SIZE_BASED
    exec_queue_capacity = planner_batch_size * 10;
#endif

    // create and and pre-allocate execution queues
    // For each mrange which will be assigned to an execution thread
    // there will be an array pointer.
    // When the batch is complete we will CAS the exec_q array to allow
    // execution threads to be

// implementtion using array class
    Array<Array<exec_queue_entry> *> * exec_queues = new Array<Array<exec_queue_entry> *>();
    exec_queues->init(g_thread_cnt);
    for (uint64_t i = 0; i < g_thread_cnt; i++) {
        Array<exec_queue_entry> * exec_q = new Array<exec_queue_entry>();
        exec_q->init(exec_queue_capacity);
        exec_queues->add(exec_q);
    }

    // Pre-allocate execution queues to be used by planners and executors
    // this should eliminate the overhead of memory allocation
    uint64_t  exec_queue_pool_size = 10;
    for (uint64_t i = 0; i < g_thread_cnt; i++) {
        for (uint64_t j = 0; j < exec_queue_pool_size; j++){
            Array<exec_queue_entry> * exec_q = new Array<exec_queue_entry>();
            exec_q->init(exec_queue_capacity);
            while(!work_queue.exec_queue_free_list[i]->push(exec_q)){}
        }
    }

    // Pre-allocate planner's transaction contexts
    uint64_t  txn_ctxs_pool_size = 10;
    for (uint64_t i = 0; i < g_plan_thread_cnt; i++) {
        for (uint64_t j = 0; j < txn_ctxs_pool_size; j++){
            transaction_context * txn_ctxs_tmp = (transaction_context *) mem_allocator.alloc(
                    sizeof(transaction_context)*planner_batch_size);
            memset(txn_ctxs_tmp, 0,sizeof(transaction_context)*planner_batch_size );
            while(!work_queue.txn_ctxs_free_list[i]->push(txn_ctxs_tmp)){}
        }
    }

    transaction_context * txn_ctxs = NULL;
    if (work_queue.txn_ctxs_free_list[_planner_id]->pop(txn_ctxs)){
        memset(txn_ctxs, 0,sizeof(transaction_context)*planner_batch_size );
    }
    else{
        txn_ctxs = (transaction_context *) mem_allocator.alloc(sizeof(transaction_context)*planner_batch_size);
    }
    priority_group * planner_pg = (priority_group *) mem_allocator.alloc(sizeof(priority_group));
    planner_pg->planner_id = _planner_id;
    planner_pg->txn_ctxs = txn_ctxs;

#if DEBUG_QUECC
    Array<uint64_t> mrange_cnts;
    mrange_cnts.init(g_thread_cnt);

    for (uint64_t i = 0; i < g_thread_cnt; i++) {
        mrange_cnts.add(0);
    }
    uint64_t total_msg_processed_cnt = 0;
    uint64_t total_access_cnt = 0;
#endif
    planner_txn_id = txn_prefix_planner_base;
    txn_dep_graph = new hash_table_t();

    uint64_t batch_starting_txn_id = planner_txn_id;

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

        if (force_batch_delivery){
            slot_num = (batch_id % g_batch_map_length);

            if (BATCHING_MODE == TIME_BASED) {
                INC_STATS(_thd_id, plan_time_batch_cnts[_planner_id], 1)
                force_batch_delivery = false;
            } else{
                INC_STATS(_thd_id, plan_size_batch_cnts[_planner_id], 1)
            }

//            DEBUG_Q("Batch complete\n")
            // a batch is ready to be delivered to the the execution threads.
            // we will have a batch queue for each of the planners and they are scanned in known order
            // by execution threads
            // We also have a batch queue for each of the executor that is mapped to bucket
            // for now, we will assign a bucket to each executor
            // All we need to do is to automically CAS the pointer to the address of the accumulated batch
            // and the execution threads who are spinning can start execution

            // Here major ranges have one-to-one mapping to worker threads
            prof_starttime = get_sys_clock();
            for (uint64_t i = 0; i < g_thread_cnt; i++){
//                DEBUG_Q("old value for ptr to  exec_q[%ld][%ld][%ld] is %ld\n", _planner_id, i, batch_id, exe_q_ptr);
//                DEBUG_Q("new value for ptr to  exec_q[%ld][%ld][%ld] is %ld\n", _planner_id, i, batch_id, (uint64_t) exec_queues->get(i));

//                DEBUG_Q("Batch %ld is ready! setting map slot [%ld][%ld][%ld]\n", batch_id, i, _planner_id, slot_num);


                Array<exec_queue_entry> * exec_q = exec_queues->get(i);
                // use batch_partition
                batch_partition * batch_part = (batch_partition *) mem_allocator.alloc(sizeof(batch_partition));
                batch_part->planner_id = _planner_id;
                batch_part->batch_id = batch_id;
                batch_part->exec_q = exec_q;
                batch_part->single_q = true;
                batch_part->batch_part_status.store(0);
                batch_part->exec_q_status.store(0);

                // Check if we need to split
                // 10 is the number of operations in each transaction, we are assuming YCSB here
                exec_queue_limit = (batch_cnt/g_thread_cnt) * 10 * EXECQ_CAP_FACTOR;

                if (exec_queue_limit < exec_q->size()){
//                    DEBUG_Q("Planner_%ld : We need to split for range %ld, current size = %ld, limit = %ld,"
//                                    " batch_cnt = %ld, planner_batch_size = %ld, batch_id = %ld"
//                                    "\n",
//                            _planner_id, i, exec_q->size(), exec_queue_limit, batch_cnt, planner_batch_size, batch_id);

                    batch_part->single_q = false;

                    boost::heap::priority_queue<SplitEntry, boost::heap::compare<SplitEntryCompareSize>> pq;

                    uint64_t split_rounds = 0;

                    SplitEntry * root = new SplitEntry();
                    root->exec_q = exec_q;
                    root->range_start = (i*bucket_size);
                    root->range_end = ((i+1)*bucket_size);
                    root->range_size = bucket_size;
                    SplitEntry * top_entry = root;

                    while (true){
                        //split current queue in half
//                        Array<exec_queue_entry> * cexec_q = exec_queues->get(i);
                        // Allocate memory for new exec_qs
                        Array<exec_queue_entry> ** nexec_qs = (Array<exec_queue_entry> **) mem_allocator.alloc(sizeof(Array<exec_queue_entry> *)*2);

                        if (work_queue.exec_queue_free_list[i]->pop(nexec_qs[0])){
                            nexec_qs[0]->clear();
                        }
                        else{
                            nexec_qs[0] = new Array<exec_queue_entry>();
                            nexec_qs[0]->init(exec_queue_capacity);
                        }

                        if (work_queue.exec_queue_free_list[i]->pop(nexec_qs[1])){
                            nexec_qs[1]->clear();
                        }
                        else{
                            nexec_qs[1] = new Array<exec_queue_entry>();
                            nexec_qs[1]->init(exec_queue_capacity);
                        }

                        //split current exec_q
                        for (uint64_t j = 0; j < top_entry->exec_q->size(); j++){
                            exec_queue_entry exec_qe = top_entry->exec_q->get(j);
                            ycsb_request *req = (ycsb_request *) &exec_qe.req_buffer;
//                        uint32_t get_split(uint64_t key, uint_32_t range_cnt, uint64_t range_start, uint64_t range_end);
                            uint32_t newrange = get_split(req->key, 2, top_entry->range_start, top_entry->range_end);
//                            min_range_cnts[newrange]++;
                            nexec_qs[newrange]->add(exec_qe);
                        }

                        //TODO(tq), use je_malloc instead of new?
                        SplitEntry * nsp_entries = new SplitEntry[2];
                        uint64_t range_size = ((top_entry->range_end - top_entry->range_start)/2);
                        nsp_entries[0].exec_q = nexec_qs[0];
                        nsp_entries[0].range_start = top_entry->range_start;
                        nsp_entries[0].range_end = range_size+top_entry->range_start;
                        nsp_entries[0].range_size = range_size;
                        nsp_entries[0].children[0] = NULL;
                        nsp_entries[0].children[1] = NULL;

                        nsp_entries[1].exec_q = nexec_qs[1];
                        nsp_entries[1].range_start = range_size+top_entry->range_start;
                        nsp_entries[1].range_end = top_entry->range_end;
                        nsp_entries[1].range_size = range_size;
                        nsp_entries[1].children[0] = NULL;
                        nsp_entries[1].children[1] = NULL;


                        pq.push(nsp_entries[0]);
                        pq.push(nsp_entries[1]);

                        // update top entry with new ranges
                        top_entry->children[0] = &nsp_entries[0];
                        top_entry->children[1] = &nsp_entries[1];
//                        top_entry->exec_q->release();

//                        DEBUG_Q("Planner_%ld : Split exec_q of mrange(%ld), pq.size = %ld, split rounds = %ld, into 2 subranges, "
//                                        "parent : size = %ld, rs = %ld, re = %ld, "
//                                        "child_0 : size = %ld, rs = %ld, re = %ld, "
//                                        "child_1 : size = %ld, rs = %ld, re = %ld"
//                                        "\n",
//                                _planner_id, i, pq.size(), split_rounds,
//                                top_entry->exec_q->size(), top_entry->range_start, top_entry->range_end,
//                                top_entry->children[0]->exec_q->size(), top_entry->children[0]->range_start, top_entry->children[0]->range_end,
//                                top_entry->children[1]->exec_q->size(), top_entry->children[1]->range_start, top_entry->children[1]->range_end
//                        );

                        // Recucle execution queue
//                        if (split_rounds != 0){
                            // skip the root, for now
                            while(!work_queue.exec_queue_free_list[i]->push(top_entry->exec_q)) {};
//                        }
                        top_entry->exec_q = NULL;

                        if(pq.top().exec_q->size() <= exec_queue_limit){

                            break;
                        }
                        else {
                            split_rounds++;
                            top_entry = new SplitEntry();
                            top_entry->exec_q = pq.top().exec_q;
                            top_entry->range_start = pq.top().range_start;
                            top_entry->range_end = pq.top().range_end;
                            top_entry->children[0] = pq.top().children[0];
                            top_entry->children[1] = pq.top().children[1];

                            pq.pop();
                        }

                    }

                    // Visualize the exec_queues
//                    DEBUG_Q("Planner_%ld : Visualizing splits of mrange %ld at batch_id: %ld\n", _planner_id, i, batch_id);
//                    boost::heap::priority_queue<SplitEntry, boost::heap::compare<SplitEntryCompareStartRange>> sorted;
//                    while (!pq.empty()){
//                        sorted.push(pq.top());
//                        pq.pop();
//                    }
//                    while (!sorted.empty()){
//                        sorted.top().printEntries(i, _planner_id);;
//                        sorted.pop();
//                    }

                    // Allocate memory for exec_qs
                    batch_part->sub_exec_qs_cnt = pq.size();

                    batch_part->exec_qs = (Array<exec_queue_entry> **) mem_allocator.alloc(
                            sizeof(Array<exec_queue_entry> *)*batch_part->sub_exec_qs_cnt);
                    memset(batch_part->exec_qs, 0, sizeof(Array<exec_queue_entry> *)*batch_part->sub_exec_qs_cnt);

                    batch_part->exec_qs_status = (atomic<uint64_t> *) mem_allocator.alloc(
                            sizeof(uint64_t)*batch_part->sub_exec_qs_cnt);
                    memset(batch_part->exec_qs_status, 0, sizeof(uint64_t)*batch_part->sub_exec_qs_cnt);

                    for (uint64_t j = 0; j < batch_part->sub_exec_qs_cnt; j++){
                        batch_part->exec_qs[j] = pq.top().exec_q;
                        pq.pop();
                        batch_part->exec_qs_status[j].store(AVAILABLE);
//                        uint64_t tmp_pq_size = pq.size();
//                        assert((tmp_pq_size-j-1) == (batch_part->sub_exec_qs_cnt-j-2));
                    }


//                    uint64_t j = 0;
//                    for (boost::heap::priority_queue<SplitEntry, boost::heap::compare<SplitEntryCompare>>::iterator it = pq.begin(); it != pq.end(); ++it){
//                        DEBUG_Q("Planner_%ld : of mrange(%ld), subrange_cnt(%ld) = %ld"
//                                        "\n",
//                                _planner_id, i,j, ((SplitEntry *) *it).exec_q->size()
//                        );
//                        j++;
//                    }
//                    mem_allocator.free(min_range_cnts, sizeof(uint64_t)*2);
                }

                expected = 0;
                desired = (uint64_t) batch_part;
                //TODO(tq): make sure we need these memory fences
//                std::atomic_thread_fence(std::memory_order_seq_cst);
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

//                if (batch_id == 1024){
//                    DEBUG_Q("PT_%ld : For batch %ld : failing to SET batch_pg_map slot [%ld][%ld]\n", _planner_id, batch_id, slot_num, _planner_id);
//                }

                // Deliver batch partition to the repective ET
                while(!work_queue.batch_map[slot_num][i][_planner_id].compare_exchange_strong(
                        expected, desired)){
                    // this should not happen after spinning but can happen if simulation is done
//                    M_ASSERT_V(false, "For batch %ld : failing to SET map slot [%ld][%ld][%ld]\n", batch_id, slot_num, i, _planner_id);
                }
//                std::atomic_thread_fence(std::memory_order_seq_cst);
//                DEBUG_Q("Planner_%ld :Batch_%ld for range_%ld ready! b_slot = %ld\n", _planner_id, batch_id, i, slot_num);
            }

            planner_pg->batch_txn_cnt = batch_cnt;
            planner_pg->batch_id = batch_id;
            planner_pg->txn_dep_graph = txn_dep_graph;
            planner_pg->batch_starting_txn_id = batch_starting_txn_id;


            // Spin here
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


//            std::atomic_thread_fence(std::memory_order_seq_cst);
            // TODO(tq): reuse from a free list instead of allocating new mem. block
//            txn_ctxs = (transaction_context *) mem_allocator.alloc(sizeof(transaction_context)*planner_batch_size);

            if (work_queue.txn_ctxs_free_list[_planner_id]->pop(txn_ctxs)){
                memset(txn_ctxs, 0,sizeof(transaction_context)*planner_batch_size );
            }
            else{
                txn_ctxs = (transaction_context *) mem_allocator.alloc(sizeof(transaction_context)*planner_batch_size);
            }

            planner_pg = (priority_group *) mem_allocator.alloc(sizeof(priority_group));
            planner_pg->planner_id = _planner_id;
            planner_pg->txn_ctxs = txn_ctxs;

            // reset data structures and execution queues for the new batch
            prof_starttime = get_sys_clock();
            exec_queues->clear();
            for (uint64_t i = 0; i < g_thread_cnt; i++) {
                Array<exec_queue_entry> * exec_q = NULL;
                if (work_queue.exec_queue_free_list[i]->pop(exec_q)){
                    exec_q->clear();
//                    DEBUG_Q("Planner_%ld : Reusing exec_q\n", _planner_id);
                    INC_STATS(_thd_id, plan_reuse_exec_queue_cnt[_planner_id], 1);
                }
                else{
                    exec_q = new Array<exec_queue_entry>();
                    exec_q->init(exec_queue_capacity);
//                    DEBUG_Q("Planner_%ld: Allocating new exec_q\n", _planner_id);
                    INC_STATS(_thd_id, plan_alloc_exec_queue_cnt[_planner_id], 1);
                }
                exec_queues->add(exec_q);
            }
            INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock()-prof_starttime);
            INC_STATS(_thd_id, plan_batch_cnts[_planner_id], 1);
            INC_STATS(_thd_id, plan_batch_process_time[_planner_id], get_sys_clock() - batch_start_time);

            for (auto it = access_table.begin(); it != access_table.end(); ++it){
                delete it->second;
            }
            access_table.clear();

            txn_dep_graph = new hash_table_t();
            M_ASSERT_V(access_table.size() == 0, "Access table is not empty!!\n");
            M_ASSERT_V(txn_dep_graph->size() == 0, "TDG table is not empty!!\n");

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
                    ycsb_request *ycsb_req = ycsb_msg->requests.get(j);
                    uint64_t key = ycsb_req->key;
//                    DEBUG_Q("Planner_%d looking up bucket for key %ld\n", _planner_id, key);
                    uint64_t idx = get_bucket(key);
//                    DEBUG_Q("Planner_%d using bucket %ld for key %ld\n", _planner_id,idx, key);

                    // TODO(tq): move to statitic module
#if DEBUG_QUECC
                    uint64_t nval = mrange_cnts.get(idx) + 1;
                    mrange_cnts.set(idx, nval);
                    total_access_cnt++;
#endif

                    // create execution entry, for now it will contain only one request
                    // we dont need to allocate memory here
                    prof_starttime = get_sys_clock();
                    exec_queue_entry *entry = (exec_queue_entry *) mem_allocator.alloc(sizeof(exec_queue_entry));
                    INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock() - prof_starttime);
                    Array<exec_queue_entry> *mrange = exec_queues->get(idx);
                    // increment of batch mrange to use the next entry slot
                    entry->txn_id = planner_txn_id;
                    entry->txn_ctx = tctx;
                    entry->batch_id = batch_id;
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
                    mem_allocator.free(entry, sizeof(exec_queue_entry));
                    INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock() - prof_starttime);


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
    INC_STATS(_thd_id, plan_total_time[_planner_id], get_sys_clock()-plan_starttime);
#if DEBUG_QUECC
    DEBUG_Q("Planner_%ld: Total access cnt = %ld, and processed %ld msgs\n", _planner_id, total_access_cnt, total_msg_processed_cnt);
    for (uint64_t i = 0; i < g_thread_cnt; i++) {
        DEBUG_Q("Planner_%ld: Access cnt for bucket_%ld = %ld\n",_planner_id, i, mrange_cnts.get(i));
    }
#endif
    printf("FINISH PT %ld:%ld\n",_node_id,_thd_id);
    fflush(stdout);
    return FINISH;

}