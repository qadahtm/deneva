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

#include <tpcc_helper.h>
#include "global.h"
#include "manager.h"
#include "thread.h"
#include "worker_thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "math.h"
#include "helper.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "work_queue.h"
#include "logger.h"
#include "message.h"
#include "abort_queue.h"
#include "maat.h"
#include "ycsb.h"
#include "index_base.h"
#include "index_hash.h"
#include "index_btree.h"
#include "plock.h"

void WorkerThread::setup() {

    if (get_thd_id() == 0) {
        send_init_done_to_all_nodes();
    }
    _thd_txn_id = 0;

}

RC WorkerThread::process(Message *msg) {
    RC rc __attribute__ ((unused)) = ERROR;

#if CC_ALG == CALVIN
    bool remote_seq = txn_man->return_id != g_node_id;
#endif
    DEBUG("%ld Processing %ld %d\n", get_thd_id(), msg->get_txn_id(), msg->get_rtype());
//    DEBUG_Q("%ld Processing txn_id=%ld rtype=%d \n",get_thd_id(),msg->get_txn_id(),msg->get_rtype());

    assert(msg->get_rtype() == CL_QRY || msg->get_txn_id() != UINT64_MAX);
#if PROFILE_EXEC_TIMING
    uint64_t starttime = get_sys_clock();
#endif
    switch (msg->get_rtype()) {
        case RPASS:
            //rc = process_rpass(msg);
            break;
        case RPREPARE:

            rc = process_rprepare(msg);
            break;
        case RFWD:
            rc = process_rfwd(msg);
            break;
        case RQRY:
            rc = process_rqry(msg);
            break;
        case RQRY_CONT:
            rc = process_rqry_cont(msg);
            break;
        case RQRY_RSP:
            rc = process_rqry_rsp(msg);
            break;
        case RFIN:
            rc = process_rfin(msg);
            break;
        case RACK_PREP:
            rc = process_rack_prep(msg);
            break;
        case RACK_FIN:
            rc = process_rack_rfin(msg);
            break;
        case RTXN_CONT:
            rc = process_rtxn_cont(msg);
            break;
        case CL_QRY:
        case RTXN:
#if CC_ALG == CALVIN
            rc = process_calvin_rtxn(msg);
#else
            rc = process_rtxn(msg);
#endif
            break;
        case LOG_FLUSHED:
            rc = process_log_flushed(msg);
            break;
        case LOG_MSG:
            rc = process_log_msg(msg);
            break;
        case LOG_MSG_RSP:
            rc = process_log_msg_rsp(msg);
            break;
        default:
            printf("Msg: %d\n", msg->get_rtype());
            fflush(stdout);
            assert(false);
            break;
    }
#if PROFILE_EXEC_TIMING
    uint64_t timespan = get_sys_clock() - starttime;
    INC_STATS(_thd_id, worker_process_time, timespan);
    INC_STATS(_thd_id, worker_process_time_by_type[msg->rtype], timespan);
#endif
    INC_STATS(_thd_id, worker_process_cnt, 1);
    INC_STATS(_thd_id, worker_process_cnt_by_type[msg->rtype], 1);
    DEBUG("%ld EndProcessing %d %ld\n", get_thd_id(), msg->get_rtype(), msg->get_txn_id());
#if CC_ALG == CALVIN
    if (remote_seq && msg->rtype == CL_QRY) {
        Message::release_message(msg);
    }
#endif

    return rc;
}

void WorkerThread::check_if_done(RC rc) {
#if !SINGLE_NODE
    if (txn_man->waiting_for_response())
        return;
#endif
    if (rc == Commit)
        commit();
    if (rc == Abort)
        abort();
}

void WorkerThread::release_txn_man() {
//    DEBUG_Q("WT_%ld: Releaseing txn man txn_id = %ld\n",_thd_id, txn_man->get_txn_id());
    txn_table.release_transaction_manager(get_thd_id(), txn_man->get_txn_id(), txn_man->get_batch_id());
    txn_man = NULL;
}
#if CC_ALG == CALVIN
void WorkerThread::calvin_wrapup() {
#if ABORT_MODE
    if (txn_man->get_rc() == Abort){
        txn_man->abort();
    }
    else{
        txn_man->release_locks(RCOK);
        txn_man->set_rc(Commit);
        txn_man->commit_stats();
    }
#else
    txn_man->release_locks(RCOK);
    txn_man->commit_stats();
#endif
    DEBUG_Q("N_%d:WT_%lu: (%ld,%ld) wrap up - calvin ack to %ld with %s, rc = %d, txn_return_id=%lu\n", g_node_id, get_thd_id(), txn_man->get_txn_id(), txn_man->get_batch_id(), txn_man->return_id,
            (txn_man->get_rc() == Abort)? "Abort" : "Commit", txn_man->get_rc(), txn_man->return_id);
#if !SINGLE_NODE
    if (txn_man->return_id == g_node_id) {
        work_queue.sequencer_enqueue(_thd_id, Message::create_message(txn_man, CALVIN_ACK));
    } else {
        msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, CALVIN_ACK), txn_man->return_id);
    }
#else
    work_queue.sequencer_enqueue(_thd_id, Message::create_message(txn_man, CALVIN_ACK));
#endif
    release_txn_man();
}
#endif

// Can't use txn_man after this function
void WorkerThread::commit() {
    //TxnManager * txn_man = txn_table.get_transaction_manager(txn_id,0);
    //txn_man->release_locks(RCOK);
    //        txn_man->commit_stats();
    assert(txn_man);
//    assert(IS_LOCAL(txn_man->get_txn_id()));
#if PROFILE_EXEC_TIMING
    uint64_t timespan = get_sys_clock() - txn_man->txn_stats.starttime;
    DEBUG("WT_%ld: COMMIT %ld %f -- %f\n",_thd_id,txn_man->get_txn_id(),simulation->seconds_from_start(get_sys_clock()),(double)timespan/ BILLION);
#endif
//    DEBUG_Q("COMMIT %ld %f -- %f\n", txn_man->get_txn_id(), simulation->seconds_from_start(get_sys_clock()),
//          (double) timespan / BILLION);

    // Send result back to client
#if !SERVER_GENERATE_QUERIES
    auto resp_msg = (ClientResponseMessage *) Message::create_message(txn_man, CL_RSP);
#if ABORT_MODE
    assert(txn_man->dd_abort == false);
#endif
    resp_msg->rc = Commit;
    msg_queue.enqueue(_thd_id, resp_msg, txn_man->client_id);
#endif
    // remove txn from pool
    // -- actually return to txn_mgr pool
    release_txn_man();
    // Do not use txn_man after this

}

void WorkerThread::abort() {
#if PROFILE_EXEC_TIMING
    DEBUG("ABORT %ld -- %f\n", txn_man->get_txn_id(), (double) get_sys_clock() - run_starttime / BILLION);
#endif
//    DEBUG_Q("WT_%ld: ABORT txn_id=%ld,txn_man_thd_id=%ld,thd_id=%ld -- %f\n",_thd_id, txn_man->get_txn_id(),txn_man->get_thd_id(),get_thd_id(), (double) get_sys_clock() - run_starttime / BILLION);
    // TODO: TPCC Rollback here
    // TQ: we should not do TPCC rollback here, it should be done earlier
    bool aenqueue = true;
#if ABORT_MODE
    if (txn_man->dd_abort){
        DEBUG_Q("%s node aborting txnman.txn_id=%lu with dd_Abort\n",IS_LOCAL(txn_man->get_txn_id())? "Local": "Remote",txn_man->get_txn_id());
        if (IS_LOCAL(txn_man->get_txn_id())){
            auto resp_msg = (ClientResponseMessage *) Message::create_message(txn_man, CL_RSP);
            resp_msg->rc = Abort;
            msg_queue.enqueue(_thd_id, resp_msg, txn_man->client_id);
        }
        release_txn_man();
        aenqueue = false;
    }
#endif

    if (!aenqueue)
        return;

#if ABORT_THREAD || ABORT_QUEUES
    ++txn_man->abort_cnt;
    txn_man->reset();
#if ABORT_QUEUES
    uint64_t penalty = work_queue.abort_queues[txn_man->get_thd_id()]->enqueue(txn_man->get_thd_id(), txn_man->get_txn_id(), txn_man->get_abort_cnt());
    txn_man->txn_stats.total_abort_time += penalty;
#else
    uint64_t penalty = abort_queue.enqueue(get_thd_id(), txn_man->get_txn_id(), txn_man->get_abort_cnt());
    txn_man->txn_stats.total_abort_time += penalty;
#endif
#else
    // on abort just release txn manager no retry
    release_txn_man();
#endif
}

TxnManager *WorkerThread::get_transaction_manager(Message *msg) {
#if CC_ALG == CALVIN
    TxnManager * local_txn_man = txn_table.get_transaction_manager(get_thd_id(),msg->get_txn_id(),msg->get_batch_id());
    DEBUG("WT_%lu: (%lu,%lu) get txn_man(%lu)\n",_thd_id,msg->get_txn_id(),msg->get_batch_id(),(uint64_t)local_txn_man);
#else
    TxnManager *local_txn_man = txn_table.get_transaction_manager(get_thd_id(), msg->get_txn_id(), 0);
#endif
    return local_txn_man;
}

RC WorkerThread::run() {

#if MODE == NORMAL_MODE
    return run_normal_mode();
#elif MODE == FIXED_MODE
    return run_fixed_mode();
#else
    M_ASSERT(false, "Selected mode is not supported anymore\n");
    return FINISH;
#endif
}

#if MODE == FIXED_MODE
RC WorkerThread::run_fixed_mode() {
    tsetup();
    printf("Running WorkerThread %ld in fixed mode\n", _thd_id);
//#if CC_ALG != QUECC
//    M_ASSERT_V(false, "Fixed mode is not supported\n");
//#endif
#if CC_ALG == QUECC
//#if DEBUG_QUECC
//    exec_active[_thd_id]->store(0);
//#endif
#if !PIPELINED
    _planner_id = _thd_id;
    txn_prefix_planner_base = (_planner_id * txn_prefix_base);
    assert(UINT64_MAX > (txn_prefix_planner_base+txn_prefix_base));

    DEBUG_Q("WT_%ld thread started, txn_ids start at %ld \n", _planner_id, txn_prefix_planner_base);
    printf("WT_%ld: worker thread started in non-pipelined mode\n", _thd_id);

#if SPLIT_MERGE_ENABLED && SPLIT_STRATEGY == LAZY_SPLIT
    uint64_t exec_queue_limit = quecc_pool.exec_queue_capacity;
#endif

#if SPLIT_MERGE_ENABLED

#if SPLIT_STRATEGY == EAGER_SPLIT
    ((Array<uint64_t> *)exec_qs_ranges_tmp)->init(g_exec_qs_max_size);
    exec_queues_tmp = new Array<Array<exec_queue_entry> *>();
    ((Array<uint64_t> *)exec_queues_tmp)->init(g_exec_qs_max_size);
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

        exec_qs_ranges->add(exec_qs_ranges->get(i-1)+bucket_size);
    }
#elif WORKLOAD == TPCC
    for (uint64_t i =0; i<g_num_wh; ++i){

        if (i == 0){
            exec_qs_ranges->add(bucket_size);
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

    planner_txn_id = txn_prefix_planner_base;
    if (_planner_id == 0){
        // start txn_id of planner 0 with 1
        planner_txn_id = 1;
    }
    query_cnt = 0;

//    M_ASSERT_V(g_part_cnt >= g_thread_cnt,
//               "PT_%ld: Number of paritions must be geq to number of planners."
//                       " g_part_cnt = %d, g_plan_thread_cnt = %d\n", _planner_id, g_part_cnt, g_plan_thread_cnt);
#endif

    uint64_t batch_slot = 0;

    RC rc __attribute__((unused)) = RCOK;

    TxnManager * my_txn_man;
    _wl->get_txn_man(my_txn_man);
    my_txn_man->init(_thd_id, _wl);
    my_txn_man->register_thread(this);
//#if NUMA_ENABLED
//    uint64_t * eq_comp_cnts = (uint64_t *) mem_allocator.alloc_local(sizeof(uint64_t)*g_exec_qs_max_size);
//#else
    uint64_t * eq_comp_cnts = (uint64_t *) mem_allocator.alloc(sizeof(uint64_t)*g_exec_qs_max_size);
//#endif
    // Initialize access list
//    for (uint64_t b =0; b < BATCH_MAP_LENGTH; ++b){
//        for (uint64_t j= 0; j < g_plan_thread_cnt; ++j){
//            priority_group * planner_pg = &work_queue.batch_pg_map[b][j];
//            for (uint64_t i=0; i < planner_batch_size; ++i){
//                planner_pg->txn_ctxs[i].accesses = new Array<Access*>();
//                planner_pg->txn_ctxs[i].accesses->init(MAX_ROW_PER_TXN);
//            }
//        }
//    }
    idle_starttime = 0;
    SRC src = SUCCESS;
    uint64_t batch_proc_starttime = 0;
    uint64_t hl_prof_starttime = 0;
    DEBUG_Q("WT_%ld: starting ... wbatch_id = %ld\n",_thd_id, wbatch_id);
    wbatch_id = 0;
    while (wbatch_id < SIM_BATCH_CNT){

        if( batch_proc_starttime == 0 && simulation->is_warmup_done()){
            batch_proc_starttime = get_sys_clock();
        }
        else {
            batch_proc_starttime =0;
        }
        // allows using the batch_map in circular manner
        batch_slot = wbatch_id % g_batch_map_length;
//        batch_slot = work_queue.gbatch_id % g_batch_map_length;

#if PIPELINED
        if (batch_proc_starttime > 0){
            hl_prof_starttime = get_sys_clock();
        }
        src = execute_batch(batch_slot, eq_comp_cnts, my_txn_man);
        if (batch_proc_starttime > 0) {
            INC_STATS(_thd_id, wt_hl_exec_time[_thd_id], get_sys_clock() - hl_prof_starttime);
        }
        if (src == SUCCESS) {

#if PARALLEL_COMMIT
            // ET is done with its PGs for the current batch
//            DEBUG_Q("ET_%ld: done with my batch partition, batch_id = %ld, going to wait for all ETs to finish\n", _thd_id, wbatch_id);

            if (batch_proc_starttime > 0){
                hl_prof_starttime = get_sys_clock();
            }
            src = sync_on_execution_phase_end(batch_slot);
            if (batch_proc_starttime > 0){
                INC_STATS(_thd_id, wt_hl_sync_exec_time[_thd_id], get_sys_clock()-hl_prof_starttime);
            }
            if (src == BREAK){
                goto end_et;
            }

//            DEBUG_Q("ET_%ld: starting parallel commit, batch_id = %ld\n", _thd_id, wbatch_id);

            // process txn contexts in the current batch that are assigned to me (deterministically)
            if (batch_proc_starttime > 0){
                hl_prof_starttime = get_sys_clock();
            }
            commit_batch(batch_slot);
            if (batch_proc_starttime > 0){
                INC_STATS(_thd_id, wt_hl_commit_time[_thd_id], get_sys_clock()-hl_prof_starttime);
            }

            // indicate that I am done with all commit phase
//            DEBUG_Q("ET_%ld: is done with commit task for batch_slot = %ld\n", _thd_id, batch_slot);

            if (batch_proc_starttime > 0){
                hl_prof_starttime = get_sys_clock();
            }
            src = sync_on_commit_phase_end(batch_slot);
            if (batch_proc_starttime > 0){
                INC_STATS(_thd_id, wt_hl_sync_commit_time[_thd_id], get_sys_clock()-hl_prof_starttime);
            }
            if (src == BREAK){
                goto end_et;
            }

            if (batch_proc_starttime > 0){
                hl_prof_starttime = get_sys_clock();
            }
            batch_cleanup(batch_slot);
            if (batch_proc_starttime > 0){
                INC_STATS(_thd_id, wt_hl_cleanup_time[_thd_id], get_sys_clock()-hl_prof_starttime);
            }
#else
#if COMMIT_BEHAVIOR == AFTER_BATCH_COMP
            work_queue.batch_map_comp_cnts[batch_slot].fetch_add(1);
#if CT_ENABLED
            while (!simulation->is_done() && work_queue.batch_map_comp_cnts[batch_slot].load() != 0){
                // spin
//                DEBUG_Q("ET_%ld : Spinning waiting for batch %ld to be committed at slot %ld, current_cnt = %d\n",
//                        _thd_id, wbatch_id, batch_slot, work_queue.batch_map_comp_cnts[batch_slot].load());
            }

#else
//            DEBUG_Q("ET_%ld: completed all PGs !!!!\n", _thd_id);
            if (_thd_id == 0){
                // Thread 0 acts as commit thread
//                assert(idle_starttime == 0);
                if (idle_starttime > 0){
                    INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                }
                idle_starttime = get_sys_clock();
                while (!simulation->is_done() && (work_queue.batch_map_comp_cnts[batch_slot].load() != g_thread_cnt)){
                    // SPINN Here untill all ETs are done
//                    SAMPLED_DEBUG_Q("ET_%ld: waiting for other ETs to be done, wbatch_id = %ld at slot = %ld, val = %d, thd_cnt = %d"
//                                    "\n",
//                            _thd_id,  wbatch_id, batch_slot,work_queue.batch_map_comp_cnts[batch_slot].load(), g_thread_cnt);
                }
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                idle_starttime =0;
                // TODO(tq): collect stats for committing

//                DEBUG_Q("ET_%ld: All ETs are done for, wbatch_id = %ld at slot = %ld"
//                                "\n",
//                        _thd_id,  wbatch_id, batch_slot);

//                DEBUG_Q("ET_%ld: allowing PTs to procced, wbatch_id = %ld at slot = %ld"
//                                "\n",
//                        _thd_id,  wbatch_id, batch_slot);
                desired8 = PG_AVAILABLE;
                expected8 = PG_READY;
                for (uint64_t i =0; i < g_plan_thread_cnt; ++i){
                    quecc_commit_starttime = get_sys_clock();
                    uint64_t commit_cnt = 0;
                    priority_group * planner_pg = NULL;
                    uint64_t planner_batch_size = g_batch_size/g_plan_thread_cnt;
                    planner_pg = &work_queue.batch_pg_map[batch_slot][i];
                    assert(planner_pg->status.load() == PG_READY);
                    transaction_context * txn_ctxs = planner_pg->txn_ctxs;
                    volatile bool canCommit = true;
                    //loop over all transactions in the priority group
                    volatile bool cascading_abort = false;

                    for (uint64_t j = 0; j < planner_batch_size; j++){
                        if (txn_ctxs[j].txn_state != TXN_READY_TO_ABORT){
#if BUILD_TXN_DEPS
                            // We are ready to commit, now we need to check if we need to abort due to dependent aborted transactions
                            // to check if we need to abort, we lookup transaction dependency graph
                            auto search = planner_pg->txn_dep_graph->find(txn_ctxs[j].txn_id);
                            if (search != planner_pg->txn_dep_graph->end()){
                                // print dependenent transactions for now.
                                if (search->second->size() > 0){
                                    // there are dependen transactions
            //                        DEBUG_Q("CT_%ld : txn_id = %ld depends on %ld other transactions\n", _thd_id, txn_ctxs[i].txn_id, search->second->size());
            //                        for(std::vector<uint64_t>::iterator it = search->second->begin(); it != search->second->end(); ++it) {
            //                            DEBUG_Q("CT_%ld : txn_id = %ld depends on txn_id = %ld\n", _thd_id, txn_ctxs[i].txn_id, (uint64_t) *it);
            //                        }
                                    uint64_t d_txn_id = search->second->back();
                                    uint64_t d_txn_ctx_idx = d_txn_id-planner_pg->batch_starting_txn_id;
                                    M_ASSERT_V(txn_ctxs[d_txn_ctx_idx].txn_id == d_txn_id,
                                               "Txn_id mismatch for d_ctx_txn_id %ld == tdg_d_txn_id %ld , d_txn_ctx_idx = %ld,"
                                                       "c_txn_id = %ld, batch_starting_txn_id = %ld, txn_ctxs(%ld) \n",
                                               txn_ctxs[d_txn_ctx_idx].txn_id,
                                               d_txn_id, d_txn_ctx_idx, txn_ctxs[i].txn_id, planner_pg->batch_starting_txn_id,
                                               (uint64_t)txn_ctxs
                                    );
                                    if (txn_ctxs[d_txn_ctx_idx].txn_state == TXN_READY_TO_ABORT){
                                        // abort due to dependencies on an aborted txn
            //                            DEBUG_Q("CT_%ld : going to abort txn_id = %ld due to dependencies\n", _thd_id, txn_ctxs[i].txn_id);
                                        canCommit = false;
#if ROW_ACCESS_TRACKING
                                        cascading_abort = true;
#endif
                                    }
                                }
                            }
#endif // #if BUILD_TXN_DEPS

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

                                msg_queue.enqueue(_thd_id, rsp_msg, txn_ctxs[j].return_node_id);
#endif
                                commit_cnt++;
                                wt_release_accesses(&txn_ctxs[j], cascading_abort, false);
                            }
                            else{
                                // need to do cascading abort
                                wt_release_accesses(&txn_ctxs[j], cascading_abort, true);
                            }
                        }
                        else {
                            // transaction should be aborted due to integrity constraints
                            wt_release_accesses(&txn_ctxs[j], cascading_abort, true);
                        }

#if WORKLOAD == TPCC
                        //TODO(tq): refactor this to benchmark implementation
                        txn_ctxs[j].o_id.store(-1);
#endif
                    }

                    INC_STATS(_thd_id, txn_cnt, commit_cnt);
                    INC_STATS(_thd_id, total_txn_abort_cnt, planner_batch_size-commit_cnt);

                    INC_STATS(_thd_id,exec_txn_commit_time[_thd_id],get_sys_clock() - quecc_commit_starttime);

                    while(!work_queue.batch_pg_map[batch_slot][i].status.compare_exchange_strong(expected8, desired8)){
                        M_ASSERT_V(false, "Reset failed for PG map, this should not happen\n");
                    };
                }

//                DEBUG_Q("ET_%ld: allowing PTs to procced, wbatch_id = %ld at slot = %ld"
//                                "\n",
//                        _thd_id,  wbatch_id, batch_slot);
                desired8 = 0;
                expected8 = g_thread_cnt;
                while(!work_queue.batch_map_comp_cnts[batch_slot].compare_exchange_strong(expected8, desired8)){};
//                INC_STATS(_thd_id, txn_cnt, g_batch_size);
            }
            else {
                if (idle_starttime > 0){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                }
                idle_starttime = get_sys_clock();
//                DEBUG_Q("ET_%ld: Done with my batch partition going to wait for others\n", _thd_id);
                while (!simulation->is_done() && work_queue.batch_map_comp_cnts[batch_slot].load() != 0){
                    // SPINN Here untill all ETs are done and batch is committed
                }
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                idle_starttime =0;
            }
#endif //if CT_ENABLED
            //TODO(tq) fix stat collection for idle time to include the spinning below

#endif // COMMIT_BEHAVIOR == AFTER_BATCH_COMP

#endif // if PARALLEL_COMMIT
            wbatch_id++;
//            DEBUG_Q("ET_%ld : ** Committed batch %ld, and moving to next batch %ld at [%ld][%ld][%ld] \n",
//                    _thd_id, wbatch_id-1, wbatch_id, batch_slot, _thd_id, wplanner_id);
        }
#else
        // Plan
//        DEBUG_Q("WT_%ld: going to plan batch_id = %ld at batch_slot = %ld, plan_comp_cnt = %d\n",
//                _thd_id, wbatch_id, batch_slot, work_queue.batch_plan_comp_cnts[batch_slot].load());
//        DEBUG_Q("WT_%ld: going to plan batch_id = %ld at batch_slot = %ld\n",
//                _thd_id, wbatch_id, batch_slot);
        if (batch_proc_starttime > 0){
            hl_prof_starttime = get_sys_clock();
        }
//        M_ASSERT_V(wbatch_id == work_queue.gbatch_id, "ET_%ld: plan stage - batch id mismatch wbatch_id=%ld, gbatch_id=%ld\n", _thd_id, wbatch_id, work_queue.gbatch_id);
#if DEBUG_QUECC
        stage = 0;
#endif
        src = plan_batch(batch_slot, my_txn_man);
        if (batch_proc_starttime > 0) {
            INC_STATS(_thd_id, wt_hl_plan_time[_thd_id], get_sys_clock() - hl_prof_starttime);
        }
        if (src == SUCCESS){
            //Sync
//            DEBUG_Q("WT_%ld: going to sync for planning phase end, batch_id = %ld at batch_slot = %ld\n", _thd_id, wbatch_id, batch_slot);
            if (batch_proc_starttime > 0){
                hl_prof_starttime = get_sys_clock();
            }
            src = sync_on_planning_phase_end(batch_slot);
            if (batch_proc_starttime > 0){
                INC_STATS(_thd_id, wt_hl_sync_plan_time[_thd_id], get_sys_clock()-hl_prof_starttime);
            }

//            DEBUG_Q("WT_%ld: Starting execution phase, batch_id = %ld at batch_slot = %ld, idle_time = %f\n",
//                    _thd_id, wbatch_id, batch_slot, stats._stats[_thd_id]->exec_idle_time[_thd_id]/BILLION);

            // Execute
            if (batch_proc_starttime > 0){
                hl_prof_starttime = get_sys_clock();
            }
//            M_ASSERT_V(wbatch_id == work_queue.gbatch_id, "ET_%ld: exec stage - batch id mismatch wbatch_id=%ld, gbatch_id=%ld\n", _thd_id, wbatch_id, work_queue.gbatch_id);
#if DEBUG_QUECC
            stage = 1;
#endif
            src = execute_batch(batch_slot,eq_comp_cnts, my_txn_man);
            if (batch_proc_starttime > 0){
                INC_STATS(_thd_id, wt_hl_exec_time[_thd_id], get_sys_clock()-hl_prof_starttime);
            }
            if (src == SUCCESS) {

#if PARALLEL_COMMIT
                // ET is done with its PGs for the current batch
//            DEBUG_Q("ET_%ld: done with my batch partition, batch_id = %ld, going to wait for all ETs to finish\n", _thd_id, wbatch_id);
//            DEBUG_Q("WT_%ld: going to sync for execution phase end, batch_id = %ld at batch_slot = %ld\n", _thd_id, wbatch_id, batch_slot);
                // Sync
                // spin on map_comp_cnts

                if (batch_proc_starttime > 0){
                    hl_prof_starttime = get_sys_clock();
                }
                src = sync_on_execution_phase_end(batch_slot);
                if (batch_proc_starttime > 0){
                    INC_STATS(_thd_id, wt_hl_sync_exec_time[_thd_id], get_sys_clock()-hl_prof_starttime);
                }
                //Commit

//                DEBUG_Q("ET_%ld: starting parallel commit, batch_id = %ld\n", _thd_id, wbatch_id);

                // process txn contexts in the current batch that are assigned to me (deterministically)
                if (batch_proc_starttime > 0){
                    hl_prof_starttime = get_sys_clock();
                }
//                M_ASSERT_V(wbatch_id == work_queue.gbatch_id, "ET_%ld: commit stage - batch id mismatch wbatch_id=%ld, gbatch_id=%ld\n", _thd_id, wbatch_id, work_queue.gbatch_id);
#if DEBUG_QUECC
                stage = 2;
#endif
                commit_batch(batch_slot);
                if (batch_proc_starttime > 0){
                    INC_STATS(_thd_id, wt_hl_commit_time[_thd_id], get_sys_clock()-hl_prof_starttime);
                }

                // indicate that I am done with all commit phase
//                DEBUG_Q("ET_%ld: is done with commit task, going to sync for commit\n", _thd_id);

                if (batch_proc_starttime > 0){
                    hl_prof_starttime = get_sys_clock();
                }
                src = sync_on_commit_phase_end(batch_slot);
                if (batch_proc_starttime > 0){
                    INC_STATS(_thd_id, wt_hl_sync_commit_time[_thd_id], get_sys_clock()-hl_prof_starttime);
                }

                if (batch_proc_starttime > 0){
                    hl_prof_starttime = get_sys_clock();
                }
                batch_cleanup(batch_slot);
                if (batch_proc_starttime > 0){
                    INC_STATS(_thd_id, wt_hl_cleanup_time[_thd_id], get_sys_clock()-hl_prof_starttime);
                }
#else
                #if COMMIT_BEHAVIOR == AFTER_BATCH_COMP
            work_queue.batch_map_comp_cnts[batch_slot].fetch_add(1);
#if CT_ENABLED
            while (!simulation->is_done() && work_queue.batch_map_comp_cnts[batch_slot].load() != 0){
                // spin
//                DEBUG_Q("ET_%ld : Spinning waiting for batch %ld to be committed at slot %ld, current_cnt = %d\n",
//                        _thd_id, wbatch_id, batch_slot, work_queue.batch_map_comp_cnts[batch_slot].load());
            }

#else
//            DEBUG_Q("ET_%ld: completed all PGs !!!!\n", _thd_id);
            if (_thd_id == 0){
                // Thread 0 acts as commit thread
//                assert(idle_starttime == 0);
                if (idle_starttime > 0){
                    INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                }
                idle_starttime = get_sys_clock();
                while (!simulation->is_done() && (work_queue.batch_map_comp_cnts[batch_slot].load() != g_thread_cnt)){
                    // SPINN Here untill all ETs are done
//                    SAMPLED_DEBUG_Q("ET_%ld: waiting for other ETs to be done, wbatch_id = %ld at slot = %ld, val = %d, thd_cnt = %d"
//                                    "\n",
//                            _thd_id,  wbatch_id, batch_slot,work_queue.batch_map_comp_cnts[batch_slot].load(), g_thread_cnt);
                }
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                idle_starttime =0;
                // TODO(tq): collect stats for committing

//                DEBUG_Q("ET_%ld: All ETs are done for, wbatch_id = %ld at slot = %ld"
//                                "\n",
//                        _thd_id,  wbatch_id, batch_slot);

//                DEBUG_Q("ET_%ld: allowing PTs to procced, wbatch_id = %ld at slot = %ld"
//                                "\n",
//                        _thd_id,  wbatch_id, batch_slot);
                desired8 = PG_AVAILABLE;
                expected8 = PG_READY;
                for (uint64_t i =0; i < g_plan_thread_cnt; ++i){
                    quecc_commit_starttime = get_sys_clock();
                    uint64_t commit_cnt = 0;
                    priority_group * planner_pg = NULL;
                    uint64_t planner_batch_size = g_batch_size/g_plan_thread_cnt;
                    planner_pg = &work_queue.batch_pg_map[batch_slot][i];
                    assert(planner_pg->status.load() == PG_READY);
                    transaction_context * txn_ctxs = planner_pg->txn_ctxs;
                    volatile bool canCommit = true;
                    //loop over all transactions in the priority group
                    volatile bool cascading_abort = false;

                    for (uint64_t j = 0; j < planner_batch_size; j++){
                        if (txn_ctxs[j].txn_state != TXN_READY_TO_ABORT){
#if BUILD_TXN_DEPS
                            // We are ready to commit, now we need to check if we need to abort due to dependent aborted transactions
                            // to check if we need to abort, we lookup transaction dependency graph
                            auto search = planner_pg->txn_dep_graph->find(txn_ctxs[j].txn_id);
                            if (search != planner_pg->txn_dep_graph->end()){
                                // print dependenent transactions for now.
                                if (search->second->size() > 0){
                                    // there are dependen transactions
            //                        DEBUG_Q("CT_%ld : txn_id = %ld depends on %ld other transactions\n", _thd_id, txn_ctxs[i].txn_id, search->second->size());
            //                        for(std::vector<uint64_t>::iterator it = search->second->begin(); it != search->second->end(); ++it) {
            //                            DEBUG_Q("CT_%ld : txn_id = %ld depends on txn_id = %ld\n", _thd_id, txn_ctxs[i].txn_id, (uint64_t) *it);
            //                        }
                                    uint64_t d_txn_id = search->second->back();
                                    uint64_t d_txn_ctx_idx = d_txn_id-planner_pg->batch_starting_txn_id;
                                    M_ASSERT_V(txn_ctxs[d_txn_ctx_idx].txn_id == d_txn_id,
                                               "Txn_id mismatch for d_ctx_txn_id %ld == tdg_d_txn_id %ld , d_txn_ctx_idx = %ld,"
                                                       "c_txn_id = %ld, batch_starting_txn_id = %ld, txn_ctxs(%ld) \n",
                                               txn_ctxs[d_txn_ctx_idx].txn_id,
                                               d_txn_id, d_txn_ctx_idx, txn_ctxs[i].txn_id, planner_pg->batch_starting_txn_id,
                                               (uint64_t)txn_ctxs
                                    );
                                    if (txn_ctxs[d_txn_ctx_idx].txn_state == TXN_READY_TO_ABORT){
                                        // abort due to dependencies on an aborted txn
            //                            DEBUG_Q("CT_%ld : going to abort txn_id = %ld due to dependencies\n", _thd_id, txn_ctxs[i].txn_id);
                                        canCommit = false;
#if ROW_ACCESS_TRACKING
                                        cascading_abort = true;
#endif
                                    }
                                }
                            }
#endif // #if BUILD_TXN_DEPS

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

                                msg_queue.enqueue(_thd_id, rsp_msg, txn_ctxs[j].return_node_id);
#endif
                                commit_cnt++;
                                wt_release_accesses(&txn_ctxs[j], cascading_abort, false);
                            }
                            else{
                                // need to do cascading abort
                                wt_release_accesses(&txn_ctxs[j], cascading_abort, true);
                            }
                        }
                        else {
                            // transaction should be aborted due to integrity constraints
                            wt_release_accesses(&txn_ctxs[j], cascading_abort, true);
                        }

#if WORKLOAD == TPCC
                        //TODO(tq): refactor this to benchmark implementation
                        txn_ctxs[j].o_id.store(-1);
#endif
                    }

                    INC_STATS(_thd_id, txn_cnt, commit_cnt);
                    INC_STATS(_thd_id, total_txn_abort_cnt, planner_batch_size-commit_cnt);

                    INC_STATS(_thd_id,exec_txn_commit_time[_thd_id],get_sys_clock() - quecc_commit_starttime);

                    while(!work_queue.batch_pg_map[batch_slot][i].status.compare_exchange_strong(expected8, desired8)){
                        M_ASSERT_V(false, "Reset failed for PG map, this should not happen\n");
                    };
                }

//                DEBUG_Q("ET_%ld: allowing PTs to procced, wbatch_id = %ld at slot = %ld"
//                                "\n",
//                        _thd_id,  wbatch_id, batch_slot);
                desired8 = 0;
                expected8 = g_thread_cnt;
                while(!work_queue.batch_map_comp_cnts[batch_slot].compare_exchange_strong(expected8, desired8)){};
//                INC_STATS(_thd_id, txn_cnt, g_batch_size);
            }
            else {
                if (idle_starttime > 0){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                }
                idle_starttime = get_sys_clock();
//                DEBUG_Q("ET_%ld: Done with my batch partition going to wait for others\n", _thd_id);
                while (!simulation->is_done() && work_queue.batch_map_comp_cnts[batch_slot].load() != 0){
                    // SPINN Here untill all ETs are done and batch is committed
                }
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                idle_starttime =0;
            }
#endif //if CT_ENABLED
            //TODO(tq) fix stat collection for idle time to include the spinning below
) expects
#endif // COMMIT_BEHAVIOR == AFTER_BATCH_COMP

#endif // if PARALLEL_COMMIT
                wbatch_id++;
//                DEBUG_Q("ET_%ld : ** Committed batch %ld, and moving to next batch %ld\n",
//                    _thd_id, wbatch_id-1, wbatch_id);

            }
        }
#endif //if PIPELINED
        if(batch_proc_starttime > 0){
            INC_STATS(_thd_id, exec_batch_proc_time[_thd_id], get_sys_clock()-batch_proc_starttime);
            batch_proc_starttime =0;
        }


    }
#else
    //TODO(tq): do count-based for other CCs
    uint64_t cnt = 0;
    uint64_t ready_starttime = 0;
    uint64_t sim_txn_cnt = (BATCH_SIZE/g_thread_cnt)*SIM_BATCH_CNT;
    RC rc;
    while(cnt < sim_txn_cnt){
        Message * msg = work_queue.dequeue(_thd_id);
        assert(msg);

        //uint64_t starttime = get_sys_clock();
        if(msg->rtype != CL_QRY || CC_ALG == CALVIN) {
            txn_man = get_transaction_manager(msg);
#if !SINGLE_NODE
            if (CC_ALG != CALVIN && IS_LOCAL(txn_man->get_txn_id())) {
            if (msg->rtype != RTXN_CONT && ((msg->rtype != RACK_PREP) || (txn_man->get_rsp_cnt() == 1))) {
              txn_man->txn_stats.work_queue_time_short += msg->lat_work_queue_time;
              txn_man->txn_stats.cc_block_time_short += msg->lat_cc_block_time;
              txn_man->txn_stats.cc_time_short += msg->lat_cc_time;
              txn_man->txn_stats.msg_queue_time_short += msg->lat_msg_queue_time;
              txn_man->txn_stats.process_time_short += msg->lat_process_time;
              /*
              if (msg->lat_network_time/BILLION > 1.0) {
                printf("%ld %d %ld -> %ld: %f %f\n",msg->txn_id, msg->rtype, msg->return_node_id,get_node_id() ,msg->lat_network_time/BILLION, msg->lat_other_time/BILLION);
              }
              */
              txn_man->txn_stats.network_time_short += msg->lat_network_time;
            }

          } else {
              txn_man->txn_stats.clear_short();
          }
          if (CC_ALG != CALVIN) {
            txn_man->txn_stats.lat_network_time_start = msg->lat_network_time;
            txn_man->txn_stats.lat_other_time_start = msg->lat_other_time;
          }
          txn_man->txn_stats.msg_queue_time += msg->mq_time;
          txn_man->txn_stats.msg_queue_time_short += msg->mq_time;
          msg->mq_time = 0;
          txn_man->txn_stats.work_queue_time += msg->wq_time;
          txn_man->txn_stats.work_queue_time_short += msg->wq_time;
          //txn_man->txn_stats.network_time += msg->ntwk_time;
          msg->wq_time = 0;
          txn_man->txn_stats.work_queue_cnt += 1;
#endif

            ready_starttime = get_sys_clock();
            bool ready = txn_man->unset_ready();
            INC_STATS(get_thd_id(),worker_activate_txn_time,get_sys_clock() - ready_starttime);
            assert(ready);
            txn_man->register_thread(this);
        }

        rc = process(msg);

        ready_starttime = get_sys_clock();
        if(txn_man) {
            bool ready = txn_man->set_ready();
            assert(ready);
        }
        INC_STATS(get_thd_id(),worker_deactivate_txn_time,get_sys_clock() - ready_starttime);

        // delete message
        ready_starttime = get_sys_clock();
#if CC_ALG != CALVIN
#if !INIT_QUERY_MSGS
        msg->release();
//        Message::release_message(msg);
#endif
#endif
        INC_STATS(get_thd_id(),worker_release_msg_time,get_sys_clock() - ready_starttime);
#if ABORT_QUEUES
        if (rc != Abort){
            cnt++;
        }
#else
        cnt++;
#endif
    }
#endif // if CC_ALG == QUECCC


    printf("FINISH WT %ld:%ld\n", _node_id, _thd_id);
    fflush(stdout);
    return FINISH;
}
#endif // if MODE == FIXED_MODE

//TODO(tq): refactor this outside worker thread
// Probably it is better to have a QueCC manager helper class for these kinds of functionality
#if CC_ALG == QUECC

#if !PIPELINED
SRC WorkerThread::plan_batch(uint64_t batch_slot, TxnManager * my_txn_man) {
//    uint64_t next_part = 0; // we have client buffers are paritioned based on part cnt
    Message * msg = NULL;
    priority_group * planner_pg;
    transaction_context * txn_ctxs = NULL;
#if PROFILE_EXEC_TIMING
    uint64_t pidle_starttime = 0;
    uint64_t pbprof_starttime = 0;
#endif
    /*
     * Client buffers holding transactions are parititoned based on their home paritions
     * Planners will determinitically dequeue transactions from these client buffers based on their query count
     * and their planner ids. This minimizes contention as it spreds the contention across multiple partitions
     *
     */


#if BATCHING_MODE == TIME_BASED
    M_ASSERT_V(false, "ET_%ld: NON-PIPELINED implementation is not supported for time-based batching\n", _thd_id);
#else
#if PROFILE_EXEC_TIMING
    plan_starttime = get_sys_clock();
#endif
    if (_planner_id >= g_plan_thread_cnt){
        // execution thread just return
        return SUCCESS;
    }
    assert(_planner_id < g_plan_thread_cnt && batch_slot < g_batch_map_length);
//    planner_pg = &work_queue.batch_pg_map[batch_slot][_worker_cluster_wide_id];
    planner_pg = &work_queue.batch_pg_map[batch_slot][QueCCPool::map_to_cwplanner_id(_planner_id)];

    DEBUG_Q("N_%d:PT_%ld: going to plan batch_id = %ld\n",g_node_id, QueCCPool::map_to_cwplanner_id(_planner_id), wbatch_id);

    // use txn_dep from planner_pg
    planner_pg->batch_starting_txn_id = planner_txn_id;

    uint64_t empty_msg_cnt = 0;

    while (true){

        // we have a complete batch
        if (pbatch_cnt == planner_batch_size){
            DEBUG_Q("N_%d:PT_%ld: batch is ready, batch_id = %ld\n",g_node_id, _planner_id, wbatch_id);
//            DEBUG_Q("WT_%ld: got enough transactions going to deliver batch at slot = %ld\n", _thd_id, batch_slot);
//            do_batch_delivery(batch_slot, planner_pg);
//#if ISOLATION_LEVEL == SERIALIZABLE
//            do_batch_delivery_mpt(batch_slot, planner_pg);
//#elif ISOLATION_LEVEL == READ_COMMITTED
            do_batch_delivery_mpt_pernode(batch_slot, planner_pg);
//#else
//            assert(false);
//#endif

//            DEBUG_Q("WT_%ld: Delivered a batch at slot = %ld\n", _thd_id, batch_slot);
            break;
        }
//        if (g_thread_cnt == g_part_cnt || g_part_cnt == 1){
//            next_part = _thd_id;
//        }
//        else{
//            next_part = (_planner_id + (g_thread_cnt * query_cnt)) % g_part_cnt;
//        }
//        DEBUG_Q("PT_%ld: going to get a query with home partition = %ld\n", _planner_id, next_part);
//        query_cnt++;
#if PROFILE_EXEC_TIMING
        pbprof_starttime = get_sys_clock();
#endif
//        msg = work_queue.plan_dequeue(_thd_id, next_part);
        msg = work_queue.plan_dequeue(_thd_id, _planner_id);
#if PROFILE_EXEC_TIMING
        INC_STATS(_thd_id, plan_queue_dequeue_time[_planner_id], get_sys_clock()-pbprof_starttime);
#endif

        if (simulation->is_done()){
            DEBUG_Q("N_%d:PT_%ld: SIM_DONE planned txn cnt = %lu for batch_id = %ld\n",g_node_id, _planner_id, pbatch_cnt, wbatch_id);
            return BREAK;
        }
#if !SINGLE_NODE
        if(!msg) {
#if PROFILE_EXEC_TIMING
            if(pidle_starttime == 0){
                pidle_starttime = get_sys_clock();
            }
#endif
            empty_msg_cnt++;
            // we have not recieved a transaction
                continue;
        }
#endif
//        DEBUG_Q("PL_%ld: got a query message, query_cnt = %ld\n", _planner_id, query_cnt);
        INC_STATS(_thd_id,plan_queue_deq_cnt[_planner_id],1);
#if PROFILE_EXEC_TIMING
        if (pidle_starttime > 0){
            // plan_idle_time includes dequeue time, which should be subtracted from it
            INC_STATS(_thd_id,plan_idle_time[_planner_id],get_sys_clock() - pidle_starttime);
            pidle_starttime = 0;
        }
#endif
        empty_msg_cnt = 0;
        txn_ctxs = planner_pg->txn_ctxs;

        switch (msg->get_rtype()) {
            case CL_QRY: {
//                pbprof_starttime = get_sys_clock();
#if ISOLATION_LEVEL == SERIALIZABLE
                plan_client_msg(msg, txn_ctxs, my_txn_man);
#elif ISOLATION_LEVEL == READ_COMMITTED
                plan_client_msg_rc(msg, txn_ctxs, my_txn_man);
#endif
//                INC_STATS(_thd_id, exec_txn_proc_time[_thd_id], get_sys_clock()-pbprof_starttime);
                break;
            }
            default:
                assert(false);
        }
    }


#endif
    return SUCCESS;
}

uint64_t WorkerThread::get_key_from_entry(exec_queue_entry * entry)
{
#if WORKLOAD == YCSB
//        ycsb_request *ycsb_req_tmp = (ycsb_request *) entry->req_buffer;
//        return ycsb_req_tmp->key;
    return entry->req.key;
#else
    return entry->rid;
#endif
}

void WorkerThread::splitMRange(Array<exec_queue_entry> *& mrange, uint64_t et_id)
{
//        uint64_t idx = get_split(key, exec_qs_ranges);
    uint64_t tidx =0;
    uint64_t nidx =0;
    Array<exec_queue_entry> * texec_q = NULL;

    for (uint64_t r =0; r < mrange->size(); ++r){
        // TODO(tq): refactor this to respective benchmark implementation
        uint64_t lid = get_key_from_entry(mrange->get_ptr(r));
        tidx = get_split(lid, exec_qs_ranges);
//            M_ASSERT_V(((Array<exec_queue_entry> volatile * )exec_queues->get(tidx)) == mrange, "PL_%ld: mismatch mrange and tidx_eq\n",_planner_id);
        nidx = get_split(lid, ((Array<uint64_t> *)exec_qs_ranges_tmp));

        texec_q =  ((Array<Array<exec_queue_entry> *> * )exec_queues_tmp)->get(nidx);
//            M_ASSERT_V(texec_q == nexec_q || texec_q == oexec_q , "PL_%ld: mismatch mrange and tidx_eq\n",_planner_id);
        // all entries must fall into one of the splits
#if DEBUG_QUECC
        if (!(nidx == tidx || nidx == (tidx+1))){
//                DEBUG_Q("PL_%ld: nidx=%ld, tidx = %ld,lid=%ld,key=%ld\n",_planner_id, nidx, idx, lid,key);
            for (uint64_t i =0; i < ((Array<uint64_t> *)exec_qs_ranges_tmp)->size(); ++i){
                DEBUG_Q("PL_%ld: old exec_qs_ranges[%lu] = %lu\n", _planner_id, i, ((Array<uint64_t> *)exec_qs_ranges_tmp)->get(i));
            }

//            for (uint64_t i =0; i < exec_queues_tmp->size(); ++i){
//                DEBUG_Q("PL_%ld: old exec_queues[%lu] size = %lu, ptr = %lu, range= %lu\n",
//                        _planner_id, i, exec_queues_tmp->get(i)->size(), (uint64_t) exec_queues_tmp->get(i), exec_qs_ranges_tmp->get(i));
//            }

            for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
                DEBUG_Q("PL_%ld: new exec_qs_ranges[%lu] = %lu\n", _planner_id, i, exec_qs_ranges->get(i));
            }
//            for (uint64_t i =0; i < exec_queues->size(); ++i){
//                DEBUG_Q("PL_%ld: new exec_queues[%lu] size = %lu, ptr = %lu, range=%lu\n",
//                        _planner_id, i, exec_queues->get(i)->size(), (uint64_t) exec_queues->get(i), exec_qs_ranges->get(i));
//            }
        }
#endif
        M_ASSERT_V(nidx == tidx || nidx == (tidx+1),"PL_%ld: nidx=%ld, tidx = %ld,lid=%ld\n",_planner_id, nidx, tidx, lid);

        texec_q->add(mrange->get(r));
    }
}

void WorkerThread::checkMRange(Array<exec_queue_entry> *& mrange, uint64_t key, uint64_t et_id)
{
#if SPLIT_MERGE_ENABLED && SPLIT_STRATEGY == EAGER_SPLIT

    int max_tries = 64; //TODO(tq): make this configurable
    int trial =0;
#if PROFILE_EXEC_TIMING
    uint64_t _prof_starttime =0;
#endif

    volatile uint64_t c_range_start;
    volatile uint64_t c_range_end;
    volatile uint64_t idx = get_split(key, exec_qs_ranges);
    volatile uint64_t split_point;
    Array<exec_queue_entry> * nexec_q = NULL;
    Array<exec_queue_entry> * oexec_q = NULL;

    mrange = exec_queues->get(idx);
    while (mrange->is_full()){
#if PROFILE_EXEC_TIMING
        if (_prof_starttime == 0){
            _prof_starttime  = get_sys_clock();
        }
#endif
        trial++;
        if (trial == max_tries){
            M_ASSERT_V(false, "Execeded max split tries\n");
        }

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

//        DEBUG_Q("Planner_%ld : Eagerly we need to split mrange ptr = %lu, key = %lu, current size = %ld,"
//                        " batch_id = %ld, c_range_start = %lu, c_range_end = %lu, split_point = %lu, trial=%d"
//                        "\n",
//                _planner_id, (uint64_t) mrange, key, mrange->size(), wbatch_id, c_range_start, c_range_end, split_point, trial);
#if EXPANDABLE_EQS
        // if we cannot split, we must expand this, otherwise, we fail
        if ((c_range_end-c_range_start) <= 1){
            // expand current EQ
            if (mrange->expand()){
                assert(!mrange->is_full());
                return;
            }
        }
#endif

        split_point = (c_range_end-c_range_start)/2;
        M_ASSERT_V(split_point, "PL_%ld: We are at a single record, and we cannot split anymore!, range_size = %ld, eq_size = %ld\n",
                   _planner_id, c_range_end-c_range_start, mrange->size());

        // compute new ranges
        ((Array<uint64_t> *)exec_qs_ranges_tmp)->clear();
        ((Array<Array<exec_queue_entry> *> *)exec_queues_tmp)->clear();
        M_ASSERT_V(exec_queues->size() == exec_qs_ranges->size(), "PL_%ld: Size mismatch : EQS(%lu) Ranges (%lu)\n",
                   _planner_id, exec_queues->size(), exec_qs_ranges->size());
        // if we have reached max number of ranges
        // drop ranges and EQs with zero entries
#if PIPELINED2
        uint64_t et_cnt = g_et_thd_cnt;
#else
        uint64_t et_cnt = g_thread_cnt;
#endif

        if (exec_qs_ranges->is_full()){
            for (uint64_t j = 0; j < exec_qs_ranges->size(); ++j) {
//                    DEBUG_Q("WT_%lu: range[%lu] %lu, has %lu eq entries\n",_thd_id, j,exec_qs_ranges->get(j),exec_queues->get(j)->size());
//                    if (j == idx-1 || exec_queues->get(j)->size() > 0 || j == exec_qs_ranges->size()-1){
//                        ((Array<Array<exec_queue_entry> *> *)exec_queues_tmp)->add(exec_queues->get(j));
//                        ((Array<uint64_t> *)exec_qs_ranges_tmp)->add(exec_qs_ranges->get(j));
//                    }
                uint64_t add_cnt = 0;
                DEBUG_Q("WT_%lu: exec_qs_ranges is full with size = %lu\n",_thd_id, exec_qs_ranges->size());
                if (j == idx-1 || j==idx || exec_queues->get(j)->size() > 0 || j == exec_qs_ranges->size()-1){
                    DEBUG_Q("WT_%lu: keeping range[%lu] %lu, has %lu eq entries\n",
                            _thd_id, j,exec_qs_ranges->get(j),exec_queues->get(j)->size());
                    ((Array<Array<exec_queue_entry> *> *)exec_queues_tmp)->add(exec_queues->get(j));
                    ((Array<uint64_t> *)exec_qs_ranges_tmp)->add(exec_qs_ranges->get(j));
                    add_cnt++;
                }
                else{
                    if (add_cnt < et_cnt && (exec_qs_ranges->size()-j) < (et_cnt)){
                        DEBUG_Q("WT_%lu: keeping an empty range[%lu] %lu, has %lu eq entries\n",
                                _thd_id, j,exec_qs_ranges->get(j),exec_queues->get(j)->size());
                        ((Array<Array<exec_queue_entry> *> *)exec_queues_tmp)->add(exec_queues->get(j));
                        ((Array<uint64_t> *)exec_qs_ranges_tmp)->add(exec_qs_ranges->get(j));
                        add_cnt++;
                    }
                    else{
                        DEBUG_Q("WT_%lu: freeing range[%lu] %lu, has %lu eq entries\n",
                                _thd_id, j,exec_qs_ranges->get(j),exec_queues->get(j)->size());
                        Array<exec_queue_entry> * tmp = exec_queues->get(j);
                        quecc_pool.exec_queue_release(tmp,0,0);
                    }
                }
            }
            exec_queues_tmp_tmp = exec_queues;
            exec_qs_ranges_tmp_tmp = exec_qs_ranges;

            exec_queues = ((Array<Array<exec_queue_entry> *> * )exec_queues_tmp);
            exec_qs_ranges = ((Array<uint64_t> *)exec_qs_ranges_tmp);

            exec_queues_tmp = exec_queues_tmp_tmp;
            exec_qs_ranges_tmp = exec_qs_ranges_tmp_tmp;

            ((Array<uint64_t> *)exec_qs_ranges_tmp)->clear();
            ((Array<Array<exec_queue_entry> *> *)exec_queues_tmp)->clear();

            // recompute idx
            idx = get_split(key, exec_qs_ranges);
            if (idx == 0){
                c_range_start = 0;
            }
            else{
                c_range_start = exec_qs_ranges->get(idx-1);
            }
            c_range_end = exec_qs_ranges->get(idx);

#if EXPANDABLE_EQS
            // if we cannot split, we must expand this, otherwise, we fail
            if ((c_range_end-c_range_start) <= 1){
                // expand current EQ
                if (mrange->expand()){
                    assert(!mrange->is_full());
                    return;
                }
            }
#endif

            split_point = (c_range_end-c_range_start)/2;
            M_ASSERT_V(split_point, "PL_%ld: We are at a single record, and we cannot split anymore!, range_size = %ld, eq_size = %ld\n",
                       _planner_id, c_range_end-c_range_start, mrange->size());

        }
        // update ranges
        // add two new and empty exec_queues
        for (uint64_t r=0; r < exec_qs_ranges->size(); ++r){
            if (r == idx){
                // insert split
                M_ASSERT_V(exec_qs_ranges->get(r) != split_point+c_range_start,
                           "PL_%ld: old range = %lu, new range = %lu",
                           _planner_id,exec_qs_ranges->get(r), split_point+c_range_start);
                ((Array<uint64_t> *)exec_qs_ranges_tmp)->add(split_point+c_range_start);
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
                ((Array<Array<exec_queue_entry> *> * )exec_queues_tmp)->add(oexec_q);
                ((Array<Array<exec_queue_entry> *> * )exec_queues_tmp)->add(nexec_q);

            }
            else{
                ((Array<Array<exec_queue_entry> *> * )exec_queues_tmp)->add(exec_queues->get(r));
            }
            ((Array<uint64_t> *)exec_qs_ranges_tmp)->add(exec_qs_ranges->get(r));
        }

        // use new ranges to split current execq
        splitMRange(mrange,et_id);

//            if(exec_queues_tmp->get(idx)->size() == 0){
//                M_ASSERT_V(false,"PT_%ld: LEFT EQ is empty after split\n",_planner_id);
//            }
//
//            if (exec_queues_tmp->get(idx+1)->size() == 0){
//                M_ASSERT_V(false,"PT_%ld: RIGHT EQ is empty after split\n",_planner_id);
//            }
        // swap data structures
        exec_queues_tmp_tmp = exec_queues;
        exec_qs_ranges_tmp_tmp = exec_qs_ranges;

        exec_queues = ((Array<Array<exec_queue_entry> *> * )exec_queues_tmp);
        exec_qs_ranges = ((Array<uint64_t> *)exec_qs_ranges_tmp);

        exec_queues_tmp = exec_queues_tmp_tmp;
        exec_qs_ranges_tmp = exec_qs_ranges_tmp_tmp;

//        DEBUG_Q("Planner_%ld : After swapping New ranges size = %ld, old ranges size = %ld"
//                        "\n",
//                _planner_id, exec_qs_ranges->size(), exec_qs_ranges_tmp->size());

        // release current mrange
//            quecc_pool.exec_queue_release(mrange,_planner_id,RAND(g_plan_thread_cnt));
        quecc_pool.exec_queue_release(mrange,QueCCPool::map_to_planner_id(_planner_id),_thd_id);
//        DEBUG_Q("PL_%ld: key =%lu, nidx=%ld, idx=%ld, trial=%d\n", _planner_id, key, nidx, idx, trial);

        // use the new ranges to assign the new execution entry
        idx = get_split(key, exec_qs_ranges);
        mrange = exec_queues->get(idx);

//#if DEBUG_QUECC
//            for (uint64_t i =0; i < exec_qs_ranges_tmp->size(); ++i){
//                DEBUG_Q("PL_%ld: old exec_qs_ranges[%lu] = %lu\n", _planner_id, i, exec_qs_ranges_tmp->get(i));
//            }
//
//            for (uint64_t i =0; i < exec_queues_tmp->size(); ++i){
//                DEBUG_Q("PL_%ld: old exec_queues[%lu] size = %lu, ptr = %lu, range= %lu\n",
//                        _planner_id, i, exec_queues_tmp->get(i)->size(), (uint64_t) exec_queues_tmp->get(i), exec_qs_ranges_tmp->get(i));
//            }
//
//            for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
//                DEBUG_Q("PL_%ld: new exec_qs_ranges[%lu] = %lu\n", _planner_id, i, exec_qs_ranges->get(i));
//            }
//            for (uint64_t i =0; i < exec_queues->size(); ++i){
//                DEBUG_Q("PL_%ld: new exec_queues[%lu] size = %lu, ptr = %lu, range=%lu\n",
//                        _planner_id, i, exec_queues->get(i)->size(), (uint64_t) exec_queues->get(i), exec_qs_ranges->get(i));
//            }
//#endif

    }
#if PROFILE_EXEC_TIMING
    if (_prof_starttime > 0){
        INC_STATS(_thd_id, plan_split_time[_planner_id], get_sys_clock()-_prof_starttime);
    }
#endif

#else
    M_ASSERT(false, "LAZY_SPLIT not supported in TPCC")
#endif
}

void WorkerThread::plan_client_msg(Message *msg, transaction_context *txn_ctxs, TxnManager *my_txn_man)
{
#if PROFILE_EXEC_TIMING
    uint64_t _txn_prof_starttime = get_sys_clock();
#endif
    // Quecc
// Query from client
//        DEBUG_Q("PT_%ld planning txn %ld, pbatch_cnt=%ld\n", _planner_id, planner_txn_id,pbatch_cnt);
    transaction_context *tctx = &txn_ctxs[pbatch_cnt];
    // reset transaction context

    tctx->txn_id = planner_txn_id;
    tctx->txn_state.store(TXN_INITIALIZED,memory_order_acq_rel);
    tctx->completion_cnt.store(0,memory_order_acq_rel);
    tctx->txn_comp_cnt.store(0,memory_order_acq_rel);
#if EXEC_BUILD_TXN_DEPS
    tctx->should_abort = false;
    tctx->commit_dep_cnt.store(0,memory_order_acq_rel);
    tctx->commit_deps->clear();
#if WORKLOAD == YCSB
//        memset(tctx->prev_tid,0, sizeof(uint64_t)*REQ_PER_QUERY);
#else
    memset(tctx->prev_tid,0, sizeof(uint64_t)*MAX_ROW_PER_TXN);
#endif // #if WORKLOAD == YCSB
#endif
#if PROFILE_EXEC_TIMING
    tctx->starttime = get_sys_clock(); // record start time of transaction
#endif
    //TODO(tq): move to repective benchmark transaction manager implementation
#if WORKLOAD == TPCC
    tctx->o_id.store(-1);
#endif

    uint64_t e8 = TXN_INITIALIZED;
    uint64_t d8 = TXN_STARTED;

    if(!tctx->txn_state.compare_exchange_strong(e8,d8,memory_order_acq_rel)){
        assert(false);
    }
#if !SERVER_GENERATE_QUERIES
    tctx->client_startts = ((ClientQueryMessage *) msg)->client_startts;
#endif

#if LADS_IN_QUECC
    // LADS
        _wl->resolve_txn_dependencies(msg, tctx,_planner_id);
#else
    // create execution entry, for now it will contain only one request
    // we need to reset the mutable values of tctx
//    entry->txn_id = planner_txn_id;
    entry->batch_id = wbatch_id;
    entry->txn_ctx = tctx;
    memset(entry->dep_nodes,0, sizeof(int)*g_node_cnt);
#if ROW_ACCESS_TRACKING
#if ROW_ACCESS_IN_CTX
    // initializ undo_buffer if needed
#if WORKLOAD == YCSB || WORKLOAD == TPCC
    M_ASSERT_V(tctx->undo_buffer_inialized, "Txn context is not initialized\n");
#else
    M_ASSERT_V(false, "undo buffer in txn ctx is not supported for  others\n");
#endif
#else
    // initialize access_lock if it is not intinialized
        if (tctx->access_lock == NULL){
            tctx->access_lock = new spinlock();
        }

#if ROLL_BACK
        assert(tctx->accesses);
        if (tctx->accesses->isInitilized()){
            // need to clear on commit phase
//        DEBUG_Q("WT_%ld: reusing tctx accesses for txn_id=%ld, pbach_cnt=%ld, items_ptr=%lu\n",
//                _thd_id, planner_txn_id, pbatch_cnt, (uint64_t) tctx->accesses->items);
            tctx->accesses->clear();
        }
        else{
//        DEBUG_Q("WT_%ld: initializing tctx accesses for txn_id =%ld, pbach_cnt=%ld, items_ptr=%lu\n",
//                _thd_id, planner_txn_id, pbatch_cnt, (uint64_t) tctx->accesses->items);
            tctx->accesses->init(MAX_ROW_PER_TXN);
        }
#endif
#endif
#endif //#if ROW_ACCESS_TRACKING
#if !SERVER_GENERATE_QUERIES
    assert(msg->return_node_id != g_node_id);
    tctx->return_node_id = msg->return_node_id;
#endif

#if WORKLOAD == YCSB
    // Analyze read-write set
    /* We need to determine the ranges needed for each key
     * We group keys that fall in the same range to be processed together
     * TODO(tq): add repartitioning
     */

    YCSBClientQueryMessage *ycsb_msg = ((YCSBClientQueryMessage *) msg);
    for (uint64_t j = 0; j < ycsb_msg->requests.size(); j++) {
//        memset(entry, 0, sizeof(exec_queue_entry));
        ycsb_request *ycsb_req = ycsb_msg->requests.get(j);
        uint64_t key = ycsb_req->key;

        // TODO(tq): use get split with dynamic ranges
        uint64_t idx = get_split(key, exec_qs_ranges);
//        DEBUG_Q("Node_%u:Planner_%lu looking up bucket for key %lu, idx=%lu\n",g_node_id, _planner_id, key, idx);
        Array<exec_queue_entry> *mrange = exec_queues->get(idx);

#if SPLIT_MERGE_ENABLED && SPLIT_STRATEGY == EAGER_SPLIT
//        et_id = eq_idx_rand->operator()(plan_rng);
        et_id = _thd_id;
        checkMRange(mrange, key, et_id);
#endif

        // Dirty code
//        ycsb_request * req_buff = (ycsb_request *) &entry->req_buffer;
        ycsb_request * req_buff = &entry->req;
        req_buff->acctype = ycsb_req->acctype;
        req_buff->key = ycsb_req->key;
        req_buff->value = ycsb_req->value;

#if YCSB_INDEX_LOOKUP_PLAN
        ((YCSBTxnManager *)my_txn_man)->lookup_key(req_buff->key,entry);
#endif
        entry->req_idx = j;
        // add entry into range/bucket queue
        // entry is a sturct, need to double check if this works
        // this actually performs a full memcopy when adding entries
        tctx->txn_comp_cnt.fetch_add(1,memory_order_acq_rel);
        mrange->add(*entry);
//        prof_starttime = get_sys_clock();

//        INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock() - prof_starttime);
    }

#elif WORKLOAD == TPCC

    // TPCC
        TPCCClientQueryMessage *tpcc_msg = ((TPCCClientQueryMessage *) msg);
        TPCCTxnManager * tpcc_txn_man = (TPCCTxnManager *)my_txn_man;
    Array<exec_queue_entry> *mrange;
#if SINGLE_NODE
        row_t * r_local;
//    uint64_t idx;
        uint64_t rid;
#endif
//        uint64_t e8 = TXN_INITIALIZED;
//        uint64_t d8 = TXN_STARTED;
#if PIPLINED
        et_id = _planner_id;
#else
        et_id = _thd_id;
#endif
        switch (tpcc_msg->txn_type) {
            case TPCC_PAYMENT:

#if !SINGLE_NODE
//                DEBUG_Q("N_%u:PT:%lu: txn_id=%lu, w_id=%lu\n",g_node_id,_worker_cluster_wide_id,tctx->txn_id,tpcc_msg->w_id);
                // index look up for warehouse record
                tpcc_txn_man->plan_payment_update_w(tpcc_msg,entry);
                checkMRange(mrange, entry->rid, et_id);
                entry->planner_id = _worker_cluster_wide_id;
                entry->txn_idx  = pbatch_cnt;
                mrange->add(*entry);

                // plan read/update district record
                tpcc_txn_man->plan_payment_update_d(tpcc_msg,entry);
                checkMRange(mrange, entry->rid, et_id);
                entry->planner_id = _worker_cluster_wide_id;
                entry->txn_idx  = pbatch_cnt;
                mrange->add(*entry);

                // plan read/update customer record
                tpcc_txn_man->plan_payment_update_c(tpcc_msg,entry);
                checkMRange(mrange, entry->rid, et_id);
                entry->planner_id = _worker_cluster_wide_id;
                entry->txn_idx  = pbatch_cnt;
                mrange->add(*entry);

                // plan insert into history
                tpcc_txn_man->plan_payment_insert_h(tpcc_msg,entry);
                checkMRange(mrange, entry->rid, et_id);
                entry->planner_id = _worker_cluster_wide_id;
                entry->txn_idx  = pbatch_cnt;
                mrange->add(*entry);
#else
                // index look up for warehouse record
                tpcc_txn_man->payment_lookup_w(tpcc_msg->w_id, r_local);
                rid = r_local->get_row_id();
                // check range for warehouse and split if needed
                checkMRange(mrange, rid, et_id);
                // create exec_qe for updating  warehouse record
                tpcc_txn_man->plan_payment_update_w(tpcc_msg->h_amount,r_local, entry);
                mrange->add(*entry);

                // plan read/update district record
                tpcc_txn_man->payment_lookup_d(tpcc_msg->w_id,tpcc_msg->d_id,tpcc_msg->d_w_id,r_local);
                rid = r_local->get_row_id();
                checkMRange(mrange, rid, et_id);
                tpcc_txn_man->plan_payment_update_d(tpcc_msg->h_amount, r_local,entry);
                mrange->add(*entry);

                // plan read/update customer record
                tpcc_txn_man->payment_lookup_c(tpcc_msg->c_id, tpcc_msg->c_w_id, tpcc_msg->c_d_id, tpcc_msg->c_last,
                                               tpcc_msg->by_last_name, r_local);

                rid = r_local->get_row_id();
                checkMRange(mrange, rid, et_id);
                tpcc_txn_man->plan_payment_update_c(tpcc_msg->h_amount, r_local, entry);
                mrange->add(*entry);

                // plan insert into history
                tpcc_txn_man->plan_payment_insert_h(tpcc_msg->w_id, tpcc_msg->d_id, tpcc_msg->c_id, tpcc_msg->c_w_id, tpcc_msg->d_w_id, tpcc_msg->h_amount, entry);
                rid = entry->rid;
                checkMRange(mrange, rid, et_id);
                mrange->add(*entry);
#endif
                break;
            case TPCC_NEW_ORDER:
#if !SINGLE_NODE
#if PLAN_NO_DIST_UPDATE_FIRST
                //plan update on district table
                tpcc_txn_man->neworder_lookup_d(tpcc_msg->w_id, tpcc_msg->d_id, r_local);
                rid = r_local->get_row_id();
                checkMRange(mrange, rid, et_id);
                tpcc_txn_man->plan_neworder_update_d(r_local,entry);
                mrange->add(*entry);
#endif
                tpcc_txn_man->plan_neworder_read_w(tpcc_msg,entry);
                checkMRange(mrange, entry->rid, et_id);
                entry->planner_id = _worker_cluster_wide_id;
                entry->txn_idx  = pbatch_cnt;
                mrange->add(*entry);

                // plan read on cust. record
                tpcc_txn_man->plan_neworder_read_c(tpcc_msg,entry);
                checkMRange(mrange, entry->rid, et_id);
                entry->planner_id = _worker_cluster_wide_id;
                entry->txn_idx  = pbatch_cnt;
                mrange->add(*entry);

#if !PLAN_NO_DIST_UPDATE_FIRST
                //plan update on district table
                tpcc_txn_man->plan_neworder_update_d(tpcc_msg,entry);
                checkMRange(mrange, entry->rid, et_id);
                entry->planner_id = _worker_cluster_wide_id;
                entry->txn_idx  = pbatch_cnt;
                mrange->add(*entry);
#endif

                // plan insert into orders
                tpcc_txn_man->plan_neworder_insert_o(tpcc_msg,entry);
                checkMRange(mrange, entry->rid, et_id);
                entry->planner_id = _worker_cluster_wide_id;
                entry->txn_idx  = pbatch_cnt;
                mrange->add(*entry);

                // plan insert into new order
                tpcc_txn_man->plan_neworder_insert_no(tpcc_msg, entry);
                checkMRange(mrange, entry->rid, et_id);
                entry->planner_id = _worker_cluster_wide_id;
                entry->txn_idx  = pbatch_cnt;
                mrange->add(*entry);

                for (uint64_t i =0; i < tpcc_msg->ol_cnt; ++i){

                    uint64_t ol_number = i;

                    // plan read an item from items
                    tpcc_txn_man->plan_neworder_read_i(tpcc_msg,ol_number,entry);
                    checkMRange(mrange, entry->rid, et_id);
                    entry->planner_id = _worker_cluster_wide_id;
                    entry->txn_idx  = pbatch_cnt;
                    mrange->add(*entry);

                    // plan update to a item's stock record
                    tpcc_txn_man->plan_neworder_update_s(tpcc_msg, ol_number,entry);
                    checkMRange(mrange, entry->rid, et_id);
                    entry->planner_id = _worker_cluster_wide_id;
                    entry->txn_idx  = pbatch_cnt;
                    mrange->add(*entry);

                    // plan insert into order_line
                    tpcc_txn_man->plan_neworder_insert_ol(tpcc_msg, ol_number, entry);
                    checkMRange(mrange, entry->rid, et_id);
                    entry->batch_id = wbatch_id;
                    entry->planner_id = _worker_cluster_wide_id;
                    entry->txn_idx  = pbatch_cnt;
                    mrange->add(*entry);
                }

#else
                // plan read on warehouse record
//                if(!entry->txn_ctx->txn_state.compare_exchange_strong(e8,d8)){
//                    assert(false);
//                }
#if PLAN_NO_DIST_UPDATE_FIRST
                //plan update on district table
                tpcc_txn_man->neworder_lookup_d(tpcc_msg->w_id, tpcc_msg->d_id, r_local);
                rid = r_local->get_row_id();
                checkMRange(mrange, rid, et_id);
                tpcc_txn_man->plan_neworder_update_d(r_local,entry);
                mrange->add(*entry);
#endif

                tpcc_txn_man->neworder_lookup_w(tpcc_msg->w_id,r_local);
                rid = r_local->get_row_id();
                // check range for warehouse and split if needed
                checkMRange(mrange, rid, et_id);
                tpcc_txn_man->plan_neworder_read_w(r_local,entry);
                mrange->add(*entry);

                // plan read on cust. record
                tpcc_txn_man->neworder_lookup_c(tpcc_msg->w_id,tpcc_msg->d_id,tpcc_msg->c_id, r_local);
                rid = r_local->get_row_id();
                checkMRange(mrange, rid, et_id);
                tpcc_txn_man->plan_neworder_read_c(r_local, entry);
                mrange->add(*entry);

#if !PLAN_NO_DIST_UPDATE_FIRST
                //plan update on district table
                tpcc_txn_man->neworder_lookup_d(tpcc_msg->w_id, tpcc_msg->d_id, r_local);
                rid = r_local->get_row_id();
                checkMRange(mrange, rid, et_id);
                tpcc_txn_man->plan_neworder_update_d(r_local,entry);
                mrange->add(*entry);
#endif
                // plan insert into orders
                tpcc_txn_man->plan_neworder_insert_o(tpcc_msg->w_id, tpcc_msg->d_id,tpcc_msg->c_id,tpcc_msg->remote,tpcc_msg->ol_cnt,tpcc_msg->o_entry_d,entry);
                rid = entry->rid;
                checkMRange(mrange, rid, et_id);
                mrange->add(*entry);

                // plan insert into new order
                tpcc_txn_man->plan_neworder_insert_no(tpcc_msg->w_id,tpcc_msg->d_id, tpcc_msg->c_id, entry);
                rid = entry->rid;
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
                    checkMRange(mrange, rid, et_id);
                    tpcc_txn_man->plan_neworder_read_i(r_local,entry);
                    mrange->add(*entry);

                    // plan update to a item's stock record
                    tpcc_txn_man->neworder_lookup_s(ol_i_id,ol_supply_w_id,r_local);
                    rid = r_local->get_row_id();
                    checkMRange(mrange, rid, et_id);
                    tpcc_txn_man->plan_neworder_update_s(ol_quantity, tpcc_msg->remote, r_local,entry);
                    mrange->add(*entry);

                    // plan insert into order_line
                    tpcc_txn_man->plan_neworder_insert_ol(ol_i_id,ol_supply_w_id,ol_quantity, ol_number, tpcc_msg->d_id, r_local, entry);
                    rid = entry->rid;
                    checkMRange(mrange, rid, et_id);
                    mrange->add(*entry);
                }
#endif
                break;
            default:
                M_ASSERT_V(false, "Only Payment(%d) and NewOrder(%d) transactions are supported, found (%ld)\n", TPCC_PAYMENT, TPCC_NEW_ORDER, tpcc_msg->txn_type);
        }


#endif

#endif //#if LADS_IN_QUECC

#if PROFILE_EXEC_TIMING
    INC_STATS(_thd_id,plan_txn_process_time[_planner_id], get_sys_clock() - _txn_prof_starttime);
#endif

    // increment for next ransaction
    planner_txn_id++;
    pbatch_cnt++;

#if !INIT_QUERY_MSGS
#if WORKLOAD == YCSB
    YCSBClientQueryMessage* cl_msg = (YCSBClientQueryMessage*)msg;
#if !SINGLE_NODE
    for(uint64_t i = 0; i < cl_msg->requests.size(); i++) {
        mem_allocator.free(cl_msg->requests[i],sizeof(ycsb_request));
    }
#endif
#elif WORKLOAD == TPCC
    TPCCClientQueryMessage* cl_msg = (TPCCClientQueryMessage*)msg;
#if !SINGLE_NODE
            if(cl_msg->txn_type == TPCC_NEW_ORDER) {
                for(uint64_t i = 0; i < cl_msg->items.size(); i++) {
                    mem_allocator.free(cl_msg->items[i],sizeof(Item_no));
                }
            }
#endif
#elif WORKLOAD == PPS
            PPSClientQueryMessage* cl_msg = (PPSClientQueryMessage*)msg;
#endif
    // Free message, as there is no need for it anymore
    msg->release();
    Message::release_message(msg);
#endif

}

#if ISOLATION_LEVEL == READ_COMMITTED
void WorkerThread::plan_client_msg_rc(Message *msg, transaction_context *txn_ctxs, TxnManager *my_txn_man)
{
#if PROFILE_EXEC_TIMING
    uint64_t _txn_prof_starttime = get_sys_clock();
#endif
    // Quecc
// Query from client
//        DEBUG_Q("PT_%ld planning txn %ld, pbatch_cnt=%ld\n", _planner_id, planner_txn_id,pbatch_cnt);
    transaction_context *tctx = &txn_ctxs[pbatch_cnt];
    // reset transaction context

    tctx->txn_id = planner_txn_id;
    tctx->txn_state.store(TXN_INITIALIZED,memory_order_acq_rel);
    tctx->completion_cnt.store(0,memory_order_acq_rel);
    tctx->txn_comp_cnt.store(0,memory_order_acq_rel);
    for (UInt32 k = 0; k < g_node_cnt; ++k) {
        tctx->commit_dep_per_node[k] = 0;
        tctx->active_nodes[k] = 0;
    }
#if EXEC_BUILD_TXN_DEPS
    tctx->should_abort = false;
    tctx->commit_dep_cnt.store(0,memory_order_acq_rel);
    tctx->commit_deps->clear();
#if WORKLOAD == YCSB
//        memset(tctx->prev_tid,0, sizeof(uint64_t)*REQ_PER_QUERY);
#else
    memset(tctx->prev_tid,0, sizeof(uint64_t)*MAX_ROW_PER_TXN);
#endif // #if WORKLOAD == YCSB
#endif
#if PROFILE_EXEC_TIMING
    tctx->starttime = get_sys_clock(); // record start time of transaction
#endif
    //TODO(tq): move to repective benchmark transaction manager implementation
#if WORKLOAD == TPCC
    tctx->o_id.store(-1);
#endif

    uint64_t e8 = TXN_INITIALIZED;
    uint64_t d8 = TXN_STARTED;

    if(!tctx->txn_state.compare_exchange_strong(e8,d8,memory_order_acq_rel)){
        assert(false);
    }
#if !SERVER_GENERATE_QUERIES
    tctx->client_startts = ((ClientQueryMessage *) msg)->client_startts;
#endif

#if LADS_IN_QUECC
    // LADS
        _wl->resolve_txn_dependencies(msg, tctx,_planner_id);
#else
    // create execution entry, for now it will contain only one request
    // we need to reset the mutable values of tctx
//    entry->txn_id = planner_txn_id;
    entry->batch_id = wbatch_id;
    entry->txn_ctx = tctx;
    memset(entry->dep_nodes,0, sizeof(int)*g_node_cnt);
#if ROW_ACCESS_TRACKING
#if ROW_ACCESS_IN_CTX
    // initializ undo_buffer if needed
#if WORKLOAD == YCSB || WORKLOAD == TPCC
    M_ASSERT_V(tctx->undo_buffer_inialized, "Txn context is not initialized\n");
#else
    M_ASSERT_V(false, "undo buffer in txn ctx is not supported for  others\n");
#endif
#else
    // initialize access_lock if it is not intinialized
        if (tctx->access_lock == NULL){
            tctx->access_lock = new spinlock();
        }

#if ROLL_BACK
        assert(tctx->accesses);
        if (tctx->accesses->isInitilized()){
            // need to clear on commit phase
//        DEBUG_Q("WT_%ld: reusing tctx accesses for txn_id=%ld, pbach_cnt=%ld, items_ptr=%lu\n",
//                _thd_id, planner_txn_id, pbatch_cnt, (uint64_t) tctx->accesses->items);
            tctx->accesses->clear();
        }
        else{
//        DEBUG_Q("WT_%ld: initializing tctx accesses for txn_id =%ld, pbach_cnt=%ld, items_ptr=%lu\n",
//                _thd_id, planner_txn_id, pbatch_cnt, (uint64_t) tctx->accesses->items);
            tctx->accesses->init(MAX_ROW_PER_TXN);
        }
#endif
#endif
#endif //#if ROW_ACCESS_TRACKING
#if !SERVER_GENERATE_QUERIES
    assert(msg->return_node_id != g_node_id);
    tctx->return_node_id = msg->return_node_id;
#endif

#if WORKLOAD == YCSB
    // Analyze read-write set
    /* We need to determine the ranges needed for each key
     * We group keys that fall in the same range to be processed together
     * TODO(tq): add repartitioning
     */

    YCSBClientQueryMessage *ycsb_msg = ((YCSBClientQueryMessage *) msg);
    for (uint64_t j = 0; j < ycsb_msg->requests.size(); j++) {
//        memset(entry, 0, sizeof(exec_queue_entry));
        ycsb_request *ycsb_req = ycsb_msg->requests.get(j);
        uint64_t key = ycsb_req->key;
        uint64_t idx = get_split(key, exec_qs_ranges);

        ycsb_request * req_buff = &entry->req;
        req_buff->acctype = ycsb_req->acctype;
        req_buff->key = ycsb_req->key;
        req_buff->value = ycsb_req->value;
        entry->req_idx = j;

//        DEBUG_Q("Node_%u:Planner_%lu looking up bucket for key %lu, idx=%lu\n",g_node_id, _planner_id, key, idx);
#if YCSB_INDEX_LOOKUP_PLAN
        ((YCSBTxnManager *)my_txn_man)->lookup_key(req_buff->key,entry);
#endif
        tctx->txn_comp_cnt.fetch_add(1,memory_order_acq_rel);
        Array<exec_queue_entry> *mrange = NULL;
        if (ycsb_req->acctype == RD){
            // add all read-only operations into read queue
            mrange = ro_exec_queues->get(idx);
        }
        else{
            assert(ycsb_req->acctype == WR);
            mrange = exec_queues->get(idx);
//#if SPLIT_MERGE_ENABLED && SPLIT_STRATEGY == EAGER_SPLIT
//            checkMRange(mrange, key, et_id);
//#endif
        }

        // add entry into range/bucket queue
        // entry is a sturct, need to double check if this works
        // this actually performs a full memcopy when adding entries
        mrange->add(*entry);
    }

#elif WORKLOAD == TPCC
    assert(false); // not supported yet
    // TPCC
        TPCCClientQueryMessage *tpcc_msg = ((TPCCClientQueryMessage *) msg);
        TPCCTxnManager * tpcc_txn_man = (TPCCTxnManager *)my_txn_man;
    Array<exec_queue_entry> *mrange;
#if SINGLE_NODE
        row_t * r_local;
//    uint64_t idx;
        uint64_t rid;
#endif
//        uint64_t e8 = TXN_INITIALIZED;
//        uint64_t d8 = TXN_STARTED;
#if PIPLINED
        et_id = _planner_id;
#else
        et_id = _thd_id;
#endif
        switch (tpcc_msg->txn_type) {
            case TPCC_PAYMENT:

#if !SINGLE_NODE
//                DEBUG_Q("N_%u:PT:%lu: txn_id=%lu, w_id=%lu\n",g_node_id,_worker_cluster_wide_id,tctx->txn_id,tpcc_msg->w_id);
                // index look up for warehouse record
                tpcc_txn_man->plan_payment_update_w(tpcc_msg,entry);
                checkMRange(mrange, entry->rid, et_id);
                entry->planner_id = _worker_cluster_wide_id;
                entry->txn_idx  = pbatch_cnt;
                mrange->add(*entry);

                // plan read/update district record
                tpcc_txn_man->plan_payment_update_d(tpcc_msg,entry);
                checkMRange(mrange, entry->rid, et_id);
                entry->planner_id = _worker_cluster_wide_id;
                entry->txn_idx  = pbatch_cnt;
                mrange->add(*entry);

                // plan read/update customer record
                tpcc_txn_man->plan_payment_update_c(tpcc_msg,entry);
                checkMRange(mrange, entry->rid, et_id);
                entry->planner_id = _worker_cluster_wide_id;
                entry->txn_idx  = pbatch_cnt;
                mrange->add(*entry);

                // plan insert into history
                tpcc_txn_man->plan_payment_insert_h(tpcc_msg,entry);
                checkMRange(mrange, entry->rid, et_id);
                entry->planner_id = _worker_cluster_wide_id;
                entry->txn_idx  = pbatch_cnt;
                mrange->add(*entry);
#else
                // index look up for warehouse record
                tpcc_txn_man->payment_lookup_w(tpcc_msg->w_id, r_local);
                rid = r_local->get_row_id();
                // check range for warehouse and split if needed
                checkMRange(mrange, rid, et_id);
                // create exec_qe for updating  warehouse record
                tpcc_txn_man->plan_payment_update_w(tpcc_msg->h_amount,r_local, entry);
                mrange->add(*entry);

                // plan read/update district record
                tpcc_txn_man->payment_lookup_d(tpcc_msg->w_id,tpcc_msg->d_id,tpcc_msg->d_w_id,r_local);
                rid = r_local->get_row_id();
                checkMRange(mrange, rid, et_id);
                tpcc_txn_man->plan_payment_update_d(tpcc_msg->h_amount, r_local,entry);
                mrange->add(*entry);

                // plan read/update customer record
                tpcc_txn_man->payment_lookup_c(tpcc_msg->c_id, tpcc_msg->c_w_id, tpcc_msg->c_d_id, tpcc_msg->c_last,
                                               tpcc_msg->by_last_name, r_local);

                rid = r_local->get_row_id();
                checkMRange(mrange, rid, et_id);
                tpcc_txn_man->plan_payment_update_c(tpcc_msg->h_amount, r_local, entry);
                mrange->add(*entry);

                // plan insert into history
                tpcc_txn_man->plan_payment_insert_h(tpcc_msg->w_id, tpcc_msg->d_id, tpcc_msg->c_id, tpcc_msg->c_w_id, tpcc_msg->d_w_id, tpcc_msg->h_amount, entry);
                rid = entry->rid;
                checkMRange(mrange, rid, et_id);
                mrange->add(*entry);
#endif
                break;
            case TPCC_NEW_ORDER:
#if !SINGLE_NODE
#if PLAN_NO_DIST_UPDATE_FIRST
                //plan update on district table
                tpcc_txn_man->neworder_lookup_d(tpcc_msg->w_id, tpcc_msg->d_id, r_local);
                rid = r_local->get_row_id();
                checkMRange(mrange, rid, et_id);
                tpcc_txn_man->plan_neworder_update_d(r_local,entry);
                mrange->add(*entry);
#endif
                tpcc_txn_man->plan_neworder_read_w(tpcc_msg,entry);
                checkMRange(mrange, entry->rid, et_id);
                entry->planner_id = _worker_cluster_wide_id;
                entry->txn_idx  = pbatch_cnt;
                mrange->add(*entry);

                // plan read on cust. record
                tpcc_txn_man->plan_neworder_read_c(tpcc_msg,entry);
                checkMRange(mrange, entry->rid, et_id);
                entry->planner_id = _worker_cluster_wide_id;
                entry->txn_idx  = pbatch_cnt;
                mrange->add(*entry);

#if !PLAN_NO_DIST_UPDATE_FIRST
                //plan update on district table
                tpcc_txn_man->plan_neworder_update_d(tpcc_msg,entry);
                checkMRange(mrange, entry->rid, et_id);
                entry->planner_id = _worker_cluster_wide_id;
                entry->txn_idx  = pbatch_cnt;
                mrange->add(*entry);
#endif

                // plan insert into orders
                tpcc_txn_man->plan_neworder_insert_o(tpcc_msg,entry);
                checkMRange(mrange, entry->rid, et_id);
                entry->planner_id = _worker_cluster_wide_id;
                entry->txn_idx  = pbatch_cnt;
                mrange->add(*entry);

                // plan insert into new order
                tpcc_txn_man->plan_neworder_insert_no(tpcc_msg, entry);
                checkMRange(mrange, entry->rid, et_id);
                entry->planner_id = _worker_cluster_wide_id;
                entry->txn_idx  = pbatch_cnt;
                mrange->add(*entry);

                for (uint64_t i =0; i < tpcc_msg->ol_cnt; ++i){

                    uint64_t ol_number = i;

                    // plan read an item from items
                    tpcc_txn_man->plan_neworder_read_i(tpcc_msg,ol_number,entry);
                    checkMRange(mrange, entry->rid, et_id);
                    entry->planner_id = _worker_cluster_wide_id;
                    entry->txn_idx  = pbatch_cnt;
                    mrange->add(*entry);

                    // plan update to a item's stock record
                    tpcc_txn_man->plan_neworder_update_s(tpcc_msg, ol_number,entry);
                    checkMRange(mrange, entry->rid, et_id);
                    entry->planner_id = _worker_cluster_wide_id;
                    entry->txn_idx  = pbatch_cnt;
                    mrange->add(*entry);

                    // plan insert into order_line
                    tpcc_txn_man->plan_neworder_insert_ol(tpcc_msg, ol_number, entry);
                    checkMRange(mrange, entry->rid, et_id);
                    entry->batch_id = wbatch_id;
                    entry->planner_id = _worker_cluster_wide_id;
                    entry->txn_idx  = pbatch_cnt;
                    mrange->add(*entry);
                }

#else
                // plan read on warehouse record
//                if(!entry->txn_ctx->txn_state.compare_exchange_strong(e8,d8)){
//                    assert(false);
//                }
#if PLAN_NO_DIST_UPDATE_FIRST
                //plan update on district table
                tpcc_txn_man->neworder_lookup_d(tpcc_msg->w_id, tpcc_msg->d_id, r_local);
                rid = r_local->get_row_id();
                checkMRange(mrange, rid, et_id);
                tpcc_txn_man->plan_neworder_update_d(r_local,entry);
                mrange->add(*entry);
#endif

                tpcc_txn_man->neworder_lookup_w(tpcc_msg->w_id,r_local);
                rid = r_local->get_row_id();
                // check range for warehouse and split if needed
                checkMRange(mrange, rid, et_id);
                tpcc_txn_man->plan_neworder_read_w(r_local,entry);
                mrange->add(*entry);

                // plan read on cust. record
                tpcc_txn_man->neworder_lookup_c(tpcc_msg->w_id,tpcc_msg->d_id,tpcc_msg->c_id, r_local);
                rid = r_local->get_row_id();
                checkMRange(mrange, rid, et_id);
                tpcc_txn_man->plan_neworder_read_c(r_local, entry);
                mrange->add(*entry);

#if !PLAN_NO_DIST_UPDATE_FIRST
                //plan update on district table
                tpcc_txn_man->neworder_lookup_d(tpcc_msg->w_id, tpcc_msg->d_id, r_local);
                rid = r_local->get_row_id();
                checkMRange(mrange, rid, et_id);
                tpcc_txn_man->plan_neworder_update_d(r_local,entry);
                mrange->add(*entry);
#endif
                // plan insert into orders
                tpcc_txn_man->plan_neworder_insert_o(tpcc_msg->w_id, tpcc_msg->d_id,tpcc_msg->c_id,tpcc_msg->remote,tpcc_msg->ol_cnt,tpcc_msg->o_entry_d,entry);
                rid = entry->rid;
                checkMRange(mrange, rid, et_id);
                mrange->add(*entry);

                // plan insert into new order
                tpcc_txn_man->plan_neworder_insert_no(tpcc_msg->w_id,tpcc_msg->d_id, tpcc_msg->c_id, entry);
                rid = entry->rid;
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
                    checkMRange(mrange, rid, et_id);
                    tpcc_txn_man->plan_neworder_read_i(r_local,entry);
                    mrange->add(*entry);

                    // plan update to a item's stock record
                    tpcc_txn_man->neworder_lookup_s(ol_i_id,ol_supply_w_id,r_local);
                    rid = r_local->get_row_id();
                    checkMRange(mrange, rid, et_id);
                    tpcc_txn_man->plan_neworder_update_s(ol_quantity, tpcc_msg->remote, r_local,entry);
                    mrange->add(*entry);

                    // plan insert into order_line
                    tpcc_txn_man->plan_neworder_insert_ol(ol_i_id,ol_supply_w_id,ol_quantity, ol_number, tpcc_msg->d_id, r_local, entry);
                    rid = entry->rid;
                    checkMRange(mrange, rid, et_id);
                    mrange->add(*entry);
                }
#endif
                break;
            default:
                M_ASSERT_V(false, "Only Payment(%d) and NewOrder(%d) transactions are supported, found (%ld)\n", TPCC_PAYMENT, TPCC_NEW_ORDER, tpcc_msg->txn_type);
        }


#endif

#endif //#if LADS_IN_QUECC

#if PROFILE_EXEC_TIMING
    INC_STATS(_thd_id,plan_txn_process_time[_planner_id], get_sys_clock() - _txn_prof_starttime);
#endif

    // increment for next ransaction
    planner_txn_id++;
    pbatch_cnt++;

#if !INIT_QUERY_MSGS
#if WORKLOAD == YCSB
    YCSBClientQueryMessage* cl_msg = (YCSBClientQueryMessage*)msg;
#if !SINGLE_NODE
    for(uint64_t i = 0; i < cl_msg->requests.size(); i++) {
        mem_allocator.free(cl_msg->requests[i],sizeof(ycsb_request));
    }
#endif
#elif WORKLOAD == TPCC
    TPCCClientQueryMessage* cl_msg = (TPCCClientQueryMessage*)msg;
#if !SINGLE_NODE
            if(cl_msg->txn_type == TPCC_NEW_ORDER) {
                for(uint64_t i = 0; i < cl_msg->items.size(); i++) {
                    mem_allocator.free(cl_msg->items[i],sizeof(Item_no));
                }
            }
#endif
#elif WORKLOAD == PPS
            PPSClientQueryMessage* cl_msg = (PPSClientQueryMessage*)msg;
#endif
    // Free message, as there is no need for it anymore
//    msg->release();
    Message::release_message(msg);
#endif

}
#endif

void WorkerThread::do_batch_delivery_mpt(uint64_t batch_slot, priority_group * planner_pg)
{

    batch_partition *batch_part = NULL;
    Array<exec_queue_entry> *exec_q_tmp = NULL;

#if BATCHING_MODE == TIME_BASED
    assert(false);
#endif // BATCHING_MODE == TIME_BASED

//            DEBUG_Q("Batch complete\n")
    // a batch is ready to be delivered to the the execution threads.
    // we will have a batch partition prepared by for each of the planners and they are scanned in known order
    // by execution threads
    // We also have a batch queue for each of the executor that is mapped to bucket
    // for now, we will assign a bucket to each executor
    // All we need to do is to automically CAS the pointer to the address of the accumulated batch
    // and the execution threads who are spinning can start execution
    // Here major ranges have one-to-one mapping to worker threads
    // No splot
#if PROFILE_EXEC_TIMING
    uint64_t _prof_starttime = get_sys_clock();
#endif


#if DEBUG_QUECC & false
        uint64_t total_eq_entries = 0;
        for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
            DEBUG_Q("ET_%ld: plan_batch - ET[%ld] = %ld\n",_thd_id,i,exec_queues->get(i)->size());
            total_eq_entries += exec_queues->get(i)->size();
        }
        DEBUG_Q("ET_%ld: plan_batch - total eq entries = %ld\n",_thd_id,total_eq_entries);
#endif


    // consider remote EQs
    /**
     * For now, we assume that there 1 EQ for each worker thread in the system
     *     Assume: exec_qs_ranges.size() == g_thread_cnt*g_node_cnt
     *     We also assume that ranges are sorted
     * Some of them will be executed locally, while others will be executed remotely
     * First for each EQ, we need to determine if it is going to be executed locally or remotely
     * If locally: assign them in the batch queue
     * If remote: create a message and enqueue to sender thread
     */

    uint64_t planner_cwid = QueCCPool::map_to_cwplanner_id(_planner_id);

    for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
        exec_q_tmp = exec_queues->get(i);

        // assign all EQs locally
        quecc_pool.batch_part_get_or_create(batch_part, planner_cwid, i);
        batch_part->exec_q = exec_q_tmp;
        batch_part->planner_pg = planner_pg;
        batch_part->batch_id = wbatch_id;

        if (batch_part->exec_q->size() == 0){
            batch_part->empty = true;
        }

        // In case batch slot is not ready, spin!
//        DEBUG_Q("N_%u:PT_%lu:CWCID_%lu:WT_%lu:TargetNode=%lu: Batch_map[%lu][%lu][%lu] , size = %lu\n",
//                g_node_id,_planner_id,planner_cwid,_thd_id,QueCCPool::get_exec_node(i),wbatch_id,planner_cwid,i, exec_q_tmp->size()
//        );
        assert(planner_cwid < (g_plan_thread_cnt*g_node_cnt));
        assert( i < (g_cluster_worker_thread_cnt));
#if SPT_WORKLOAD
        if (QueCCPool::get_exec_node(i) != g_node_id){
            assert(exec_q_tmp->size() == 0);
//                // release EQs
            quecc_pool.exec_queue_release(batch_part->exec_q, planner_cwid, i);
//                // release batch part
            quecc_pool.batch_part_release(batch_part, planner_cwid, i);
            continue;
        }
#endif
        expected = 0;
        desired = (uint64_t) batch_part;
    // multipartiton workload
        // Set batch slot first, then send out the remote EQs
        while(!work_queue.batch_map[batch_slot][planner_cwid][i].compare_exchange_strong(expected, desired)){
            if (simulation->is_done()){
                break;
            }
        };
        if (QueCCPool::get_exec_node(i) != g_node_id) {
            // remote
            // for remote EQs we keep track of their operations,
            // when their execution gets ack'ed we can update their txn contexts correctly
//            assert(exec_q_tmp->size() == 0);
//            quecc_pool.batch_part_get_or_create(batch_part, planner_cwid, i);
//            batch_part->empty = true;

            DEBUG_Q("N_%u:WT_%lu: TargetNode=%lu: Sending REMOTE EQ Batch map[%lu][%lu][%lu], size = %lu\n",
                    g_node_id,_thd_id,QueCCPool::get_exec_node(i), wbatch_id,planner_cwid,i, exec_q_tmp->size()
            );

            // create remote message and send it
            Message * req_msg = (RemoteEQMessage *) Message::create_message(REMOTE_EQ);
            req_msg->batch_id = wbatch_id;
            req_msg->return_node_id = g_node_id;
            ((RemoteEQMessage *) req_msg)->planner_id = planner_cwid;
            ((RemoteEQMessage *) req_msg)->exec_id = i;
            ((RemoteEQMessage *) req_msg)->exec_q = exec_q_tmp;
            msg_queue.enqueue(_thd_id,req_msg,QueCCPool::get_exec_node(i));

            batch_part->remote = true;
        }
        else{
            DEBUG_Q("N_%u:WT_%lu: TargetNode=%lu: Assigned LOCAL to Batch map[%lu][%lu][%lu], size = %lu\n",
                    g_node_id,_thd_id,QueCCPool::get_exec_node(i), wbatch_id,planner_cwid,i, exec_q_tmp->size()
            );
        }


    }

#if PROFILE_EXEC_TIMING
    INC_STATS(_thd_id, plan_merge_time[_planner_id], get_sys_clock()-_prof_starttime);
#endif

    // reset data structures and execution queues for the new batch
#if PROFILE_EXEC_TIMING
    _prof_starttime = get_sys_clock();
#endif
    exec_queues->clear();
    for (uint64_t i = 0; i < exec_qs_ranges->size(); i++) {
        Array<exec_queue_entry> * exec_q;
        quecc_pool.exec_queue_get_or_create(exec_q, QueCCPool::map_to_cwplanner_id(_planner_id), i);
        exec_queues->add(exec_q);
    }


#if PROFILE_EXEC_TIMING
    INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock()-prof_starttime);
    INC_STATS(_thd_id, plan_batch_process_time[_planner_id], get_sys_clock() - batch_start_time);
#endif
    INC_STATS(_thd_id, plan_batch_cnts[_planner_id], 1);

    //reset batch_cnt for next time
    pbatch_cnt = 0;
}

#if false
void WorkerThread::do_batch_delivery_mpt_rc(uint64_t batch_slot, priority_group * planner_pg)
{

    batch_partition *batch_part = NULL;
    Array<exec_queue_entry> *exec_q_tmp = NULL;

#if BATCHING_MODE == TIME_BASED
    assert(false);
#endif // BATCHING_MODE == TIME_BASED

//            DEBUG_Q("Batch complete\n")
    // a batch is ready to be delivered to the the execution threads.
    // we will have a batch partition prepared by for each of the planners and they are scanned in known order
    // by execution threads
    // We also have a batch queue for each of the executor that is mapped to bucket
    // for now, we will assign a bucket to each executor
    // All we need to do is to automically CAS the pointer to the address of the accumulated batch
    // and the execution threads who are spinning can start execution
    // Here major ranges have one-to-one mapping to worker threads
    // No splot
#if PROFILE_EXEC_TIMING
    uint64_t _prof_starttime = get_sys_clock();
#endif


#if DEBUG_QUECC & false
    uint64_t total_eq_entries = 0;
        for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
            DEBUG_Q("ET_%ld: plan_batch - ET[%ld] = %ld\n",_thd_id,i,exec_queues->get(i)->size());
            total_eq_entries += exec_queues->get(i)->size();
        }
        DEBUG_Q("ET_%ld: plan_batch - total eq entries = %ld\n",_thd_id,total_eq_entries);
#endif


    // consider remote EQs
    /**
     * For now, we assume that there 1 EQ for each worker thread in the system
     *     Assume: exec_qs_ranges.size() == g_thread_cnt*g_node_cnt
     *     We also assume that ranges are sorted
     * Some of them will be executed locally, while others will be executed remotely
     * First for each EQ, we need to determine if it is going to be executed locally or remotely
     * If locally: assign them in the batch queue
     * If remote: create a message and enqueue to sender thread
     */

    uint64_t planner_cwid = QueCCPool::map_to_cwplanner_id(_planner_id);
    batch_part->planner_pg->eq_completed_rem_cnt.store(exec_qs_ranges->size());
    assert(exec_queues->size() == g_cluster_worker_thread_cnt);
    batch_part_min_heap_t bp_minh;
    for (uint64_t i = 0; i < g_cluster_worker_thread_cnt; ++i) {
        batch_part = makeBatchPartition(planner_pg, planner_cwid,i, exec_queues);
        bp_minh.push(batch_part);
    }

    // balance load for Read-only EQs
    assert(ro_exec_queues->size() == g_cluster_worker_thread_cnt);
    for (uint64_t i = 0; i < g_cluster_worker_thread_cnt; ++i) {
        exec_q_tmp = ro_exec_queues->get(i);
        auto bp = bp_minh.top();
        bp->exec_qs->add(exec_q_tmp);
        bp_minh.pop();
        bp_minh.push(bp);
    }



    for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
        exec_q_tmp = exec_queues->get(i);

        // assign all EQs locally

//        batch_part->exec_q = exec_q_tmp;

//        if (batch_part->exec_q->size() == 0){
//            batch_part->empty = true;
//        }


        // In case batch slot is not ready, spin!
        expected = 0;
        desired = (uint64_t) batch_part;
//        DEBUG_Q("N_%u:PT_%lu:CWCID_%lu:WT_%lu:TargetNode=%lu: Batch_map[%lu][%lu][%lu] , size = %lu\n",
//                g_node_id,_planner_id,planner_cwid,_thd_id,QueCCPool::get_exec_node(i),wbatch_id,planner_cwid,i, exec_q_tmp->size()
//        );
        assert(planner_cwid < (g_plan_thread_cnt*g_node_cnt));
        assert( i < (g_cluster_worker_thread_cnt));
#if SPT_WORKLOAD
        if (QueCCPool::get_exec_node(i) != g_node_id){
            assert(exec_q_tmp->size() == 0);
//                // release EQs
            quecc_pool.exec_queue_release(batch_part->exec_q, planner_cwid, i);
//                // release batch part
            quecc_pool.batch_part_release(batch_part, planner_cwid, i);
            continue;
        }
#endif
        // multipartiton workload
        // Set batch slot first, then send out the remote EQs
        while(!work_queue.batch_map[batch_slot][planner_cwid][i].compare_exchange_strong(expected, desired)){
            if (simulation->is_done()){
                break;
            }
        };
        if (QueCCPool::get_exec_node(i) != g_node_id) {
            // remote
            // for remote EQs we keep track of their operations,
            // when their execution gets ack'ed we can update their txn contexts correctly
//            assert(exec_q_tmp->size() == 0);
//            quecc_pool.batch_part_get_or_create(batch_part, planner_cwid, i);
//            batch_part->empty = true;

            DEBUG_Q("N_%u:WT_%lu: TargetNode=%lu: Sending REMOTE EQ Batch map[%lu][%lu][%lu], size = %lu\n",
                    g_node_id,_thd_id,QueCCPool::get_exec_node(i), wbatch_id,planner_cwid,i, exec_q_tmp->size()
            );

            // create remote message and send it
            Message * req_msg = (RemoteEQMessage *) Message::create_message(REMOTE_EQ);
            req_msg->batch_id = wbatch_id;
            req_msg->return_node_id = g_node_id;
            ((RemoteEQMessage *) req_msg)->planner_id = planner_cwid;
            ((RemoteEQMessage *) req_msg)->exec_id = i;
            ((RemoteEQMessage *) req_msg)->exec_q = exec_q_tmp;
            msg_queue.enqueue(_thd_id,req_msg,QueCCPool::get_exec_node(i));

            batch_part->remote = true;
        }
        else{
            DEBUG_Q("N_%u:WT_%lu: TargetNode=%lu: Assigned LOCAL to Batch map[%lu][%lu][%lu], size = %lu\n",
                    g_node_id,_thd_id,QueCCPool::get_exec_node(i), wbatch_id,planner_cwid,i, exec_q_tmp->size()
            );
        }


    }

#if PROFILE_EXEC_TIMING
    INC_STATS(_thd_id, plan_merge_time[_planner_id], get_sys_clock()-_prof_starttime);
#endif

    // reset data structures and execution queues for the new batch
#if PROFILE_EXEC_TIMING
    _prof_starttime = get_sys_clock();
#endif
    exec_queues->clear();
    for (uint64_t i = 0; i < exec_qs_ranges->size(); i++) {
        Array<exec_queue_entry> * exec_q;
        quecc_pool.exec_queue_get_or_create(exec_q, QueCCPool::map_to_cwplanner_id(_planner_id), i);
        exec_queues->add(exec_q);
    }


#if PROFILE_EXEC_TIMING
    INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock()-prof_starttime);
    INC_STATS(_thd_id, plan_batch_process_time[_planner_id], get_sys_clock() - batch_start_time);
#endif
    INC_STATS(_thd_id, plan_batch_cnts[_planner_id], 1);

    //reset batch_cnt for next time
    pbatch_cnt = 0;
}
#endif

batch_partition *WorkerThread::makeBatchPartition(priority_group *planner_pg, uint64_t planner_cwid, uint64_t cw_eid, Array<Array<exec_queue_entry> *> * in_eqs) const
{
    batch_partition *bp;
    auto exec_q = in_eqs->get(cw_eid);
    quecc_pool.batch_part_get_or_create(bp, planner_cwid, cw_eid);
    bp->planner_pg = planner_pg;
    bp->planner_pg->eq_completed_rem_cnt.fetch_add(1);
    bp->batch_id = wbatch_id;
    bp->single_q = false;
    quecc_pool.exec_qs_get_or_create(bp->exec_qs, _planner_id);
    bp->exec_qs->add(exec_q);
    bp->cwet_id = cw_eid;
    bp->cwpt_id = planner_cwid;
    return bp;
}


void WorkerThread::do_batch_delivery_mpt_pernode(uint64_t batch_slot, priority_group * planner_pg)
{

    batch_partition *batch_part = NULL;

#if BATCHING_MODE == TIME_BASED
    assert(false);
#endif // BATCHING_MODE == TIME_BASED

//            DEBUG_Q("Batch complete\n")
    // a batch is ready to be delivered to the the execution threads.
    // we will have a batch partition prepared by for each of the planners and they are scanned in known order
    // by execution threads
    // We also have a batch queue for each of the executor that is mapped to bucket
    // for now, we will assign a bucket to each executor
    // All we need to do is to automically CAS the pointer to the address of the accumulated batch
    // and the execution threads who are spinning can start execution
    // Here major ranges have one-to-one mapping to worker threads
    // No splot
#if PROFILE_EXEC_TIMING
    uint64_t _prof_starttime = get_sys_clock();
#endif

    EQMap per_node_eqs;
    EQMap per_node_roeqs;
    for (uint64_t i =0; i < g_node_cnt; ++i){
        per_node_eqs[i] = new std::vector<eq_et_meta_t *>();
#if ISOLATION_LEVEL == READ_COMMITTED
        per_node_roeqs[i] = new std::vector<eq_et_meta_t *>();
#endif
    }
    // collect Eqs to be sent per node
    uint64_t planner_cwid = QueCCPool::map_to_cwplanner_id(_planner_id);

    //currently we assume that there is only one sub-range, EQ, RO-EQ per worker thread in the cluster.
    //  TODO(tq): Make this more generalized

    assert(exec_qs_ranges->size() == g_cluster_worker_thread_cnt);
    assert(exec_queues->size() == g_cluster_worker_thread_cnt);
#if ISOLATION_LEVEL == READ_COMMITTED
    assert(ro_exec_queues->size() == g_cluster_worker_thread_cnt);
#endif
    for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
        auto enid = QueCCPool::get_exec_node(i);

        auto md = (eq_et_meta_t *) mem_allocator.alloc(sizeof(eq_et_meta_t));
        md->et_id=i;
        md->exec_q = exec_queues->get(i);
        md->read_only = false;

        per_node_eqs[enid]->push_back(md);
//        DEBUG_Q("N_%u:WT_%lu: Adding EQ ET_Node ID=%lu: batch_id=%lu, PT=%lu, ET=%lu, size=%lu\n",
//                g_node_id,_thd_id,enid, wbatch_id,planner_cwid,i, md->exec_q->size());
#if ISOLATION_LEVEL == READ_COMMITTED
        auto ro_md = (eq_et_meta_t *) mem_allocator.alloc(sizeof(eq_et_meta_t));
        ro_md->et_id=i;
        ro_md->exec_q = ro_exec_queues->get(i);
        ro_md->read_only = true;

//        DEBUG_Q("N_%u:WT_%lu: Adding RO EQ ET_Node ID=%lu: batch_id=%lu, PT=%lu, ET=%lu, size=%lu\n",
//                g_node_id,_thd_id,enid, wbatch_id,planner_cwid,i, ro_md->exec_q->size());

        per_node_roeqs[enid]->push_back(ro_md);
#endif
    }

    // compute assignment
    assign_entry *a_tmp UNUSED = NULL;
    assign_ptr_min_heap_t assignments[NODE_CNT];
    for (uint64_t i =0; i < g_node_cnt; ++i){
        auto local_eqs = per_node_eqs[i];
        // local
        // create assingments for execqs into batch part
        assert(local_eqs->size() == g_thread_cnt);
        for (auto it = local_eqs->begin(); it != local_eqs->end(); ++it){
            auto md = *it;
            assert(QueCCPool::get_exec_node(md->et_id) == i);
            assign_entry_get_or_create(a_tmp, assign_entry_free_list);
            assign_entry_init(a_tmp, _planner_id);
            a_tmp->exec_thd_id = md->et_id;
            assign_entry_add(a_tmp,md->exec_q);
            assignments[i].push(a_tmp);
//            DEBUG_Q("N_%u:WT_%lu: Assinged EQ ET_Node ID=%lu: batch_id=%lu, PT=%lu, ET=%lu, size=%lu\n",
//                    g_node_id,_thd_id,i, wbatch_id,planner_cwid,md->et_id, md->exec_q->size());
        }
#if ISOLATION_LEVEL == READ_COMMITTED
        auto local_ro_eqs = per_node_roeqs[i];
        assert(local_ro_eqs->size() == g_thread_cnt);
        // create assingment for read only eqs with load balancing
        for (auto it = local_ro_eqs->begin(); it != local_ro_eqs->end(); ++it){
            auto md = *it;
            assert(QueCCPool::get_exec_node(md->et_id) == i);
            a_tmp = (assign_entry *) assignments[i].top();
            assign_entry_add(a_tmp, md->exec_q);
            assignments[i].pop();
            assignments[i].push(a_tmp);
//            DEBUG_Q("N_%u:WT_%lu: Assinged RO EQ ET_Node ID=%lu: batch_id=%lu, PT=%lu, ET=%lu, size=%lu\n",
//                    g_node_id,_thd_id,i, wbatch_id,planner_cwid,a_tmp->exec_thd_id, md->exec_q->size());
        }
#endif
        // assign to batch partitions
        std::set<UInt32> out_nodes;

        for (auto it=assignments[i].begin(); it != assignments[i].end(); ++it){
//            DEBUG_Q("N_%u:WT_%lu: Final assigns for ET_Node ID=%lu: batch_id=%lu, PT=%lu, ET=%lu, size=%lu\n",
//                    g_node_id,_thd_id,i, wbatch_id,planner_cwid,(*it)->exec_thd_id, (*it)->exec_qs->size());
            quecc_pool.batch_part_get_or_create(batch_part, planner_cwid, (*it)->exec_thd_id);
            batch_part->single_q = false;
            batch_part->planner_pg = planner_pg;
            batch_part->batch_id = wbatch_id;
            batch_part->cwet_id = (*it)->exec_thd_id;
            batch_part->cwpt_id = planner_cwid;
            batch_part->exec_qs =  (*it)->exec_qs;
            //TODO(tq): need to fix this to account for empty queues
            planner_pg->eq_completed_rem_cnt.fetch_add((*it)->exec_qs->size());

            uint64_t UNUSED e_cnt =0;
#if DEBUG_QUECC
            for (uint64_t ii=0; ii<(*it)->exec_qs->size(); ++ii){
                e_cnt += (*it)->exec_qs->get(ii)->size();
            }
#endif

            if (i != g_node_id) {
                // remote

                DEBUG_Q("N_%u:WT_%lu: TargetNode=%lu: Sending REMOTE %lu EQs Batch map[%lu][%lu][%lu] e_cnt = %lu, pg_tctxs=%lu\n",
                        g_node_id,_thd_id,i, batch_part->exec_qs->size(), wbatch_id,planner_cwid,(*it)->exec_thd_id,
                        e_cnt, (uint64_t)planner_pg->txn_ctxs);

                // create remote message and send it
                Message * req_msg = (RemoteEQSetMessage *) Message::create_message(REMOTE_EQ_SET);
                req_msg->batch_id = wbatch_id;
                req_msg->return_node_id = g_node_id;
                auto rs_msg = (RemoteEQSetMessage *) req_msg;
                rs_msg->planner_id = planner_cwid;
                rs_msg->exec_id = (*it)->exec_thd_id;
                rs_msg->eqs = batch_part->exec_qs;
                rs_msg->pg_txn_ctx = planner_pg->txn_ctxs;
                if (out_nodes.find(i) == out_nodes.end()){
                    rs_msg->pg_txn_ctx_size = sizeof(transaction_context)*PG_TXN_CTX_SIZE;
                    out_nodes.insert(i);
                }
                else{
                    rs_msg->pg_txn_ctx_size = 0;
                }
                msg_queue.enqueue(_thd_id,req_msg,i);
                batch_part->remote = true;
            }
            else{
                DEBUG_Q("N_%u:WT_%lu: TargetNode=%lu: Assigned LOCAL %lu EQs to Batch map[%lu][%lu][%lu] e_cnt = %lu\n",
                        g_node_id,_thd_id,i, batch_part->exec_qs->size(), wbatch_id,planner_cwid,(*it)->exec_thd_id, e_cnt);
            }



            expected = 0;
            desired = (uint64_t) batch_part;
            // multipartiton workload
            // Set batch slot first, then send out the remote EQs
            while(!work_queue.batch_map[batch_slot][planner_cwid][(*it)->exec_thd_id].compare_exchange_strong(expected, desired)){
                if (simulation->is_done()){
                    return;
                }
            };
        }

        // clean up assignment entries
        while (!assignments[i].empty()){
            assign_entry * te = (assign_entry *) assignments[i].top();
            assignments[i].pop();
            assign_entry_clear(te);
            while(!assign_entry_free_list->push(te)){}
        }
    }


    //free
    for (uint64_t i =0; i < g_node_cnt; ++i){
        for(auto it=per_node_eqs[i]->begin(); it != per_node_eqs[i]->end(); ++it){
            mem_allocator.free(*it,0);
        }
        free(per_node_eqs[i]);
#if ISOLATION_LEVEL == READ_COMMITTED
        for(auto it=per_node_roeqs[i]->begin(); it != per_node_roeqs[i]->end(); ++it){
            mem_allocator.free(*it,0);
        }
        free(per_node_roeqs[i]);
#endif
    }

#if PROFILE_EXEC_TIMING
    INC_STATS(_thd_id, plan_merge_time[_planner_id], get_sys_clock()-_prof_starttime);
#endif

    // reset data structures and execution queues for the new batch
#if PROFILE_EXEC_TIMING
    _prof_starttime = get_sys_clock();
#endif
    exec_queues->clear();
    for (uint64_t i = 0; i < exec_qs_ranges->size(); i++) {
        Array<exec_queue_entry> * exec_q;
        quecc_pool.exec_queue_get_or_create(exec_q, QueCCPool::map_to_cwplanner_id(_planner_id), i);
        exec_queues->add(exec_q);
    }
#if ISOLATION_LEVEL == READ_COMMITTED
    ro_exec_queues->clear();
    for (uint64_t i = 0; i < exec_qs_ranges->size(); i++) {
        Array<exec_queue_entry> * exec_q;
        quecc_pool.exec_queue_get_or_create(exec_q, QueCCPool::map_to_cwplanner_id(_planner_id), i);
        ro_exec_queues->add(exec_q);
    }
#endif

#if PROFILE_EXEC_TIMING
    INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock()-prof_starttime);
    INC_STATS(_thd_id, plan_batch_process_time[_planner_id], get_sys_clock() - batch_start_time);
#endif
    INC_STATS(_thd_id, plan_batch_cnts[_planner_id], 1);

    //reset batch_cnt for next time
    pbatch_cnt = 0;
}


void WorkerThread::do_batch_delivery(uint64_t batch_slot, priority_group * planner_pg)
{

#if LADS_IN_QUECC
    global_dgraph->partition_graph(_planner_id);
#else
    batch_partition *batch_part = NULL;

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
#if PROFILE_EXEC_TIMING
    uint64_t _prof_starttime = get_sys_clock();
#endif
#if SPLIT_MERGE_ENABLED

#if SPLIT_STRATEGY == EAGER_SPLIT
    // we just need compute assignment since all EQs satisfy the limit splitting is done message is processed
    Array<Array<exec_queue_entry> *> *exec_qs_tmp UNUSED = NULL;
    Array<exec_queue_entry> *exec_q_tmp = NULL;
    assign_entry *a_tmp UNUSED = NULL;
//#if DEBUG_QUECC
//        uint64_t total_eq_entries = 0;
//        for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
//            DEBUG_Q("ET_%ld: plan_batch - ET[%ld] = %ld\n",_thd_id,i,exec_queues->get(i)->size());
//            total_eq_entries += exec_queues->get(i)->size();
//        }
//        DEBUG_Q("ET_%ld: plan_batch - total eq entries = %ld\n",_thd_id,total_eq_entries);
//#endif


    // consider remote EQs

    for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
        exec_q_tmp = exec_queues->get(i);
        if (i < g_thread_cnt){
#if MERGE_STRATEGY == BALANCE_EQ_SIZE
            assign_entry_get_or_create(a_tmp, assign_entry_free_list);
            assign_entry_init(a_tmp, _planner_id);
            a_tmp->exec_thd_id = i;
            assign_entry_add(a_tmp, exec_q_tmp);
            assignment.push(a_tmp);

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
                assignment.push(a_tmp);

#elif MERGE_STRATEGY == RR
                ((Array<Array<exec_queue_entry> *> *) f_assign[i % g_thread_cnt])->add(exec_q_tmp);

#else
                    M_ASSERT_V(false,"Selected merge strategy is not supported\n");
#endif
//                    DEBUG_Q("PT_%ld: adding excess EQs to ET_%ld\n", _planner_id, a_tmp->exec_thd_id);
            }
            else{
                uint64_t et_id =  (rand() % g_thread_cnt);
//                    DEBUG_Q("PT_%ld: releasing empty to ET_%ld\n", _planner_id, et_id);
                quecc_pool.exec_queue_release(exec_q_tmp,QueCCPool::map_to_planner_id(_planner_id), et_id);
            }
        }
    }
//        M_ASSERT_V(assignment.size() == g_thread_cnt, "PL_%ld: size mismatch of assignments to threads, assignment size = %ld, thread-cnt = %d\n",
//                   _planner_id, assignment.size(), g_thread_cnt);
#if PROFILE_EXEC_TIMING
    INC_STATS(_thd_id, plan_merge_time[_planner_id], get_sys_clock()-_prof_starttime);
#endif
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
                    assignment.push(a_tmp);
                }
                else {
                    a_tmp = (assign_entry *) assignment.top();
                    assign_entry_add(a_tmp, exec_queues->get(i));
                    assignment.pop();
                    assignment.push((a_tmp);
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
//        planner_pg->batch_starting_txn_id = batch_starting_txn_id;

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
//                    DEBUG_Q("PT_%ld: assigning eq with size %ld to ET_%ld\n",_thd_id,fa_execqs->get(0)->size(), i);
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

        // In case batch slot is not ready, spin!
        expected = 0;
        desired = (uint64_t) batch_part;

//#if DEBUG_QUECC
//            print_eqs_ranges_after_swap(_planner_id, i);
//#endif
        // Deliver batch partition to the repective ET
#if BATCH_MAP_ORDER == BATCH_ET_PT
        if(!work_queue.batch_map[batch_slot][i][_planner_id].compare_exchange_strong(expected, desired)){
                // this should not happen after spinning but can happen if simulation is done
//                    M_ASSERT_V(false, "For batch %ld : failing to SET map slot [%ld][%ld][%ld]\n", batch_id, slot_num, i, _planner_id);
            }
#else
//            DEBUG_Q("PT_%ld: for batch %ld : setting map slot [%ld][%ld][%ld]\n", _planner_id, pbatch_id, batch_slot, _planner_id, i)
        if(!work_queue.batch_map[batch_slot][_planner_id][i].compare_exchange_strong(expected, desired)){

//#if DEBUG_QUECC
//                // print sync blocks
//                for (int i = 0; i < BATCH_MAP_LENGTH; ++i) {
//                    for (UInt32 j = 0; j < g_thread_cnt; ++j) {
//                        DEBUG_Q("WT_%ld: set_batch_map, batch_id=%ld, plan_sync: done[%d]=%ld, plan_next_stage=%ld\n", _thd_id,wbatch_id,j,work_queue.plan_sblocks[i][j].done, *work_queue.plan_next_stage[i]);
//                    }
//                    for (UInt32 j = 0; j < g_thread_cnt; ++j) {
//                        DEBUG_Q("WT_%ld: set_batch_map, batch_id=%ld, exec_sync: done[%d]=%ld, exec_next_stage=%ld\n",_thd_id,wbatch_id,j,work_queue.exec_sblocks[i][j].done, *work_queue.exec_next_stage[i]);
//                    }
//                    for (UInt32 j = 0; j < g_thread_cnt; ++j) {
//                        DEBUG_Q("WT_%ld: set_batch_map, batch_id=%ld, commit_sync: done[%d]=%ld, commit_next_stage=%ld\n",_thd_id,wbatch_id,j,work_queue.commit_sblocks[i][j].done, *work_queue.commit_next_stage[i]);
//                    }
//                }
//#endif
            // this should not happen after spinning but can happen if simulation is done
            M_ASSERT_V(false, "WT_%ld: For batch %ld : failing to SET map slot [%ld],  PG=[%ld], batch_map_val=%ld\n",
                       _thd_id, wbatch_id, batch_slot, _planner_id, work_queue.batch_map[batch_slot][_planner_id][i].load());
//                    SAMPLED_DEBUG_Q("PT_%ld: for batch %ld : failing to SET map slot [%ld][%ld][%ld]\n", _planner_id, pbatch_id, batch_slot, i, _planner_id)
        }

#endif
//                DEBUG_Q("PT_%ld :Batch_%ld for range_%ld ready! b_slot = %ld\n", _planner_id, batch_id, i, slot_num);
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
#if PROFILE_EXEC_TIMING
    _prof_starttime = get_sys_clock();
#endif
    exec_queues->clear();
    for (uint64_t i = 0; i < exec_qs_ranges->size(); i++) {
        Array<exec_queue_entry> * exec_q;
#if MERGE_STRATEGY == RR
        et_id = i % g_thread_cnt;
#else
        et_id = eq_idx_rand->operator()(plan_rng);
#endif
        quecc_pool.exec_queue_get_or_create(exec_q, _planner_id, et_id);
        exec_queues->add(exec_q);
    }
#endif //if LADS_IN_QUECC

#if PROFILE_EXEC_TIMING
    INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock()-prof_starttime);
    INC_STATS(_thd_id, plan_batch_process_time[_planner_id], get_sys_clock() - batch_start_time);
#endif
    INC_STATS(_thd_id, plan_batch_cnts[_planner_id], 1);

    //reset batch_cnt for next time
    pbatch_cnt = 0;
}


#endif // if !PIPELINED
#if SYNC_AFTER_PG
SRC WorkerThread::pg_sync(uint64_t batch_slot, uint64_t planner_id){
#if WT_SYNC_METHOD == SYNC_BLOCK
#if !NEXT_STAGE_ARRAY
    // not supported
        assert(false);
#endif

//#if PROFILE_EXEC_TIMING
//        uint64_t sync_idlestarttime = 0;
//#endif
    UInt32 done_cnt = 0;
    // indicate that I am done with current PG
    work_queue.pg_sblocks[batch_slot][planner_id][_thd_id].done = 1;

#if SYNC_MASTER_RR
    bool is_master = ((wbatch_id % g_thread_cnt) == _thd_id);
#else
    bool is_master = (_thd_id == 0);
#endif
    if (is_master){
//            DEBUG_Q("WT_%ld: going to wait for other WTs for batch_slot = %ld, plan_comp_cnt = %d\n",
//                    _thd_id, batch_slot, work_queue.batch_plan_comp_cnts[batch_slot].load());

        // wait for all ets to finish
        while (true){
            done_cnt = 0;
            atomic_thread_fence(memory_order_acquire);
            for (uint32_t i=0; i < g_thread_cnt; ++i){
                done_cnt += work_queue.pg_sblocks[batch_slot][planner_id][i].done;
            }
            if (done_cnt == g_thread_cnt){
                break;
            }
//#if PROFILE_EXEC_TIMING
//                if (sync_idlestarttime ==0){
//                    sync_idlestarttime = get_sys_clock();
////                    DEBUG_Q("WT_%ld: plan_stage waiting for WT_* to SET done, batch_id = %ld\n", _thd_id,wbatch_id);
//                }
//#endif
            //TQ: no need to preeempt this wait
            if (simulation->is_done()){
//#if PROFILE_EXEC_TIMING
//                    INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
//#endif
                return BREAK;
            }
        }
//            DEBUG_Q("WT_%ld: plan_stage all WT_* are done, done_cnt = %d, batch_id = %ld\n", _thd_id, done_cnt,wbatch_id);

//#if PROFILE_EXEC_TIMING
//            if (sync_idlestarttime > 0){
//                INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
//                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
//            }
//#endif
        // allow other ETs to proceed

        for (UInt32 j = 0; j < g_thread_cnt; ++j) {
            *(work_queue.pg_next_stage[batch_slot][planner_id][j]) = 1;
        }
        atomic_thread_fence(memory_order_release);

//            DEBUG_Q("WT_%ld: pg_exec_stage - going to wait for WT_* to see next stage, batch_id = %ld\n", _thd_id,wbatch_id);
        // need to wait for all threads to exit sync
        work_queue.pg_sblocks[batch_slot][planner_id][_thd_id].done = 0;
        while (true){
            done_cnt = 0;
            atomic_thread_fence(memory_order_acquire);
            for (uint32_t i=0; i < g_thread_cnt; ++i){
                done_cnt += work_queue.pg_sblocks[batch_slot][planner_id][i].done;
            }
            if (done_cnt == 0){
                break;
            }
//#if PROFILE_EXEC_TIMING
//                if (sync_idlestarttime ==0){
//                    sync_idlestarttime = get_sys_clock();
////                    DEBUG_Q("WT_%ld: plan_stage waiting for WT_* to SET done, batch_id = %ld\n", _thd_id,wbatch_id);
//                }
//#endif
            //TQ: no need to preeempt this wait
            if (simulation->is_done()){
//#if PROFILE_EXEC_TIMING
//                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
//#endif
                return BREAK;
            }
        }
//#if PROFILE_EXEC_TIMING
//            if (sync_idlestarttime > 0){
//                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
//                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
//            }
//#endif

//            DEBUG_Q("WT_%lu: pg_exec_stage - all WT_* are ready to move to next stage, batch_id = %lu, PG=%lu\n",
//                    _thd_id,wbatch_id,planner_id);
        // all other threads are ready to exit sync
        for (UInt32 j = 0; j < g_thread_cnt; ++j) {
            *(work_queue.pg_next_stage[batch_slot][planner_id][j]) = 0;
        }
        atomic_thread_fence(memory_order_release);
    }
    else{
        atomic_thread_fence(memory_order_acquire);
        while(*(work_queue.pg_next_stage[batch_slot][planner_id][_thd_id]) != 1){
//#if PROFILE_EXEC_TIMING
//                if (sync_idlestarttime ==0){
//                    sync_idlestarttime = get_sys_clock();
////                    DEBUG_Q("WT_%ld: plan_stage waiting for WT_0 to SET next_stage, batch_id = %ld\n", _thd_id,wbatch_id);
//                }
//#endif
            if (simulation->is_done()){
//#if PROFILE_EXEC_TIMING
//                    INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
//#endif
                return BREAK;
            }
            atomic_thread_fence(memory_order_acquire);
        };
//#if PROFILE_EXEC_TIMING
//            if (sync_idlestarttime > 0){
//                INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
//                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
//            }
//#endif
//            DEBUG_Q("WT_%ld: pg_exec_stage WT_0 has SET next_stage, batch_id = %ld, PG=%ld\n", _thd_id,wbatch_id,planner_id);

        work_queue.pg_sblocks[batch_slot][planner_id][_thd_id].done = 0;
        atomic_thread_fence(memory_order_release);

        atomic_thread_fence(memory_order_acquire);
        while(*(work_queue.pg_next_stage[batch_slot][planner_id][_thd_id]) != 0){
//#if PROFILE_EXEC_TIMING
//                if (sync_idlestarttime ==0){
//                    sync_idlestarttime = get_sys_clock();
////                    DEBUG_Q("WT_%ld: plan_stage waiting for WT_0 to RESET next_stage, batch_id = %ld\n", _thd_id,wbatch_id);
//                }
//#endif
            if (simulation->is_done()){
//#if PROFILE_EXEC_TIMING
//                    INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
//#endif
                return BREAK;
            }
            atomic_thread_fence(memory_order_acquire);
        };
//#if PROFILE_EXEC_TIMING
//            if (sync_idlestarttime > 0){
//                INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
//                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
//            }
//#endif
//                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
//            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
//            idle_starttime =0;
    }
    return SUCCESS;
#else
    // other syncing methods are not supported
        assert(false);
        return SUCCESS;
#endif
}
#endif // #if SYNC_AFTER_PG
SRC WorkerThread::sync_on_planning_phase_end(uint64_t batch_slot){
#if PROFILE_EXEC_TIMING
    uint64_t sync_idlestarttime = 0;
#endif
#if WT_SYNC_METHOD == CAS_GLOBAL_SC ||  WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC ||  WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
    uint16_t desired16;
        uint16_t expected16;
#endif
#if WT_SYNC_METHOD == CAS_GLOBAL_SC || WT_SYNC_METHOD == SYNC_BLOCK
    UInt32 done_cnt = 0;
#endif

    // indicate that I am done planning my transactions, and ready to start committing
#if WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC
    //        expected16 = work_queue.batch_plan_comp_cnts[batch_slot].fetch_add(1);
        do{
            expected16 = work_queue.batch_plan_comp_cnts[batch_slot].load(memory_order_acq_rel);
            desired16 = expected16 + 1;
        }while (!work_queue.batch_plan_comp_cnts[batch_slot].compare_exchange_strong(expected16,desired16,memory_order_acq_rel));


#elif WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
    expected16 = work_queue.batch_plan_comp_cnts[batch_slot].fetch_add(1,memory_order_acq_rel);
#elif WT_SYNC_METHOD == SYNC_BLOCK
    work_queue.plan_sblocks[batch_slot][_thd_id].done = 1;
#elif WT_SYNC_METHOD == CAS_GLOBAL_SC
    expected16 = 0;
        desired16 = 1;
        if (!work_queue.batch_plan_comp_status[batch_slot][_planner_id].compare_exchange_strong(expected16,desired16)){
            M_ASSERT_V(false, "WT_%ld: this should not happen, I am the only one who can SET batch_plan_comp_status\n", _thd_id);
        }
#endif

#if SYNC_MASTER_RR
    bool is_master = ((wbatch_id % g_thread_cnt) == _thd_id);
#else
    bool is_master = (_thd_id == 0);
#endif
    if (is_master){
//            DEBUG_Q("WT_%ld: going to wait for other WTs for batch_slot = %ld, plan_comp_cnt = %d\n",
//                    _thd_id, batch_slot, work_queue.batch_plan_comp_cnts[batch_slot].load());

        // wait for all ets to finish
#if WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC

        expected16 = work_queue.batch_plan_comp_cnts[batch_slot].load(memory_order_acq_rel);

            while (expected16 != g_thread_cnt){
                if (idle_starttime ==0){
                    idle_starttime = get_sys_clock();
                }
                //TQ: no need to preeempt this wait
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
                expected16 = work_queue.batch_plan_comp_cnts[batch_slot].load(memory_order_acq_rel);
            };
#elif WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
        expected16 = work_queue.batch_plan_comp_cnts[batch_slot].load(memory_order_acq_rel);
            while (expected16 != g_thread_cnt){
                if (idle_starttime ==0){
                    idle_starttime = get_sys_clock();
                }
                //TQ: no need to preeempt this wait
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
                expected16 = work_queue.batch_plan_comp_cnts[batch_slot].load(memory_order_acq_rel);
            };
#elif WT_SYNC_METHOD == SYNC_BLOCK
        while (true){
            done_cnt = 0;
            atomic_thread_fence(memory_order_acquire);
            for (uint32_t i=0; i < g_plan_thread_cnt; ++i){
                done_cnt += work_queue.plan_sblocks[batch_slot][i].done;
            }
            if (done_cnt == g_plan_thread_cnt){
                break;
            }
#if PROFILE_EXEC_TIMING
            if (sync_idlestarttime ==0){
                sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: plan_stage waiting for WT_* to SET done, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
            //TQ: no need to preeempt this wait
            if (simulation->is_done()){
#if PROFILE_EXEC_TIMING
                INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
#endif
                return BREAK;
            }
        }
//            DEBUG_Q("WT_%ld: plan_stage all WT_* are done, done_cnt = %d, batch_id = %ld\n", _thd_id, done_cnt,wbatch_id);
#elif WT_SYNC_METHOD == CAS_GLOBAL_SC
        while (true){
                done_cnt = 0;
                for (uint32_t i=0; i < g_plan_thread_cnt; ++i){
                    done_cnt += work_queue.batch_plan_comp_status[batch_slot][i].load();
                }
                if (done_cnt == g_plan_thread_cnt){
                    break;
                }
                if (idle_starttime ==0){
                    idle_starttime = get_sys_clock();
                }
                //TQ: no need to preeempt this wait
                if (simulation->is_done()){
                    INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }

            }
#endif
#if PROFILE_EXEC_TIMING
        if (sync_idlestarttime > 0){
            INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
        }
#endif
        // allow other ETs to proceed
#if WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC
        desired16 = 0;
            expected16 = g_thread_cnt;
            if(!work_queue.batch_plan_comp_cnts[batch_slot].compare_exchange_strong(expected16, desired16,memory_order_acq_rel)){
                M_ASSERT_V(false, "WT_%ld: this should not happen, I am the only one who can reset batch_plan_comp_cnts\n", _thd_id);
            };
#elif WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
        desired16 = 0;
            expected16 = g_thread_cnt;
            if(!work_queue.batch_plan_comp_cnts[batch_slot].compare_exchange_strong(expected16, desired16, memory_order_acq_rel)){
                M_ASSERT_V(false, "WT_%ld: this should not happen, I am the only one who can reset batch_plan_comp_cnts\n", _thd_id);
            };
#elif WT_SYNC_METHOD == SYNC_BLOCK
//            DEBUG_Q("WT_%ld: plan_stage - going to set next_stage for WT_*, batch_id = %ld\n", _thd_id,wbatch_id);
//            for (uint32_t i=0; i < g_plan_thread_cnt; ++i){
//                work_queue.plan_sblocks[batch_slot][i].next_stage = 1;
//                atomic_thread_fence(memory_order_release);
//            }
#if NEXT_STAGE_ARRAY
        for (UInt32 j = 0; j < g_plan_thread_cnt; ++j) {
            *(work_queue.plan_next_stage[batch_slot][j]) = 1;
        }
#else
        *(work_queue.plan_next_stage[batch_slot]) = 1;
#endif
        atomic_thread_fence(memory_order_release);

//            DEBUG_Q("WT_%ld: plan_stage - going to wait for WT_* to see next stage, batch_id = %ld\n", _thd_id,wbatch_id);
        // need to wait for all threads to exit sync
        work_queue.plan_sblocks[batch_slot][_thd_id].done = 0;
        while (true){
            done_cnt = 0;
            atomic_thread_fence(memory_order_acquire);
            for (uint32_t i=0; i < g_thread_cnt; ++i){
                done_cnt += work_queue.plan_sblocks[batch_slot][i].done;
            }
            if (done_cnt == 0){
                break;
            }
#if PROFILE_EXEC_TIMING
            if (sync_idlestarttime ==0){
                sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: plan_stage waiting for WT_* to SET done, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
            //TQ: no need to preeempt this wait
            if (simulation->is_done()){
#if PROFILE_EXEC_TIMING
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
#endif
                return BREAK;
            }
        }
#if PROFILE_EXEC_TIMING
        if (sync_idlestarttime > 0){
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
        }
#endif

//            DEBUG_Q("WT_%ld: plan_stage - all WT_* are ready to move to next stage, batch_id = %ld\n", _thd_id,wbatch_id);
        // all other threads are ready to exit sync
#if NEXT_STAGE_ARRAY
        for (UInt32 j = 0; j < g_plan_thread_cnt; ++j) {
            *(work_queue.plan_next_stage[batch_slot][j]) = 0;
        }
#else
        *(work_queue.plan_next_stage[batch_slot]) = 0;
#endif
        atomic_thread_fence(memory_order_release);

#elif WT_SYNC_METHOD == CAS_GLOBAL_SC
        expected16 = 0;
            desired16 = 1;
            // Note: start from one and skip myself
            for (uint32_t i=1; i < g_plan_thread_cnt; ++i){
                if (!work_queue.batch_plan_sync_status[batch_slot][i].compare_exchange_strong(expected16,desired16)){
                    M_ASSERT_V(false, "WT_%ld: this should not happen, I am the only one who can SET batch_plan_sync_status\n", _thd_id);
                }
            }
#endif
    }
    else{
//            DEBUG_Q("WT_%ld: going to wait for WT_0 to finalize the plan phase for batch_slot = %ld, plan_comp_cnt = %d\n",
//                    _thd_id, batch_slot, work_queue.batch_plan_comp_cnts[batch_slot].load());

//            DEBUG_Q("WT_%ld: going to wait for WT_0 to finalize the plan phase for batch_slot = %ld\n",
//                    _thd_id, batch_slot);
//                DEBUG_Q("ET_%ld: Done with my batch partition going to wait for others\n", _thd_id);
#if WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC
        while (work_queue.batch_plan_comp_cnts[batch_slot].load(memory_order_acq_rel) != 0){
                if (idle_starttime ==0){
                    idle_starttime = get_sys_clock();
                }
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
                // SPINN Here untill all ETs are done and batch is committed
            }
//                DEBUG_Q("ET_%ld: execution phase is done, starting commit phase for batch_id = %ld\n", _thd_id, wbatch_id);
#elif WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
        while (work_queue.batch_plan_comp_cnts[batch_slot].load(memory_order_acq_rel) != 0){
                if (idle_starttime ==0){
                    idle_starttime = get_sys_clock();
                }
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
                // SPINN Here untill all ETs are done and batch is committed
            }
#elif WT_SYNC_METHOD == SYNC_BLOCK
        atomic_thread_fence(memory_order_acquire);
#if NEXT_STAGE_ARRAY
        while(*(work_queue.plan_next_stage[batch_slot][_planner_id]) != 1){
#else
            while(*(work_queue.plan_next_stage[batch_slot]) != 1){
#endif
#if PROFILE_EXEC_TIMING
            if (sync_idlestarttime ==0){
                sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: plan_stage waiting for WT_0 to SET next_stage, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
            if (simulation->is_done()){
#if PROFILE_EXEC_TIMING
                INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
#endif
                return BREAK;
            }
            atomic_thread_fence(memory_order_acquire);
        };
#if PROFILE_EXEC_TIMING
        if (sync_idlestarttime > 0){
            INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
        }
#endif
//            DEBUG_Q("WT_%ld: plan_stage WT_0 has SET next_stage, batch_id = %ld\n", _thd_id,wbatch_id);

        work_queue.plan_sblocks[batch_slot][_thd_id].done = 0;
        atomic_thread_fence(memory_order_release);

        atomic_thread_fence(memory_order_acquire);
#if NEXT_STAGE_ARRAY
        while(*(work_queue.plan_next_stage[batch_slot][_planner_id]) != 0){
#else
            while(*(work_queue.plan_next_stage[batch_slot]) != 0){
#endif
#if PROFILE_EXEC_TIMING
            if (sync_idlestarttime ==0){
                sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: plan_stage waiting for WT_0 to RESET next_stage, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
            if (simulation->is_done()){
#if PROFILE_EXEC_TIMING
                INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
#endif
                return BREAK;
            }
            atomic_thread_fence(memory_order_acquire);
        };
#elif WT_SYNC_METHOD == CAS_GLOBAL_SC
        // wait until WT_0 sets sync_status
            while (work_queue.batch_plan_sync_status[batch_slot][_planner_id].load() != 1){
                if (idle_starttime ==0){
                    idle_starttime = get_sys_clock();
                }
                if (simulation->is_done()){
                    INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
                SAMPLED_DEBUG_Q("WT_%ld: waiting for WT_0 to SET batch_plan_sync_status, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
#if PROFILE_EXEC_TIMING
        if (sync_idlestarttime > 0){
            INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
        }
#endif
//                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
//            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
//            idle_starttime =0;
    }
#if WT_SYNC_METHOD == CAS_GLOBAL_SC
    // Reset plan_comp_status for next time since sync is done
        expected16 = 1;
        desired16 = 0;
        if (!work_queue.batch_plan_comp_status[batch_slot][_planner_id].compare_exchange_strong(expected16,desired16)){
            M_ASSERT_V(false, "WT_%ld: this should not happen, I am the only one who can RESET batch_plan_comp_status\n", _thd_id);
        }
        // reset sync status for next batch
        if (_thd_id != 0 && !work_queue.batch_plan_sync_status[batch_slot][_planner_id].compare_exchange_strong(expected16,desired16)){
            M_ASSERT_V(false, "WT_%ld: this should not happen, I am the only one who can RESET batch_plan_sync_status\n", _thd_id);
        }
#elif WT_SYNC_METHOD == SYNC_BLOCK

//        DEBUG_Q("WT_%ld: plan_stage - going to RESET both done, next_stage and moving to the next stage, batch_id = %ld, gbatch_id=%ld\n",
//                _thd_id,wbatch_id,work_queue.gbatch_id);
#endif
    return SUCCESS;
}

SRC WorkerThread::sync_on_execution_phase_end(uint64_t batch_slot){
#if PROFILE_EXEC_TIMING
    uint64_t sync_idlestarttime =0;
#endif
#if WT_SYNC_METHOD == CAS_GLOBAL_SC ||  WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC ||  WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
    uint16_t desired16;
        uint16_t expected16;
#endif
#if WT_SYNC_METHOD == CAS_GLOBAL_SC || WT_SYNC_METHOD == SYNC_BLOCK
    UInt32 done_cnt = 0;
#endif
    // indicate that I am done processing my transactions, and ready to start committing
#if WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC
    //        expected16 = work_queue.batch_map_comp_cnts[batch_slot].fetch_add(1);
        do{
            expected16 = work_queue.batch_map_comp_cnts[batch_slot].load(memory_order_acq_rel);
            desired16 = expected16 + 1;
        }while (!work_queue.batch_map_comp_cnts[batch_slot].compare_exchange_strong(expected16,desired16,memory_order_acq_rel));
#elif WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
    expected16 = work_queue.batch_map_comp_cnts[batch_slot].fetch_add(1,memory_order_acq_rel);
#elif WT_SYNC_METHOD == SYNC_BLOCK
    work_queue.exec_sblocks[batch_slot][_thd_id].done = 1;
    atomic_thread_fence(memory_order_release);
#elif WT_SYNC_METHOD == CAS_GLOBAL_SC
    expected16 = 0;
        desired16 = 1;
        if (!work_queue.batch_exec_comp_status[batch_slot][_thd_id].compare_exchange_strong(expected16,desired16)){
            M_ASSERT_V(false, "WT_%ld: this should not happen, I am the only one who can SET batch_exec_comp_status\n", _thd_id);
        }
#endif
#if SYNC_MASTER_RR
    bool is_master = ((wbatch_id % g_thread_cnt) == _thd_id);
#else
    bool is_master = (_thd_id == 0);
#endif
    if (is_master){
//            DEBUG_Q("ET_%ld: going to wait for other ETs for batch_id = %ld, map_com_cnts = %d\n",
//                    _thd_id, wbatch_id, work_queue.batch_map_comp_cnts[batch_slot].load());
        // wait for all ets to finish
#if WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC
        expected16 = work_queue.batch_map_comp_cnts[batch_slot].load(memory_order_acq_rel);
            while (expected16 != g_thread_cnt){
                if (idle_starttime == 0){
                    idle_starttime = get_sys_clock();
                }
                //TQ: no need to preeempt this wait
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
                expected16 = work_queue.batch_map_comp_cnts[batch_slot].load(memory_order_acq_rel);
            };
#elif WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
        expected16 = work_queue.batch_map_comp_cnts[batch_slot].load(memory_order_acq_rel);
            while (expected16 != g_thread_cnt){
                if (idle_starttime == 0){
                    idle_starttime = get_sys_clock();
                }
                //TQ: no need to preeempt this wait
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
                expected16 = work_queue.batch_map_comp_cnts[batch_slot].load(memory_order_acq_rel);
            };
#elif WT_SYNC_METHOD == SYNC_BLOCK
        while (true){
            done_cnt = 0;
            atomic_thread_fence(memory_order_acquire);
            for (uint32_t i=0; i < g_thread_cnt; ++i){
                done_cnt += work_queue.exec_sblocks[batch_slot][i].done;
            }
            if (done_cnt == g_thread_cnt){
                break;
            }
#if PROFILE_EXEC_TIMING
            if (sync_idlestarttime ==0){
                sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: exec_stage waiting for WT_* to SET done, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
            //TQ: no need to preeempt this wait
            if (simulation->is_done()){
#if PROFILE_EXEC_TIMING
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
#endif
                return BREAK;
            }

        }
//            DEBUG_Q("WT_%ld: exec_stage all WT_* are done, done_cnt = %d, batch_id = %ld\n", _thd_id, done_cnt,wbatch_id);
#elif WT_SYNC_METHOD == CAS_GLOBAL_SC
        while (true){
                done_cnt = 0;
                for (uint32_t i=0; i < g_thread_cnt; ++i){
                    done_cnt += work_queue.batch_exec_comp_status[batch_slot][i].load();
                }
                if (done_cnt == g_thread_cnt){
                    break;
                }
                if (idle_starttime ==0){
                    idle_starttime = get_sys_clock();
                }
                //TQ: no need to preeempt this wait
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }

            }
#endif
#if PROFILE_EXEC_TIMING
        if (sync_idlestarttime > 0){
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
        }
#endif

        // allow other ETs to proceed
#if WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC
        desired16 = 0;
            expected16 = g_thread_cnt;
            if(!work_queue.batch_map_comp_cnts[batch_slot].compare_exchange_strong(expected16, desired16,memory_order_acq_rel)){
                M_ASSERT_V(false, "ET_%ld: this should not happen, I am the only one who can reset batch_map_comp_cnts\n", _thd_id);
            };
#elif WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
        desired16 = 0;
            expected16 = g_thread_cnt;
            if(!work_queue.batch_map_comp_cnts[batch_slot].compare_exchange_strong(expected16, desired16,memory_order_acq_rel)){
                M_ASSERT_V(false, "ET_%ld: this should not happen, I am the only one who can reset batch_map_comp_cnts\n", _thd_id);
            };
#elif WT_SYNC_METHOD == SYNC_BLOCK
//            DEBUG_Q("WT_%ld: exec_stage - going to SET next_stage WT_*, batch_id = %ld\n", _thd_id,wbatch_id);
//            for (uint32_t i=0; i < g_thread_cnt; ++i){
//                work_queue.exec_sblocks[batch_slot][i].next_stage = 1;
//                atomic_thread_fence(memory_order_release);
//            }
#if NEXT_STAGE_ARRAY
        for (UInt32 j = 0; j < g_thread_cnt; ++j) {
            *(work_queue.exec_next_stage[batch_slot][j]) = 1;
        }
#else
        *(work_queue.exec_next_stage[batch_slot]) = 1;
#endif
        // need to wait for all threads to exit sync
        work_queue.exec_sblocks[batch_slot][_thd_id].done = 0;
        atomic_thread_fence(memory_order_release);

        while (true){
            done_cnt = 0;
            atomic_thread_fence(memory_order_acquire);
            for (uint32_t i=0; i < g_thread_cnt; ++i){
                done_cnt += work_queue.exec_sblocks[batch_slot][i].done;
            }
            if (done_cnt == 0){
                break;
            }
#if PROFILE_EXEC_TIMING
            if (sync_idlestarttime ==0){
                sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: commit_stage waiting for WT_* to SET done, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
            //TQ: no need to preeempt this wait
            if (simulation->is_done()){
#if PROFILE_EXEC_TIMING
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
#endif
                return BREAK;
            }
        }
#if PROFILE_EXEC_TIMING
        if (sync_idlestarttime > 0){
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
        }
#endif
#if NEXT_STAGE_ARRAY
        for (UInt32 j = 0; j < g_thread_cnt; ++j) {
            * (work_queue.exec_next_stage[batch_slot][j]) = 0;
        }
#else
        *(work_queue.exec_next_stage[batch_slot]) = 0;
#endif
        atomic_thread_fence(memory_order_release);

#elif WT_SYNC_METHOD == CAS_GLOBAL_SC
        expected16 = 0;
            desired16 = 1;
            // NOTE: starting form 1, skip myself
            for (uint32_t i=1; i < g_thread_cnt; ++i){
                if (!work_queue.batch_exec_sync_status[batch_slot][i].compare_exchange_strong(expected16,desired16)){
                    M_ASSERT_V(false, "WT_%ld: this should not happen, I am the only one who can SET batch_exec_sync_status\n", _thd_id);
                }
            }
#endif

    }
    else{
//            DEBUG_Q("ET_%ld: going to wait for ET_0 to finalize the execution phase for batch_id = %ld, map_comp_cnts = %d\n",
//                    _thd_id, wbatch_id, work_queue.batch_map_comp_cnts[batch_slot].load());

//                DEBUG_Q("ET_%ld: Done with my batch partition going to wait for others\n", _thd_id);
#if WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC
        while (work_queue.batch_map_comp_cnts[batch_slot].load(memory_order_acq_rel) != 0){
                if (idle_starttime == 0){
                    idle_starttime = get_sys_clock();
                }
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
                // SPINN Here untill all ETs are done and batch is committed
            }
#elif WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
        while (work_queue.batch_map_comp_cnts[batch_slot].load(memory_order_acq_rel) != 0){
                if (idle_starttime == 0){
                    idle_starttime = get_sys_clock();
                }
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
                // SPINN Here untill all ETs are done and batch is committed
            }
#elif WT_SYNC_METHOD == SYNC_BLOCK
        atomic_thread_fence(memory_order_acquire);
#if NEXT_STAGE_ARRAY
        while (*work_queue.exec_next_stage[batch_slot][_thd_id] != 1){
#else
            while (*work_queue.exec_next_stage[batch_slot] != 1){
#endif
#if PROFILE_EXEC_TIMING
            if (sync_idlestarttime ==0){
                sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: exec_stage waiting for WT_0 to SET next_stage, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
            if (simulation->is_done()){
#if PROFILE_EXEC_TIMING
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
#endif
                return BREAK;
            }
            atomic_thread_fence(memory_order_acquire);
        }
#if PROFILE_EXEC_TIMING
        if (sync_idlestarttime > 0){
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
        }
#endif

        work_queue.exec_sblocks[batch_slot][_thd_id].done = 0;
        atomic_thread_fence(memory_order_release);

        atomic_thread_fence(memory_order_acquire);
#if NEXT_STAGE_ARRAY
        while (*work_queue.exec_next_stage[batch_slot][_thd_id] != 0){
#else
            while (*work_queue.exec_next_stage[batch_slot] != 0){
#endif
#if PROFILE_EXEC_TIMING
            if (sync_idlestarttime ==0){
                sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: exec_stage waiting for WT_0 to SET next_stage, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
            if (simulation->is_done()){
#if PROFILE_EXEC_TIMING
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
#endif
                return BREAK;
            }
            atomic_thread_fence(memory_order_acquire);
        }


#elif WT_SYNC_METHOD == CAS_GLOBAL_SC
        while (work_queue.batch_exec_sync_status[batch_slot][_thd_id].load() != 1){
                if (idle_starttime ==0){
                    idle_starttime = get_sys_clock();
                }
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
                SAMPLED_DEBUG_Q("WT_%ld: waiting for WT_0 to SET batch_exec_sync_status, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
#if PROFILE_EXEC_TIMING
        if (sync_idlestarttime > 0){
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
        }
#endif
//                DEBUG_Q("ET_%ld: execution phase is done, starting commit phase for batch_id = %ld\n", _thd_id, wbatch_id);
//                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
//            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
//            idle_starttime =0;
    }

#if WT_SYNC_METHOD == CAS_GLOBAL_SC
    // Reset batch_exec_comp_status for next time since sync is done
        expected16 = 1;
        desired16 = 0;
        if (!work_queue.batch_exec_comp_status[batch_slot][_thd_id].compare_exchange_strong(expected16,desired16)){
            M_ASSERT_V(false, "WT_%ld: this should not happen, I am the only one who can RESET batch_exec_comp_status\n", _thd_id);
        }
        if (_thd_id != 0 && !work_queue.batch_exec_sync_status[batch_slot][_thd_id].compare_exchange_strong(expected16,desired16)){
            M_ASSERT_V(false, "WT_%ld: this should not happen, I am the only one who can RESET batch_exec_sync_status\n", _thd_id);
        }
#elif WT_SYNC_METHOD == SYNC_BLOCK
//        DEBUG_Q("WT_%ld: exec_stage - going to RESET both done, next_stage and moving to the next stage, batch_id = %ld, gbatch_id=%ld\n",
//                _thd_id,wbatch_id,work_queue.gbatch_id);
#endif
    return SUCCESS;
}

SRC WorkerThread::sync_on_commit_phase_end(uint64_t batch_slot){
#if WT_SYNC_METHOD == SYNC_BLOCK
    return sync_on_commit_phase_end_sync_block(batch_slot);
#elif WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
    return sync_on_commit_phase_end_atomic(batch_slot);
#else
    assert(false);
    return SUCCESS;
#endif
}

SRC WorkerThread::sync_on_commit_phase_end_atomic(uint64_t batch_slot){
    assert(WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL);
    assert(false);
    return SUCCESS;
}
SRC WorkerThread::sync_on_commit_phase_end_sync_block(uint64_t batch_slot){
    assert(WT_SYNC_METHOD == SYNC_BLOCK);
    assert(!SYNC_MASTER_RR);
    assert(NEXT_STAGE_ARRAY);

#if PROFILE_EXEC_TIMING
    uint64_t sync_idlestarttime =0;
#endif
    UInt32 done_cnt = 0;
    work_queue.commit_sblocks[batch_slot][_thd_id].done = 1;
    atomic_thread_fence(memory_order_release);

    bool is_master = (_thd_id == 0);
    if (is_master){
        while (true){
            done_cnt = 0;
            atomic_thread_fence(memory_order_acquire);
            for (uint32_t i=0; i < g_thread_cnt; ++i){
                done_cnt += work_queue.commit_sblocks[batch_slot][i].done;
            }
            if (done_cnt == g_thread_cnt){
                break;
            }
#if PROFILE_EXEC_TIMING
            if (sync_idlestarttime ==0){
                sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: commit_stage waiting for WT_* to SET done, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
            //TQ: no need to preeempt this wait
            if (simulation->is_done()){
#if PROFILE_EXEC_TIMING
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
#endif
                return BREAK;
            }
        }

#if PROFILE_EXEC_TIMING
        if (sync_idlestarttime > 0){
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
        }
#endif
        // cleanup remote txn context

        //TQ: At this point all the local threads are sync'ed and arrived at here
        for (uint32_t i = 0; i < g_node_cnt; ++i) {
            if (i != g_node_id) {
                DEBUG_Q("N_%u:WT_%lu: Sending RDONE msg for batch_id=%lu to node=%u\n",g_node_id,_thd_id,wbatch_id,i);
                Message * batch_done_msg = Message::create_message(RDONE);
                batch_done_msg->batch_id = wbatch_id;
                msg_queue.enqueue(_thd_id,batch_done_msg,i);
            }
        }

        DEBUG_Q("N_%u:WT_%lu: Waiting for other nodes to finish for batch_id=%lu\n",g_node_id,_thd_id,wbatch_id);

        while(!simulation->is_done() && quecc_pool.batch_deps[batch_slot].load(memory_order_acq_rel) > 0){}

        if (simulation->is_done()){
            return BREAK;
        }
        // we should have received messages from all nodes
        //reste batch_deps
        int32_t de = 0;
        if(!quecc_pool.batch_deps[batch_slot].compare_exchange_strong(de,(NODE_CNT-1),memory_order_acq_rel)){
            assert(false);
        }

        DEBUG_Q("N_%u:WT_%lu: all nodes are done for batch_id=%lu\n",g_node_id,_thd_id,wbatch_id);

        // it is safe now since we are at the end of the batch
#if WORKLOAD == TPCC
        for (uint64_t i = 0; i < g_cluster_worker_thread_cnt; ++i) {
            if (g_node_id != QueCCPool::get_plan_node(i)){
                transaction_context * pg_tctxs = work_queue.batch_pg_map[batch_slot][i].txn_ctxs;
                for (uint64_t j = 0; j < planner_batch_size; ++j) {
//                    DEBUG_Q("N_%u:WT_%lu: RESET O_ID for batch_map[%lu][%lu], txn_idx=%lu\n",
//                            g_node_id,_worker_cluster_wide_id,wbatch_id,i,j);
                    pg_tctxs[j].o_id.store(-1,memory_order_acq_rel);
                }
            }
        }
#endif

        for (UInt32 j = 0; j < g_thread_cnt; ++j) {
            *(work_queue.commit_next_stage[batch_slot][j]) = 1;
        }
        atomic_thread_fence(memory_order_release);

        // need to wait for all threads to exit sync
        work_queue.commit_sblocks[batch_slot][_thd_id].done = 0;

        while (true){
            done_cnt = 0;
            atomic_thread_fence(memory_order_acquire);
            for (uint32_t i=0; i < g_thread_cnt; ++i){
                done_cnt += work_queue.commit_sblocks[batch_slot][i].done;
            }
            if (done_cnt == 0){
                break;
            }
#if PROFILE_EXEC_TIMING
            if (sync_idlestarttime ==0){
                sync_idlestarttime = get_sys_clock();
                DEBUG_Q("WT_%ld: commit_stage waiting for WT_* to SET done, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
            //TQ: no need to preeempt this wait
            if (simulation->is_done()){
#if PROFILE_EXEC_TIMING
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
#endif
                DEBUG_Q("N_%u:WT_%lu: SIM_DONE waiting in commit sync for batch_id=%lu\n",g_node_id,_thd_id,wbatch_id);
                return BREAK;
            }
        }

        DEBUG_Q("N_%u:WT_%lu: all local threads are done for batch_id=%lu\n",g_node_id,_thd_id,wbatch_id);
#if PROFILE_EXEC_TIMING
        if (sync_idlestarttime > 0){
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
        }
#endif
        for (UInt32 j = 0; j < g_thread_cnt; ++j) {
            *(work_queue.commit_next_stage[batch_slot][j]) = 0;
        }
        atomic_thread_fence(memory_order_release);
    }
    else{
        DEBUG_Q("N_%d:ET_%ld: going to wait for ET_0 to finalize the commit phase\n",g_node_id, _thd_id);


        atomic_thread_fence(memory_order_acquire);
        while (*work_queue.commit_next_stage[batch_slot][_thd_id] != 1){
#if PROFILE_EXEC_TIMING
            if (sync_idlestarttime ==0){
                sync_idlestarttime = get_sys_clock();
                    DEBUG_Q("WT_%ld: commit stage waiting for WT_0 to SET next_stage, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
            if (simulation->is_done()){
#if PROFILE_EXEC_TIMING
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
#endif
                return BREAK;
            }
        }
#if PROFILE_EXEC_TIMING
        if (sync_idlestarttime > 0){
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
        }
#endif
        work_queue.commit_sblocks[batch_slot][_thd_id].done = 0;
        atomic_thread_fence(memory_order_release);
        atomic_thread_fence(memory_order_acquire);
        while (*work_queue.commit_next_stage[batch_slot][_thd_id] != 0){
#if PROFILE_EXEC_TIMING
            if (sync_idlestarttime ==0){
                sync_idlestarttime = get_sys_clock();
                    DEBUG_Q("WT_%ld: commit stage waiting for WT_0 to SET next_stage, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
            if (simulation->is_done()){
#if PROFILE_EXEC_TIMING
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
#endif
                return BREAK;
            }
            atomic_thread_fence(memory_order_acquire);
        }
#if PROFILE_EXEC_TIMING
        if (sync_idlestarttime > 0){
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
        }
#endif
    }
    DEBUG_Q("N_%u:ET_%ld: commit sync is done for batch_slot=%ld, going to work on the next batch\n", g_node_id, _worker_cluster_wide_id,batch_slot);
    return SUCCESS;
}

// TODO(tq): remove this _mix function
/*
SRC WorkerThread::sync_on_commit_phase_end_mix(uint64_t batch_slot){
#if PROFILE_EXEC_TIMING
    uint64_t sync_idlestarttime =0;
#endif
#if WT_SYNC_METHOD == CAS_GLOBAL_SC ||  WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC ||  WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
    //        uint8_t desired8;
//        uint8_t expected8;
        uint16_t desired16;
        uint16_t expected16;
#endif
#if WT_SYNC_METHOD == CAS_GLOBAL_SC || WT_SYNC_METHOD == SYNC_BLOCK
    UInt32 done_cnt = 0;
#endif
#if WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC
    //        expected16 = work_queue.batch_commit_et_cnts[batch_slot].fetch_add(1);
        do{
            expected16 = work_queue.batch_commit_et_cnts[batch_slot].load(memory_order_acq_rel);
            desired16 = expected16 + 1;
        }while (!work_queue.batch_commit_et_cnts[batch_slot].compare_exchange_strong(expected16,desired16,memory_order_acq_rel));
#elif WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
    expected16 = work_queue.batch_commit_et_cnts[batch_slot].fetch_add(1,memory_order_acq_rel);
#elif WT_SYNC_METHOD == SYNC_BLOCK
    work_queue.commit_sblocks[batch_slot][_thd_id].done = 1;
    atomic_thread_fence(memory_order_release);
#elif WT_SYNC_METHOD == CAS_GLOBAL_SC
    expected16 = 0;
        desired16 = 1;
        if (!work_queue.batch_commit_comp_status[batch_slot][_thd_id].compare_exchange_strong(expected16,desired16)){
            M_ASSERT_V(false, "WT_%ld: this should not happen, I am the only one who can SET batch_commit_comp_status\n", _thd_id);
        }
#endif
#if SYNC_MASTER_RR
    bool is_master = ((wbatch_id % g_thread_cnt) == _thd_id);
#else
    bool is_master = (_thd_id == 0);
#endif
    if (is_master){
        // wait for others to finish

        // wait for all ets to finish
//                DEBUG_Q("ET_%ld: going to wait for other ETs to finish their commit for all PGs\n", _thd_id);
#if WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC
        expected16 = work_queue.batch_commit_et_cnts[batch_slot].load(memory_order_acq_rel);
            while (expected16 != g_thread_cnt){
                if (idle_starttime ==0){
                    idle_starttime = get_sys_clock();
                }
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
                expected16 = work_queue.batch_commit_et_cnts[batch_slot].load(memory_order_acq_rel);
            };
#elif WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
        expected16 = work_queue.batch_commit_et_cnts[batch_slot].load(memory_order_acq_rel);
            while (expected16 != g_thread_cnt){
                if (idle_starttime ==0){
                    idle_starttime = get_sys_clock();
                }
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
                expected16 = work_queue.batch_commit_et_cnts[batch_slot].load(memory_order_acq_rel);
            };
#elif WT_SYNC_METHOD == SYNC_BLOCK
        while (true){
            done_cnt = 0;
            atomic_thread_fence(memory_order_acquire);
            for (uint32_t i=0; i < g_thread_cnt; ++i){
                done_cnt += work_queue.commit_sblocks[batch_slot][i].done;
            }
            if (done_cnt == g_thread_cnt){
                break;
            }
#if PROFILE_EXEC_TIMING
            if (sync_idlestarttime ==0){
                sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: commit_stage waiting for WT_* to SET done, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
            //TQ: no need to preeempt this wait
            if (simulation->is_done()){
#if PROFILE_EXEC_TIMING
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
#endif
                return BREAK;
            }
        }
//            DEBUG_Q("WT_%ld: commit_stage all WT_* are done, done_cnt = %d, batch_id = %ld\n", _thd_id, done_cnt,wbatch_id);
#elif WT_SYNC_METHOD == CAS_GLOBAL_SC
        while (true){
                done_cnt = 0;
                for (uint32_t i=0; i < g_thread_cnt; ++i){
                    done_cnt += work_queue.batch_commit_comp_status[batch_slot][i].load();
                }
                if (done_cnt == g_thread_cnt){
                    break;
                }
                if (idle_starttime ==0){
                    idle_starttime = get_sys_clock();
                }
                //TQ: no need to preeempt this wait
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
            }
#endif
#if PROFILE_EXEC_TIMING
        if (sync_idlestarttime > 0){
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
        }
#endif
//                DEBUG_Q("ET_%ld: all other ETs has finished their commit\n", _thd_id);

        // cleanup remote txn context

        //TQ: At this point all the local threads are sync'ed and arrived at here
        for (uint32_t i = 0; i < g_node_cnt; ++i) {
            if (i != g_node_id) {
                DEBUG_Q("N_%u:WT_%lu: Sending RDONE msg for batch_id=%lu to node=%u\n",g_node_id,_thd_id,wbatch_id,i);
                Message * batch_done_msg = Message::create_message(RDONE);
                batch_done_msg->batch_id = wbatch_id;
                msg_queue.enqueue(_thd_id,batch_done_msg,i);
            }
        }

        DEBUG_Q("N_%u:WT_%lu: Waiting for other nodes to finish for batch_id=%lu\n",g_node_id,_thd_id,wbatch_id);

        while(!simulation->is_done() && quecc_pool.batch_deps[batch_slot].load(memory_order_acq_rel) > 0){}

        if (simulation->is_done()){
            return BREAK;
        }
        // we should have received messages from all nodes
        //reste batch_deps
        int32_t de = 0;
        if(!quecc_pool.batch_deps[batch_slot].compare_exchange_strong(de,(NODE_CNT-1),memory_order_acq_rel)){
            assert(false);
        }

        DEBUG_Q("N_%u:WT_%lu: all nodes are done for batch_id=%lu\n",g_node_id,_thd_id,wbatch_id);
        //TODO(tq): remove this later
#if SYNC_MASTER_BATCH_CLEANUP
        // cleanup my batch part and allow planners waiting on me to
            for (uint64_t i = 0; i < g_plan_thread_cnt; ++i){
                for (uint64_t j = 0; j < g_thread_cnt; ++j) {
                    cleanup_batch_part(batch_slot, i,j);
                }
#if PIPELINEDL
                priority_group * planner_pg = &work_queue.batch_pg_map[batch_slot][i];
#if EXEC_BUILD_TXN_DEPS
                for (int j = 0; j < THREAD_CNT; ++j) {
                    hash_table_tctx_t * tdg = planner_pg->exec_tdg[j];
                    for (auto it = tdg->begin(); it != tdg->end(); ++it){
                        Array<transaction_context *> * tmp = it->second;
                        quecc_pool.txn_ctx_list_release(tmp, j);
                    }
                    tdg->clear();
                }
#endif
                // Reset PG map so that planners can continue
                uint8_t desired8 = PG_AVAILABLE;
                uint8_t expected8 = PG_READY;
                if(!planner_pg->status.compare_exchange_strong(expected8, desired8)){
                    M_ASSERT_V(false, "Reset failed for PG map, this should not happen\n");
                };

#endif
            }
#endif // -- #if SYNC_MASTER_BATCH_CLEANUP

        // it is safe now since we are at the end of the batch
#if WORKLOAD == TPCC
        for (uint64_t i = 0; i < g_cluster_worker_thread_cnt; ++i) {
            if (g_node_id != QueCCPool::get_plan_node(i)){
                transaction_context * pg_tctxs = work_queue.batch_pg_map[batch_slot][i].txn_ctxs;
                for (uint64_t j = 0; j < planner_batch_size; ++j) {
//                    DEBUG_Q("N_%u:WT_%lu: RESET O_ID for batch_map[%lu][%lu], txn_idx=%lu\n",
//                            g_node_id,_worker_cluster_wide_id,wbatch_id,i,j);
                    pg_tctxs[j].o_id.store(-1,memory_order_acq_rel);
                }
            }
        }
#endif

        // allow other ETs to proceed
#if WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC
        desired16 = 0;
            expected16 = g_thread_cnt;
            if(!work_queue.batch_commit_et_cnts[batch_slot].compare_exchange_strong(expected16, desired16, memory_order_acq_rel)){
                M_ASSERT_V(false, "ET_%ld: this should not happen, I am the only one who can reset commit_et_cnt\n", _thd_id);
            };
#elif WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
        desired16 = 0;
            expected16 = g_thread_cnt;
            if(!work_queue.batch_commit_et_cnts[batch_slot].compare_exchange_strong(expected16, desired16,memory_order_acq_rel)){
                M_ASSERT_V(false, "ET_%ld: this should not happen, I am the only one who can reset commit_et_cnt\n", _thd_id);
            };
#elif WT_SYNC_METHOD == SYNC_BLOCK
//            DEBUG_Q("WT_%ld: commit_stage going to SET next_stage for WT_*, batch_id = %ld\n", _thd_id,wbatch_id);
//            for (uint32_t i=0; i < g_thread_cnt; ++i){
//                work_queue.commit_sblocks[batch_slot][i].next_stage = 1;
//            }
#if NEXT_STAGE_ARRAY
        for (UInt32 j = 0; j < g_thread_cnt; ++j) {
            *(work_queue.commit_next_stage[batch_slot][j]) = 1;
        }
#else
        *(work_queue.commit_next_stage[batch_slot]) = 1;
#endif
        atomic_thread_fence(memory_order_release);

        // need to wait for all threads to exit sync
        work_queue.commit_sblocks[batch_slot][_thd_id].done = 0;
        while (true){
            done_cnt = 0;
            atomic_thread_fence(memory_order_acquire);
            for (uint32_t i=0; i < g_thread_cnt; ++i){
                done_cnt += work_queue.commit_sblocks[batch_slot][i].done;
            }
            if (done_cnt == 0){
                break;
            }
#if PROFILE_EXEC_TIMING
            if (sync_idlestarttime ==0){
                sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: commit_stage waiting for WT_* to SET done, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
            //TQ: no need to preeempt this wait
            if (simulation->is_done()){
#if PROFILE_EXEC_TIMING
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
#endif
                DEBUG_Q("N_%u:WT_%lu: SIM_DONE waiting in commit sync for batch_id=%lu\n",g_node_id,_thd_id,wbatch_id);
                return BREAK;
            }
        }

        DEBUG_Q("N_%u:WT_%lu: all local threads are done for batch_id=%lu\n",g_node_id,_thd_id,wbatch_id);
#if PROFILE_EXEC_TIMING
        if (sync_idlestarttime > 0){
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
        }
#endif
#if NEXT_STAGE_ARRAY
        for (UInt32 j = 0; j < g_thread_cnt; ++j) {
            *(work_queue.commit_next_stage[batch_slot][j]) = 0;
        }
#else
        *(work_queue.commit_next_stage[batch_slot]) = 0;
#endif
        atomic_thread_fence(memory_order_release);

#elif WT_SYNC_METHOD == CAS_GLOBAL_SC
        expected16 = 0;
            desired16 = 1;
            // NOTE: starting form 1, skip myself
            for (uint32_t i=1; i < g_thread_cnt; ++i){
                if (!work_queue.batch_commit_sync_status[batch_slot][i].compare_exchange_strong(expected16,desired16)){
                    M_ASSERT_V(false, "WT_%ld: this should not happen, I am the only one who can SET batch_commit_sync_status\n", _thd_id);
                }
            }
#endif
    }
    else{
                DEBUG_Q("N_%d:ET_%ld: going to wait for ET_0 to finalize the commit phase\n",g_node_id, _thd_id);
//                DEBUG_Q("ET_%ld: Done with my batch partition going to wait for others\n", _thd_id);
#if WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC
        while (work_queue.batch_commit_et_cnts[batch_slot].load(memory_order_acq_rel) != 0){
                if (idle_starttime == 0){
                    idle_starttime = get_sys_clock();
                }
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
                // SPINN Here untill all ETs are done and batch is committed
            }
#elif WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
        while (work_queue.batch_commit_et_cnts[batch_slot].load(memory_order_acq_rel) != 0){
                if (idle_starttime == 0){
                    idle_starttime = get_sys_clock();
                }
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
                // SPINN Here untill all ETs are done and batch is committed
            }
#elif WT_SYNC_METHOD == SYNC_BLOCK
        atomic_thread_fence(memory_order_acquire);
#if NEXT_STAGE_ARRAY
        while (*work_queue.commit_next_stage[batch_slot][_thd_id] != 1){
#else
            assert(false);
            while (*work_queue.commit_next_stage[batch_slot] != 1){

        }
#endif
#if PROFILE_EXEC_TIMING
            if (sync_idlestarttime ==0){
                sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: commit stage waiting for WT_0 to SET next_stage, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
            if (simulation->is_done()){
#if PROFILE_EXEC_TIMING
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
#endif
                return BREAK;
            }
        }
#if PROFILE_EXEC_TIMING
        if (sync_idlestarttime > 0){
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
        }
#endif
        work_queue.commit_sblocks[batch_slot][_thd_id].done = 0;
        atomic_thread_fence(memory_order_release);

        atomic_thread_fence(memory_order_acquire);
#if NEXT_STAGE_ARRAY
        while (*work_queue.commit_next_stage[batch_slot][_thd_id] != 0){
#else
            while (*work_queue.commit_next_stage[batch_slot] != 0){
#endif
#if PROFILE_EXEC_TIMING
            if (sync_idlestarttime ==0){
                sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: commit stage waiting for WT_0 to SET next_stage, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
            if (simulation->is_done()){
#if PROFILE_EXEC_TIMING
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
#endif
                return BREAK;
            }
            atomic_thread_fence(memory_order_acquire);
        }
#elif WT_SYNC_METHOD == CAS_GLOBAL_SC
        while (work_queue.batch_commit_sync_status[batch_slot][_thd_id].load() != 1){
                if (idle_starttime ==0){
                    idle_starttime = get_sys_clock();
                }
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
                SAMPLED_DEBUG_Q("WT_%ld: waiting for WT_0 to SET batch_commit_sync_status, batch_id = %ld\n", _thd_id,wbatch_id);
            }
#endif
#if PROFILE_EXEC_TIMING
        if (sync_idlestarttime > 0){
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
        }
#endif
//                DEBUG_Q("ET_%ld: commit phase is done for batch_slot=%ld, going to work on the next batch\n", _thd_id,batch_slot);
//                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
//            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
//            idle_starttime =0;
    }
#if WT_SYNC_METHOD == CAS_GLOBAL_SC
    // Reset batch_exec_comp_status for next time since sync is done
        expected16 = 1;
        desired16 = 0;
        if (!work_queue.batch_commit_comp_status[batch_slot][_thd_id].compare_exchange_strong(expected16,desired16)){
            M_ASSERT_V(false, "WT_%ld: this should not happen, I am the only one who can RESET batch_commit_comp_status\n", _thd_id);
        }
        if (_thd_id != 0 && !work_queue.batch_commit_sync_status[batch_slot][_thd_id].compare_exchange_strong(expected16,desired16)){
            M_ASSERT_V(false, "WT_%ld: this should not happen, I am the only one who can RESET batch_commit_sync_status\n", _thd_id);
        }
#elif WT_SYNC_METHOD == SYNC_BLOCK
//        DEBUG_Q("WT_%ld: commit_stage RESET both done and next_stage, batch_id = %ld, gbatch_id = %ld\n",
//                _thd_id,wbatch_id, work_queue.gbatch_id);
#endif
    DEBUG_Q("N_%u:ET_%ld: commit sync is done for batch_slot=%ld, going to work on the next batch\n", g_node_id, _worker_cluster_wide_id,batch_slot);
    return SUCCESS;
}
*/
SRC WorkerThread::batch_cleanup(uint64_t batch_slot){

#if PIPELINED2
    if (_planner_id < g_plan_thread_cnt){
        return SUCCESS;
    }
#endif
    //TQ: we need a sync after this cleanup
#if !SYNC_MASTER_BATCH_CLEANUP
// cleanup my batch part and allow planners waiting on me to
    for (uint64_t i = 0; i < g_cluster_planner_thread_cnt; ++i){
#if SPT_WORKLOAD
        if (QueCCPool::get_plan_node(i) != g_node_id){
            continue;
        }
#endif
        cleanup_batch_part(batch_slot, i);
    }
#endif // -- #if SYNC_MASTER_BATCH_CLEANUP

    return SUCCESS;
}

SRC WorkerThread::batch_cleanup_et_oriented(uint64_t batch_slot){

    //TQ: we need a sync after this cleanup
#if !SYNC_MASTER_BATCH_CLEANUP
// cleanup my batch part and allow planners waiting on me to
    for (uint64_t i = 0; i < g_cluster_planner_thread_cnt; ++i){
        cleanup_batch_part(batch_slot, i, _worker_cluster_wide_id);
    }
#endif // -- #if SYNC_MASTER_BATCH_CLEANUP

    return SUCCESS;
}

SRC WorkerThread::batch_cleanup_pt_oriented(uint64_t batch_slot){

    //TQ: we need a sync after this cleanup
#if !SYNC_MASTER_BATCH_CLEANUP
// cleanup my batch part and allow planners waiting on me to
    for (uint64_t i = 0; i < g_cluster_worker_thread_cnt; ++i){
        cleanup_batch_part(batch_slot, _planner_id, i );
    }
#endif // -- #if SYNC_MASTER_BATCH_CLEANUP

    return SUCCESS;
}

SRC WorkerThread::execute_batch(uint64_t batch_slot, uint64_t * eq_comp_cnts, TxnManager * my_txn_man) {

#if PIPELINED2
    if (_planner_id < g_plan_thread_cnt){
        // this is a planner thread and should not perform executionm in pipelined version
        return SUCCESS;
    }
#endif
    wplanner_id = 0;

    while (true){

#if PIPELINED
        batch_partition * batch_part = NULL;
        // wait for batch to be ready
        if (wait_for_batch_ready(batch_slot, wplanner_id, batch_part) == BATCH_WAIT){
            if (simulation->is_done()){
#if PROFILE_EXEC_TIMING
                if(idle_starttime > 0) {
                    INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime = 0;
                }
#endif
                return BREAK;
            }
            continue;
        }

#if DEBUG_QUECC
        uint64_t total_eq_entries = 0;
//        for (uint64_t i=0; i < g_plan_thread_cnt; ++i){
//            batch_part = (batch_partition *)  work_queue.batch_map[batch_slot][i][_thd_id].load();
//        assert(batch_part->single_q);
            if (batch_part->empty){
                DEBUG_Q("ET_%ld: batch_part is empty for batch_id = %lu\n", _thd_id, wbatch_id);
            }
            else{
                if (batch_part->single_q){
                    total_eq_entries+= batch_part->exec_q->size();
                }
                else{
                    for (uint64_t j = 0; j < batch_part->exec_qs->size(); ++j) {
                        total_eq_entries += batch_part->exec_qs->get(j)->size();
                    }
                }
            }

//        DEBUG_Q("ET_%ld: exec_batch - total eq entries from PL_%ld = %ld\n", _thd_id,i, batch_part->exec_q->size());
//        }
        DEBUG_Q("ET_%ld: exec_batch_id = %ld, PG=%lu - total eq entries = %ld\n", _thd_id, wbatch_id, wplanner_id,total_eq_entries);
#endif

//        #if BATCH_MAP_ORDER == BATCH_ET_PT
//        batch_part = (batch_partition *)  work_queue.batch_map[batch_slot][_thd_id][wplanner_id].load();
//#else
//        batch_part = (batch_partition *)  work_queue.batch_map[batch_slot][wplanner_id][_thd_id].load();
//#endif
//
//        if ((uint64_t) batch_part == 0){
//            if (idle_starttime == 0){
//                idle_starttime = get_sys_clock();
//            }
////            SAMPLED_DEBUG_Q("ET_%ld: Got nothing from planning layer, wbatch_id = %ld,"
////                            "\n",
////                    _thd_id,  wbatch_id);
//            continue;
//        }
//
//        // collect some stats
////        quecc_batch_part_proc_starttime = get_sys_clock();
#if PROFILE_EXEC_TIMING
        if(idle_starttime > 0) {
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
            idle_starttime = 0;
        }
#endif

#endif
//        DEBUG_Q("N_%u:ET_%lu: got a PG from planner %lu, batch_slot = %lu\n",g_node_id,_thd_id, wplanner_id, batch_slot);
#if SPT_WORKLOAD
        if (QueCCPool::get_plan_node(wplanner_id) != g_node_id){
            goto skip_exec;
        }

        if (execute_batch_part(batch_slot, eq_comp_cnts, my_txn_man) == BREAK){
            return BREAK;
        }
        skip_exec:

#else
        if (execute_batch_part(batch_slot, eq_comp_cnts, my_txn_man) == BREAK){
            return BREAK;
        }
#endif

#if SYNC_AFTER_PG
        // sync on PG done
#if PROFILE_EXEC_TIMING
        uint64_t hl_prof_starttime = get_sys_clock();
#endif

        if (pg_sync(batch_slot, wplanner_id) == BREAK){
#if PROFILE_EXEC_TIMING
            INC_STATS(_thd_id, wt_pg_sync_exec_time[_thd_id], get_sys_clock()-hl_prof_starttime);
#endif
            return BREAK;
        }
#if PROFILE_EXEC_TIMING
        INC_STATS(_thd_id, wt_pg_sync_exec_time[_thd_id], get_sys_clock()-hl_prof_starttime);
#endif
#endif
//        DEBUG_Q("N_%u:ET_%lu: finished PG from planner %lu, batch_slot = %lu, batch_id=%lu\n",
//                g_node_id,_thd_id, wplanner_id, batch_slot,wbatch_id);

        // go to the next batch partition prepared by the next planner since the previous one has been committed
        wplanner_id++;
//        INC_STATS(_thd_id, exec_batch_part_proc_time[_thd_id], get_sys_clock()-quecc_batch_part_proc_starttime);
//        quecc_batch_part_proc_starttime = 0;
        INC_STATS(_thd_id, exec_batch_part_cnt[_thd_id], 1);

//        uint64_t pg_cnt = g_cluster_worker_thread_cnt;
        uint64_t pg_cnt = g_plan_thread_cnt*g_node_cnt;
        if (wplanner_id == (pg_cnt)){
//            INC_STATS(_thd_id, exec_batch_part_proc_time[_thd_id], get_sys_clock()-quecc_batch_proc_starttime);
            INC_STATS(_thd_id, exec_batch_cnt[_thd_id], 1);
            return SUCCESS;
        }
    }
    M_ASSERT_V(false, "this should not happend\n");
    return FATAL_ERROR;
}

SRC WorkerThread::wait_for_batch_ready(uint64_t batch_slot, uint64_t wplanner_id, batch_partition *& batch_part) {

    batch_part = get_batch_part(batch_slot, wplanner_id);
#if ATOMIC_PG_STATUS
    if (((uint64_t) batch_part) == 0){
#if PROFILE_EXEC_TIMING
        if (idle_starttime == 0){
            idle_starttime = get_sys_clock();
//            DEBUG_Q("ET_%ld: Got nothing from planning layer, wbatch_id = %ld,"
//                            "\n",
//                    _thd_id,  wbatch_id);
        }
#endif
        return BATCH_WAIT;
    }
#else
    /* TQ: unlike using batch partition which is more fine-grained in the sense that it allows ETs to proceed as soon as its
     * batch partition is ready.
     * With using the ready variable, we wait untill all batch partitions from the same PG are ready. It is like an
     * implicit sync, where all executors start together.
    */
    atomic_thread_fence(memory_order_acquire);
    if (work_queue.batch_pg_map[batch_slot][wplanner_id].ready != 1){
        if (idle_starttime == 0){
            idle_starttime = get_sys_clock();
        }
        return BATCH_WAIT;
    }
#endif

    return BATCH_READY;
}

SRC WorkerThread::commit_batch(uint64_t batch_slot){
    std::list<uint64_t> * pending_txns = new std::list<uint64_t>();
    priority_group * planner_pg = NULL;
    SRC src_res = SUCCESS;
#if PROFILE_EXEC_TIMING
    uint64_t quecc_commit_starttime = 0;
    uint64_t abort_time;
    uint64_t commit_time;
    uint64_t timespan_long;
#endif
    uint64_t txn_per_pg = quecc_pool.planner_batch_size;
    uint64_t txn_wl_per_et UNUSED = txn_per_pg / g_cluster_worker_thread_cnt;
    uint64_t txn_commit_seq UNUSED =0; // the transaction order in the batch
    uint64_t commit_et_id =0;
    uint64_t commit_cnt = 0;
    uint64_t abort_cnt = 0;

    RC rc;
#if FIXED_COMMIT_THREAD_CNT
    uint64_t commit_thread_cnt = COMMIT_THREAD_CNT;
#else
    uint64_t commit_thread_cnt = g_thread_cnt;
#endif


    for (uint64_t i=0; i < g_cluster_planner_thread_cnt;++i){
        // Skipp committing PGs that are not planned on this node
//         Should only skip if I am a passive node??
        // if no transactions abort, we just need to commit spectulative writes here
        //otherwise, we need to wait for abort

        if (QueCCPool::get_plan_node(i) != g_node_id) continue;

        planner_pg = &work_queue.batch_pg_map[batch_slot][i];

        if(_thd_id == 0){
            DEBUG_Q("N_%u:WT_%lu: going to commit for PG[%lu], pg_index=%lu, batch_id=%lu, eq_comp_rem_cnt=%lu\n",
                    g_node_id, _worker_cluster_wide_id, QueCCPool::map_to_cwplanner_id(i),i,wbatch_id,planner_pg->eq_completed_rem_cnt.load());
        }
        // block here untill all Eqs are processed for PG
        while(planner_pg->eq_completed_rem_cnt.load() != 0){
            if (simulation->is_done()){
                DEBUG_Q("N_%u:WT_%lu: Waiting to commit for PG[%lu], pg_index=%lu, batch_id=%lu, eq_comp_rem_cnt=%lu\n",
                        g_node_id, _worker_cluster_wide_id, QueCCPool::map_to_cwplanner_id(i),i,wbatch_id,planner_pg->eq_completed_rem_cnt.load());
                return BREAK;
            }
        };

        abort_cnt = 0;
        commit_cnt = 0;
#if PROFILE_EXEC_TIMING
        quecc_commit_starttime = get_sys_clock();
#endif
        // committing a PG
        for (uint64_t j = 0; j < txn_per_pg; ++j){

//#if BATCH_SIZE >= TXN_CNT_COMMIT_THRESHOLD
//            txn_commit_seq = (i*txn_per_pg) + j;
//            commit_et_id = (txn_commit_seq / txn_wl_per_et) % commit_thread_cnt;
            commit_et_id = j % commit_thread_cnt; // Using RR to assign txns to CTs
//#else
//            commit_et_id = 0; // single threaded commit
//#endif
            if (_thd_id < commit_thread_cnt && commit_et_id == _thd_id){
                // I should be committing this transaction
//                DEBUG_Q("N_%u:ET_%ld:CWID_%lu: is trying to commit txn_id = %ld, batch_id = %ld, txn_per_pg=%lu, txn_ctx_ptr=%lu\n",
//                        g_node_id,_thd_id,_worker_cluster_wide_id, planner_pg->txn_ctxs[j].txn_id, wbatch_id,txn_per_pg, (uint64_t)&planner_pg->txn_ctxs[j]);
//                rc = commit_txn(planner_pg, j);
                rc = commit_txn(&planner_pg->txn_ctxs[j]);
                if (rc == Commit){
//                    finalize_txn_commit(&planner_pg->txn_ctxs[j],rc);
#if PROFILE_EXEC_TIMING
                    commit_time = get_sys_clock();
                    timespan_long = commit_time - planner_pg->txn_ctxs[j].starttime;
                    // Committing
                    INC_STATS_ARR(_thd_id, first_start_commit_latency, timespan_long);
#endif
                    // Sending response to client a
#if !SERVER_GENERATE_QUERIES && !EARLY_CL_RESPONSE
                    Message * rsp_msg = Message::create_message(CL_RSP);
                                rsp_msg->txn_id = planner_pg->txn_ctxs[j].txn_id;
                                rsp_msg->batch_id = wbatch_id; // using batch_id from local, we can also use the one in the context
                                ((ClientResponseMessage *) rsp_msg)->client_startts = planner_pg->txn_ctxs[j].client_startts;
                                ((ClientResponseMessage *) rsp_msg)->rc = rc;
                                rsp_msg->lat_work_queue_time = 0;
                                rsp_msg->lat_msg_queue_time = 0;
                                rsp_msg->lat_cc_block_time = 0;
                                rsp_msg->lat_cc_time = 0;
                                rsp_msg->lat_process_time = 0;
                                rsp_msg->lat_network_time = 0;
                                rsp_msg->lat_other_time = 0;

                                msg_queue.enqueue(_thd_id, rsp_msg, planner_pg->txn_ctxs[j].return_node_id);
#endif
                    wt_release_accesses(&planner_pg->txn_ctxs[j],rc);
//                    e8 = TXN_READY_TO_COMMIT;
//                    d8 = TXN_COMMITTED;
//                    if (!planner_pg->txn_ctxs[j].txn_state.compare_exchange_strong(e8,d8,memory_order_acq_rel)){
//                        M_ASSERT_V(false, "ET_%ld: trying to commit a transaction with invalid status\n", _thd_id);
//                    }

//                    DEBUG_Q("ET_%ld: committed transaction txn_id = %ld, batch_id = %ld, txn_per_pg=%lu\n",
//                            _thd_id, planner_pg->txn_ctxs[j].txn_id, wbatch_id,txn_per_pg);
                    commit_cnt++;
                }
                else if (rc == WAIT){
//                    DEBUG_Q("ET_%ld: WAIT transaction txn_id = %ld, batch_id = %ld, txn_per_pg=%lu\n",
//                            _thd_id, planner_pg->txn_ctxs[j].txn_id, wbatch_id,txn_per_pg);
                    pending_txns->push_back(j);
                }
                else{
                    assert(rc == Abort);
                    assert(false); // not supported for now
                    abort_cnt++;
#if PROFILE_EXEC_TIMING
                    abort_time = get_sys_clock();
                    timespan_long = abort_time - planner_pg->txn_ctxs[j].starttime;
                    INC_STATS_ARR(_thd_id, start_abort_commit_latency, timespan_long);
#endif
                    // need to do cascading abort
                    // copying back original value is done during release

//                    if (planner_pg->txn_ctxs[j].txn_state.load(memory_order_acq_rel) == TXN_READY_TO_COMMIT){
//                        wt_release_accesses(&planner_pg->txn_ctxs[j], rc);
//                        e8 = TXN_READY_TO_COMMIT;
//                        d8 = TXN_ABORTED;
//                        if (!planner_pg->txn_ctxs[j].txn_state.compare_exchange_strong(e8,d8,memory_order_acq_rel)){
//                            M_ASSERT_V(false, "ET_%ld: trying to abort a transaction with invalid status\n", _thd_id);
//                        }
//                    }
//                    else{
//                        assert(planner_pg->txn_ctxs[j].txn_state.load(memory_order_acq_rel) == TXN_READY_TO_ABORT);
//                        wt_release_accesses(&planner_pg->txn_ctxs[j], rc);
//
//                        e8 = TXN_READY_TO_ABORT;
//                        d8 = TXN_ABORTED;
//                        if (!planner_pg->txn_ctxs[j].txn_state.compare_exchange_strong(e8,d8,memory_order_acq_rel)){
//                            M_ASSERT_V(false, "ET_%ld: trying to abort a transaction with invalid status\n", _thd_id);
//                        }
//                    }
                }
#if EXEC_BUILD_TXN_DEPS
                if (rc != WAIT){
                    // transaction j has committed. We need to decrement CommitDepCnt for dependent transactions
                    transaction_context * j_ctx = &planner_pg->txn_ctxs[j];
                    for (auto it = j_ctx->commit_deps->begin(); it != j_ctx->commit_deps->end(); it++ ){
                        transaction_context * d_ctx = (*it);
                        int64_t tmp;
                        int64_t tmp2;
                        do {
                            tmp = d_ctx->commit_dep_cnt.load(memory_order_acq_rel);
                            tmp2 = tmp -1;
//                            M_ASSERT_V(tmp2 >=0, "CT_%ld: committing txn_id=%ld, updating dep from %ld counter to %ld, for txn_id=%ld, commit_deps size=%lu\n",
//                                       _thd_id, j_ctx->txn_id, tmp, tmp2,d_ctx->txn_id, j_ctx->commit_deps->size());
                        }while(!d_ctx->commit_dep_cnt.compare_exchange_strong(tmp, tmp2, memory_order_acq_rel));

                        if (rc == Abort){
                            d_ctx->should_abort = true;
                        }
                    }

                }
#endif
            }

        }

        // check for transactions that requires the result status of their dependents transactions
        // and are pending
        while(!pending_txns->empty()){
            uint64_t txn_idx = pending_txns->front();
//            rc = commit_txn(planner_pg, txn_idx);
            rc = commit_txn(&planner_pg->txn_ctxs[txn_idx]);
            if (rc == WAIT){
                /// collect wait times
                if (simulation->is_done()){
//                DEBUG_Q("ET_%ld: wating on txn_id = %ld, batch_id = %ld, rc = %d, commit_cnt = %ld, commit_dep_cnt=%ld\n",
//                            _thd_id, planner_pg->txn_ctxs[txn_idx].txn_id, wbatch_id, rc,
//                                commit_cnt, planner_pg->txn_ctxs[txn_idx].commit_dep_cnt.load(memory_order_acq_rel));

                    DEBUG_Q("ET_%ld: waiting on committing txn_id = %ld, completion_cnt = %lu, txn_comp_cnt=%lu , batch_id = %lu, PG = %lu, rc = %d, commit_cnt = %ld\n",
                            _thd_id, planner_pg->txn_ctxs[txn_idx].txn_id,planner_pg->txn_ctxs[txn_idx].completion_cnt.load(),planner_pg->txn_ctxs[txn_idx].txn_comp_cnt.load(), wbatch_id, i, rc,
                            commit_cnt);
                    src_res = BREAK;
                    break;
                }
                continue;
            }
            if (rc == Commit){
#if !SERVER_GENERATE_QUERIES & !EARLY_CL_RESPONSE
                Message * rsp_msg = Message::create_message(CL_RSP);
                rsp_msg->txn_id = planner_pg->txn_ctxs[txn_idx].txn_id;
                rsp_msg->batch_id = wbatch_id; // using batch_id from local, we can also use the one in the context
                ((ClientResponseMessage *) rsp_msg)->client_startts = planner_pg->txn_ctxs[txn_idx].client_startts;
                ((ClientResponseMessage *) rsp_msg)->rc = rc;
                rsp_msg->lat_work_queue_time = 0;
                rsp_msg->lat_msg_queue_time = 0;
                rsp_msg->lat_cc_block_time = 0;
                rsp_msg->lat_cc_time = 0;
                rsp_msg->lat_process_time = 0;
                rsp_msg->lat_network_time = 0;
                rsp_msg->lat_other_time = 0;

                msg_queue.enqueue(_thd_id, rsp_msg, planner_pg->txn_ctxs[txn_idx].return_node_id);
#endif
                commit_cnt++;
                wt_release_accesses(&planner_pg->txn_ctxs[txn_idx], rc);
//                e8 = TXN_READY_TO_COMMIT;
//                d8 = TXN_COMMITTED;
//                if (!planner_pg->txn_ctxs[txn_idx].txn_state.compare_exchange_strong(e8,d8,memory_order_acq_rel)){
//                    M_ASSERT_V(false, "ET_%ld: trying to commit a transaction with invalid status\n", _thd_id);
//                }
                // icrement abort
            }
            else if (rc == Abort){
                abort_cnt++;
                wt_release_accesses(&planner_pg->txn_ctxs[txn_idx],rc);
//                e8 = TXN_READY_TO_COMMIT;
//                d8 = TXN_ABORTED;
//                if (!planner_pg->txn_ctxs[txn_idx].txn_state.compare_exchange_strong(e8,d8,memory_order_acq_rel)){
//                    M_ASSERT_V(false, "ET_%ld: trying to abort a transaction with invalid status\n", _thd_id);
//                }
            }
#if EXEC_BUILD_TXN_DEPS
            transaction_context * j_ctx = &planner_pg->txn_ctxs[txn_idx];
            for (auto it = j_ctx->commit_deps->begin(); it != j_ctx->commit_deps->end(); it++ ){
                transaction_context * d_ctx = (*it);
                // decrement counter for dependent txns
                int64_t tmp;
                int64_t tmp2;
                do {
                    tmp = d_ctx->commit_dep_cnt.load(memory_order_acq_rel);
                    tmp2 = tmp -1;
//                    M_ASSERT_V(tmp2 >=0, "CT_%ld: committing txn_id=%ld, updating dep from %ld counter to %ld, for txn_id=%ld, commit_deps size=%lu\n",
//                               _thd_id, j_ctx->txn_id, tmp, tmp2,d_ctx->txn_id, j_ctx->commit_deps->size());
                }while(!d_ctx->commit_dep_cnt.compare_exchange_strong(tmp, tmp2, memory_order_acq_rel));
                if (rc == Abort){
                    d_ctx->should_abort = true;
                }
            }
#endif
            pending_txns->pop_front();
        }

        DEBUG_Q("N_%u:ET_%ld: committed %ld transactions, batch_id = %lu, PG=%lu, pg_idx=%lu\n",
                g_node_id,_thd_id, commit_cnt, wbatch_id,QueCCPool::map_to_cwplanner_id(i), i);
        INC_STATS(_thd_id, txn_cnt, commit_cnt);
        INC_STATS(_thd_id, total_txn_abort_cnt, abort_cnt);
//                DEBUG_Q("ET_%ld: commit count = %ld, PG=%ld, txn_per_pg = %ld, batch_id = %ld\n",
//                        _thd_id, commit_cnt, i, txn_per_pg, wbatch_id);
#if PROFILE_EXEC_TIMING
        INC_STATS(_thd_id,exec_txn_commit_time[_thd_id],get_sys_clock() - quecc_commit_starttime);
#endif
    }
#if COUNT_BASED_SIM_ENABLED
    simulation->inc_txn_cnt(commit_cnt+abort_cnt);
#endif
    free(pending_txns);
    return src_res;
}

batch_partition * WorkerThread::get_batch_part(uint64_t batch_slot, uint64_t wplanner_id){
    return get_batch_part(batch_slot,wplanner_id,_thd_id);
}

batch_partition * WorkerThread::get_batch_part(uint64_t batch_slot, uint64_t wplanner_id, uint64_t et_id){
    batch_partition * ret;
#if BATCH_MAP_ORDER == BATCH_ET_PT
    ret = (batch_partition *)  work_queue.batch_map[batch_slot][et_id][wplanner_id].load();
#else
    ret = (batch_partition *)  work_queue.batch_map[batch_slot][wplanner_id][et_id].load();
//    ret = (batch_partition *)  work_queue.batch_map[batch_slot][wplanner_id][et_id].load(memory_order_acq_rel);
#endif
    return  ret;
}

void WorkerThread::cleanup_batch_part(uint64_t batch_slot, uint64_t wplanner_id){
//    DEBUG_Q("N_%u:WT_%lu: cleaning up my batch_part wplanner_id=%lu for batch_id = %lu\n",
//            g_node_id,_worker_cluster_wide_id, wplanner_id,wbatch_id);
    cleanup_batch_part(batch_slot, wplanner_id, QueCCPool::map_to_et_id(_worker_cluster_wide_id));
}

void WorkerThread::cleanup_batch_part(uint64_t batch_slot, uint64_t wplanner_id, uint64_t et_id){

    // reset batch_map_slot to zero after processing it
    // reset map slot to 0 to allow planners to use the slot
    batch_partition * batch_part = get_batch_part(batch_slot, wplanner_id, et_id);
    assert(batch_part);

    DEBUG_Q("N_%u:WT_%lu:: cleaning up batch part for batch_map[%lu][%lu][%lu] part_batch_id=%lu\n",
            g_node_id,_worker_cluster_wide_id,wbatch_id,wplanner_id, et_id,batch_part->batch_id);
    assert(batch_part->batch_id == wbatch_id);
#if PROFILE_EXEC_TIMING
    uint64_t quecc_prof_time;
#endif
    if (batch_part->empty){
//#if PROFILE_EXEC_TIMING
//        quecc_prof_time = get_sys_clock();
//#endif
//        quecc_pool.batch_part_release(batch_part, wplanner_id, et_id);
//#if PROFILE_EXEC_TIMING
//        INC_STATS(_thd_id,exec_mem_free_time[et_id],get_sys_clock() - quecc_prof_time);
//#endif
//            DEBUG_Q("ET_%ld: PG %ld is empty to th next PG\n",_thd_id, wplanner_id);
        goto bc_end;
    }
#if PROFILE_EXEC_TIMING
    quecc_prof_time = get_sys_clock();
#endif
    if (!batch_part->single_q){
        // free batch_partition
        batch_part->exec_qs->clear();
        quecc_pool.exec_qs_release(batch_part->exec_qs, wplanner_id);
//            quecc_pool.exec_qs_status_release(batch_part->exec_qs_status, wplanner_id, _thd_id);
    }
    bc_end:
//        DEBUG_Q("ET_%ld: For batch %ld , batch partition cleanup complete at map slot [%ld][%ld][%ld] \n",
//                _thd_id, wbatch_id, batch_slot, _thd_id, wplanner_id);
    // free/release batch_part
    quecc_pool.batch_part_release(batch_part, wplanner_id, et_id);
#if PROFILE_EXEC_TIMING
    INC_STATS(_thd_id,exec_mem_free_time[et_id],get_sys_clock() - quecc_prof_time);
#endif

    uint64_t desired = 0;
    uint64_t expected = (uint64_t) batch_part;
#if BATCH_MAP_ORDER == BATCH_ET_PT
    while(!work_queue.batch_map[batch_slot][_thd_id][wplanner_id].compare_exchange_strong(expected, desired)){
            DEBUG_Q("ET_%ld: failing to RESET map slot \n", _thd_id);
        }
#else
//    DEBUG_Q("N_%u:ET_%ld: RESET batch map slot=%ld, PG=%ld, batch_id=%ld \n", g_node_id, _worker_cluster_wide_id, batch_slot, wplanner_id, wbatch_id);
    assert(wplanner_id < (g_plan_thread_cnt*g_node_cnt));
#if PIPELINED2
    assert(et_id < (g_et_thd_cnt*g_node_cnt));
#else
    assert(et_id < (g_thread_cnt*g_node_cnt));
#endif

    assert(batch_slot < g_batch_map_length);
    if(!work_queue.batch_map[batch_slot][wplanner_id][et_id].compare_exchange_strong(expected, desired)){
        M_ASSERT_V(false, "ET_%ld: failing to RESET map slot \n", et_id);
    }
//    if(!work_queue.batch_map[batch_slot][wplanner_id][et_id].compare_exchange_strong(expected, desired, memory_order_acq_rel)){
//        M_ASSERT_V(false, "ET_%ld: failing to RESET map slot \n", et_id);
//    }
#endif
}

batch_partition * WorkerThread::get_curr_batch_part(uint64_t batch_slot){
    batch_partition * batch_part;
#if PIPELINED2
    uint64_t  et_i = QueCCPool::map_to_et_id(_worker_cluster_wide_id);
#else
    uint64_t  et_i = _worker_cluster_wide_id;
#endif
    DEBUG_Q("N_%u:WT_%lu: going to return %s batch part Batch_map[%lu][%lu][%lu] for execution\n",
            g_node_id,_worker_cluster_wide_id, (g_node_id == QueCCPool::get_plan_node(wplanner_id))? "local" : "remote", wbatch_id, wplanner_id, et_i);
#if BATCH_MAP_ORDER == BATCH_ET_PT
    batch_part = (batch_partition *)  work_queue.batch_map[batch_slot][_thd_id][wplanner_id].load();
#else
    do{
//        batch_part = (batch_partition *)  work_queue.batch_map[batch_slot][wplanner_id][et_i].load(memory_order_acq_rel);
        batch_part = (batch_partition *)  work_queue.batch_map[batch_slot][wplanner_id][et_i].load();
        if (simulation->is_done()){
            DEBUG_Q("N_%u:WT_%lu: SIM_DONE while waiting on %s batch part Batch_map[%lu][%lu][%lu] for execution\n",
                    g_node_id,_worker_cluster_wide_id, (g_node_id == QueCCPool::get_plan_node(wplanner_id))? "local" : "remote", wbatch_id, wplanner_id, et_i);
            return batch_part;
        }
    } while(batch_part == 0 || (batch_part->batch_id != wbatch_id));
#endif
    M_ASSERT_V(batch_part, "N_%u:WT_%ld: batch part pointer is zero, PG=%ld, batch_slot = %ld, batch_id = %ld!!\n",
               g_node_id,_thd_id, wplanner_id, batch_slot, wbatch_id);
    return batch_part;
}

#if EXEC_BUILD_TXN_DEPS
bool WorkerThread::add_txn_dep(transaction_context * context, uint64_t d_txn_id, priority_group * d_planner_pg){
    transaction_context * d_tctx = get_tctx_from_pg(d_txn_id, d_planner_pg);
    if (d_tctx != NULL && context->txn_id > d_txn_id){
        // found txn context for dependent d_txn_id
        // increment my dep counter in my context
//            M_ASSERT_V(context->txn_id != 1,"WT_%lu: Dependency for the first tansaction in the batch!!! d_txn_id=%lu\n",_thd_id,d_txn_id)
        M_ASSERT_V(context->txn_id > d_txn_id,"WT_%lu: dependency on a later transaction!!! txn_id=%lu depends on d_txn_id=%lu\n",
                   _thd_id,context->txn_id,d_txn_id)
        int64_t e=0;
        do {
            e = context->commit_dep_cnt.load(memory_order_acq_rel);
            M_ASSERT_V(e >= 0, "ET_%ld: commit dep count = %ld\n", _thd_id, e);
        }while(!context->commit_dep_cnt.compare_exchange_strong(e,e+1,memory_order_acq_rel));
        // add my txn pointer to dependency set
        d_tctx->depslock->lock();
        d_tctx->commit_deps->push_back(context);
        d_tctx->depslock->unlock();
        return true;
    }
    else{
        return false;
    }
};
#endif
/**
 * capturing dependnecies as follows:
 * Each accessed row has a last TID field
 * Use last TID to look up txn context from PGs
 * Update commit dependencies count if needed
 * @param batch_slot
 * @param _entry
 * @param rc
 */
void WorkerThread::capture_txn_deps(uint64_t batch_slot, exec_queue_entry * _entry, RC rc)
{
#if EXEC_BUILD_TXN_DEPS
    bool check_dep = true;
#if WORKLOAD == TPCC
    if (entry->type == TPCC_PAYMENT_INSERT_H
            || entry->type == TPCC_NEWORDER_INSERT_NO
            || entry->type == TPCC_NEWORDER_INSERT_O
            || entry->type == TPCC_NEWORDER_INSERT_OL) {
            check_dep = false;
        }
#endif
    if (rc == RCOK && check_dep){
        uint64_t last_tid = _entry->row->last_tid;
        if (last_tid == 0){
            //  if this is a read, and record has not been update before, so no dependency
            return;
        }
        if (last_tid == _entry->txn_id){
            // I just wrote to this row, // get previous txn_id from context
            last_tid = _entry->txn_ctx->prev_tid[_entry->req_idx];
        }
        if (last_tid == 0){
            // if this is a write, we are the first to write to this record, so no dependency
            return;
        }

        /// DEBUGGING
//            if (last_tid >= entry->txn_id){
//                DEBUG_Q("ET_%ld: going to add invalid dependency : entry_txnid=%ld, row_last_tid=%ld, entry->row_last_tid=%ld, entry->req_idx=%lu, PG=%ld, batch_id=%ld, on key=%ld\n",
//                        _thd_id,entry->txn_id,last_tid, entry->row->last_tid,entry->req_idx, wplanner_id,wbatch_id,get_key_from_entry(entry));
//                assert(false);
//            }
//            DEBUG_Q("WT_%ld: txn_id=%lu, last_tid=%lu\n", _thd_id, entry->txn_id, last_tid);

        int64_t cplan_id = (int64_t)wplanner_id;
        for (int64_t j = cplan_id; j >= 0; --j) {
            batch_partition * part = get_batch_part(batch_slot,j,_thd_id);
            if (add_txn_dep(_entry->txn_ctx,last_tid,part->planner_pg)){
//                    DEBUG_Q("ET_%ld: added dependency : entry_txnid=%ld, row_last_tid=%ld, PG=%ld, batch_id=%ld, on key=%ld\n",
//                            _thd_id,entry->txn_id,last_tid, wplanner_id,wbatch_id,get_key_from_entry(entry));
                break;
            }
        }
    }
#endif
}

SRC WorkerThread::execute_batch_part(uint64_t batch_slot, uint64_t *eq_comp_cnts, TxnManager * my_txn_man)
{
    batch_partition * batch_part = get_curr_batch_part(batch_slot);

    if (!batch_part) return BREAK;

    Array<exec_queue_entry> * exec_q;
    uint64_t batch_part_eq_cnt = 0;
    exec_queue_entry * exec_qe_ptr UNUSED = NULL;
    uint64_t w_exec_q_index = 0;
    RC rc = RCOK;
    SRC src = SUCCESS;

    DEBUG_Q("N_%u:ET_%ld: going to work on Batch map[%lu][%lu][%lu]\n",g_node_id, _worker_cluster_wide_id,wbatch_id,wplanner_id,QueCCPool::map_to_et_id(_worker_cluster_wide_id));

    memset(eq_comp_cnts,0, sizeof(uint64_t)*g_exec_qs_max_size);


#if PROFILE_EXEC_TIMING
    uint64_t quecc_prof_time =0;
//    uint64_t quecc_txn_wait_starttime =0;
#endif

    if (batch_part->empty){
        goto end_remote_eq;
    }

    //select an execution queue to work on.
    if (batch_part->single_q){
        exec_q = batch_part->exec_q;
        batch_part_eq_cnt =1;
    }
    else {
        batch_part_eq_cnt = batch_part->exec_qs->size();
        exec_q = batch_part->exec_qs->get(w_exec_q_index);
    }

    while(batch_part_eq_cnt > 0){
        DEBUG_Q("N_%u:ET_%lu: Got a pointer for map slot PG = [%lu] is %ld, current_batch_id = %lu,"
                "batch partition exec_q size = %lu entries, exec_qs_size = %lu, w_exec_q_index=%lu, batch_part_eq_cnt=%lu"
                "\n",
                g_node_id,_worker_cluster_wide_id, wplanner_id, ((uint64_t) exec_q), wbatch_id,
                exec_q->size(), batch_part->exec_qs->size(), w_exec_q_index, batch_part_eq_cnt);

        while(exec_q->size() > eq_comp_cnts[w_exec_q_index] && rc == RCOK) {
#if PROFILE_EXEC_TIMING
            quecc_prof_time = get_sys_clock();
#endif
            exec_qe_ptr = exec_q->get_ptr(eq_comp_cnts[w_exec_q_index]);
#if PROFILE_EXEC_TIMING
            INC_STATS(_thd_id,exec_entry_deq_time[_thd_id],get_sys_clock() - quecc_prof_time);
#endif

//            DEBUG_Q("N_%u:ET_%lu: Processing an entry, batch_id=%lu, planner_id = %lu\n",
//                            g_node_id,_worker_cluster_wide_id, wbatch_id, wplanner_id);
#if PROFILE_EXEC_TIMING
            quecc_prof_time = get_sys_clock();
#endif
            if (QueCCPool::get_plan_node(wplanner_id) == g_node_id){
                my_txn_man->update_context = true;
            }
            else{
                my_txn_man->update_context = false;
            }
            rc = my_txn_man->run_quecc_txn(exec_qe_ptr);
            capture_txn_deps(batch_slot, exec_qe_ptr, rc);
#if PROFILE_EXEC_TIMING
            INC_STATS(_thd_id,exec_txn_proc_time[_thd_id],get_sys_clock() - quecc_prof_time);
#endif
            eq_comp_cnts[w_exec_q_index]++;
        }

        if (rc == RCOK){
            batch_part_eq_cnt--;
            DEBUG_Q("N_%u:ET_%lu: Processing EQ is done! batch_id=%lu, planner_id = %lu\n",
                    g_node_id,_worker_cluster_wide_id, wbatch_id, wplanner_id);
        }
        else{
            assert(rc == WAIT);
            DEBUG_Q("N_%u:ET_%lu: Processing EQ is STALLED! batch_id=%lu, planner_id = %lu\n",
                    g_node_id,_worker_cluster_wide_id, wbatch_id, wplanner_id);
            if (simulation->is_done()){
                src = BREAK;
                goto clean_up;
            }
        }

        move_to_next_eq(batch_part, eq_comp_cnts, exec_q, w_exec_q_index, batch_part_eq_cnt);
    }

    clean_up:
    // done with all assigned EQs or sim is done!
    // release them to pool

    if (batch_part->single_q){
        exec_q = batch_part->exec_q;
        DEBUG_Q("N_%u:ET_%lu: Releaseing during ET execution at the end of SINGLE PG[%lu], ptr=%lu\n",
                g_node_id,_worker_cluster_wide_id, wplanner_id, (uint64_t) exec_q);
        quecc_pool.exec_queue_release(exec_q, wplanner_id, _worker_cluster_wide_id);
        if (QueCCPool::get_plan_node(wplanner_id) == g_node_id) {
            batch_part->planner_pg->eq_completed_rem_cnt.fetch_sub(1);
        }
    }
    else{
        for (uint64_t i=0; i < batch_part->exec_qs->size(); ++i){
            exec_q = batch_part->exec_qs->get(i);
            DEBUG_Q("N_%u:ET_%lu: Releaseing during ET execution at the end of PG[%lu], ptr=%lu\n",
                    g_node_id,_worker_cluster_wide_id, wplanner_id, (uint64_t) exec_q);
            quecc_pool.exec_queue_release(exec_q, wplanner_id, _worker_cluster_wide_id);
            if (QueCCPool::get_plan_node(wplanner_id) == g_node_id){
                batch_part->planner_pg->eq_completed_rem_cnt.fetch_sub(1);
            }
        }
    }

    end_remote_eq:
    if (batch_part->remote){

        // Broadcast EQs to all nodes (for now). We only need to send to active nodes + coordinator
        for (UInt32 k=0; k < g_node_cnt; ++k){
            if (k != g_node_id){
                DEBUG_Q("N_%u:ET_%lu: Sending ACK, Execution of remote EQ is done: batch_id=%lu, planner_id=%lu, remote node=%d, original_plan node=%s\n",
                        g_node_id, _worker_cluster_wide_id, wbatch_id,wplanner_id,k, (QueCCPool::get_plan_node(wplanner_id) == k)? "yes":"no");
                RemoteEQAckMessage * req_ack = (RemoteEQAckMessage *) Message::create_message(REMOTE_EQ_ACK);
                req_ack->batch_id = wbatch_id;
                req_ack->planner_id = wplanner_id;
                req_ack->exec_id = _worker_cluster_wide_id;
//                msg_queue.enqueue(_thd_id,req_ack,QueCCPool::get_plan_node(wplanner_id));
                msg_queue.enqueue(_thd_id,req_ack,k);
            }
        }

    }
    else{
        DEBUG_Q("N_%u:ET_%lu: Execution of local EQ is done: batch_id=%lu, planner_id=%lu\n",
                g_node_id, _worker_cluster_wide_id, wbatch_id,wplanner_id);
    }

    return src;
}

RC WorkerThread::commit_txn(transaction_context * j_ctx) {
    RC rc = Commit;
    // count of txn frags in the txn
    int64_t frag_txn_cnt =  j_ctx->txn_comp_cnt.load(memory_order_acq_rel);
    // count of txn frags completed
    int64_t frag_comp_cnt = j_ctx->completion_cnt.load(memory_order_acq_rel);

#if EXEC_BUILD_TXN_DEPS
    // below code is not working!!
//    assert(false);
//    DEBUG_Q("CT_%ld:  batch_id=%lu, PG=%lu, trying to commit txn_id %lu, txn_comp_cnt=%ld, completion_cnt=%ld\n",
//                _thd_id, wbatch_id, wplanner_id, j_ctx->txn_id, frag_txn_cnt, frag_comp_cnt);
//    j_ctx->depslock->lock();
    j_ctx->depslock->lock();
    if (frag_comp_cnt == frag_txn_cnt){
        // all frags has been processed
        // check if it has dependency on other txns
        if (j_ctx->commit_dep_cnt.load(memory_order_acq_rel) > 0){
//            DEBUG_Q("CT_%ld: txn_id %lu has %lu dependent txns that has not committed or aborted, batch_id=%lu\n",
//                    _thd_id, txn_ctxs[j].txn_id, txn_ctxs[j].commit_dep_cnt.load(memory_order_acq_rel),wbatch_id);
            rc =  WAIT;
            goto commit_txn_end;
        }
//        else{
//            if (j_ctx->should_abort){
////                    DEBUG_Q("CT_%ld: txn_id %lu should be aborted\n",
////                            _thd_id, txn_ctxs[j].txn_id);
//                wt_release_accesses(j_ctx,Abort);
////                j_ctx->txn_state.store(TXN_ABORTED,memory_order_acq_rel);
//                rc = Abort;
//                goto commit_txn_end;
//            }
//        }
//#if DEBUG_QUECC
//        if (j_ctx->commit_et_id != -1){
////            DEBUG_Q("ET_%lu: wbatch_id=%lu, state=%lu,commit_et_id=%ld, txn_id=%lu\n",
////                    _thd_id,wbatch_id, j_ctx->txn_state.load(memory_order_acq_rel), j_ctx->commit_et_id, j_ctx->txn_id);
//            assert(false);
//        }
//#endif
        // transaction j has committed. We need to decrement CommitDepCnt for dependent transactions
        for (auto it = j_ctx->commit_deps->begin(); it != j_ctx->commit_deps->end(); it++ ){
            transaction_context * d_ctx = (*it);
            d_ctx->commit_dep_cnt.fetch_add(-1, memory_order_acq_rel);
//            if (rc == Abort){
//                d_ctx->should_abort.store(true);
//            }
        }
        // Commit txn
//        DEBUG_Q("ET_%ld: commmitting transaction txn_id = %ld, batch_id = %ld\n",
//                _thd_id, j_ctx->txn_id, wbatch_id);
//        j_ctx->txn_state.store(TXN_COMMITTED,memory_order_acq_rel);
//#if DEBUG_QUECC
//        j_ctx->commit_et_id = (int64_t)_thd_id;
//#endif
        rc =  Commit;
        goto commit_txn_end;
    }
    else{
        // some of the frags has not completed
//        DEBUG_Q("ET_%lu: some fragments are not completed, txn_idx = %lu\n",_thd_id,txn_idx);
        rc =  WAIT; // return wait because we don't sync after exec
        goto commit_txn_end;
    }
    commit_txn_end:
    j_ctx->depslock->unlock();
#else
    if (frag_comp_cnt < frag_txn_cnt){
        rc = WAIT;
    }
    else if(frag_comp_cnt > frag_txn_cnt){
        assert(false);
    }
#endif // #if EXEC_BUILD_TXN_DEPS
    return rc;
}

void WorkerThread::wt_release_accesses(transaction_context * context, RC rc)
{
#if ROW_ACCESS_TRACKING
    // releaase accesses
//    if (DEBUG_QUECC){
//        M_ASSERT_V(!cascading_abort, "cascading abort is not false!!\n");
//        M_ASSERT_V(!rollback, "rollback is not false!!\n");
//    }
//    DEBUG_Q("ET_%ld: cacading abort = %d, rollback = %d\n", _thd_id, cascading_abort, rollback);
#if ROLL_BACK
#if ROW_ACCESS_IN_CTX
    // we don't need to free any memory to release accesses as it will be recycled for the next batch
    // roll back writes
    uint64_t row_cnt = context->txn_comp_cnt.load(memory_order_acq_rel);
    M_ASSERT_V(row_cnt <= MAX_ROW_PER_TXN, "WT_%lu: row_cnt = %lu", _thd_id, row_cnt);
    for (uint64_t i = 0; i < row_cnt; ++i) {
        if (context->a_types[i] == WR){
            if (rc == Abort){
                if (context->orig_rows[i]->last_tid == context->txn_id){
                    uint64_t rec_size = context->orig_rows[i]->get_tuple_size();
                    context->orig_rows[i]->last_tid = context->prev_tid[i];
                    uint64_t ri = i*rec_size;
                    memcpy(context->orig_rows[i]->data,&context->undo_buffer_data[ri], rec_size);
                    INC_STATS(_thd_id, record_recov_cnt[_thd_id], 1);
                }
            }
        }
    }

#else
    for (uint64_t k = 0; k < context->accesses->size(); k++) {
        M_ASSERT_V(context->accesses->get(k), "Zero pointer for access \n");
        uint64_t ctid = context->accesses->get(k)->thd_id;
        Access * access = context->accesses->get(k);


        if (access->type == WR){
            if (rc == Abort){
                // if ctx.tid == row.last_tid
                // restore original data and the original tid
                if (context->txn_id == access->orig_row->last_tid){
                    access->orig_row->last_tid = access->prev_tid;
                    access->orig_row->copy(access->orig_data);
                    INC_STATS(_thd_id, record_recov_cnt[_thd_id], 1);
                }
            }
            //            access->orig_data->free_row();
            row_pool.put(ctid, access->orig_data);
            access->orig_data->free_row_pool(_thd_id);
        }

        access_pool.put(ctid, access);
    }
    context->accesses->clear();
#endif
#endif
#endif
};

void WorkerThread::move_to_next_eq(const batch_partition *batch_part, const uint64_t *eq_comp_cnts,
                     Array<exec_queue_entry> *&exec_q, uint64_t &w_exec_q_index, uint64_t & batch_part_eq_cnt) const
{
    for (uint64_t i = 0; i < batch_part->exec_qs->size(); ++i){
        if (i != w_exec_q_index && batch_part->exec_qs->get(i)->size() > eq_comp_cnts[i]){
            w_exec_q_index = i;
            exec_q = batch_part->exec_qs->get(i);
            return;
        }
    }
    //FIX me now I dont switch from completeed EQ
    // could not switch, using the same the same
    return;
};

transaction_context * WorkerThread::get_tctx_from_pg(uint64_t txnid, priority_group *planner_pg)
{
    transaction_context * d_tctx;
    uint64_t d_txn_ctx_idx = txnid-planner_pg->batch_starting_txn_id;

    if (d_txn_ctx_idx >= planner_batch_size){
        return NULL;
    }
    else{
        d_tctx = &planner_pg->txn_ctxs[d_txn_ctx_idx];
//            DEBUG_Q("ET_%ld: d_txn_ctx_idx=%ld, txn_id=%ld, batch_starting_txn_id=%ld, d_txn_id=%lu\n",
//                    _thd_id,d_txn_ctx_idx,txnid,planner_pg->batch_starting_txn_id, d_tctx->txn_id);
        return d_tctx;
    }
}
#endif // if CC_ALG == QUECC

RC WorkerThread::run_normal_mode() {
    tsetup();
    printf("N_%u: Running WorkerThread %ld\n", g_node_id, _thd_id);
    fflush(stdout);
#if !(CC_ALG == QUECC || CC_ALG == DUMMY_CC)
#if PROFILE_EXEC_TIMING
    uint64_t ready_starttime;
#endif
#endif

#if CC_ALG == QUECC
#if !PIPELINED
    _planner_id = _thd_id;
    _worker_cluster_wide_id = _thd_id + (g_node_id * g_thread_cnt);
    txn_prefix_planner_base = (_planner_id * txn_prefix_base);
    assert(UINT64_MAX > (txn_prefix_planner_base+txn_prefix_base));

    DEBUG_Q("WT_%lu thread started, txn_ids start at %lu \n", _planner_id, txn_prefix_planner_base);
    printf("Node_%u:WT_%lu: (worker_cluster_wide_id=%lu) worker thread started in non-pipelined mode\n", g_node_id, _thd_id, _worker_cluster_wide_id);
    fflush(stdout);
#if SPLIT_MERGE_ENABLED && SPLIT_STRATEGY == LAZY_SPLIT
    uint64_t exec_queue_limit = quecc_pool.exec_queue_capacity;
#endif

#if SPLIT_MERGE_ENABLED

#if SPLIT_STRATEGY == EAGER_SPLIT
    ((Array<uint64_t> *)exec_qs_ranges_tmp)->init(g_exec_qs_max_size);
    exec_queues_tmp = new Array<Array<exec_queue_entry> *>();
    ((Array<uint64_t> *)exec_queues_tmp)->init(g_exec_qs_max_size);
#endif
#endif


    // Array to track ranges
    exec_qs_ranges->init(g_exec_qs_max_size);
#if WORKLOAD == YCSB
    // allow planners to be less than ETs
    assert(g_thread_cnt >= g_plan_thread_cnt);
//    uint64_t range_start_node = g_node_id * (g_thread_cnt * bucket_size);
    uint64_t range_start_node = 0;
    uint64_t total_eq_cnt = g_cluster_worker_thread_cnt;
    for (uint64_t i =0; i<total_eq_cnt; ++i){
        uint64_t range_boundary = range_start_node + ((i+1)*bucket_size);
        DEBUG_Q("Node_id=%u,WT_id=%lu,i=%lu, range_boundary=%lu\n",g_node_id, _thd_id,i,range_boundary);
        exec_qs_ranges->add(range_boundary);
    }
    assert(total_eq_cnt*bucket_size == g_synth_table_size);

//    for (uint64_t i =0; i<g_thread_cnt-1; ++i){
//
//        if (i == 0){
//            exec_qs_ranges->add(bucket_size);
//            continue;
//        }
//        exec_qs_ranges->add(exec_qs_ranges->get(i-1)+bucket_size);
//    }
//    if (g_node_id == g_node_cnt-1){
//        DEBUG_Q("Node_id=%u,WT_id=%lu, adding sythntablesize\n",g_node_id, _thd_id);
//        exec_qs_ranges->add(g_synth_table_size); // last range is the table size
//    }
//    else{
//        DEBUG_Q("Node_id=%u,WT_id=%lu, adding bucket_size\n",g_node_id, _thd_id);
//        exec_qs_ranges->add(exec_qs_ranges->get(g_thread_cnt-1)+bucket_size);
//    }
#elif WORKLOAD == TPCC
    uint64_t bucket_cnt = g_cluster_worker_thread_cnt;
#if SINGLE_NODE
    if (g_num_wh < g_thread_cnt) {
        bucket_cnt = g_thread_cnt;
        bucket_size = UINT64_MAX/bucket_cnt;
    }
    for (uint64_t i =0; i<bucket_cnt-1; ++i){

        if (i == 0){
            exec_qs_ranges->add(bucket_size);
            continue;
        }

        exec_qs_ranges->add(exec_qs_ranges->get(i-1)+bucket_size);
    }
    exec_qs_ranges->add(UINT64_MAX); // last range is the table size
#else
    assert(g_cluster_worker_thread_cnt <= g_num_wh);
    // there is no need to use RID space partitioing for TPCC
    // NOTE!!!! This breaks the SPLIT algorithm
    for (uint64_t i =0; i<g_cluster_worker_thread_cnt; ++i){
        exec_qs_ranges->add(0);
    }
#endif
#else
    M_ASSERT(false,"only YCSB and TPCC are currently supported\n")
#endif

#if SPLIT_MERGE_ENABLED
    exec_queues->init(g_exec_qs_max_size);
#if ISOLATION_LEVEL == READ_COMMITTED
    ro_exec_queues->init(g_exec_qs_max_size);
#endif
#else
    exec_queues->init(g_thread_cnt);
#endif

#if WORKLOAD == TPCC
    for (uint64_t i = 0; i < bucket_cnt; i++) {
        Array<exec_queue_entry> * exec_q;
        quecc_pool.exec_queue_get_or_create(exec_q, _planner_id, i % g_thread_cnt);
            exec_queues->add(exec_q);
    }

    if (_thd_id == 0){
        // cleanup remote txn context
        // it is safe now since we are at the end of the batch
        for(uint64_t batch_slot=0; batch_slot<g_batch_map_length;++batch_slot){
            for (uint64_t i = 0; i < g_cluster_worker_thread_cnt; ++i) {
                if (g_node_id != QueCCPool::get_plan_node(i)){
                    transaction_context * pg_tctxs = work_queue.batch_pg_map[batch_slot][i].txn_ctxs;
                    for (uint64_t j = 0; j < planner_batch_size; ++j) {
                        pg_tctxs[j].o_id.store(-1,memory_order_acq_rel);
                    }
                }
            }
        }
    }
#else
    for (uint64_t i = 0; i < g_cluster_worker_thread_cnt; i++) {
        Array<exec_queue_entry> * exec_q;
        quecc_pool.exec_queue_get_or_create(exec_q, _worker_cluster_wide_id, i);
        exec_queues->add(exec_q);
#if ISOLATION_LEVEL == READ_COMMITTED
        quecc_pool.exec_queue_get_or_create(exec_q, _worker_cluster_wide_id, i);
        ro_exec_queues->add(exec_q);
#endif
    }
#endif

    planner_txn_id = txn_prefix_planner_base;
    if (_planner_id == 0){
        // start txn_id of planner 0 with 1
        planner_txn_id = 1;
    }
    query_cnt = 0;
#endif

    uint64_t batch_slot = 0;

    RC rc __attribute__((unused)) = RCOK;

    TxnManager * my_txn_man;
    _wl->get_txn_man(my_txn_man);
    my_txn_man->init(_thd_id, _wl);
    my_txn_man->register_thread(this);
    my_txn_man->_wt_id = _worker_cluster_wide_id;

//#if NUMA_ENABLED
//    uint64_t * eq_comp_cnts = (uint64_t *) mem_allocator.alloc_local(sizeof(uint64_t)*g_exec_qs_max_size);
//#else
    uint64_t * eq_comp_cnts = (uint64_t *) mem_allocator.alloc(sizeof(uint64_t)*g_exec_qs_max_size);
//#endif

    idle_starttime = 0;
    SRC src = SUCCESS;
#if PROFILE_EXEC_TIMING
    uint64_t batch_proc_starttime = 0;
    uint64_t hl_prof_starttime = 0;
#endif

#endif

    while (!simulation->is_done()) {
        txn_man = NULL;
        heartbeat();
        progress_stats();

#if CC_ALG == QUECC
#if PROFILE_EXEC_TIMING
        if( batch_proc_starttime == 0 && simulation->is_warmup_done()){
            batch_proc_starttime = get_sys_clock();
        }
        else {
            batch_proc_starttime =0;
        }
#endif
        // allows using the batch_map in circular manner
        batch_slot = wbatch_id % g_batch_map_length;

#if PIPELINED
#if PROFILE_EXEC_TIMING
        if (batch_proc_starttime > 0){
            hl_prof_starttime = get_sys_clock();
        }
#endif
        src = execute_batch(batch_slot, eq_comp_cnts, my_txn_man);
#if PROFILE_EXEC_TIMING
        if (batch_proc_starttime > 0) {
            INC_STATS(_thd_id, wt_hl_exec_time[_thd_id], get_sys_clock() - hl_prof_starttime);
        }
#endif
        if (src == SUCCESS) {

#if PARALLEL_COMMIT
            // ET is done with its PGs for the current batch
//            DEBUG_Q("ET_%ld: done with my batch partition, batch_id = %ld, going to wait for all ETs to finish\n", _thd_id, wbatch_id);
#if PROFILE_EXEC_TIMING
            if (batch_proc_starttime > 0){
                hl_prof_starttime = get_sys_clock();
            }
#endif
            src = sync_on_execution_phase_end(batch_slot);
#if PROFILE_EXEC_TIMING
            if (batch_proc_starttime > 0){
                INC_STATS(_thd_id, wt_hl_sync_exec_time[_thd_id], get_sys_clock()-hl_prof_starttime);
            }
#endif
            if (src == BREAK){
                goto end_et;
            }

//            DEBUG_Q("ET_%ld: starting parallel commit, batch_id = %ld\n", _thd_id, wbatch_id);

            // process txn contexts in the current batch that are assigned to me (deterministically)
#if PROFILE_EXEC_TIMING
            if (batch_proc_starttime > 0){
                hl_prof_starttime = get_sys_clock();
            }
#endif
            commit_batch(batch_slot);
#if PROFILE_EXEC_TIMING
            if (batch_proc_starttime > 0){
                INC_STATS(_thd_id, wt_hl_commit_time[_thd_id], get_sys_clock()-hl_prof_starttime);
            }
#endif

            // indicate that I am done with all commit phase
//            DEBUG_Q("ET_%ld: is done with commit task for batch_slot = %ld\n", _thd_id, batch_slot);
#if !PIPELINED
#if PROFILE_EXEC_TIMING
            if (batch_proc_starttime > 0){
                hl_prof_starttime = get_sys_clock();
            }
#endif
            batch_cleanup(batch_slot);
#if PROFILE_EXEC_TIMING
            if (batch_proc_starttime > 0){
                INC_STATS(_thd_id, wt_hl_cleanup_time[_thd_id], get_sys_clock()-hl_prof_starttime);
            }
#endif
#if PROFILE_EXEC_TIMING
            if (batch_proc_starttime > 0){
                hl_prof_starttime = get_sys_clock();
            }
#endif
#endif // #if !PIPELINED
            src = sync_on_commit_phase_end(batch_slot);
#if PROFILE_EXEC_TIMING
            if (batch_proc_starttime > 0){
                INC_STATS(_thd_id, wt_hl_sync_commit_time[_thd_id], get_sys_clock()-hl_prof_starttime);
            }
#endif
            if (src == BREAK){
                goto end_et;
            }
#if PIPELINED
            #if PROFILE_EXEC_TIMING
            if (batch_proc_starttime > 0){
                hl_prof_starttime = get_sys_clock();
            }
#endif
            batch_cleanup(batch_slot);
#if PROFILE_EXEC_TIMING
            if (batch_proc_starttime > 0){
                INC_STATS(_thd_id, wt_hl_cleanup_time[_thd_id], get_sys_clock()-hl_prof_starttime);
            }
#endif
#if PROFILE_EXEC_TIMING
            if (batch_proc_starttime > 0){
                hl_prof_starttime = get_sys_clock();
            }
#endif
#endif // #if PIPELINED

#else
#if COMMIT_BEHAVIOR == AFTER_BATCH_COMP
            work_queue.batch_map_comp_cnts[batch_slot].fetch_add(1);
#if CT_ENABLED
            while (!simulation->is_done() && work_queue.batch_map_comp_cnts[batch_slot].load() != 0){
                // spin
//                DEBUG_Q("ET_%ld : Spinning waiting for batch %ld to be committed at slot %ld, current_cnt = %d\n",
//                        _thd_id, wbatch_id, batch_slot, work_queue.batch_map_comp_cnts[batch_slot].load());
            }

#else
//            DEBUG_Q("ET_%ld: completed all PGs !!!!\n", _thd_id);
            if (_thd_id == 0){
                // Thread 0 acts as commit thread
//                assert(idle_starttime == 0);
                if (idle_starttime > 0){
                    INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                }
                idle_starttime = get_sys_clock();
                while (!simulation->is_done() && (work_queue.batch_map_comp_cnts[batch_slot].load() != g_thread_cnt)){
                    // SPINN Here untill all ETs are done
//                    SAMPLED_DEBUG_Q("ET_%ld: waiting for other ETs to be done, wbatch_id = %ld at slot = %ld, val = %d, thd_cnt = %d"
//                                    "\n",
//                            _thd_id,  wbatch_id, batch_slot,work_queue.batch_map_comp_cnts[batch_slot].load(), g_thread_cnt);
                }
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                idle_starttime =0;
                // TODO(tq): collect stats for committing

//                DEBUG_Q("ET_%ld: All ETs are done for, wbatch_id = %ld at slot = %ld"
//                                "\n",
//                        _thd_id,  wbatch_id, batch_slot);

//                DEBUG_Q("ET_%ld: allowing PTs to procced, wbatch_id = %ld at slot = %ld"
//                                "\n",
//                        _thd_id,  wbatch_id, batch_slot);
                desired8 = PG_AVAILABLE;
                expected8 = PG_READY;
                for (uint64_t i =0; i < g_plan_thread_cnt; ++i){
                    quecc_commit_starttime = get_sys_clock();
                    uint64_t commit_cnt = 0;
                    priority_group * planner_pg = NULL;
                    uint64_t planner_batch_size = g_batch_size/g_plan_thread_cnt;
                    planner_pg = &work_queue.batch_pg_map[batch_slot][i];
                    assert(planner_pg->status.load() == PG_READY);
                    transaction_context * txn_ctxs = planner_pg->txn_ctxs;
                    volatile bool canCommit = true;
                    //loop over all transactions in the priority group
                    volatile bool cascading_abort = false;

                    for (uint64_t j = 0; j < planner_batch_size; j++){
                        if (txn_ctxs[j].txn_state != TXN_READY_TO_ABORT){
#if BUILD_TXN_DEPS
                            // We are ready to commit, now we need to check if we need to abort due to dependent aborted transactions
                            // to check if we need to abort, we lookup transaction dependency graph
                            auto search = planner_pg->txn_dep_graph->find(txn_ctxs[j].txn_id);
                            if (search != planner_pg->txn_dep_graph->end()){
                                // print dependenent transactions for now.
                                if (search->second->size() > 0){
                                    // there are dependen transactions
            //                        DEBUG_Q("CT_%ld : txn_id = %ld depends on %ld other transactions\n", _thd_id, txn_ctxs[i].txn_id, search->second->size());
            //                        for(std::vector<uint64_t>::iterator it = search->second->begin(); it != search->second->end(); ++it) {
            //                            DEBUG_Q("CT_%ld : txn_id = %ld depends on txn_id = %ld\n", _thd_id, txn_ctxs[i].txn_id, (uint64_t) *it);
            //                        }
                                    uint64_t d_txn_id = search->second->back();
                                    uint64_t d_txn_ctx_idx = d_txn_id-planner_pg->batch_starting_txn_id;
                                    M_ASSERT_V(txn_ctxs[d_txn_ctx_idx].txn_id == d_txn_id,
                                               "Txn_id mismatch for d_ctx_txn_id %ld == tdg_d_txn_id %ld , d_txn_ctx_idx = %ld,"
                                                       "c_txn_id = %ld, batch_starting_txn_id = %ld, txn_ctxs(%ld) \n",
                                               txn_ctxs[d_txn_ctx_idx].txn_id,
                                               d_txn_id, d_txn_ctx_idx, txn_ctxs[i].txn_id, planner_pg->batch_starting_txn_id,
                                               (uint64_t)txn_ctxs
                                    );
                                    if (txn_ctxs[d_txn_ctx_idx].txn_state == TXN_READY_TO_ABORT){
                                        // abort due to dependencies on an aborted txn
            //                            DEBUG_Q("CT_%ld : going to abort txn_id = %ld due to dependencies\n", _thd_id, txn_ctxs[i].txn_id);
                                        canCommit = false;
#if ROW_ACCESS_TRACKING
                                        cascading_abort = true;
#endif
                                    }
                                }
                            }
#endif // #if BUILD_TXN_DEPS

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

                                msg_queue.enqueue(_thd_id, rsp_msg, txn_ctxs[j].return_node_id);
#endif
                                commit_cnt++;
                                wt_release_accesses(&txn_ctxs[j], cascading_abort, false);
                            }
                            else{
                                // need to do cascading abort
                                wt_release_accesses(&txn_ctxs[j], cascading_abort, true);
                            }
                        }
                        else {
                            // transaction should be aborted due to integrity constraints
                            wt_release_accesses(&txn_ctxs[j], cascading_abort, true);
                        }

#if WORKLOAD == TPCC
                        //TODO(tq): refactor this to benchmark implementation
                        txn_ctxs[j].o_id.store(-1);
#endif
                    }

                    INC_STATS(_thd_id, txn_cnt, commit_cnt);
                    INC_STATS(_thd_id, total_txn_abort_cnt, planner_batch_size-commit_cnt);

                    INC_STATS(_thd_id,exec_txn_commit_time[_thd_id],get_sys_clock() - quecc_commit_starttime);

                    while(!work_queue.batch_pg_map[batch_slot][i].status.compare_exchange_strong(expected8, desired8)){
                        M_ASSERT_V(false, "Reset failed for PG map, this should not happen\n");
                    };
                }

//                DEBUG_Q("ET_%ld: allowing PTs to procced, wbatch_id = %ld at slot = %ld"
//                                "\n",
//                        _thd_id,  wbatch_id, batch_slot);
                desired8 = 0;
                expected8 = g_thread_cnt;
                while(!work_queue.batch_map_comp_cnts[batch_slot].compare_exchange_strong(expected8, desired8)){};
//                INC_STATS(_thd_id, txn_cnt, g_batch_size);
            }
            else {
                if (idle_starttime > 0){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                }
                idle_starttime = get_sys_clock();
//                DEBUG_Q("ET_%ld: Done with my batch partition going to wait for others\n", _thd_id);
                while (!simulation->is_done() && work_queue.batch_map_comp_cnts[batch_slot].load() != 0){
                    // SPINN Here untill all ETs are done and batch is committed
                }
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                idle_starttime =0;
            }
#endif //if CT_ENABLED
            //TODO(tq) fix stat collection for idle time to include the spinning below

#endif // COMMIT_BEHAVIOR == AFTER_BATCH_COMP

#endif // if PARALLEL_COMMIT
            wbatch_id++;
//            DEBUG_Q("ET_%ld : ** Committed batch %ld, and moving to next batch %ld at [%ld][%ld][%ld] \n",
//                    _thd_id, wbatch_id-1, wbatch_id, batch_slot, _thd_id, wplanner_id);
        }
#else
        // Plan
//        DEBUG_Q("WT_%ld: going to plan batch_id = %ld at batch_slot = %ld, plan_comp_cnt = %d\n",
//                _thd_id, wbatch_id, batch_slot, work_queue.batch_plan_comp_cnts[batch_slot].load());
//        DEBUG_Q("WT_%ld: going to plan batch_id = %ld at batch_slot = %ld\n",
//                _thd_id, wbatch_id, batch_slot);
#if PROFILE_EXEC_TIMING
        if (batch_proc_starttime > 0){
            hl_prof_starttime = get_sys_clock();
        }
#endif
        src = plan_batch(batch_slot, my_txn_man);
#if PROFILE_EXEC_TIMING
        if (batch_proc_starttime > 0) {
            INC_STATS(_thd_id, wt_hl_plan_time[_thd_id], get_sys_clock() - hl_prof_starttime);
        }
#endif
        if (src == SUCCESS){
        //Sync
//            DEBUG_Q("WT_%ld: going to sync for planning phase end, batch_id = %ld at batch_slot = %ld\n", _thd_id, wbatch_id, batch_slot);
#if PROFILE_EXEC_TIMING
            if (batch_proc_starttime > 0){
                hl_prof_starttime = get_sys_clock();
            }
#endif
//            src = sync_on_planning_phase_end(batch_slot);
#if PROFILE_EXEC_TIMING
            if (batch_proc_starttime > 0){
                INC_STATS(_thd_id, wt_hl_sync_plan_time[_thd_id], get_sys_clock()-hl_prof_starttime);
            }
#endif
            if (src == BREAK){
                goto end_et;
            }

//            DEBUG_Q("WT_%ld: Starting execution phase, batch_id = %ld at batch_slot = %ld, idle_time = %f\n",
//                    _thd_id, wbatch_id, batch_slot, stats._stats[_thd_id]->exec_idle_time[_thd_id]/BILLION);
        // Execute
#if PROFILE_EXEC_TIMING
            if (batch_proc_starttime > 0){
                hl_prof_starttime = get_sys_clock();
            }
#endif
//            M_ASSERT_V(wbatch_id == work_queue.gbatch_id, "ET_%ld: exec stage - batch id mismatch wbatch_id=%ld, gbatch_id=%ld\n", _thd_id, wbatch_id, work_queue.gbatch_id);
            src = execute_batch(batch_slot,eq_comp_cnts, my_txn_man);
#if PROFILE_EXEC_TIMING
            if (batch_proc_starttime > 0){
                INC_STATS(_thd_id, wt_hl_exec_time[_thd_id], get_sys_clock()-hl_prof_starttime);
            }
#endif
            if (src == SUCCESS) {

                // ET is done with its PGs for the current batch
//            DEBUG_Q("ET_%ld: done with my batch partitions, batch_id = %ld, going to wait for all ETs to finish\n", _thd_id, wbatch_id);

//            DEBUG_Q("N_%u:WT_%lu: going to sync for execution phase end, batch_id = %ld at batch_slot = %ld\n",g_node_id,_worker_cluster_wide_id, wbatch_id, batch_slot);
                // Sync
                // spin on map_comp_cnts
#if PROFILE_EXEC_TIMING
                if (batch_proc_starttime > 0){
                    hl_prof_starttime = get_sys_clock();
                }
#endif
//                src = sync_on_execution_phase_end(batch_slot);
#if PROFILE_EXEC_TIMING
                if (batch_proc_starttime > 0){
                    INC_STATS(_thd_id, wt_hl_sync_exec_time[_thd_id], get_sys_clock()-hl_prof_starttime);
                }
#endif
                if (src == BREAK){
                    goto end_et;
                }
                //Commit
//                DEBUG_Q("N_%u:ET_%ld: starting parallel commit, batch_id = %ld\n",g_node_id,_worker_cluster_wide_id, wbatch_id);

                // process txn contexts in the current batch that are assigned to me (deterministically)
#if !EARLY_CL_RESPONSE
#if PROFILE_EXEC_TIMING
                if (batch_proc_starttime > 0){
                    hl_prof_starttime = get_sys_clock();
                }
#endif
//                M_ASSERT_V(wbatch_id == work_queue.gbatch_id, "ET_%ld: commit stage - batch id mismatch wbatch_id=%ld, gbatch_id=%ld\n", _thd_id, wbatch_id, work_queue.gbatch_id);
                src = commit_batch(batch_slot);

                if (src == BREAK){
                    goto end_et;
                }
#if PROFILE_EXEC_TIMING
                if (batch_proc_starttime > 0){
                    INC_STATS(_thd_id, wt_hl_commit_time[_thd_id], get_sys_clock()-hl_prof_starttime);
                }
#endif

                // indicate that I am done with all commit phase
                DEBUG_Q("N_%u:ET_%ld: is done with commit task, going to cleanup and sync for commit\n",g_node_id, _worker_cluster_wide_id);
#endif


                // Cleanup my parts of the batch
#if PROFILE_EXEC_TIMING
                if (batch_proc_starttime > 0){
                    hl_prof_starttime = get_sys_clock();
                }
#endif
#if !LADS_IN_QUECC
                batch_cleanup(batch_slot);
#endif
#if PROFILE_EXEC_TIMING
                if (batch_proc_starttime > 0){
                    INC_STATS(_thd_id, wt_hl_cleanup_time[_thd_id], get_sys_clock()-hl_prof_starttime);
                }
#endif

#if PROFILE_EXEC_TIMING
                if (batch_proc_starttime > 0){
                    hl_prof_starttime = get_sys_clock();
                }
#endif
#if SYNC_ON_COMMIT
                src = sync_on_commit_phase_end(batch_slot);
#endif
#if PROFILE_EXEC_TIMING
                if (batch_proc_starttime > 0){
                    INC_STATS(_thd_id, wt_hl_sync_commit_time[_thd_id], get_sys_clock()-hl_prof_starttime);
                }
#endif
                if (src == BREAK){
                    goto end_et;
                }


                wbatch_id++;
#if PIPELINED2
                if (_planner_id < g_plan_thread_cnt){
                    DEBUG_Q("N_%u:WT_%ld : ** Planned batch %ld, and moving to next batch %ld\n",
                            g_node_id,_worker_cluster_wide_id, wbatch_id-1, wbatch_id);
                }
                else{
                    DEBUG_Q("N_%u:WT_%ld : ** Committed batch %ld, and moving to next batch %ld\n",
                            g_node_id,_worker_cluster_wide_id, wbatch_id-1, wbatch_id);
                }
#else
                DEBUG_Q("N_%u:ET_%ld : ** Committed batch %ld, and moving to next batch %ld\n",
                    g_node_id,_worker_cluster_wide_id, wbatch_id-1, wbatch_id);
#endif
            }
        }
#endif //if PIPELINED
#if PROFILE_EXEC_TIMING
        if(batch_proc_starttime > 0){
            INC_STATS(_thd_id, exec_batch_proc_time[_thd_id], get_sys_clock()-batch_proc_starttime);
            batch_proc_starttime =0;
        }
#endif

#elif CC_ALG == DUMMY_CC
        Message * msg = work_queue.dequeue(get_thd_id());
        if(!msg) {
            if(idle_starttime ==0){
                idle_starttime = get_sys_clock();
            }
            continue;
        }
        if(idle_starttime > 0) {
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
            idle_starttime = 0;
        }

        // this does nothing on receiving a client query, just respond with commit
        if (msg->rtype == CL_QRY){

#if !SERVER_GENERATE_QUERIES
            Message * rsp_msg = Message::create_message(CL_RSP);
            rsp_msg->txn_id = 0;
            rsp_msg->batch_id = 0; // using batch_id from local, we can also use the one in the context
            ((ClientResponseMessage *) rsp_msg)->client_startts = ((ClientQueryMessage *)msg)->client_startts;
            rsp_msg->lat_work_queue_time = 0;
            rsp_msg->lat_msg_queue_time = 0;
            rsp_msg->lat_cc_block_time = 0;
            rsp_msg->lat_cc_time = 0;
            rsp_msg->lat_process_time = 0;
            rsp_msg->lat_network_time = 0;
            rsp_msg->lat_other_time = 0;

            msg_queue.enqueue(get_thd_id(), rsp_msg, ((ClientQueryMessage *)msg)->return_node_id);
#endif
            INC_STATS(get_thd_id(), txn_cnt, 1);
            msg->release();
            // explicitly release message
            // TQ: this does not seem to be called within releasing the message
            // which may be resulting in a memory leak
            // TODO(tq): verify that there is no memory leak
            Message::release_message(msg);
        }
#else
        Message * msg = work_queue.dequeue(_thd_id);
        if(!msg) {
#if PROFILE_EXEC_TIMING
            if(idle_starttime ==0){
                idle_starttime = get_sys_clock();
            }
#endif
            continue;
        }
#if PROFILE_EXEC_TIMING
        if(idle_starttime > 0) {
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
            idle_starttime = 0;
        }
#endif

        //uint64_t starttime = get_sys_clock();
        if(msg->rtype != CL_QRY || CC_ALG == CALVIN) {
            // restarting working on an active transaction
          txn_man = get_transaction_manager(msg);
#if !SINGLE_NODE
          if (CC_ALG != CALVIN && IS_LOCAL(txn_man->get_txn_id())) {
            if (msg->rtype != RTXN_CONT && ((msg->rtype != RACK_PREP) || (txn_man->get_rsp_cnt() == 1))) {
              txn_man->txn_stats.work_queue_time_short += msg->lat_work_queue_time;
              txn_man->txn_stats.cc_block_time_short += msg->lat_cc_block_time;
              txn_man->txn_stats.cc_time_short += msg->lat_cc_time;
              txn_man->txn_stats.msg_queue_time_short += msg->lat_msg_queue_time;
              txn_man->txn_stats.process_time_short += msg->lat_process_time;
              /*
              if (msg->lat_network_time/BILLION > 1.0) {
                printf("%ld %d %ld -> %ld: %f %f\n",msg->txn_id, msg->rtype, msg->return_node_id,get_node_id() ,msg->lat_network_time/BILLION, msg->lat_other_time/BILLION);
              }
              */
              txn_man->txn_stats.network_time_short += msg->lat_network_time;
            }

          } else {
              txn_man->txn_stats.clear_short();
          }
          if (CC_ALG != CALVIN) {
            txn_man->txn_stats.lat_network_time_start = msg->lat_network_time;
            txn_man->txn_stats.lat_other_time_start = msg->lat_other_time;
          }
          txn_man->txn_stats.msg_queue_time += msg->mq_time;
          txn_man->txn_stats.msg_queue_time_short += msg->mq_time;
          msg->mq_time = 0;
          txn_man->txn_stats.work_queue_time += msg->wq_time;
          txn_man->txn_stats.work_queue_time_short += msg->wq_time;
          //txn_man->txn_stats.network_time += msg->ntwk_time;
          msg->wq_time = 0;
          txn_man->txn_stats.work_queue_cnt += 1;
#endif
#if PROFILE_EXEC_TIMING
          ready_starttime = get_sys_clock();
#endif
          bool ready = txn_man->unset_ready();
#if PROFILE_EXEC_TIMING
          INC_STATS(get_thd_id(),worker_activate_txn_time,get_sys_clock() - ready_starttime);
#endif
          if(!ready) {
            // Return to work queue, end processing
            DEBUG_Q("WT_%lu: txn_man to available for (%lu,%lu)\n",_thd_id, msg->get_txn_id(),msg->get_batch_id());
            work_queue.enqueue(get_thd_id(),msg,true);
            continue;
          }
          txn_man->register_thread(this);
        }

        process(msg);
#if PROFILE_EXEC_TIMING
        ready_starttime = get_sys_clock();
#endif
        if(txn_man) {
          bool ready = txn_man->set_ready();
          assert(ready);
        }
#if PROFILE_EXEC_TIMING
        INC_STATS(get_thd_id(),worker_deactivate_txn_time,get_sys_clock() - ready_starttime);
#endif

#if CC_ALG != CALVIN
#if !INIT_QUERY_MSGS
#if PROFILE_EXEC_TIMING
        // delete message
        ready_starttime = get_sys_clock();
#endif
        msg->release();
        Message::release_message(msg);
#endif //#if !INIT_QUERY_MSGS
#endif // #if CC_ALG != CALVIN
#if PROFILE_EXEC_TIMING
        INC_STATS(get_thd_id(),worker_release_msg_time,get_sys_clock() - ready_starttime);
#endif
#endif // if QueCCC
    }

#if CC_ALG == QUECC
    end_et:
    for (uint64_t j = 0; j < exec_queues->size(); ++j) {
        Array<exec_queue_entry> * eq = exec_queues->get(j);
        quecc_pool.exec_queue_release(eq,0,0);
    }
    exec_queues->clear();
    exec_queues->release();

#if ISOLATION_LEVEL == READ_COMMITTED
    for (uint64_t j = 0; j < ro_exec_queues->size(); ++j) {
        Array<exec_queue_entry> * eq = ro_exec_queues->get(j);
        quecc_pool.exec_queue_release(eq,0,0);
    }
    ro_exec_queues->clear();
    ro_exec_queues->release();
#endif

#endif

    printf("FINISH WT %ld:%ld\n", _node_id, _thd_id);
    fflush(stdout);
    return FINISH;
}

RC WorkerThread::process_rfin(Message *msg) {
    DEBUG("RFIN %ld\n", msg->get_txn_id());
    assert(CC_ALG != CALVIN);

    M_ASSERT_V(!IS_LOCAL(msg->get_txn_id()), "RFIN local: %ld %ld/%d\n", msg->get_txn_id(),
               msg->get_txn_id() % g_node_cnt, g_node_id);
#if CC_ALG == MAAT
    txn_man->set_commit_timestamp(((FinishMessage*)msg)->commit_timestamp);
#endif
    RC mrc = ((FinishMessage *) msg)->rc;
    if (mrc == Abort) {
#if ABORT_MODE
    txn_man->dd_abort = ((FinishMessage *) msg)->dd_abort;
#endif
        txn_man->abort();
        txn_man->reset();
        txn_man->reset_query();
//        msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, RACK_FIN), GET_NODE_ID(msg->get_txn_id()));
        assert(txn_man->get_txn_id() == msg->get_txn_id());
        msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, RACK_FIN), msg->get_return_id());
        return mrc;
    }
    txn_man->commit();
    //if(!txn_man->query->readonly() || CC_ALG == OCC)
    if (!((FinishMessage *) msg)->readonly || CC_ALG == MAAT || CC_ALG == OCC){
//        msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, RACK_FIN), GET_NODE_ID(msg->get_txn_id()));
        msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, RACK_FIN), msg->get_return_id());
    }
    release_txn_man();

    return RCOK;
}

RC WorkerThread::process_rack_prep(Message *msg) {
    DEBUG("RPREP_ACK %ld\n", msg->get_txn_id());

    RC rc = RCOK;

    int responses_left = txn_man->received_response(((AckMessage *) msg)->rc);
    assert(responses_left >= 0);
#if CC_ALG == MAAT
    // Integrate bounds
    uint64_t lower = ((AckMessage*)msg)->lower;
    uint64_t upper = ((AckMessage*)msg)->upper;
    if(lower > time_table.get_lower(get_thd_id(),msg->get_txn_id())) {
      time_table.set_lower(get_thd_id(),msg->get_txn_id(),lower);
    }
    if(upper < time_table.get_upper(get_thd_id(),msg->get_txn_id())) {
      time_table.set_upper(get_thd_id(),msg->get_txn_id(),upper);
    }
    DEBUG("%ld bound set: [%ld,%ld] -> [%ld,%ld]\n",msg->get_txn_id(),lower,upper,time_table.get_lower(get_thd_id(),msg->get_txn_id()),time_table.get_upper(get_thd_id(),msg->get_txn_id()));
    if(((AckMessage*)msg)->rc != RCOK) {
      time_table.set_state(get_thd_id(),msg->get_txn_id(),MAAT_ABORTED);
    }
#endif
    if (responses_left > 0)
        return WAIT;

    // Done waiting
    if (txn_man->get_rc() == RCOK) {
        rc = txn_man->validate();
    }
    if (rc == Abort || txn_man->get_rc() == Abort) {
        txn_man->txn->rc = Abort;
        rc = Abort;
    }
    txn_man->send_finish_messages();
    if (rc == Abort) {
        txn_man->abort();
    } else {
        txn_man->commit();
    }

    return rc;
}

RC WorkerThread::process_rack_rfin(Message *msg) {
    DEBUG("RFIN_ACK %ld\n", msg->get_txn_id());

    RC rc = RCOK;

    int responses_left = txn_man->received_response(((AckMessage *) msg)->rc);
    assert(responses_left >= 0);
    if (responses_left > 0)
        return WAIT;

    // Done waiting
    txn_man->txn_stats.twopc_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;

    if (txn_man->get_rc() == RCOK) {
        //txn_man->commit();
        commit();
    } else {
        //txn_man->abort();
        abort();
    }
    return rc;
}

RC WorkerThread::process_rqry_rsp(Message *msg) {
    DEBUG("RQRY_RSP txn_id=%ld\n", msg->get_txn_id());
    assert(IS_LOCAL(msg->get_txn_id()));

    txn_man->txn_stats.remote_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;

    if (((QueryResponseMessage *) msg)->rc == Abort) {
        txn_man->start_abort();
        return Abort;
    }

    RC rc = txn_man->run_txn();
    check_if_done(rc);

//    if (rc != RCOK || rc != Abort){
//        DEBUG_Q("TH_%ld: RC=%d\n", _thd_id,rc);
//    }

    return rc;

}

RC WorkerThread::process_rqry(Message *msg) {
    DEBUG("RQRY %ld\n", msg->get_txn_id());
    M_ASSERT_V(!IS_LOCAL(msg->get_txn_id()), "RQRY local: %ld %ld/%d\n", msg->get_txn_id(),
               msg->get_txn_id() % g_node_cnt, g_node_id);
    assert(!IS_LOCAL(msg->get_txn_id()));
    RC rc = RCOK;

    msg->copy_to_txn(txn_man);

#if CC_ALG == MVCC
    txn_table.update_min_ts(get_thd_id(),txn_man->get_txn_id(),0,txn_man->get_timestamp());
#endif
#if CC_ALG == MAAT
    time_table.init(get_thd_id(),txn_man->get_txn_id());
#endif

    rc = txn_man->run_txn();

    // Send response
    if (rc != WAIT) {
        msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, RQRY_RSP), txn_man->return_id);
    }
    return rc;
}

RC WorkerThread::process_rqry_cont(Message *msg) {
    DEBUG("RQRY_CONT %ld\n", msg->get_txn_id());
    assert(!IS_LOCAL(msg->get_txn_id()));
    RC rc = RCOK;

    txn_man->run_txn_post_wait();
    rc = txn_man->run_txn();

    // Send response
    if (rc != WAIT) {
        msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, RQRY_RSP), txn_man->return_id);
    }
    return rc;
}


RC WorkerThread::process_rtxn_cont(Message *msg) {
    DEBUG("RTXN_CONT %ld\n", msg->get_txn_id());
    assert(IS_LOCAL(msg->get_txn_id()));

    txn_man->txn_stats.local_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;

    txn_man->run_txn_post_wait();
    RC rc = txn_man->run_txn();
    check_if_done(rc);
    return RCOK;
}

RC WorkerThread::process_rprepare(Message *msg) {
    DEBUG("RPREP %ld\n", msg->get_txn_id());
    RC rc = RCOK;

    // Validate transaction
    rc = txn_man->validate();
    txn_man->set_rc(rc);
    msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, RACK_PREP), msg->return_node_id);
    // Clean up as soon as abort is possible
    if (rc == Abort) {
        txn_man->abort();
    }

    return rc;
}

uint64_t WorkerThread::get_next_txn_id() {
    uint64_t txn_id;
    do{
        txn_id = (get_node_id() + get_thd_id() * g_node_cnt)
                 + (g_thread_cnt * g_node_cnt * _thd_txn_id);
    }while(GET_NODE_ID(txn_id) != g_node_id);
    ++_thd_txn_id;
    return txn_id;
}

RC WorkerThread::process_rtxn(Message *msg) {
    RC rc = RCOK;
    uint64_t txn_id = UINT64_MAX;
    uint64_t ctid = _thd_id;

    if (msg->get_rtype() == CL_QRY) {
        // This is a new transaction

        // Only set new txn_id when txn first starts
        txn_id = get_next_txn_id();
        msg->txn_id = txn_id;

        // Put txn in txn_table
        DEBUG("WT_%ld: Getting txn man txn_id = %ld\n",_thd_id, txn_id);
        txn_man = txn_table.get_transaction_manager(ctid, txn_id, 0);
        txn_man->register_thread(this);
#if PROFILE_EXEC_TIMING
        uint64_t ready_starttime = get_sys_clock();
#endif
        bool ready = txn_man->unset_ready();
#if PROFILE_EXEC_TIMING
        INC_STATS(ctid, worker_activate_txn_time, get_sys_clock() - ready_starttime);
#endif
        assert(ready);
        if (CC_ALG == WAIT_DIE) {
            txn_man->set_timestamp(get_next_ts());
        }
#if PROFILE_EXEC_TIMING
        txn_man->txn_stats.starttime = get_sys_clock();
        txn_man->txn_stats.restart_starttime = txn_man->txn_stats.starttime;
        DEBUG("WT_%ld: START %ld %f %lu\n",_thd_id, txn_man->get_txn_id(), simulation->seconds_from_start(get_sys_clock()),
                txn_man->txn_stats.starttime);
#endif
        // this should make a copy of the query object to the txn_man
        msg->copy_to_txn(txn_man);
        INC_STATS(ctid, local_txn_start_cnt, 1);

    } else {
        txn_man->txn_stats.restart_starttime = get_sys_clock();
        txn_id = msg->txn_id;
        DEBUG("WT_%ld: RESTART %ld %f %lu\n", _thd_id, txn_man->get_txn_id(), simulation->seconds_from_start(get_sys_clock()),
              txn_man->txn_stats.starttime);
    }

    // Get new timestamps
    if (is_cc_new_timestamp()) {
        txn_man->set_timestamp(get_next_ts());
    }
#if CC_ALG == MVCC
    txn_table.update_min_ts(get_thd_id(),txn_id,0,txn_man->get_timestamp());
#endif

#if CC_ALG == OCC
    txn_man->set_start_timestamp(get_next_ts());
#endif
#if CC_ALG == MAAT
    time_table.init(get_thd_id(),txn_man->get_txn_id());
    assert(time_table.get_lower(get_thd_id(),txn_man->get_txn_id()) == 0);
    assert(time_table.get_upper(get_thd_id(),txn_man->get_txn_id()) == UINT64_MAX);
    assert(time_table.get_state(get_thd_id(),txn_man->get_txn_id()) == MAAT_RUNNING);
#endif

    rc = init_phase();
    if (rc != RCOK)
        return rc;

#if CC_ALG == HSTORE
    rc = part_lock_man.lock(txn_man, &txn_man->query->partitions, txn_man->query->partitions.size());
    if (rc == RCOK){
        // Execute transaction
        rc = txn_man->run_hstore_txn();
        part_lock_man.unlock(txn_man, &txn_man->query->partitions, txn_man->query->partitions.size());
//        DEBUG_Q("WT_%ld: executed multipart txn_id = %ld with RCOK = %d, is new order? %d\n",
//                _thd_id, txn_id, (rc == RCOK), ((TPCCQuery *)txn_man->query)->txn_type == TPCC_NEW_ORDER);
    }
    else{
        assert(rc == Abort);
        txn_man->start_abort();
    }
#else
    // Execute transaction
    rc = txn_man->run_txn();
#endif

    check_if_done(rc);

    return rc;
}

RC WorkerThread::init_phase() {
    RC rc = RCOK;
    //m_query->part_touched[m_query->part_touched_cnt++] = m_query->part_to_access[0];
    return rc;
}


RC WorkerThread::process_log_msg(Message *msg) {
    assert(ISREPLICA);
    DEBUG("REPLICA PROCESS %ld\n", msg->get_txn_id());
    LogRecord *record = logger.createRecord(&((LogMessage *) msg)->record);
    logger.enqueueRecord(record);
    return RCOK;
}

RC WorkerThread::process_log_msg_rsp(Message *msg) {
    DEBUG("REPLICA RSP %ld\n", msg->get_txn_id());
    txn_man->repl_finished = true;
    if (txn_man->log_flushed)
        commit();
    return RCOK;
}

RC WorkerThread::process_log_flushed(Message *msg) {
    DEBUG("LOG FLUSHED %ld\n", msg->get_txn_id());
    if (ISREPLICA) {
        msg_queue.enqueue(get_thd_id(), Message::create_message(msg->txn_id, LOG_MSG_RSP), GET_NODE_ID(msg->txn_id));
        return RCOK;
    }

    txn_man->log_flushed = true;
    if (g_repl_cnt == 0 || txn_man->repl_finished)
        commit();
    return RCOK;
}

RC WorkerThread::process_rfwd(Message *msg) {
    DEBUG("WT_%lu:RFWD (%ld,%ld)\n",_thd_id, msg->get_txn_id(), msg->get_batch_id());
#if CC_ALG == CALVIN
    txn_man->txn_stats.remote_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
    assert(CC_ALG == CALVIN);
#if ABORT_MODE
    auto rfwd_msg = (ForwardMessage *) msg;
    if (rfwd_msg->rc == Abort){
        txn_man->dd_abort = true;
        txn_man->set_rc(Abort);
    }
#endif
    int responses_left = txn_man->received_response(((ForwardMessage *) msg)->rc);
    assert(responses_left >= 0);
    if (txn_man->calvin_collect_phase_done()) {
        assert(ISSERVERN(txn_man->return_id));
#if ABORT_MODE
        if (((ForwardMessage *) msg)->rc == Abort){
            txn_man->set_rc(Abort);
            txn_man->dd_abort = true;
        }
#endif
        RC rc = txn_man->run_calvin_txn();
        if (rc == RCOK && txn_man->calvin_exec_phase_done()) {
            bool clean = txn_man->return_id != g_node_id;
            calvin_wrapup();
            if (clean){
                // participant node is not the same as sequencer node
                // release message here, otherwise sequencer will release
                // free msg, queries
#if WORKLOAD == YCSB
                YCSBClientQueryMessage* cl_msg = (YCSBClientQueryMessage*)msg;
#if !SINGLE_NODE
                for(uint64_t i = 0; i < cl_msg->requests.size(); i++) {
                    DEBUG_M("Sequencer::process_ack() ycsb_request free\n");
                    mem_allocator.free(cl_msg->requests[i],sizeof(ycsb_request));
                }
#endif
#elif WORKLOAD == TPCC
                TPCCClientQueryMessage* cl_msg = (TPCCClientQueryMessage*)msg;
#if !SINGLE_NODE
            if(cl_msg->txn_type == TPCC_NEW_ORDER) {
                for(uint64_t i = 0; i < cl_msg->items.size(); i++) {
                    DEBUG_M("Sequencer::process_ack() items free\n");
                    mem_allocator.free(cl_msg->items[i],sizeof(Item_no));
                }
            }
#endif
#elif WORKLOAD == PPS
            PPSClientQueryMessage* cl_msg = (PPSClientQueryMessage*)msg;

#endif
            }

            return RCOK;
        }
    }
    return WAIT;
#else
    return RCOK;
#endif


}
#if CC_ALG == CALVIN
RC WorkerThread::process_calvin_rtxn(Message *msg) {
    DEBUG_Q("N_%u:WT_%lu: START (%lu,%lu) %f %lu, seq_node=%lu\n", g_node_id, get_thd_id(), txn_man->get_txn_id(), txn_man->get_batch_id(), simulation->seconds_from_start(get_sys_clock()),
          txn_man->txn_stats.starttime, msg->return_node_id);
    assert(ISSERVERN(txn_man->return_id));
    txn_man->txn_stats.local_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
    // Execute
    RC rc = txn_man->run_calvin_txn();
    //if((txn_man->phase==6 && rc == RCOK) || txn_man->active_cnt == 0 || txn_man->participant_cnt == 1) {
    if (rc == RCOK && txn_man->calvin_exec_phase_done()) {
        bool clean = txn_man->return_id != g_node_id;
        calvin_wrapup();

        if (clean){

            // participant node is not the same as sequencer node
            // release message here, otherwise sequencer will release
            // free msg, queries
#if WORKLOAD == YCSB
        YCSBClientQueryMessage* cl_msg = (YCSBClientQueryMessage*)msg;
#if !SINGLE_NODE
            for(uint64_t i = 0; i < cl_msg->requests.size(); i++) {
                DEBUG_M("Sequencer::process_ack() ycsb_request free\n");
                mem_allocator.free(cl_msg->requests[i],sizeof(ycsb_request));
            }
#endif
#elif WORKLOAD == TPCC
        TPCCClientQueryMessage* cl_msg = (TPCCClientQueryMessage*)msg;
#if !SINGLE_NODE
            if(cl_msg->txn_type == TPCC_NEW_ORDER) {
                for(uint64_t i = 0; i < cl_msg->items.size(); i++) {
                    DEBUG_M("Sequencer::process_ack() items free\n");
                    mem_allocator.free(cl_msg->items[i],sizeof(Item_no));
                }
            }
#endif
#elif WORKLOAD == PPS
            PPSClientQueryMessage* cl_msg = (PPSClientQueryMessage*)msg;

#endif
        }

    }
    return rc;

}
#endif
bool WorkerThread::is_cc_new_timestamp() {
    return (CC_ALG == MVCC || CC_ALG == TIMESTAMP);
}

ts_t WorkerThread::get_next_ts() {
    if (g_ts_batch_alloc) {
        if (_curr_ts % g_ts_batch_num == 0) {
            _curr_ts = glob_manager.get_ts(get_thd_id());
            _curr_ts++;
        } else {
            _curr_ts++;
        }
        return _curr_ts - 1;
    } else {
        _curr_ts = glob_manager.get_ts(get_thd_id());
        return _curr_ts;
    }
}

WorkerThread::~WorkerThread() {

}



