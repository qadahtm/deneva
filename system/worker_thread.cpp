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

void WorkerThread::process(Message *msg) {
    RC rc __attribute__ ((unused));

    DEBUG("%ld Processing %ld %d\n", get_thd_id(), msg->get_txn_id(), msg->get_rtype());
//    DEBUG_Q("%ld Processing %ld %d \n",get_thd_id(),msg->get_txn_id(),msg->get_rtype());

    assert(msg->get_rtype() == CL_QRY || msg->get_txn_id() != UINT64_MAX);
    uint64_t starttime = get_sys_clock();
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
    uint64_t timespan = get_sys_clock() - starttime;
    INC_STATS(_thd_id, worker_process_cnt, 1);
    INC_STATS(_thd_id, worker_process_time, timespan);
    INC_STATS(_thd_id, worker_process_cnt_by_type[msg->rtype], 1);
    INC_STATS(_thd_id, worker_process_time_by_type[msg->rtype], timespan);
    DEBUG("%ld EndProcessing %d %ld\n", get_thd_id(), msg->get_rtype(), msg->get_txn_id());
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

void WorkerThread::calvin_wrapup() {
    DEBUG("(%ld,%ld) calvin ack to %ld\n", txn_man->get_txn_id(), txn_man->get_batch_id(), txn_man->return_id);
    txn_man->release_locks(RCOK);
    txn_man->commit_stats();
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

// Can't use txn_man after this function
void WorkerThread::commit() {
    //TxnManager * txn_man = txn_table.get_transaction_manager(txn_id,0);
    //txn_man->release_locks(RCOK);
    //        txn_man->commit_stats();
    assert(txn_man);
//    assert(IS_LOCAL(txn_man->get_txn_id()));

    uint64_t timespan = get_sys_clock() - txn_man->txn_stats.starttime;
//    DEBUG_Q("COMMIT %ld %f -- %f\n", txn_man->get_txn_id(), simulation->seconds_from_start(get_sys_clock()),
//          (double) timespan / BILLION);
    DEBUG("COMMIT %ld %f -- %f\n",txn_man->get_txn_id(),simulation->seconds_from_start(get_sys_clock()),(double)timespan/ BILLION);
    // Send result back to client
#if !SERVER_GENERATE_QUERIES
    msg_queue.enqueue(_thd_id, Message::create_message(txn_man, CL_RSP), txn_man->client_id);
#endif
    // remove txn from pool
    // -- actually return to txn_mgr pool
    release_txn_man();
    // Do not use txn_man after this

}

void WorkerThread::abort() {

    DEBUG("ABORT %ld -- %f\n", txn_man->get_txn_id(), (double) get_sys_clock() - run_starttime / BILLION);
    // TODO: TPCC Rollback here

    ++txn_man->abort_cnt;
    txn_man->reset();
#if ABORT_THREAD
    uint64_t penalty = abort_queue.enqueue(get_thd_id(), txn_man->get_txn_id(), txn_man->get_abort_cnt());

    txn_man->txn_stats.total_abort_time += penalty;
#endif
}

TxnManager *WorkerThread::get_transaction_manager(Message *msg) {
#if CC_ALG == CALVIN
    TxnManager * local_txn_man = txn_table.get_transaction_manager(get_thd_id(),msg->get_txn_id(),msg->get_batch_id());
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

RC WorkerThread::run_fixed_mode() {
    tsetup();
    printf("Running WorkerThread %ld\n", _thd_id);
    M_ASSERT_V(false, "Fixed mode is not supported\n");
//
//#if CC_ALG == QUECC
////    uint64_t quecc_prof_time = 0;
////#if COMMIT_BEHAVIOR == IMMEDIATE
////    uint64_t quecc_commit_starttime = 0;
////#endif
//    uint64_t quecc_batch_proc_starttime = 0;
//    uint64_t quecc_batch_part_proc_starttime = 0;
////    uint64_t quecc_mem_free_startts = 0;
//    uint64_t wplanner_id = 0;
//
//    Array<exec_queue_entry> *exec_q = NULL;
//    batch_partition * batch_part = NULL;
////    uint64_t desired = 0;
////    uint64_t expected = 0;
////#if COMMIT_BEHAVIOR != IMMEDIATE
////    uint8_t desired8 = 0;
////    uint8_t expected8 = 0;
////#endif
//    RC rc __attribute__ ((unused)) = RCOK;
//
//    TxnManager * my_txn_man;
//    _wl->get_txn_man(my_txn_man);
//    my_txn_man->init(_thd_id, _wl);
//
//    for (uint64_t batch_slot = 0; batch_slot < g_batch_map_length; batch_slot++){
//        txn_man = NULL;
//        // allows using the batch_map in circular manner
//
//        while (true){
//#if BATCH_MAP_ORDER == BATCH_ET_PT
//            batch_part = (batch_partition *)  work_queue.batch_map[batch_slot][_thd_id][wplanner_id].load();
//#else
//            batch_part = (batch_partition *)  work_queue.batch_map[batch_slot][wplanner_id][_thd_id].load();
//#endif
//            M_ASSERT_V(((uint64_t) batch_part) != 0, "In fixed mode this should not happen\n");
////            M_ASSERT_V(batch_part->batch_id == wbatch_id, "Batch part map slot [%ld][%ld][%ld],"
////                    " wbatch_id=%ld, batch_part_batch_id = %ld\n",
////                       batch_slot,_thd_id, wplanner_id, wbatch_id, batch_part->batch_id);
//
//            bool batch_partition_done = false;
//            uint64_t w_exec_q_index = 0;
//            while (!batch_partition_done){
//
//                //select an execution queue to work on.
//                if (batch_part->single_q){
//                    exec_q = batch_part->exec_q;
//                }
//                else {
////                    desired = WORKING;
////                    expected = AVAILABLE;
////                    if (!batch_part->exec_qs_status[w_exec_q_index].compare_exchange_strong(expected, desired)){
////                        // we need to fail here because we should be the first one
////                        M_ASSERT_V(false, "ET_%ld : Could not reserve exec_q at %ld, status = %ld, wbatch_id = %ld, batch_id = %ld\n",
////                                   _thd_id, w_exec_q_index, batch_part->exec_qs_status[w_exec_q_index].load(), wbatch_id, batch_part->batch_id);
////                    }
//                    exec_q = batch_part->exec_qs->get(w_exec_q_index);
//                }
//
//                // process exec_q
//                quecc_batch_part_proc_starttime = get_sys_clock();
//                if(idle_starttime > 0) {
//                    INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
//                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
//                    idle_starttime = 0;
//                }
//                if (wplanner_id == 0 && quecc_batch_proc_starttime == 0){
//                    quecc_batch_proc_starttime = get_sys_clock();
//                }
//
////            DEBUG_Q("Processing batch partition from planner[%ld]\n", wplanner_id);
//
//                for (uint64_t i = 0; i < exec_q->size(); ++i) {
//                    exec_queue_entry exec_qe  __attribute__ ((unused));
//                    exec_qe = exec_q->get(i);
//
////                    M_ASSERT_V(exec_qe.txn_id == exec_qe.txn_ctx->txn_id,
////                               "ET_%ld : Executed QueCC txn fragment, txn_id mismatch, wbatch_id = %ld, ctx_batch_id = %ld, entry_txn_id = %ld, ctx_txn_id = %ld, \n",
////                               _thd_id, wbatch_id, exec_qe.txn_ctx->batch_id, exec_qe.txn_id, exec_qe.txn_ctx->txn_id);
////                    M_ASSERT_V(exec_qe.txn_ctx->batch_id == wbatch_id,
////                               "ET_%ld : Executed QueCC txn fragment, batch_id mismatch wbatch_id = %ld, ctx_batch_id = %ld, entry_txn_id = %ld, ctx_txn_id = %ld, \n",
////                               _thd_id, wbatch_id, exec_qe.txn_ctx->batch_id, exec_qe.txn_id, exec_qe.txn_ctx->txn_id);
////
////                    M_ASSERT_V(exec_qe.batch_id == wbatch_id, "Batch part map slot [%ld][%ld][%ld], batch_id mismatch"
////                            " wbatch_id=%ld, ebatch_id = %ld, at exec_q_entry[%ld]\n",
////                               batch_slot,_thd_id, wplanner_id, wbatch_id, exec_qe.batch_id, i);
//
//                    // Use txnManager to execute transaction frament
//#if QUECC_DB_ACCESS
//                    rc = my_txn_man->run_quecc_txn(&exec_qe);
//                    if (!simulation->is_done()){
//                        assert(rc == RCOK);
//                    }
//                    else{
//                        break;
//                    }
//
//#endif
////#if COMMIT_BEHAVIOR == IMMEDIATE
////                    uint64_t comp_cnt;
////#endif
////                    if (rc == RCOK){
////                        quecc_prof_time = get_sys_clock();
////#if COMMIT_BEHAVIOR == IMMEDIATE
////                        comp_cnt = exec_qe.txn_ctx->completion_cnt.fetch_add(1);
////#endif
////                        INC_STATS(_thd_id, exec_txn_ctx_update[_thd_id], get_sys_clock()-quecc_prof_time);
////                        INC_STATS(_thd_id, exec_txn_frag_cnt[_thd_id], 1);
////                    }
//
//
//                    // TQ: we are committing now, which is done one by one the threads only
//                    // this allows lower latency for transactions.
//                    // Since there is no logging this is okay
//                    // TODO(tq): consider committing as a batch with logging enabled
//                    // Execution thrad that will execute the last operation will commit
////#if COMMIT_BEHAVIOR == IMMEDIATE
////                    if (comp_cnt == (REQ_PER_QUERY-1)) {
////#if CT_ENABLED
////                    exec_qe.txn_ctx->txn_state = TXN_READY_TO_COMMIT;
////#else
//
////                        quecc_commit_starttime = get_sys_clock();
////                  DEBUG_Q("Commting txn %ld, with e_thread %ld\n", exec_qe.txn_id, get_thd_id());
////                  DEBUG_Q("txn_man->return_id = %ld , exec_qe.return_node_id = %ld, g_node_id= %d\n",
////                          txn_man->return_id, exec_qe.return_node_id, g_node_id);
//
////                    DEBUG_Q("thread_%ld: Committing with txn(%ld,%ld,%ld)\n", _thd_id, wbatch_id, exec_qe.txn_id,
////                            exec_qe.req_id);
//
//                        // We are ready to commit, now we need to check if we need to abort.
//
//                        // Committing
//                        // Sending response to client a
////                        quecc_prof_time = get_sys_clock();
////#if !SERVER_GENERATE_QUERIES
////                        Message * rsp_msg = Message::create_message(CL_RSP);
////                    rsp_msg->txn_id = exec_qe.txn_id;
////                    rsp_msg->batch_id = wbatch_id; // using batch_id from local, we can also use the one in the context
////                    ((ClientResponseMessage *) rsp_msg)->client_startts = exec_qe.txn_ctx->client_startts;
//////                    ((ClientResponseMessage *) rsp_msg)->batch_id = wbatch_id;
////                    rsp_msg->lat_work_queue_time = 0;
////                    rsp_msg->lat_msg_queue_time = 0;
////                    rsp_msg->lat_cc_block_time = 0;
////                    rsp_msg->lat_cc_time = 0;
////                    rsp_msg->lat_process_time = 0;
////                    rsp_msg->lat_network_time = 0;
////                    rsp_msg->lat_other_time = 0;
////
////                    msg_queue.enqueue(get_thd_id(), rsp_msg, exec_qe.return_node_id);
////                    INC_STATS(_thd_id, exec_resp_msg_create_time[_thd_id], get_sys_clock()-quecc_prof_time);
////#endif
////#if COMMIT_BEHAVIOR == IMMEDIATE
////                        INC_STATS(_thd_id, txn_cnt, 1);
////#endif
////                        INC_STATS(_thd_id, exec_txn_cnts[_thd_id], 1);
//
//                        //TODO(tq): how to handle txn_contexts in this case
//                        // Free memory
//                        // Free txn context
////                    DEBUG_Q("ET_%ld : commtting txn_id = %ld with comp_cnt %ld\n", _thd_id, exec_qe.txn_ctx->txn_id, comp_cnt);
//
//                        // we always commit
////                        INC_STATS(_thd_id, exec_txn_commit_time[_thd_id], get_sys_clock()-quecc_commit_starttime);
//                        //TODO(tq): how to handle logic-induced aborts
//
////                    }
////#endif
//                }
//                // recycle exec_q
////                quecc_mem_free_startts = get_sys_clock();
////                exec_queue_release(exec_q, wplanner_id);
////                quecc_pool.exec_queue_release(exec_q, wplanner_id,_thd_id);
////                INC_STATS(_thd_id, exec_mem_free_time[_thd_id], get_sys_clock() - quecc_mem_free_startts);
//
//                if (!batch_part->single_q){
//                    // set the status of this processed EQ to complete
//                    // TODO(tq): use pre-constants instead of literals
////                    desired = COMPLETED;
////                    expected = WORKING;
////
////                    if (!batch_part->exec_qs_status[w_exec_q_index].compare_exchange_strong(expected, desired)){
////                        // we need to fail here because we should be the only one who can do this
////                        M_ASSERT_V(false, "ET_%ld : Could not set exec_q at %ld to COMPLETED\n", _thd_id, w_exec_q_index);
////                    }
//
//                    // move the next exec_q withing same batch partition
//                    w_exec_q_index++;
//
//                    // check if we are done with all exec_qs within the same batch_partition
//                    if (w_exec_q_index == batch_part->exec_qs->size()){
//                        batch_partition_done = true;
//                    }
//                }
//                else{
//                    batch_partition_done = true;
//                }
//            }
//
//            // reset batch_map_slot to zero after processing it
//            // reset map slot to 0 to allow planners to use the slot
//            // removed for fixed-mode
////        desired = 0;
////        expected = (uint64_t) batch_part;
////        while(!work_queue.batch_map[batch_slot][_thd_id][wplanner_id].compare_exchange_strong(
////                expected, desired)){
////            DEBUG_Q("ET_%ld: failing to RESET map slot \n", _thd_id);
////        }
//
//            if (!batch_part->single_q){
//                // release exec_queues
//                for (uint64_t i=0; i < batch_part->exec_qs->size(); ++i){
//                    exec_q = batch_part->exec_qs->get(i);
//                    quecc_pool.exec_queue_release(exec_q, wplanner_id, _thd_id);
//                }
//                // free batch_partition
//                batch_part->exec_qs->clear();
//                quecc_pool.exec_qs_release(batch_part->exec_qs, wplanner_id);
//
////                while(!work_queue.exec_qs_free_list[wplanner_id]->push(batch_part->exec_qs)){
////                    M_ASSERT_V(false, "Should not happen");
////                };
//                // TODO(tq): recycle insted
////                mem_allocator.free(batch_part->exec_qs_status, sizeof(atomic<uint64_t> *)*batch_part->sub_exec_qs_cnt);
////                quecc_pool.exec_qs_status_release(batch_part->exec_qs_status, wplanner_id,_thd_id);
//            }
//            else{
//                quecc_pool.exec_queue_release(batch_part->exec_q, wplanner_id,_thd_id);
//            }
//
////        DEBUG_Q("For batch %ld , batch partition processing complete at map slot [%ld][%ld][%ld] \n",
////                wbatch_id, batch_slot, _thd_id, wplanner_id);
//
//            // free batch_part
//            //TODO(tq): use pool and recycle
////            mem_allocator.free(batch_part, sizeof(batch_partition));
//            quecc_pool.batch_part_release(batch_part, wplanner_id, _thd_id);
//
//
////#if CT_ENABLED && COMMIT_BEHAVIOR == AFTER_PG_COMP
////            work_queue.batch_map_comp_cnts[batch_slot][wplanner_id].fetch_add(1);
////
////        // before going to the next planner, spin here if not all other partitions of the same planners have completed
////        // we actually need to wait untill the priority group has been fully committed.
////
////        while (work_queue.batch_map_comp_cnts[batch_slot][wplanner_id].load() != 0){
////            // spin
//////            DEBUG_Q("ET_%ld : Spinning waiting for priority group %ld to be COMMITTED\n", _thd_id, wplanner_id);
////        }
//////        DEBUG_Q("ET_%ld : Going to process the next priority group %ld in batch_id = %ld\n", _thd_id, priority_group, wbatch_id);
////#endif
//            // go to the next batch partition prepared by the next planner since the previous one has been committed
//            wplanner_id++;
//            if (wplanner_id == g_plan_thread_cnt) {
////                DEBUG_Q("ET_%ld: done with all PGs, wbatch_id = %ld at slot = %ld"
////                                "\n",
////                        _thd_id,  wbatch_id, batch_slot);
//
////#if COMMIT_BEHAVIOR == AFTER_BATCH_COMP
////                work_queue.batch_map_comp_cnts[batch_slot].fetch_add(1);
////                if (_thd_id == 0){
////
////                    // Thread 0 acts as commit thread
////
////                    // Wait for all transactions to complete their PG within the current batch
////                    while ((work_queue.batch_map_comp_cnts[batch_slot].load() != g_thread_cnt)){
////                        // SPINN Here untill all ETs are done
//////                    DEBUG_Q("ET_%ld: waiting for other ETs to be done, wbatch_id = %ld at slot = %ld, val = %d, thd_cnt = %d"
//////                                    "\n",
//////                            _thd_id,  wbatch_id, batch_slot,work_queue.batch_map_comp_cnts[batch_slot].load(), g_thread_cnt);
////                    }
////                    DEBUG_Q("ET_%ld: All ETs are done for, wbatch_id = %ld at slot = %ld"
////                                "\n",
////                        _thd_id,  wbatch_id, batch_slot);
////
////                    // TODO(tq): add check dependencies here
////
////                    // consider all transactions committed
////                    INC_STATS(_thd_id, txn_cnt, g_batch_size);
////
////                    // TODO(tq): use RR for this, now we just statically asisng this task to the ET_0
//////                    desired = PG_AVAILABLE;
//////                    expected = PG_READY;
//////                    for (uint64_t i =0; i < g_plan_thread_cnt; ++i){
//////                        while(!work_queue.batch_pg_map[batch_slot][i].status.compare_exchange_strong(expected, desired)){};
//////                    }
////
//////                DEBUG_Q("ET_%ld: allowing PTs to procced, wbatch_id = %ld at slot = %ld"
//////                                "\n",
//////                        _thd_id,  wbatch_id, batch_slot);
////                    desired8 = 0;
////                    expected8 = g_thread_cnt;
////                    while(!work_queue.batch_map_comp_cnts[batch_slot].compare_exchange_strong(expected8, desired8)){};
////                }
////                else {
////                    while (work_queue.batch_map_comp_cnts[batch_slot].load() != 0){
////                        // SPINN Here until batch is committed
//////                        DEBUG_Q("ET_%ld: waiting for main ET to be done, wbatch_id = %ld at slot = %ld, val = %d, thd_cnt = %d"
//////                                        "\n",
//////                                _thd_id,  wbatch_id, batch_slot,work_queue.batch_map_comp_cnts[batch_slot].load(), g_thread_cnt);
////                    }
////                }
////                //TODO(tq) fix stat collection for idle time to include the spinning below
////
////#endif
//                wbatch_id++;
//                wplanner_id = 0;
////                DEBUG_Q("ET_%ld : ** Completed batch %ld, and moving to next batch %ld at [%ld][%ld][%ld] \n",
////                    _thd_id, wbatch_id-1, wbatch_id, batch_slot, _thd_id, wplanner_id);
//                INC_STATS(_thd_id, exec_batch_cnt[_thd_id], 1);
//                INC_STATS(_thd_id, exec_batch_proc_time[_thd_id], get_sys_clock() - quecc_batch_proc_starttime);
//                quecc_batch_proc_starttime = 0;
//                INC_STATS(_thd_id, exec_batch_part_proc_time[_thd_id], get_sys_clock()-quecc_batch_part_proc_starttime);
//                INC_STATS(_thd_id, exec_batch_part_cnt[_thd_id], 1);
//                break;
//            }
//        } // end of while(true) -- this should be an indication of a completed batch
//
//    } // end of for loop
//#else
//    M_ASSERT_V(false, "Selected cc_alg is not supported by fixed_mode. Use normal_mode instead.\n");
//
////#if CC_ALG == CALVIN || CC_ALG == MAAT
////    M_ASSERT_V(false, "Selected cc_alg is not supported by fixed_mode. Use normal_mode instead.\n");
////#endif
//
//#if !SERVER_GENERATE_QUERIES
//    M_ASSERT_V(false, "Set server_generate_queries to true for fixed_mode to work.\n");
//#endif
//
//    M_ASSERT_V(((g_batch_size*g_batch_map_length) % g_thread_cnt) == 0, "Transaction workload cannot be divided evenly to threads.\n");
//
//    uint64_t thd_wl_cnt = (g_batch_size*g_batch_map_length)/g_thread_cnt;
//    uint64_t ready_starttime;
//
//    for (uint64_t k=0; k < thd_wl_cnt; ++k){
//        Message * msg = work_queue.dequeue(get_thd_id());
//        if(!msg) {
//            M_ASSERT_V(false, "This should not happen.\n");
//        }
//        if(!(msg->rtype == CL_QRY || msg->rtype == RTXN)){
//            M_ASSERT_V(false, "This should not happen. msg->rtype = %d\n", msg->rtype);
//        }
//
//        process(msg);
//
////        ready_starttime = get_sys_clock();
////        if(txn_man) {
////            bool ready = txn_man->set_ready();
////            assert(ready);
////        }
////        INC_STATS(get_thd_id(),worker_deactivate_txn_time,get_sys_clock() - ready_starttime);
//
//        // In fixed mode, we eed to release txn_man after processing message
////        release_txn_man();
//
//        // delete message
//        ready_starttime = get_sys_clock();
//        msg->release();
//        INC_STATS(get_thd_id(),worker_release_msg_time,get_sys_clock() - ready_starttime);
//
//    }
//
//
//#endif
    printf("FINISH WT %ld:%ld\n", _node_id, _thd_id);
    fflush(stdout);
    return FINISH;
}

//TODO(tq): refactor this outside worker thread
// Probably it is better to have a QueCC manager helper class for these kinds of functionality
#if CC_ALG == QUECC

#if !PIPELINED
inline SRC WorkerThread::plan_batch(uint64_t batch_slot, TxnManager * my_txn_man) {

    uint64_t next_part = 0; // we have client buffers are paritioned based on part cnt
    Message * msg = NULL;
    transaction_context * txn_ctxs = NULL;
    priority_group * planner_pg;

    /*
     * Client buffers holding transactions are parititoned based on their home paritions
     * Planners will determinitically dequeue transactions from these client buffers based on their query count
     * and their planner ids. This minimizes contention as it spreds the contention across multiple partitions
     *
     */


#if BATCHING_MODE == TIME_BASED
    M_ASSERT_V(false, "ET_%ld: NON-PIPELINED implementation is not supported for time-based batching\n", _thd_id);
#else

    plan_starttime = get_sys_clock();
    planner_pg = &work_queue.batch_pg_map[batch_slot][_planner_id];
    if (!planner_pg->initialized){
        // since we are doing effectively one batch at a time.
        planner_pg->txn_dep_graph = new hash_table_t();
        planner_pg->initialized = true;
    }
    else{
        M_ASSERT_V(planner_pg->txn_dep_graph->size() == 0, "ET_%ld: non-zero size for txn_dep_graph???\n", _thd_id);
    }




    while (true){

        // we have a complete batch
        if (pbatch_cnt == planner_batch_size){
//            DEBUG_Q("WT_%ld: got enough transactions going to deliver batch at slot = %ld\n", _thd_id, batch_slot);
            do_batch_delivery(batch_slot, planner_pg);
//            DEBUG_Q("WT_%ld: Delivered a batch at slot = %ld\n", _thd_id, batch_slot);
            break;
        }

        next_part = (_planner_id + (g_thread_cnt * query_cnt)) % g_part_cnt;
    //        SAMPLED_DEBUG_Q("PT_%ld: going to get a query with home partition = %ld\n", _planner_id, next_part);
        query_cnt++;
        prof_starttime = get_sys_clock();
        msg = work_queue.plan_dequeue(_thd_id, next_part);
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

        if (idle_starttime > 0){
            // plan_idle_time includes dequeue time, which should be subtracted from it
            INC_STATS(_thd_id,exec_idle_time[_planner_id],get_sys_clock() - idle_starttime);
            idle_starttime = 0;
        }

        txn_ctxs = planner_pg->txn_ctxs;

        if (pbatch_cnt == 0){
            // first transaction in batch
            M_ASSERT_V(planner_pg->txn_dep_graph->size() == 0,"WT_%ld: first txn in batch_id = %ld\n", _thd_id, wbatch_id);
        }

        switch (msg->get_rtype()) {
            case CL_QRY: {
                prof_starttime = get_sys_clock();
                plan_client_msg(msg, txn_ctxs, my_txn_man);
                INC_STATS(_thd_id, exec_txn_proc_time[_thd_id], get_sys_clock()-prof_starttime);
                break;
            }
            default:
                assert(false);
        }
    }


#endif



    return SUCCESS;
}

#endif // if !PIPELINED

inline SRC WorkerThread::execute_batch(uint64_t batch_slot, uint64_t * eq_comp_cnts, TxnManager * my_txn_man) {

    uint64_t wplanner_id = 0;
    batch_partition * batch_part = 0;

    while (true){

#if PIPELINED

        // wait for batch to be ready
        if (wait_for_batch_ready(batch_slot, wplanner_id, batch_part) == BATCH_WAIT){
            if (simulation->is_done()){
                if(idle_starttime > 0) {
                    INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime = 0;
                }
                return BREAK;
            }
            continue;
        }



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
        if(idle_starttime > 0) {
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
            idle_starttime = 0;
        }

#else

#if BATCH_MAP_ORDER == BATCH_ET_PT
        batch_part = (batch_partition *)  work_queue.batch_map[batch_slot][_thd_id][wplanner_id].load();
#else
        batch_part = (batch_partition *)  work_queue.batch_map[batch_slot][wplanner_id][_thd_id].load();
#endif

        M_ASSERT_V(batch_part, "WT_%ld: batch part pointer is zero, PG=%ld, batch_slot = %ld, batch_id = %ld!!\n",
                   _thd_id, wplanner_id, batch_slot, wbatch_id);
#endif
//        DEBUG_Q("ET_%ld: got a PG from planner %ld, batch_slot = %ld, batch_part = %lu\n",_thd_id, wplanner_id, batch_slot, (uint64_t) batch_part);

//        if (wplanner_id == 0){
//            quecc_batch_proc_starttime = get_sys_clock();
//        }

        if (execute_batch_part(batch_part, eq_comp_cnts, my_txn_man, wplanner_id) == BREAK){
            return BREAK;
        }

//        DEBUG_Q("ET_%ld: finished PG from planner %ld, batch_slot = %ld, batch_part = %lu\n",_thd_id, wplanner_id, batch_slot, (uint64_t) batch_part);

        cleanup_batch_part(batch_slot, wplanner_id, batch_part);

//        DEBUG_Q("ET_%ld: cleanup PG from planner %ld, batch_slot = %ld\n",_thd_id, wplanner_id, batch_slot);


#if CT_ENABLED && COMMIT_BEHAVIOR == AFTER_PG_COMP
        M_ASSERT_V(false, "not supported anymore \n");
        work_queue.batch_map_comp_cnts[batch_slot][wplanner_id].fetch_add(1);

        // before going to the next planner, spin here if not all other partitions of the same planners have completed
        // we actually need to wait untill the priority group has been fully committed.

        while (!simulation->is_done() && work_queue.batch_map_comp_cnts[batch_slot][wplanner_id].load() != 0){
            // spin
//            DEBUG_Q("ET_%ld : Spinning waiting for priority group %ld to be COMMITTED\n", _thd_id, wplanner_id);
        }
//        DEBUG_Q("ET_%ld : Going to process the next priority group %ld in batch_id = %ld\n", _thd_id, priority_group, wbatch_id);
#endif
        // go to the next batch partition prepared by the next planner since the previous one has been committed
        wplanner_id++;
//        INC_STATS(_thd_id, exec_batch_part_proc_time[_thd_id], get_sys_clock()-quecc_batch_part_proc_starttime);
//        quecc_batch_part_proc_starttime = 0;
        INC_STATS(_thd_id, exec_batch_part_cnt[_thd_id], 1);

        if (wplanner_id == g_plan_thread_cnt){
//            INC_STATS(_thd_id, exec_batch_part_proc_time[_thd_id], get_sys_clock()-quecc_batch_proc_starttime);
            INC_STATS(_thd_id, exec_batch_cnt[_thd_id], 1);
            return SUCCESS;
        }
    }

    M_ASSERT_V(false, "this should not happend\n");
    return FATAL_ERROR;
}

inline SRC WorkerThread::wait_for_batch_ready(uint64_t batch_slot, uint64_t wplanner_id, batch_partition *& batch_part) {

#if BATCH_MAP_ORDER == BATCH_ET_PT
    batch_part = (batch_partition *)  work_queue.batch_map[batch_slot][_thd_id][wplanner_id].load();
#else
    batch_part = (batch_partition *)  work_queue.batch_map[batch_slot][wplanner_id][_thd_id].load();
#endif

    if (((uint64_t) batch_part) == 0){
        if (idle_starttime == 0){
            idle_starttime = get_sys_clock();
        }
//            SAMPLED_DEBUG_Q("ET_%ld: Got nothing from planning layer, wbatch_id = %ld,"
//                            "\n",
//                    _thd_id,  wbatch_id);
        return BATCH_WAIT;
    }

    return BATCH_READY;
}

inline RC WorkerThread::commit_batch(uint64_t batch_slot){
    std::list<uint64_t> * pending_txns = new std::list<uint64_t>();
    priority_group * planner_pg = NULL;

    uint64_t quecc_commit_starttime = 0;

    uint64_t txn_per_pg = g_batch_size / g_plan_thread_cnt;
    uint64_t txn_wl_per_et = txn_per_pg / g_thread_cnt;
    uint64_t txn_commit_seq=0; // the transaction order in the batch
    uint64_t commit_et_id =0;
    uint64_t commit_cnt = 0;
//    uint8_t e8 =0;
//    uint8_t d8 =0;
    uint64_t e8 =0;
    uint64_t d8 =0;

    uint64_t abort_time;
    uint64_t commit_time;
    uint64_t timespan_long;

    RC rc;


    for (uint64_t i=0; i < g_plan_thread_cnt;++i){

        commit_cnt = 0;
        quecc_commit_starttime = get_sys_clock();
        // committing a PG
        for (uint64_t j = 0; j < txn_per_pg; ++j){
            txn_commit_seq = (i*txn_per_pg) + j;
            commit_et_id = (txn_commit_seq / txn_wl_per_et) % g_thread_cnt;

            if (commit_et_id == _thd_id){
                // I should be committing this transaction
                planner_pg = &work_queue.batch_pg_map[batch_slot][i];
                assert(planner_pg->status.load() == PG_READY);
                rc = commit_txn(planner_pg, j);
                if (rc == Commit){
                    commit_time = get_sys_clock();
                    timespan_long = commit_time - planner_pg->txn_ctxs[j].starttime;

                    // Committing
                    INC_STATS_ARR(_thd_id, first_start_commit_latency, timespan_long);
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
                    wt_release_accesses(&planner_pg->txn_ctxs[j], false, false);
                    e8 = TXN_READY_TO_COMMIT;
                    d8 = TXN_COMMITTED;
                    if (!planner_pg->txn_ctxs[j].txn_state.compare_exchange_strong(e8,d8)){
                        M_ASSERT_V(false, "ET_%ld: trying to commit a transaction with invalid status\n", _thd_id);
                    }

//                    DEBUG_Q("ET_%ld: committed transaction txn_id = %ld, batch_id = %ld\n",
//                            _thd_id, planner_pg->txn_ctxs[j].txn_id, wbatch_id);
                    commit_cnt++;
                }
                else if (rc == WAIT){
                    pending_txns->push_back(j);
                }
                else{
                    assert(rc == Abort);
                    abort_time = get_sys_clock();
                    timespan_long = abort_time - planner_pg->txn_ctxs[j].starttime;
                    INC_STATS_ARR(_thd_id, start_abort_commit_latency, timespan_long);
                    // need to do cascading abort
                    // copying back original value is done during release
                    if (planner_pg->txn_ctxs[j].txn_state.load() == TXN_READY_TO_COMMIT){
                        wt_release_accesses(&planner_pg->txn_ctxs[j], true, true);
                        e8 = TXN_READY_TO_COMMIT;
                        d8 = TXN_ABORTED;
                        if (!planner_pg->txn_ctxs[j].txn_state.compare_exchange_strong(e8,d8)){
                            M_ASSERT_V(false, "ET_%ld: trying to abort a transaction with invalid status\n", _thd_id);
                        }
                    }
                    else{
                        assert(planner_pg->txn_ctxs[j].txn_state.load() == TXN_READY_TO_ABORT);
                        wt_release_accesses(&planner_pg->txn_ctxs[j], false, true);

                        e8 = TXN_READY_TO_ABORT;
                        d8 = TXN_ABORTED;
                        if (!planner_pg->txn_ctxs[j].txn_state.compare_exchange_strong(e8,d8)){
                            M_ASSERT_V(false, "ET_%ld: trying to abort a transaction with invalid status\n", _thd_id);
                        }
                    }
                }
            }

        }
        // check for transactions that requires the result status of their dependents transactions
        // and are pending

        while(!pending_txns->empty()){
            uint64_t txn_idx = pending_txns->front();
            rc = commit_txn(planner_pg, txn_idx);
            if (rc == WAIT){
                /// collect wait times
//                DEBUG_Q("ET_%ld: wating on txn_id = %ld, batch_id = %ld, rc = %d, commit_cnt = %ld\n",
//                            _thd_id, planner_pg->txn_ctxs[txn_idx].txn_id, wbatch_id, rc, commit_cnt);

                continue;
            }
            if (rc == Commit){
                commit_cnt++;
                wt_release_accesses(&planner_pg->txn_ctxs[txn_idx], false, false);
                e8 = TXN_READY_TO_COMMIT;
                d8 = TXN_COMMITTED;
                if (!planner_pg->txn_ctxs[txn_idx].txn_state.compare_exchange_strong(e8,d8)){
                    M_ASSERT_V(false, "ET_%ld: trying to commit a transaction with invalid status\n", _thd_id);
                }
                // icrement abort
            }
            else if (rc == Abort){
                wt_release_accesses(&planner_pg->txn_ctxs[txn_idx], true, true);
                e8 = TXN_READY_TO_COMMIT;
                d8 = TXN_ABORTED;
                if (!planner_pg->txn_ctxs[txn_idx].txn_state.compare_exchange_strong(e8,d8)){
                    M_ASSERT_V(false, "ET_%ld: trying to abort a transaction with invalid status\n", _thd_id);
                }
            }
            pending_txns->pop_front();
        }

//        DEBUG_Q("ET_%ld: committed %ld transactions, batch_id = %ld, PG=%ld\n",
//                _thd_id, commit_cnt, wbatch_id, i);
        INC_STATS(_thd_id, txn_cnt, commit_cnt);
//                DEBUG_Q("ET_%ld: commit count = %ld, PG=%ld, txn_per_pg = %ld, batch_id = %ld\n",
//                        _thd_id, commit_cnt, i, txn_per_pg, wbatch_id);
        //TODO(tq): collect abort cnts??
        INC_STATS(_thd_id,exec_txn_commit_time[_thd_id],get_sys_clock() - quecc_commit_starttime);
    }

    return RCOK;
}

inline RC WorkerThread::commit_txn(priority_group * planner_pg, uint64_t txn_idx){

    transaction_context * txn_ctxs = planner_pg->txn_ctxs;
    uint64_t j = txn_idx;

//    DEBUG_Q("ET_%ld:trying to commit txn_id = %ld\n",_thd_id, txn_ctxs[j].txn_id);
    // check if transactio is ready to commit
    if (txn_ctxs[j].txn_state.load() == TXN_READY_TO_COMMIT){
        // check for any dependent transaction

#if BUILD_TXN_DEPS
        // We are ready to commit, now we need to check if we need to abort due to dependent aborted transactions
        // to check if we need to abort, we lookup transaction dependency graph
        auto search = planner_pg->txn_dep_graph->find(txn_ctxs[j].txn_id);
        if (search != planner_pg->txn_dep_graph->end()){
            // print dependenent transactions for now.
            if (search->second->size() > 0){
                // there are dependen transactions
//                DEBUG_Q("ET_%ld : txn_id = %ld depends on %ld other transactions\n", _thd_id, txn_ctxs[j].txn_id, search->second->size());
//                for(std::vector<uint64_t>::iterator it = search->second->begin(); it != search->second->end(); ++it) {
//                    DEBUG_Q("ET_%ld : txn_id = %ld depends on txn_id = %ld\n", _thd_id, txn_ctxs[j].txn_id, (uint64_t) *it);
//                }
                uint64_t d_txn_id = search->second->back();
                uint64_t d_txn_ctx_idx = d_txn_id-planner_pg->batch_starting_txn_id;
                M_ASSERT_V(txn_ctxs[d_txn_ctx_idx].txn_id == d_txn_id,
                           "ET_%ld: Txn_id mismatch for d_ctx_txn_id %ld == tdg_d_txn_id %ld , d_txn_ctx_idx = %ld,"
                                   "c_txn_id = %ld, batch_starting_txn_id = %ld, txn_ctxs(%ld), batch_id=%ld, j = %ld \n", _thd_id,
                           txn_ctxs[d_txn_ctx_idx].txn_id,
                           d_txn_id, d_txn_ctx_idx, txn_ctxs[j].txn_id, planner_pg->batch_starting_txn_id,
                           (uint64_t)txn_ctxs, wbatch_id, j
                );
                if (txn_ctxs[d_txn_ctx_idx].txn_state == TXN_READY_TO_ABORT){
                    // abort due to dependencies on an aborted txn
                    //                            DEBUG_Q("CT_%ld : going to abort txn_id = %ld due to dependencies\n", _thd_id, txn_ctxs[i].txn_id);
                    return Abort;
//                    canCommit = false;
//#if ROW_ACCESS_TRACKING
//                    cascading_abort = true;
//#endif
                }
                else if (txn_ctxs[d_txn_ctx_idx].txn_state.load() == TXN_READY_TO_COMMIT){
                    // queue up this transaction j in the pedning txn list and check later if it can commit
//                    SAMPLED_DEBUG_Q("ET_%ld:current txn_id = %ld, depends on txn_id = %ld, which has not committed, batch_id=%ld\n",
//                            _thd_id, txn_ctxs[j].txn_id, txn_ctxs[d_txn_ctx_idx].txn_id,wbatch_id);
                    return WAIT;
                }
                else if (txn_ctxs[d_txn_ctx_idx].txn_state.load() == TXN_COMMITTED){
//                    SAMPLED_DEBUG_Q("ET_%ld:current txn_id = %ld, depends on txn_id = %ld, which has committed, batch_id=%ld\n",
//                            _thd_id, txn_ctxs[j].txn_id, txn_ctxs[d_txn_ctx_idx].txn_id,wbatch_id);
                    return Commit;
//                    canCommit = true;
//#if ROW_ACCESS_TRACKING
//                    cascading_abort = false;
//#endif
                }
                else{
                    M_ASSERT_V(false, "ET_%ld: found invalid transaction state of dependent txn, state = %ld\n",
                               _thd_id, txn_ctxs[d_txn_ctx_idx].txn_state.load());

                }
            }
        }
#endif // #if BUILD_TXN_DEPS

    }
    else if (txn_ctxs[j].txn_state.load() == TXN_READY_TO_ABORT){
        //     abort transaction, this abort decision is done by an ET during execution phase
        return Abort;
    }
    else {
        M_ASSERT_V(false, "ET_%ld: transaction state is not valid. state = %ld, txn_id = %ld, batch_id=%ld, pt_cnt = %d, et_cnt=%d\n",
                   _thd_id, txn_ctxs[j].txn_state.load(), txn_ctxs[j].txn_id, wbatch_id, g_plan_thread_cnt, g_thread_cnt);
    }

    return Commit;
}


inline void WorkerThread::wt_release_accesses(transaction_context * context, bool cascading_abort, bool rollback){
#if ROW_ACCESS_TRACKING
    // releaase accesses

    for (uint64_t k = 0; k < context->accesses->size(); k++) {
        M_ASSERT_V(context->accesses->get(k), "Zero pointer for access \n");
        uint64_t ctid = context->accesses->get(k)->thd_id;
        Access * access = context->accesses->get(k);

#if ROLL_BACK
        if (rollback && access->type == WR && !cascading_abort){
            // restore only the first
            access->data->copy(access->orig_data);
        }
        row_pool.put(ctid, access->orig_data);
#endif
        access_pool.put(ctid, access);
    }
    context->accesses->clear();
//    assert(context->accesses->size() == 0);
#endif
}

#endif

RC WorkerThread::run_normal_mode() {
    tsetup();
    printf("Running WorkerThread %ld\n", _thd_id);
#if !(CC_ALG == QUECC || CC_ALG == DUMMY_CC)
    uint64_t ready_starttime;
#endif

#if CC_ALG == QUECC
#if DEBUG_QUECC
    exec_active[_thd_id]->store(true);
#endif
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

#if BUILD_TXN_DEPS
    txn_dep_graph = new hash_table_t();
#endif

    batch_starting_txn_id = planner_txn_id;
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

    uint64_t * eq_comp_cnts = (uint64_t *) mem_allocator.alloc(sizeof(uint64_t)*g_exec_qs_max_size);

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
#endif

    while (!simulation->is_done()) {
        txn_man = NULL;
        heartbeat();
        progress_stats();

#if CC_ALG == QUECC

        // allows using the batch_map in circular manner
        batch_slot = wbatch_id % g_batch_map_length;

#if PIPELINED
        if (execute_batch(batch_slot, eq_comp_cnts, my_txn_man) == SUCCESS) {

#if PARALLEL_COMMIT
            // ET is done with its PGs for the current batch
//            DEBUG_Q("ET_%ld: done with my batch partition, batch_id = %ld, going to wait for all ETs to finish\n", _thd_id, wbatch_id);

            // spin on map_comp_cnts
            if (sync_on_execution_phase_end(batch_slot) == BREAK){
                goto end_et;
            }

//            DEBUG_Q("ET_%ld: starting parallel commit, batch_id = %ld\n", _thd_id, wbatch_id);

            // process txn contexts in the current batch that are assigned to me (deterministically)
            commit_batch(batch_slot);

            // indicate that I am done with all commit phase
//            DEBUG_Q("ET_%ld: is done with commit task for batch_slot = %ld\n", _thd_id, batch_slot);

            if (sync_on_commit_phase_end(batch_slot) == BREAK){
                goto end_et;
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
#if BARRIER_SYNC
        pthread_barrier_wait(&plan_phase_start_bar);
#endif
        if (plan_batch(batch_slot, my_txn_man) == SUCCESS){
        //Sync
//            DEBUG_Q("WT_%ld: going to sync for planning phase end, batch_id = %ld at batch_slot = %ld\n", _thd_id, wbatch_id, batch_slot);

#if BARRIER_SYNC
            pthread_barrier_wait(&plan_phase_end_bar);
#endif
            if (sync_on_planning_phase_end(batch_slot) == BREAK){
                goto end_et;
            }

//                DEBUG_Q("WT_%ld: Starting execution phase, batch_id = %ld at batch_slot = %ld, idle_time = %f\n",
//                        _thd_id, wbatch_id, batch_slot, stats._stats[_thd_id]->exec_idle_time[_thd_id]/BILLION);
#if BARRIER_SYNC
            pthread_barrier_wait(&exec_phase_start_bar);
#endif
        // Execute
            if (execute_batch(batch_slot,eq_comp_cnts, my_txn_man) == SUCCESS) {

#if PARALLEL_COMMIT
                // ET is done with its PGs for the current batch
//            DEBUG_Q("ET_%ld: done with my batch partition, batch_id = %ld, going to wait for all ETs to finish\n", _thd_id, wbatch_id);

//                DEBUG_Q("WT_%ld: going to sync for execution phase end, batch_id = %ld at batch_slot = %ld\n", _thd_id, wbatch_id, batch_slot);
                // Sync
                // spin on map_comp_cnts
#if BARRIER_SYNC
                pthread_barrier_wait(&exec_phase_end_bar);
#endif
                if (sync_on_execution_phase_end(batch_slot) == BREAK){
                    goto end_et;
                }
                //Commit
#if BARRIER_SYNC
                pthread_barrier_wait(&commit_phase_start_bar);
#endif
//                DEBUG_Q("ET_%ld: starting parallel commit, batch_id = %ld\n", _thd_id, wbatch_id);

                // process txn contexts in the current batch that are assigned to me (deterministically)
                commit_batch(batch_slot);

                // indicate that I am done with all commit phase
//                DEBUG_Q("ET_%ld: is done with commit task, going to sync for commit\n", _thd_id);

#if BARRIER_SYNC
                pthread_barrier_wait(&commit_phase_end_bar);
#endif

                if (sync_on_commit_phase_end(batch_slot) == BREAK){
                    goto end_et;
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
//                DEBUG_Q("ET_%ld : ** Committed batch %ld, and moving to next batch %ld\n",
//                    _thd_id, wbatch_id-1, wbatch_id);

            }

        }

#endif //if PIPELINED

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
            if(idle_starttime ==0){
                idle_starttime = get_sys_clock();
            }
            continue;
        }
        if(idle_starttime > 0) {
            INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
            idle_starttime = 0;
        }

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
          if(!ready) {
            // Return to work queue, end processing
            work_queue.enqueue(get_thd_id(),msg,true);
            continue;
          }
          txn_man->register_thread(this);
        }

        process(msg);

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
#endif // if QueCCC
    }

#if CC_ALG == QUECC
    end_et:
    // Some PTs may be spinning and we need to let them go
    // so zero-out all slots that belong to this ET
//    for (uint64_t i = 0; i < g_batch_map_length; i++){
//        for (uint64_t j= 0; j < g_plan_thread_cnt; j++){
//#if BATCH_MAP_ORDER == BATCH_ET_PT
//            work_queue.batch_map[i][_thd_id][j].store(0);
//#else
//            work_queue.batch_map[i][j][_thd_id].store(0);
//#endif
//        }
//    }

//    if (_thd_id == 0){
//
//        for (uint64_t i = 0; i < g_batch_map_length; ++i){
//            for (uint64_t j = 0; j < g_plan_thread_cnt; ++j){
//                // we need to signal all ETs that are spinning
//                // Signal all spinning PTs
//#if BATCHING_MODE == SIZE_BASED
//                work_queue.batch_pg_map[i][j].status.store(PG_AVAILABLE);
//#else
//                work_queue.batch_pg_map[i][j].store(desired);
//#endif
//            }
//        }
//    }

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

    if (((FinishMessage *) msg)->rc == Abort) {
        txn_man->abort();
        txn_man->reset();
        txn_man->reset_query();
        msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, RACK_FIN), GET_NODE_ID(msg->get_txn_id()));
        return Abort;
    }
    txn_man->commit();
    //if(!txn_man->query->readonly() || CC_ALG == OCC)
    if (!((FinishMessage *) msg)->readonly || CC_ALG == MAAT || CC_ALG == OCC)
        msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, RACK_FIN), GET_NODE_ID(msg->get_txn_id()));
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
    DEBUG("RQRY_RSP %ld\n", msg->get_txn_id());
    assert(IS_LOCAL(msg->get_txn_id()));

    txn_man->txn_stats.remote_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;

    if (((QueryResponseMessage *) msg)->rc == Abort) {
        txn_man->start_abort();
        return Abort;
    }

    RC rc = txn_man->run_txn();
    check_if_done(rc);

    if (rc != RCOK || rc != Abort){
        DEBUG_Q("TH_%ld: RC=%d", _thd_id,rc);
    }

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
    uint64_t txn_id = (get_node_id() + get_thd_id() * g_node_cnt)
                      + (g_thread_cnt * g_node_cnt * _thd_txn_id);
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
//        DEBUG_Q("WT_%ld: Getting txn man txn_id = %ld\n",_thd_id, txn_id);
        txn_man = txn_table.get_transaction_manager(ctid, txn_id, 0);
        txn_man->register_thread(this);

        uint64_t ready_starttime = get_sys_clock();
        bool ready = txn_man->unset_ready();
        INC_STATS(ctid, worker_activate_txn_time, get_sys_clock() - ready_starttime);
        assert(ready);
        if (CC_ALG == WAIT_DIE) {
            txn_man->set_timestamp(get_next_ts());
        }

        txn_man->txn_stats.starttime = get_sys_clock();
        txn_man->txn_stats.restart_starttime = txn_man->txn_stats.starttime;
        // this should make a copy of the query object to the txn_man
        msg->copy_to_txn(txn_man);
        DEBUG("START %ld %f %lu\n", txn_man->get_txn_id(), simulation->seconds_from_start(get_sys_clock()),
              txn_man->txn_stats.starttime);
        INC_STATS(ctid, local_txn_start_cnt, 1);

    } else {
        txn_man->txn_stats.restart_starttime = get_sys_clock();
        txn_id = msg->txn_id;
        DEBUG("RESTART %ld %f %lu\n", txn_man->get_txn_id(), simulation->seconds_from_start(get_sys_clock()),
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
    if (txn_man->query->partitions.size() > 1){
        assert(txn_man->query->partitions_touched.size() == 0);
        rc = part_lock_man.lock(txn_man, &txn_man->query->partitions, txn_man->query->partitions.size());
        if (rc == RCOK){
            // Execute transaction
            rc = txn_man->run_hstore_txn();
            part_lock_man.unlock(txn_man, &txn_man->query->partitions, txn_man->query->partitions.size());
//        DEBUG_Q("WT_%ld: executed multipart txn_id = %ld with RCOK = %d, is new order? %d\n",
//                _thd_id, txn_id, (rc == RCOK), ((TPCCQuery *)txn_man->query)->txn_type == TPCC_NEW_ORDER);
        }
    }
    else{
        // There is at least one partitions and partition id must equal thread id
        M_ASSERT_V(txn_man->query->partitions[0] == _thd_id,
                   "THD_%ld: mismatch partition id = %ld, thd_id = %ld", _thd_id,
                   txn_man->query->partitions[0], _thd_id);
        rc = txn_man->run_hstore_txn();
//        DEBUG_Q("WT_%ld: executed single part txn_id = %ld with RCOK = %d, is new order? %d\n",
//                _thd_id, txn_id, (rc == RCOK), ((TPCCQuery *)txn_man->query)->txn_type == TPCC_NEW_ORDER );
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
    DEBUG("RFWD (%ld,%ld)\n", msg->get_txn_id(), msg->get_batch_id());
    txn_man->txn_stats.remote_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
    assert(CC_ALG == CALVIN);
    int responses_left = txn_man->received_response(((ForwardMessage *) msg)->rc);
    assert(responses_left >= 0);
    if (txn_man->calvin_collect_phase_done()) {
        assert(ISSERVERN(txn_man->return_id));
        RC rc = txn_man->run_calvin_txn();
        if (rc == RCOK && txn_man->calvin_exec_phase_done()) {
            calvin_wrapup();
            return RCOK;
        }
    }
    return WAIT;

}

RC WorkerThread::process_calvin_rtxn(Message *msg) {

    DEBUG("START %ld %f %lu\n", txn_man->get_txn_id(), simulation->seconds_from_start(get_sys_clock()),
          txn_man->txn_stats.starttime);
#if !SINGLE_NODE
    assert(ISSERVERN(txn_man->return_id));
#endif
    txn_man->txn_stats.local_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
    // Execute
    RC rc = txn_man->run_calvin_txn();
    //if((txn_man->phase==6 && rc == RCOK) || txn_man->active_cnt == 0 || txn_man->participant_cnt == 1) {
    if (rc == RCOK && txn_man->calvin_exec_phase_done()) {
        calvin_wrapup();
    }
    return RCOK;

}

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


