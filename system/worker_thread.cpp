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
    INC_STATS(get_thd_id(), worker_process_cnt, 1);
    INC_STATS(get_thd_id(), worker_process_time, timespan);
    INC_STATS(get_thd_id(), worker_process_cnt_by_type[msg->rtype], 1);
    INC_STATS(get_thd_id(), worker_process_time_by_type[msg->rtype], timespan);
    DEBUG("%ld EndProcessing %d %ld\n", get_thd_id(), msg->get_rtype(), msg->get_txn_id());
}

void WorkerThread::check_if_done(RC rc) {
    if (txn_man->waiting_for_response())
        return;
    if (rc == Commit)
        commit();
    if (rc == Abort)
        abort();
}

void WorkerThread::release_txn_man() {
    txn_table.release_transaction_manager(get_thd_id(), txn_man->get_txn_id(), txn_man->get_batch_id());
    txn_man = NULL;
}

void WorkerThread::calvin_wrapup() {
    txn_man->release_locks(RCOK);
    txn_man->commit_stats();
    DEBUG("(%ld,%ld) calvin ack to %ld\n", txn_man->get_txn_id(), txn_man->get_batch_id(), txn_man->return_id);
    if (txn_man->return_id == g_node_id) {
        work_queue.sequencer_enqueue(_thd_id, Message::create_message(txn_man, CALVIN_ACK));
    } else {
        msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, CALVIN_ACK), txn_man->return_id);
    }
    release_txn_man();
}

// Can't use txn_man after this function
void WorkerThread::commit() {
    //TxnManager * txn_man = txn_table.get_transaction_manager(txn_id,0);
    //txn_man->release_locks(RCOK);
    //        txn_man->commit_stats();
    assert(txn_man);
    assert(IS_LOCAL(txn_man->get_txn_id()));

    uint64_t timespan = get_sys_clock() - txn_man->txn_stats.starttime;
    DEBUG("COMMIT %ld %f -- %f\n", txn_man->get_txn_id(), simulation->seconds_from_start(get_sys_clock()),
          (double) timespan / BILLION);
//    DEBUG_Q("COMMIT %ld %f -- %f\n",txn_man->get_txn_id(),simulation->seconds_from_start(get_sys_clock()),(double)timespan/ BILLION);
    // Send result back to client
#if !SERVER_GENERATE_QUERIES
    msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, CL_RSP), txn_man->client_id);
#endif
    // remove txn from pool
    release_txn_man();
    // Do not use txn_man after this

}

void WorkerThread::abort() {

    DEBUG("ABORT %ld -- %f\n", txn_man->get_txn_id(), (double) get_sys_clock() - run_starttime / BILLION);
    // TODO: TPCC Rollback here

    ++txn_man->abort_cnt;
    txn_man->reset();

    uint64_t penalty = abort_queue.enqueue(get_thd_id(), txn_man->get_txn_id(), txn_man->get_abort_cnt());

    txn_man->txn_stats.total_abort_time += penalty;

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

#if CC_ALG == QUECC
    uint64_t idle_starttime = 0;
//    uint64_t quecc_prof_time = 0;
//#if COMMIT_BEHAVIOR == IMMEDIATE
//    uint64_t quecc_commit_starttime = 0;
//#endif
    uint64_t quecc_batch_proc_starttime = 0;
    uint64_t quecc_batch_part_proc_starttime = 0;
//    uint64_t quecc_mem_free_startts = 0;
    uint64_t wbatch_id = 0;
    uint64_t wplanner_id = 0;

    Array<exec_queue_entry> *exec_q = NULL;
    batch_partition * batch_part = NULL;
//    uint64_t desired = 0;
//    uint64_t expected = 0;
//#if COMMIT_BEHAVIOR != IMMEDIATE
//    uint8_t desired8 = 0;
//    uint8_t expected8 = 0;
//#endif
    RC rc __attribute__ ((unused)) = RCOK;

    TxnManager * my_txn_man;
    _wl->get_txn_man(my_txn_man);
    my_txn_man->init(_thd_id, _wl);

    for (uint64_t batch_slot = 0; batch_slot < g_batch_map_length; batch_slot++){
        txn_man = NULL;
        // allows using the batch_map in circular manner

        while (true){
            batch_part = (batch_partition *)  work_queue.batch_map[batch_slot][_thd_id][wplanner_id].load();
            M_ASSERT_V(((uint64_t) batch_part) != 0, "In fixed mode this should not happen\n");
//            M_ASSERT_V(batch_part->batch_id == wbatch_id, "Batch part map slot [%ld][%ld][%ld],"
//                    " wbatch_id=%ld, batch_part_batch_id = %ld\n",
//                       batch_slot,_thd_id, wplanner_id, wbatch_id, batch_part->batch_id);

            bool batch_partition_done = false;
            uint64_t w_exec_q_index = 0;
            while (!batch_partition_done){

                //select an execution queue to work on.
                if (batch_part->single_q){
                    exec_q = batch_part->exec_q;
                }
                else {
//                    desired = WORKING;
//                    expected = AVAILABLE;
//                    if (!batch_part->exec_qs_status[w_exec_q_index].compare_exchange_strong(expected, desired)){
//                        // we need to fail here because we should be the first one
//                        M_ASSERT_V(false, "ET_%ld : Could not reserve exec_q at %ld, status = %ld, wbatch_id = %ld, batch_id = %ld\n",
//                                   _thd_id, w_exec_q_index, batch_part->exec_qs_status[w_exec_q_index].load(), wbatch_id, batch_part->batch_id);
//                    }
                    exec_q = batch_part->exec_qs->get(w_exec_q_index);
                }

                // process exec_q
                quecc_batch_part_proc_starttime = get_sys_clock();
                if(idle_starttime > 0) {
                    INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime = 0;
                }
                if (wplanner_id == 0 && quecc_batch_proc_starttime == 0){
                    quecc_batch_proc_starttime = get_sys_clock();
                }

//            DEBUG_Q("Processing batch partition from planner[%ld]\n", wplanner_id);

                for (uint64_t i = 0; i < exec_q->size(); ++i) {
                    exec_queue_entry exec_qe  __attribute__ ((unused));
                    exec_qe = exec_q->get(i);

//                    M_ASSERT_V(exec_qe.txn_id == exec_qe.txn_ctx->txn_id,
//                               "ET_%ld : Executed QueCC txn fragment, txn_id mismatch, wbatch_id = %ld, ctx_batch_id = %ld, entry_txn_id = %ld, ctx_txn_id = %ld, \n",
//                               _thd_id, wbatch_id, exec_qe.txn_ctx->batch_id, exec_qe.txn_id, exec_qe.txn_ctx->txn_id);
//                    M_ASSERT_V(exec_qe.txn_ctx->batch_id == wbatch_id,
//                               "ET_%ld : Executed QueCC txn fragment, batch_id mismatch wbatch_id = %ld, ctx_batch_id = %ld, entry_txn_id = %ld, ctx_txn_id = %ld, \n",
//                               _thd_id, wbatch_id, exec_qe.txn_ctx->batch_id, exec_qe.txn_id, exec_qe.txn_ctx->txn_id);
//
//                    M_ASSERT_V(exec_qe.batch_id == wbatch_id, "Batch part map slot [%ld][%ld][%ld], batch_id mismatch"
//                            " wbatch_id=%ld, ebatch_id = %ld, at exec_q_entry[%ld]\n",
//                               batch_slot,_thd_id, wplanner_id, wbatch_id, exec_qe.batch_id, i);

                    // Use txnManager to execute transaction frament
                    rc = my_txn_man->run_quecc_txn(&exec_qe);
                    assert(rc == RCOK);
//#if COMMIT_BEHAVIOR == IMMEDIATE
//                    uint64_t comp_cnt;
//#endif
//                    if (rc == RCOK){
//                        quecc_prof_time = get_sys_clock();
//#if COMMIT_BEHAVIOR == IMMEDIATE
//                        comp_cnt = exec_qe.txn_ctx->completion_cnt.fetch_add(1);
//#endif
//                        INC_STATS(_thd_id, exec_txn_ctx_update[_thd_id], get_sys_clock()-quecc_prof_time);
//                        INC_STATS(_thd_id, exec_txn_frag_cnt[_thd_id], 1);
//                    }


                    // TQ: we are committing now, which is done one by one the threads only
                    // this allows lower latency for transactions.
                    // Since there is no logging this is okay
                    // TODO(tq): consider committing as a batch with logging enabled
                    // Execution thrad that will execute the last operation will commit
//#if COMMIT_BEHAVIOR == IMMEDIATE
//                    if (comp_cnt == (REQ_PER_QUERY-1)) {
//#if CT_ENABLED
//                    exec_qe.txn_ctx->txn_state = TXN_READY_TO_COMMIT;
//#else

//                        quecc_commit_starttime = get_sys_clock();
//                  DEBUG_Q("Commting txn %ld, with e_thread %ld\n", exec_qe.txn_id, get_thd_id());
//                  DEBUG_Q("txn_man->return_id = %ld , exec_qe.return_node_id = %ld, g_node_id= %d\n",
//                          txn_man->return_id, exec_qe.return_node_id, g_node_id);

//                    DEBUG_Q("thread_%ld: Committing with txn(%ld,%ld,%ld)\n", _thd_id, wbatch_id, exec_qe.txn_id,
//                            exec_qe.req_id);

                        // We are ready to commit, now we need to check if we need to abort.

                        // Committing
                        // Sending response to client a
//                        quecc_prof_time = get_sys_clock();
//#if !SERVER_GENERATE_QUERIES
//                        Message * rsp_msg = Message::create_message(CL_RSP);
//                    rsp_msg->txn_id = exec_qe.txn_id;
//                    rsp_msg->batch_id = wbatch_id; // using batch_id from local, we can also use the one in the context
//                    ((ClientResponseMessage *) rsp_msg)->client_startts = exec_qe.txn_ctx->client_startts;
////                    ((ClientResponseMessage *) rsp_msg)->batch_id = wbatch_id;
//                    rsp_msg->lat_work_queue_time = 0;
//                    rsp_msg->lat_msg_queue_time = 0;
//                    rsp_msg->lat_cc_block_time = 0;
//                    rsp_msg->lat_cc_time = 0;
//                    rsp_msg->lat_process_time = 0;
//                    rsp_msg->lat_network_time = 0;
//                    rsp_msg->lat_other_time = 0;
//
//                    msg_queue.enqueue(get_thd_id(), rsp_msg, exec_qe.return_node_id);
//                    INC_STATS(_thd_id, exec_resp_msg_create_time[_thd_id], get_sys_clock()-quecc_prof_time);
//#endif
//#if COMMIT_BEHAVIOR == IMMEDIATE
//                        INC_STATS(_thd_id, txn_cnt, 1);
//#endif
//                        INC_STATS(_thd_id, exec_txn_cnts[_thd_id], 1);

                        //TODO(tq): how to handle txn_contexts in this case
                        // Free memory
                        // Free txn context
//                    DEBUG_Q("ET_%ld : commtting txn_id = %ld with comp_cnt %ld\n", _thd_id, exec_qe.txn_ctx->txn_id, comp_cnt);

                        // we always commit
//                        INC_STATS(_thd_id, exec_txn_commit_time[_thd_id], get_sys_clock()-quecc_commit_starttime);
                        //TODO(tq): how to handle logic-induced aborts

//                    }
//#endif
                }
                // recycle exec_q
//                quecc_mem_free_startts = get_sys_clock();
//                exec_queue_release(exec_q, wplanner_id);
//                quecc_pool.exec_queue_release(exec_q, wplanner_id,_thd_id);
//                INC_STATS(_thd_id, exec_mem_free_time[_thd_id], get_sys_clock() - quecc_mem_free_startts);

                if (!batch_part->single_q){
                    // set the status of this processed EQ to complete
                    // TODO(tq): use pre-constants instead of literals
//                    desired = COMPLETED;
//                    expected = WORKING;
//
//                    if (!batch_part->exec_qs_status[w_exec_q_index].compare_exchange_strong(expected, desired)){
//                        // we need to fail here because we should be the only one who can do this
//                        M_ASSERT_V(false, "ET_%ld : Could not set exec_q at %ld to COMPLETED\n", _thd_id, w_exec_q_index);
//                    }

                    // move the next exec_q withing same batch partition
                    w_exec_q_index++;

                    // check if we are done with all exec_qs within the same batch_partition
                    if (w_exec_q_index == batch_part->sub_exec_qs_cnt){
                        batch_partition_done = true;
                    }
                }
                else{
                    batch_partition_done = true;
                }
            }

            // reset batch_map_slot to zero after processing it
            // reset map slot to 0 to allow planners to use the slot
            // removed for fixed-mode
//        desired = 0;
//        expected = (uint64_t) batch_part;
//        while(!work_queue.batch_map[batch_slot][_thd_id][wplanner_id].compare_exchange_strong(
//                expected, desired)){
//            DEBUG_Q("ET_%ld: failing to RESET map slot \n", _thd_id);
//        }

            if (!batch_part->single_q){
                // release exec_queues
                for (uint64_t i=0; i < batch_part->exec_qs->size(); ++i){
                    exec_q = batch_part->exec_qs->get(i);
                    quecc_pool.exec_queue_release(exec_q, wplanner_id, _thd_id);
                }
                // free batch_partition
                batch_part->exec_qs->clear();
                quecc_pool.exec_qs_release(batch_part->exec_qs, wplanner_id);

//                while(!work_queue.exec_qs_free_list[wplanner_id]->push(batch_part->exec_qs)){
//                    M_ASSERT_V(false, "Should not happen");
//                };
                // TODO(tq): recycle insted
//                mem_allocator.free(batch_part->exec_qs_status, sizeof(atomic<uint64_t> *)*batch_part->sub_exec_qs_cnt);
                quecc_pool.exec_qs_status_release(batch_part->exec_qs_status, wplanner_id);
            }
            else{
                quecc_pool.exec_queue_release(batch_part->exec_q, wplanner_id,_thd_id);
            }

//        DEBUG_Q("For batch %ld , batch partition processing complete at map slot [%ld][%ld][%ld] \n",
//                wbatch_id, batch_slot, _thd_id, wplanner_id);

            // free batch_part
            //TODO(tq): use pool and recycle
//            mem_allocator.free(batch_part, sizeof(batch_partition));
            quecc_pool.batch_part_release(batch_part, wplanner_id, _thd_id);


//#if CT_ENABLED && COMMIT_BEHAVIOR == AFTER_PG_COMP
//            work_queue.batch_map_comp_cnts[batch_slot][wplanner_id].fetch_add(1);
//
//        // before going to the next planner, spin here if not all other partitions of the same planners have completed
//        // we actually need to wait untill the priority group has been fully committed.
//
//        while (work_queue.batch_map_comp_cnts[batch_slot][wplanner_id].load() != 0){
//            // spin
////            DEBUG_Q("ET_%ld : Spinning waiting for priority group %ld to be COMMITTED\n", _thd_id, wplanner_id);
//        }
////        DEBUG_Q("ET_%ld : Going to process the next priority group %ld in batch_id = %ld\n", _thd_id, priority_group, wbatch_id);
//#endif
            // go to the next batch partition prepared by the next planner since the previous one has been committed
            wplanner_id++;
            if (wplanner_id == g_plan_thread_cnt) {
//                DEBUG_Q("ET_%ld: done with all PGs, wbatch_id = %ld at slot = %ld"
//                                "\n",
//                        _thd_id,  wbatch_id, batch_slot);

//#if COMMIT_BEHAVIOR == AFTER_BATCH_COMP
//                work_queue.batch_map_comp_cnts[batch_slot].fetch_add(1);
//                if (_thd_id == 0){
//
//                    // Thread 0 acts as commit thread
//
//                    // Wait for all transactions to complete their PG within the current batch
//                    while ((work_queue.batch_map_comp_cnts[batch_slot].load() != g_thread_cnt)){
//                        // SPINN Here untill all ETs are done
////                    DEBUG_Q("ET_%ld: waiting for other ETs to be done, wbatch_id = %ld at slot = %ld, val = %d, thd_cnt = %d"
////                                    "\n",
////                            _thd_id,  wbatch_id, batch_slot,work_queue.batch_map_comp_cnts[batch_slot].load(), g_thread_cnt);
//                    }
//                    DEBUG_Q("ET_%ld: All ETs are done for, wbatch_id = %ld at slot = %ld"
//                                "\n",
//                        _thd_id,  wbatch_id, batch_slot);
//
//                    // TODO(tq): add check dependencies here
//
//                    // consider all transactions committed
//                    INC_STATS(_thd_id, txn_cnt, g_batch_size);
//
//                    // TODO(tq): use RR for this, now we just statically asisng this task to the ET_0
////                    desired = PG_AVAILABLE;
////                    expected = PG_READY;
////                    for (uint64_t i =0; i < g_plan_thread_cnt; ++i){
////                        while(!work_queue.batch_pg_map[batch_slot][i].status.compare_exchange_strong(expected, desired)){};
////                    }
//
////                DEBUG_Q("ET_%ld: allowing PTs to procced, wbatch_id = %ld at slot = %ld"
////                                "\n",
////                        _thd_id,  wbatch_id, batch_slot);
//                    desired8 = 0;
//                    expected8 = g_thread_cnt;
//                    while(!work_queue.batch_map_comp_cnts[batch_slot].compare_exchange_strong(expected8, desired8)){};
//                }
//                else {
//                    while (work_queue.batch_map_comp_cnts[batch_slot].load() != 0){
//                        // SPINN Here until batch is committed
////                        DEBUG_Q("ET_%ld: waiting for main ET to be done, wbatch_id = %ld at slot = %ld, val = %d, thd_cnt = %d"
////                                        "\n",
////                                _thd_id,  wbatch_id, batch_slot,work_queue.batch_map_comp_cnts[batch_slot].load(), g_thread_cnt);
//                    }
//                }
//                //TODO(tq) fix stat collection for idle time to include the spinning below
//
//#endif
                wbatch_id++;
                wplanner_id = 0;
//                DEBUG_Q("ET_%ld : ** Completed batch %ld, and moving to next batch %ld at [%ld][%ld][%ld] \n",
//                    _thd_id, wbatch_id-1, wbatch_id, batch_slot, _thd_id, wplanner_id);
                INC_STATS(_thd_id, exec_batch_cnt[_thd_id], 1);
                INC_STATS(_thd_id, exec_batch_proc_time[_thd_id], get_sys_clock() - quecc_batch_proc_starttime);
                quecc_batch_proc_starttime = 0;
                INC_STATS(_thd_id, exec_batch_part_proc_time[_thd_id], get_sys_clock()-quecc_batch_part_proc_starttime);
                INC_STATS(_thd_id, exec_batch_part_cnt[_thd_id], 1);
                break;
            }
        } // end of while(true) -- this should be an indication of a completed batch

    } // end of for loop
#else
    M_ASSERT_V(false, "Selected cc_alg is not supported by fixed_mode. Use normal_mode instead.\n");

//#if CC_ALG == CALVIN || CC_ALG == MAAT
//    M_ASSERT_V(false, "Selected cc_alg is not supported by fixed_mode. Use normal_mode instead.\n");
//#endif

#if !SERVER_GENERATE_QUERIES
    M_ASSERT_V(false, "Set server_generate_queries to true for fixed_mode to work.\n");
#endif

    M_ASSERT_V(((g_batch_size*g_batch_map_length) % g_thread_cnt) == 0, "Transaction workload cannot be divided evenly to threads.\n");

    uint64_t thd_wl_cnt = (g_batch_size*g_batch_map_length)/g_thread_cnt;
    uint64_t ready_starttime;

    for (uint64_t k=0; k < thd_wl_cnt; ++k){
        Message * msg = work_queue.dequeue(get_thd_id());
        if(!msg) {
            M_ASSERT_V(false, "This should not happen.\n");
        }
        if(!(msg->rtype == CL_QRY || msg->rtype == RTXN)){
            M_ASSERT_V(false, "This should not happen. msg->rtype = %d\n", msg->rtype);
        }

        process(msg);

//        ready_starttime = get_sys_clock();
//        if(txn_man) {
//            bool ready = txn_man->set_ready();
//            assert(ready);
//        }
//        INC_STATS(get_thd_id(),worker_deactivate_txn_time,get_sys_clock() - ready_starttime);

        // In fixed mode, we eed to release txn_man after processing message
//        release_txn_man();

        // delete message
        ready_starttime = get_sys_clock();
        msg->release();
        INC_STATS(get_thd_id(),worker_release_msg_time,get_sys_clock() - ready_starttime);

    }


#endif
    printf("FINISH WT %ld:%ld\n", _node_id, _thd_id);
    fflush(stdout);
    return FINISH;
}


RC WorkerThread::run_normal_mode() {
    tsetup();
    printf("Running WorkerThread %ld\n", _thd_id);
#if !(CC_ALG == QUECC || CC_ALG == DUMMY_CC)
    uint64_t ready_starttime;
#endif
    uint64_t idle_starttime = 0;

#if CC_ALG == QUECC
//    uint64_t quecc_prof_time __attribute__((unused)) = 0;
//    uint64_t quecc_commit_starttime = 0;
    uint64_t quecc_batch_proc_starttime = 0;
    uint64_t quecc_batch_part_proc_starttime = 0;
    uint64_t quecc_mem_free_startts = 0;
    uint64_t wbatch_id = 0;
    uint64_t wplanner_id = 0;
    uint64_t batch_slot = 0;
    Array<exec_queue_entry> *exec_q = NULL;
    batch_partition * batch_part = NULL;
    uint64_t desired = 0;
    uint64_t expected = 0;
#if COMMIT_BEHAVIOR != IMMEDIATE
    uint8_t desired8 = 0;
    uint8_t expected8 = 0;
#endif
    RC rc __attribute__((unused)) = RCOK;
#endif
    TxnManager * my_txn_man;
    _wl->get_txn_man(my_txn_man);
    my_txn_man->init(_thd_id, _wl);
//    bool batch_done = false;

    while (!simulation->is_done()) {
        txn_man = NULL;
        heartbeat();
        progress_stats();

#if CC_ALG == QUECC
        // allows using the batch_map in circular manner
        batch_slot = wbatch_id % g_batch_map_length;

        // fences are used to ensure we have the latest update.
        // However, this is a conservative step, we may not actually need it
        // TODO(tq): remove if not needed
        std::atomic_thread_fence(std::memory_order_seq_cst);
        batch_part = (batch_partition *)  work_queue.batch_map[batch_slot][_thd_id][wplanner_id].load();
        std::atomic_thread_fence(std::memory_order_seq_cst);

        if (((uint64_t) batch_part) == 0){
            if (idle_starttime == 0){
                idle_starttime = get_sys_clock();
            }
//            DEBUG_Q("ET_%ld: Got nothing from planning layer, wbatch_id = %ld,"
//                            "\n",
//                    _thd_id,  wbatch_id);
            continue;
        }

//        M_ASSERT_V(batch_part->batch_id == wbatch_id, "Batch part map slot [%ld][%ld][%ld],"
//                " wbatch_id=%ld, batch_part_batch_id = %ld\n",
//                   batch_slot,_thd_id, wplanner_id, wbatch_id, batch_part->batch_id);

        bool batch_partition_done = false;
        uint64_t w_exec_q_index = 0;
        while (!batch_partition_done && !simulation->is_done()){

            //select an execution queue to work on.
            if (batch_part->single_q){
                exec_q = batch_part->exec_q;
            }
            else {
                desired8 = WORKING;
                expected8 = AVAILABLE;
                if (!batch_part->exec_qs_status[w_exec_q_index].compare_exchange_strong(expected8, desired8)){
                    // we need to fail here because we should be the first one
//                    M_ASSERT_V(false, "ET_%ld : Could not reserve exec_q at %ld, status = %ld, wbatch_id = %ld, batch_id = %ld\n",
//                               _thd_id, w_exec_q_index, batch_part->exec_qs_status[w_exec_q_index].load(), wbatch_id, batch_part->batch_id);
                }
                exec_q = batch_part->exec_qs->get(w_exec_q_index);
            }

            // process exec_q
            quecc_batch_part_proc_starttime = get_sys_clock();
            if(idle_starttime > 0) {
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                idle_starttime = 0;
            }
            if (wplanner_id == 0 && quecc_batch_proc_starttime == 0){
                quecc_batch_proc_starttime = get_sys_clock();
            }


//            DEBUG_Q("ET_%ld: Got a pointer for map slot [%ld][%ld][%ld] is %ld, current_batch_id = %ld,"
//                            "batch partition exec_q size = %ld entries, single queue = %d, sub_queues_cnt = %ld"
//                            "\n",
//                    _thd_id, batch_slot,_thd_id, wplanner_id, ((uint64_t) exec_q), wbatch_id,
//                    exec_q->size(), batch_part->single_q, batch_part->sub_exec_qs_cnt);

//            DEBUG_Q("Processing batch partition from planner[%ld]\n", wplanner_id);
            uint64_t qsize __attribute__ ((unused));
            qsize = exec_q->size();

            for (uint64_t i = 0; i < exec_q->size(); ++i) {
                exec_queue_entry exec_qe __attribute__ ((unused)) = exec_q->get(i);
//                assert(exec_qe.txn_ctx->batch_id == wbatch_id);
//                M_ASSERT_V(exec_qe.txn_id == exec_qe.txn_ctx->txn_id,
//                           "ET_%ld : Executed QueCC txn fragment, txn_id mismatch, wbatch_id = %ld, ctx_batch_id = %ld, entry_txn_id = %ld, ctx_txn_id = %ld, \n",
//                           _thd_id, wbatch_id, exec_qe.txn_ctx->batch_id, exec_qe.txn_id, exec_qe.txn_ctx->txn_id);
//                M_ASSERT_V(exec_qe.txn_ctx->batch_id == wbatch_id,
//                           "ET_%ld : Executed QueCC txn fragment, batch_id mismatch wbatch_id = %ld, ctx_batch_id = %ld, entry_txn_id = %ld, ctx_txn_id = %ld, \n",
//                           _thd_id, wbatch_id, exec_qe.txn_ctx->batch_id, exec_qe.txn_id, exec_qe.txn_ctx->txn_id);
//
//                M_ASSERT_V(exec_qe.batch_id == wbatch_id, "Batch part map slot [%ld][%ld][%ld], batch_id mismatch"
//                        " wbatch_id=%ld, ebatch_id = %ld, at exec_q_entry[%ld]\n",
//                           batch_slot,_thd_id, wplanner_id, wbatch_id, exec_qe.batch_id, i);

                //TODO(tq): check if this transaction is already aborted

                // Use txnManager to execute transaction frament
                rc = my_txn_man->run_quecc_txn(&exec_qe);

//                uint64_t comp_cnt;
//                if (rc == RCOK){
//                    quecc_prof_time = get_sys_clock();
//                    comp_cnt = exec_qe.txn_ctx->completion_cnt.fetch_add(1);
//                    INC_STATS(_thd_id, exec_txn_ctx_update[_thd_id], get_sys_clock()-quecc_prof_time);
//                    INC_STATS(_thd_id, exec_txn_frag_cnt[_thd_id], 1);
//                }


                // TQ: we are committing now, which is done one by one the threads only
                // this allows lower latency for transactions.
                // Since there is no logging this is okay
                // TODO(tq): consider committing as a batch with logging enabled
                // Execution thrad that will execute the last operation will commit

//                if (comp_cnt == (REQ_PER_QUERY-1)) {
//#if CT_ENABLED
//                    INC_STATS(_thd_id, exec_txn_cnts[_thd_id], 1);
//                    exec_qe.txn_ctx->txn_state = TXN_READY_TO_COMMIT;
//#else

//                    quecc_commit_starttime = get_sys_clock();
//                  DEBUG_Q("Commting txn %ld, with e_thread %ld\n", exec_qe.txn_id, get_thd_id());
//                  DEBUG_Q("txn_man->return_id = %ld , exec_qe.return_node_id = %ld, g_node_id= %d\n",
//                          txn_man->return_id, exec_qe.return_node_id, g_node_id);

//                    DEBUG_Q("thread_%ld: Committing with txn(%ld,%ld,%ld)\n", _thd_id, wbatch_id, exec_qe.txn_id,
//                            exec_qe.req_id);

                    // We are ready to commit, now we need to check if we need to abort.

                    // Committing
                    // Sending response to client a
//                    quecc_prof_time = get_sys_clock();
//#if !SERVER_GENERATE_QUERIES
//                    Message * rsp_msg = Message::create_message(CL_RSP);
//                    rsp_msg->txn_id = exec_qe.txn_id;
//                    rsp_msg->batch_id = wbatch_id; // using batch_id from local, we can also use the one in the context
//                    ((ClientResponseMessage *) rsp_msg)->client_startts = exec_qe.txn_ctx->client_startts;
////                    ((ClientResponseMessage *) rsp_msg)->batch_id = wbatch_id;
//                    rsp_msg->lat_work_queue_time = 0;
//                    rsp_msg->lat_msg_queue_time = 0;
//                    rsp_msg->lat_cc_block_time = 0;
//                    rsp_msg->lat_cc_time = 0;
//                    rsp_msg->lat_process_time = 0;
//                    rsp_msg->lat_network_time = 0;
//                    rsp_msg->lat_other_time = 0;
//
//                    msg_queue.enqueue(get_thd_id(), rsp_msg, exec_qe.return_node_id);
//                    INC_STATS(_thd_id, exec_resp_msg_create_time[_thd_id], get_sys_clock()-quecc_prof_time);
//#endif

//                    INC_STATS(get_thd_id(), txn_cnt, 1);

                    //TODO(tq): how to handle txn_contexts in this case
                    // Free memory
                    // Free txn context
//                    DEBUG_Q("ET_%ld : commtting txn_id = %ld with comp_cnt %ld\n", _thd_id, exec_qe.txn_ctx->txn_id, comp_cnt);

//                    quecc_mem_free_startts = get_sys_clock();
//                    mem_allocator.free(exec_qe.txn_ctx, sizeof(transaction_context));
//                    while(!work_queue.txn_ctx_free_list[exec_qe.planner_id]->push(exec_qe.txn_ctx)){};
//                    INC_STATS(_thd_id, exec_mem_free_time[_thd_id], get_sys_clock() - quecc_mem_free_startts);
                    // we always commit
//                    INC_STATS(_thd_id, exec_txn_commit_time[_thd_id], get_sys_clock()-quecc_commit_starttime);
                    //TODO(tq): how to handle logic-induced aborts
//#endif
//                }
//                else if (comp_cnt == 0){
//                    exec_qe.txn_ctx->txn_state = TXN_STARTED;
//                }

            }
            // recycle exec_q
            quecc_mem_free_startts = get_sys_clock();
//            exec_q->clear();
//            while(!work_queue.exec_queue_free_list[wplanner_id]->push(exec_q)) {};
            quecc_pool.exec_queue_release(exec_q, wplanner_id, _thd_id);
            INC_STATS(_thd_id, exec_mem_free_time[_thd_id], get_sys_clock() - quecc_mem_free_startts);

            if (!batch_part->single_q){
                // set the status of this processed EQ to complete
                // TODO(tq): use pre-constants instead of literals
                desired8 = COMPLETED;
                expected8 = WORKING;

                if (!batch_part->exec_qs_status[w_exec_q_index].compare_exchange_strong(expected8, desired8)){
                    // we need to fail here because we should be the only one who can do this
                    M_ASSERT_V(false, "ET_%ld : Could not set exec_q at %ld to COMPLETED\n", _thd_id, w_exec_q_index);
                }

                // move the next exec_q withing same batch partition
                w_exec_q_index++;

                // check if we are done with all exec_qs within the same batch_partition
                if (w_exec_q_index == batch_part->sub_exec_qs_cnt){
                    batch_partition_done = true;
                }
            }
            else{
                batch_partition_done = true;
            }
        }

        if (!batch_partition_done){
            // this can only happen at the end of the simulation
            continue;
        }

        // reset batch_map_slot to zero after processing it
        // reset map slot to 0 to allow planners to use the slot
        desired = 0;
        expected = (uint64_t) batch_part;
        while(!work_queue.batch_map[batch_slot][_thd_id][wplanner_id].compare_exchange_strong(
                expected, desired)){
            DEBUG_Q("ET_%ld: failing to RESET map slot \n", _thd_id);
        }

        if (!batch_part->single_q){
            // free batch_partition
            //TODO(tq): recycle batch_partition memory blocks
//            mem_allocator.free(batch_part->exec_qs, sizeof(Array<exec_queue_entry> *)*batch_part->sub_exec_qs_cnt);
            batch_part->exec_qs->clear();

            quecc_pool.exec_qs_release(batch_part->exec_qs, wplanner_id);
//            while(!work_queue.exec_qs_free_list[wplanner_id]->push(batch_part->exec_qs)){
//                M_ASSERT_V(false, "Should not happen");
//            };
            // TODO(tq): recycle insted
//            mem_allocator.free(batch_part->exec_qs_status, sizeof(atomic<uint64_t> *)*batch_part->sub_exec_qs_cnt);
            quecc_pool.exec_qs_status_release(batch_part->exec_qs_status, wplanner_id);
        }

//        DEBUG_Q("For batch %ld , batch partition processing complete at map slot [%ld][%ld][%ld] \n",
//                wbatch_id, batch_slot, _thd_id, wplanner_id);

        // free batch_part
        //TODO(tq): use pool and recycle
//        mem_allocator.free(batch_part, sizeof(batch_partition));
        quecc_pool.batch_part_release(batch_part, wplanner_id, _thd_id);

#if CT_ENABLED && COMMIT_BEHAVIOR == AFTER_PG_COMP
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
        if (wplanner_id == g_plan_thread_cnt) {
#if COMMIT_BEHAVIOR == AFTER_BATCH_COMP
            work_queue.batch_map_comp_cnts[batch_slot].fetch_add(1);
#if CT_ENABLED
            while (!simulation->is_done() && work_queue.batch_map_comp_cnts[batch_slot].load() != 0){
                // spin
//                DEBUG_Q("ET_%ld : Spinning waiting for batch %ld to be committed at slot %ld, current_cnt = %d\n",
//                        _thd_id, wbatch_id, batch_slot, work_queue.batch_map_comp_cnts[batch_slot].load());
            }

#else
            if (_thd_id == 0){
                // Thread 0 acts as commit thread
                while (!simulation->is_done() && (work_queue.batch_map_comp_cnts[batch_slot].load() != g_thread_cnt)){
                    // SPINN Here untill all ETs are done
//                    DEBUG_Q("ET_%ld: waiting for other ETs to be done, wbatch_id = %ld at slot = %ld, val = %d, thd_cnt = %d"
//                                    "\n",
//                            _thd_id,  wbatch_id, batch_slot,work_queue.batch_map_comp_cnts[batch_slot].load(), g_thread_cnt);
                }
//                DEBUG_Q("ET_%ld: All ETs are done for, wbatch_id = %ld at slot = %ld"
//                                "\n",
//                        _thd_id,  wbatch_id, batch_slot);

//                DEBUG_Q("ET_%ld: allowing PTs to procced, wbatch_id = %ld at slot = %ld"
//                                "\n",
//                        _thd_id,  wbatch_id, batch_slot);
                // TODO(tq): use RR for this, now we just statically asisng this task to the ET_0
                desired8 = PG_AVAILABLE;
                expected8 = PG_READY;
                for (uint64_t i =0; i < g_plan_thread_cnt; ++i){
                    while(!work_queue.batch_pg_map[batch_slot][i].status.compare_exchange_strong(expected8, desired8)){};
                }

//                DEBUG_Q("ET_%ld: allowing PTs to procced, wbatch_id = %ld at slot = %ld"
//                                "\n",
//                        _thd_id,  wbatch_id, batch_slot);
                desired8 = 0;
                expected8 = g_thread_cnt;
                while(!work_queue.batch_map_comp_cnts[batch_slot].compare_exchange_strong(expected8, desired8)){};

                INC_STATS(get_thd_id(), txn_cnt, g_batch_size);
            }
            else {
                while (!simulation->is_done() && work_queue.batch_map_comp_cnts[batch_slot].load() != 0){
                    // SPINN Here untill all ETs are done
                }
            }
#endif //if CT_ENABLED
            //TODO(tq) fix stat collection for idle time to include the spinning below

#endif
            wbatch_id++;
            batch_slot =  wbatch_id % g_batch_map_length;
            wplanner_id = 0;
//            DEBUG_Q("ET_%ld : ** Completed batch %ld, and moving to next batch %ld at [%ld][%ld][%ld] \n",
//                    _thd_id, wbatch_id-1, wbatch_id, batch_slot, _thd_id, wplanner_id);
            INC_STATS(_thd_id, exec_batch_cnt[_thd_id], 1);
            INC_STATS(_thd_id, exec_batch_proc_time[_thd_id], get_sys_clock() - quecc_batch_proc_starttime);
            quecc_batch_proc_starttime = 0;
        }
        INC_STATS(_thd_id, exec_batch_part_proc_time[_thd_id], get_sys_clock()-quecc_batch_part_proc_starttime);
        INC_STATS(_thd_id, exec_batch_part_cnt[_thd_id], 1);


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

        //uint64_t starttime = get_sys_clock();

        if(msg->rtype != CL_QRY || CC_ALG == CALVIN) {
          txn_man = get_transaction_manager(msg);

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
        msg->release();
#endif
        INC_STATS(get_thd_id(),worker_release_msg_time,get_sys_clock() - ready_starttime);
#endif // if QueCCC
    }

#if CC_ALG == QUECC
    // Some PTs may be spinning and we need to let them go
    // so zero-out all slots that belong to this ET
    for (uint64_t i = 0; i < g_batch_map_length; i++){
        for (uint64_t j= 0; j < g_plan_thread_cnt; j++){
            work_queue.batch_map[i][_thd_id][j].store(0);
        }
    }

    if (_thd_id == 0){
        desired = PG_AVAILABLE;

        for (uint64_t i = 0; i < g_batch_map_length; ++i){
            for (uint64_t j = 0; j < g_plan_thread_cnt; ++j){
                // we need to signal all ETs that are spinning
                // Signal all spinning PTs
#if BATCHING_MODE == SIZE_BASED
                work_queue.batch_pg_map[i][j].status.store(desired);
#else
                work_queue.batch_pg_map[i][j].store(desired);
#endif
            }
        }
    }

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

    if (msg->get_rtype() == CL_QRY) {
        // This is a new transaction

        // Only set new txn_id when txn first starts
        txn_id = get_next_txn_id();
        msg->txn_id = txn_id;

        // Put txn in txn_table
        txn_man = txn_table.get_transaction_manager(get_thd_id(), txn_id, 0);
        txn_man->register_thread(this);
#if MODE != FIXED_MODE
        uint64_t ready_starttime = get_sys_clock();
        bool ready = txn_man->unset_ready();
        INC_STATS(get_thd_id(), worker_activate_txn_time, get_sys_clock() - ready_starttime);
        assert(ready);
#endif
        if (CC_ALG == WAIT_DIE) {
            txn_man->set_timestamp(get_next_ts());
        }

        txn_man->txn_stats.starttime = get_sys_clock();
        txn_man->txn_stats.restart_starttime = txn_man->txn_stats.starttime;
        msg->copy_to_txn(txn_man);
        DEBUG("START %ld %f %lu\n", txn_man->get_txn_id(), simulation->seconds_from_start(get_sys_clock()),
              txn_man->txn_stats.starttime);
        INC_STATS(get_thd_id(), local_txn_start_cnt, 1);

    } else {
        txn_man->txn_stats.restart_starttime = get_sys_clock();
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

    // Execute transaction
    rc = txn_man->run_txn();
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

    assert(ISSERVERN(txn_man->return_id));
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


