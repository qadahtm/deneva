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
    tsetup();
    printf("Running WorkerThread %ld\n", _thd_id);
#if !(CC_ALG == QUECC || CC_ALG == DUMMY_CC)
    uint64_t ready_starttime;
#endif
    uint64_t idle_starttime = 0;

#if CC_ALG == QUECC
    uint64_t quecc_prof_time = 0;
    uint64_t quecc_commit_starttime = 0;
    uint64_t quecc_batch_proc_starttime = 0;
    uint64_t quecc_batch_part_proc_starttime = 0;
    uint64_t quecc_mem_free_startts = 0;
    uint64_t wbatch_id = 0;
    uint64_t wplanner_id = 0;
    uint64_t batch_slot = 0;
    Array<exec_queue_entry> *exec_q = NULL;
#endif
    while (!simulation->is_done()) {
        txn_man = NULL;
        heartbeat();
        progress_stats();

#if CC_ALG == QUECC
        // allows using the batch_map in circular manner
        batch_slot = wbatch_id % g_batch_map_length;
        // we should spin here if pointer is not set
        std::atomic_thread_fence(std::memory_order_seq_cst);
        exec_q = (Array<exec_queue_entry> *) work_queue.batch_map[batch_slot][_thd_id][wplanner_id].load();
        std::atomic_thread_fence(std::memory_order_seq_cst);

        if (((uint64_t) exec_q) == 0){
            if (idle_starttime == 0){
                idle_starttime = get_sys_clock();
            }
            continue;
        }

        // reset batch_map_slot to zero before processing it
        // reset map slot to 0
//        Array<exec_queue_entry> * desired = (Array<exec_queue_entry> *) 0;
        uint64_t desired = 0;
        uint64_t expected = (uint64_t) exec_q;
        while(!work_queue.batch_map[batch_slot][_thd_id][wplanner_id].compare_exchange_strong(
                expected, desired)){
            DEBUG_Q("failing to RESET map slot \n");
        }

//      DEBUG_Q("Pointer for map slot [%d][%ld][%ld] is %ld, going to spin if 0\n", 0, _thd_id, wbatch_id, exec_q_ptr);
//        if (((uint64_t) exec_q) != 0) {
            quecc_batch_part_proc_starttime = get_sys_clock();
            if(idle_starttime > 0) {
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                idle_starttime = 0;
            }
            if (wplanner_id == 0 && quecc_batch_proc_starttime == 0){
                quecc_batch_proc_starttime = get_sys_clock();
            }


            DEBUG_Q("Pointer for map slot [%ld][%ld][%ld] is %ld, current_batch_id = %ld,  batch partition size = %ld entries\n",
                    batch_slot,_thd_id, wplanner_id, ((uint64_t) exec_q), wbatch_id,
                    exec_q->size());

//            DEBUG_Q("Processing batch partition from planner[%ld]\n", wplanner_id);
            for (uint64_t i = 0; i < exec_q->size(); ++i) {
                exec_queue_entry exec_qe = exec_q->get(i);
//                assert(exec_qe.txn_ctx->batch_id == wbatch_id);
                if (exec_qe.batch_id != wbatch_id) {
                    fprintf(stdout, "Assertion will fail when processing batch part map slot [%ld][%ld][%ld],"
                                    " wbatch_id=%ld, ebatch_id = %ld, at exec_q_entry[%ld]\n",
                            batch_slot,_thd_id, wplanner_id, wbatch_id, exec_qe.batch_id, i);
                    fflush(stdout);
                }
                assert(exec_qe.batch_id == wbatch_id);

                row_t *quecc_row;

                //TQ: dirty code: using a char buffer to store ycsb_request
                ycsb_request *req = (ycsb_request *) &exec_qe.req_buffer;

                quecc_prof_time = get_sys_clock();
                // get pointer to record in row
                YCSBWorkload *my_wl = (YCSBWorkload *) this->_wl;
                int part_id = my_wl->key_to_part(req->key);

                itemid_t *item;
                INDEX *index = my_wl->the_index;
                index->index_read(req->key, item, part_id, _thd_id);

                INC_STATS(_thd_id, exec_txn_index_lookup_time[_thd_id], get_sys_clock()-quecc_prof_time);

                quecc_prof_time = get_sys_clock();
                // just access row, no need to go through lock manager path
                quecc_row = ((row_t *) item->location);

                // perfrom access
                if (req->acctype == RD || req->acctype == SCAN) {
                    int fid = 0;
                    char *data = quecc_row->get_data();
                    //TQ: attribute unused cause GCC compiler not ot produce a warning as fval is no used
                    uint64_t fval __attribute__ ((unused));
                    // TQ: perform the actual read by
                    // However this only reads 8 bytes of the data
                    fval = *(uint64_t *) (&data[fid * 100]);

                } else {
                    if (req->acctype != WR) {
                        DEBUG_Q("Access type must be %d == %d\n", WR, req->acctype);
                    }
                    assert(req->acctype == WR);
                    int fid = 0;
                    char *data = quecc_row->get_data();
                    //TQ: here the we are zeroing the first 8 bytes
                    // 100 below is the number of bytes for each field
                    *(uint64_t *) (&data[fid * 100]) = 0;
                }
                INC_STATS(_thd_id, exec_txn_proc_time[_thd_id], get_sys_clock()-quecc_prof_time);
                // TQ: declare this operation as executed
                // since we only have a single operation, we just increment by one
//                DEBUG_Q("Executed QueCC txn(%ld,%ld,%ld)\n", wbatch_id, exec_qe.txn_id, exec_qe.req_id);

                assert(exec_qe.txn_id == exec_qe.txn_ctx->txn_id);
                quecc_prof_time = get_sys_clock();
//                uint64_t comp_cnt = ATOM_ADD_FETCH(exec_qe.txn_ctx->completion_cnt, 1);
                uint64_t comp_cnt = exec_qe.txn_ctx->completion_cnt.fetch_add(1);
                INC_STATS(_thd_id, exec_txn_ctx_update[_thd_id], get_sys_clock()-quecc_prof_time);
                INC_STATS(_thd_id, exec_txn_frag_cnt[_thd_id], 1);

                // TQ: we are committing now, which is done one by one the threads only
                // this allows lower latency for transactions.
                // Since there is no logging this is okay
                // TODO(tq): consider committing as a batch with logging enabled
                // TODO(tq): Hardcoding for 10 operations, use a paramter instead
                // Execution thrad that will execute the last operation will commit
//                if (comp_cnt == 10) {
                if (comp_cnt == 9) {
                    quecc_commit_starttime = get_sys_clock();
//                  DEBUG_Q("Commting txn %ld, with e_thread %ld\n", exec_qe.txn_id, get_thd_id());
//                  DEBUG_Q("txn_man->return_id = %ld , exec_qe.return_node_id = %ld, g_node_id= %d\n",
//                          txn_man->return_id, exec_qe.return_node_id, g_node_id);

//                    DEBUG_Q("thread_%ld: Committing with txn(%ld,%ld,%ld)\n", _thd_id, wbatch_id, exec_qe.txn_id,
//                            exec_qe.req_id);
//TODO(tq): collect stats
                    // Committing
                    // Sending response to client a
                    quecc_prof_time = get_sys_clock();
#if !SERVER_GENERATE_QUERIES
                    Message * rsp_msg = Message::create_message(CL_RSP);
                    rsp_msg->txn_id = exec_qe.txn_id;
                    rsp_msg->batch_id = wbatch_id; // using batch_id from local, we can also use the one in the context
                    ((ClientResponseMessage *) rsp_msg)->client_startts = exec_qe.txn_ctx->client_startts;
//                    ((ClientResponseMessage *) rsp_msg)->batch_id = wbatch_id;
                    rsp_msg->lat_work_queue_time = 0;
                    rsp_msg->lat_msg_queue_time = 0;
                    rsp_msg->lat_cc_block_time = 0;
                    rsp_msg->lat_cc_time = 0;
                    rsp_msg->lat_process_time = 0;
                    rsp_msg->lat_network_time = 0;
                    rsp_msg->lat_other_time = 0;

                    msg_queue.enqueue(get_thd_id(), rsp_msg, exec_qe.return_node_id);
#endif
                    INC_STATS(_thd_id, exec_resp_msg_create_time[_thd_id], get_sys_clock()-quecc_prof_time);
#if QUECC_DEBUG
                    work_queue.inflight_msg.fetch_sub(1);
#endif
                    INC_STATS(_thd_id, exec_txn_cnts[_thd_id], 1);
                    INC_STATS(get_thd_id(), txn_cnt, 1);

                    // Free memory
                    // Free txn context
                    quecc_mem_free_startts = get_sys_clock();
                    mem_allocator.free(exec_qe.txn_ctx, sizeof(transaction_context));
//                    while(!work_queue.txn_ctx_free_list[exec_qe.planner_id]->push(exec_qe.txn_ctx)){};
                    INC_STATS(_thd_id, exec_mem_free_time[_thd_id], get_sys_clock() - quecc_mem_free_startts);
                    // we always commit
                    INC_STATS(_thd_id, exec_txn_commit_time[_thd_id], get_sys_clock()-quecc_commit_starttime);
                    //TODO(tq): how to handle logic-induced aborts
                }

            }
            DEBUG_Q("For batch %ld , batch partition processing complete with %ld entries processed at map slot [%ld][%ld][%ld] \n",
                    wbatch_id, exec_q->size(), batch_slot, _thd_id, wplanner_id);
            // relaese
            quecc_mem_free_startts = get_sys_clock();
//            exec_q->release();
            while(!work_queue.exec_queue_free_list[_thd_id]->push(exec_q)) {};
            INC_STATS(_thd_id, exec_mem_free_time[_thd_id], get_sys_clock() - quecc_mem_free_startts);

            // go to the next batch partition prepared by the next planner
            wplanner_id++;
            if (wplanner_id == g_plan_thread_cnt) {
                wbatch_id++;
                batch_slot =  wbatch_id % g_batch_map_length;
                wplanner_id = 0;
                DEBUG_Q("** Completed batch %ld, and moving to next batch %ld at [%ld][%ld][%ld] \n",
                        wbatch_id-1, wbatch_id, batch_slot, _thd_id, wplanner_id);
                INC_STATS(_thd_id, exec_batch_cnt[_thd_id], 1);
                INC_STATS(_thd_id, exec_batch_proc_time[_thd_id], get_sys_clock() - quecc_batch_proc_starttime);
                quecc_batch_proc_starttime = 0;
            }
            INC_STATS(_thd_id, exec_batch_part_proc_time[_thd_id], get_sys_clock()-quecc_batch_part_proc_starttime);
            INC_STATS(_thd_id, exec_batch_part_cnt[_thd_id], 1);
//        }
//        else{
//            if (idle_starttime == 0){
//                idle_starttime = get_sys_clock();
//            }
//        }

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
    printf("FINISH %ld:%ld\n", _node_id, _thd_id);
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
        uint64_t ready_starttime = get_sys_clock();
        bool ready = txn_man->unset_ready();
        INC_STATS(get_thd_id(), worker_activate_txn_time, get_sys_clock() - ready_starttime);
        assert(ready);
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


