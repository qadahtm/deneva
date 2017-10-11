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

#ifndef _WORKERTHREAD_H_
#define _WORKERTHREAD_H_

#include "global.h"
#include "work_queue.h"
#include "quecc_thread.h"
#include "txn.h"

class Workload;
class Message;

#if CC_ALG == QUECC
enum SRC { SUCCESS=0, BREAK, BATCH_READY, BATCH_WAIT, FATAL_ERROR };
#endif

class WorkerThread : public Thread {
public:
    RC run();
    RC run_normal_mode();
    RC run_fixed_mode();
    void setup();
    void process(Message * msg);
    void check_if_done(RC rc);
    void release_txn_man();
    void commit();
    void abort();
    TxnManager * get_transaction_manager(Message * msg);
    void calvin_wrapup();
    RC process_rfin(Message * msg);
    RC process_rfwd(Message * msg);
    RC process_rack_rfin(Message * msg);
    RC process_rack_prep(Message * msg);
    RC process_rqry_rsp(Message * msg);
    RC process_rqry(Message * msg);
    RC process_rqry_cont(Message * msg);
    RC process_rinit(Message * msg);
    RC process_rprepare(Message * msg);
    RC process_rpass(Message * msg);
    RC process_rtxn(Message * msg);
    RC process_calvin_rtxn(Message * msg);
    RC process_rtxn_cont(Message * msg);
    RC process_log_msg(Message * msg);
    RC process_log_msg_rsp(Message * msg);
    RC process_log_flushed(Message * msg);
    RC init_phase();
    uint64_t get_next_txn_id();
    bool is_cc_new_timestamp();

#if CC_ALG == QUECC

    inline SRC sync_on_planning_phase_end(uint64_t &idle_starttime, uint64_t batch_slot) ALWAYS_INLINE{
        uint8_t desired8;
        uint8_t expected8;

        if (_thd_id == 0){
            if (idle_starttime > 0){
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
            }
            idle_starttime = get_sys_clock();
            // wait for all ets to finish
            while (work_queue.batch_plan_comp_cnts[batch_slot].load() != g_thread_cnt){
                //TQ: no need to preeempt this wait
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
            };
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
            idle_starttime =0;

            // allow other ETs to proceed
            desired8 = 0;
            expected8 = g_thread_cnt;
            if(!work_queue.batch_plan_comp_cnts[batch_slot].compare_exchange_strong(expected8, desired8)){
                M_ASSERT_V(false, "ET_%ld: this should not happen, I am the only one who can reset batch_plan_comp_cnts\n", _thd_id);
            };

        }
        else{
//                DEBUG_Q("ET_%ld: going to wait for ET_0 to finalize the commit phase for batch_id = %ld\n", _thd_id, wbatch_id);
            if (idle_starttime > 0){
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
            }
            idle_starttime = get_sys_clock();
//                DEBUG_Q("ET_%ld: Done with my batch partition going to wait for others\n", _thd_id);
            while (work_queue.batch_plan_comp_cnts[batch_slot].load() != 0){
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
                // SPINN Here untill all ETs are done and batch is committed
            }
//                DEBUG_Q("ET_%ld: execution phase is done, starting commit phase for batch_id = %ld\n", _thd_id, wbatch_id);
//                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
            idle_starttime =0;
        }

        return SUCCESS;
    }

    inline SRC sync_on_execution_phase_end(uint64_t &idle_starttime, uint64_t batch_slot) ALWAYS_INLINE{
        uint8_t desired8;
        uint8_t expected8;

        if (_thd_id == 0){
            if (idle_starttime > 0){
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
            }
            idle_starttime = get_sys_clock();
            // wait for all ets to finish
            while (work_queue.batch_map_comp_cnts[batch_slot].load() != g_thread_cnt){
                //TQ: no need to preeempt this wait
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
            };
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
            idle_starttime =0;

            // allow other ETs to proceed
            desired8 = 0;
            expected8 = g_thread_cnt;
            if(!work_queue.batch_map_comp_cnts[batch_slot].compare_exchange_strong(expected8, desired8)){
                M_ASSERT_V(false, "ET_%ld: this should not happen, I am the only one who can reset batch_map_comp_cnts\n", _thd_id);
            };

        }
        else{
//                DEBUG_Q("ET_%ld: going to wait for ET_0 to finalize the commit phase for batch_id = %ld\n", _thd_id, wbatch_id);
            if (idle_starttime > 0){
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
            }
            idle_starttime = get_sys_clock();
//                DEBUG_Q("ET_%ld: Done with my batch partition going to wait for others\n", _thd_id);
            while (work_queue.batch_map_comp_cnts[batch_slot].load() != 0){
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
                // SPINN Here untill all ETs are done and batch is committed
            }
//                DEBUG_Q("ET_%ld: execution phase is done, starting commit phase for batch_id = %ld\n", _thd_id, wbatch_id);
//                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
            idle_starttime =0;
        }

        return SUCCESS;
    }


    inline SRC sync_on_commit_phase_end(uint64_t &idle_starttime, uint64_t batch_slot) ALWAYS_INLINE{
        uint8_t desired8;
        uint8_t expected8;

        if (_thd_id == 0){
            // wait for others to finish
            if (idle_starttime > 0){
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
//                    INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
            }

            idle_starttime = get_sys_clock();
            // wait for all ets to finish
//                DEBUG_Q("ET_%ld: going to wait for other ETs to finish their commit for all PGs\n", _thd_id);
            while (work_queue.batch_commit_et_cnts[batch_slot].load() != g_thread_cnt){
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
            };
//                DEBUG_Q("ET_%ld: all other ETs has finished their commit\n", _thd_id);
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
            idle_starttime =0;

            // Reset PG map so that planners can continue
            desired8 = PG_AVAILABLE;
            expected8 = PG_READY;
            for (uint64_t i = 0; i < g_plan_thread_cnt; ++i){
                if(!work_queue.batch_pg_map[batch_slot][i].status.compare_exchange_strong(expected8, desired8)){
                    M_ASSERT_V(false, "Reset failed for PG map, this should not happen\n");
                };
            }

            // allow other ETs to proceed
            desired8 = 0;
            expected8 = g_thread_cnt;
            if(!work_queue.batch_commit_et_cnts[batch_slot].compare_exchange_strong(expected8, desired8)){
                M_ASSERT_V(false, "ET_%ld: this should not happen, I am the only one who can reset commit_et_cnt\n", _thd_id);
            };
        }
        else{
//                DEBUG_Q("ET_%ld: going to wait for ET_0 to finalize the commit phase\n", _thd_id);
            if (idle_starttime > 0){
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
            }
            idle_starttime = get_sys_clock();
//                DEBUG_Q("ET_%ld: Done with my batch partition going to wait for others\n", _thd_id);
            while (work_queue.batch_commit_et_cnts[batch_slot].load() != 0){
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
                    idle_starttime =0;
                    return BREAK;
                }
                // SPINN Here untill all ETs are done and batch is committed
            }
//                DEBUG_Q("ET_%ld: commit phase is done, going to work on the next batch\n", _thd_id);
//                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
            INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - idle_starttime);
            idle_starttime =0;
        }

        return SUCCESS;
    }

    inline SRC wait_for_batch_ready(uint64_t batch_slot, uint64_t wplanner_id,
                                    batch_partition *& batch_part, uint64_t &idle_starttime) ALWAYS_INLINE;

    inline void cleanup_batch_part(uint64_t batch_slot, uint64_t wplanner_id, batch_partition *& batch_part) ALWAYS_INLINE{

//        DEBUG_Q("ET_%ld: PG %ld is done moving to th next PG\n",_thd_id, wplanner_id);

        // reset batch_map_slot to zero after processing it
        // reset map slot to 0 to allow planners to use the slot
        uint64_t desired = 0;
        uint64_t expected = (uint64_t) batch_part;
#if BATCH_MAP_ORDER == BATCH_ET_PT
        while(!work_queue.batch_map[batch_slot][_thd_id][wplanner_id].compare_exchange_strong(expected, desired)){
            DEBUG_Q("ET_%ld: failing to RESET map slot \n", _thd_id);
        }
#else
        if(!work_queue.batch_map[batch_slot][wplanner_id][_thd_id].compare_exchange_strong(expected, desired)){
            DEBUG_Q("ET_%ld: failing to RESET map slot \n", _thd_id);
            assert(false);
        }
#endif
        uint64_t quecc_prof_time = get_sys_clock();
        if (!batch_part->single_q){
            // free batch_partition
            batch_part->exec_qs->clear();
            quecc_pool.exec_qs_release(batch_part->exec_qs, wplanner_id);
//            quecc_pool.exec_qs_status_release(batch_part->exec_qs_status, wplanner_id, _thd_id);
        }

//        DEBUG_Q("ET_%ld: For batch %ld , batch partition processing complete at map slot [%ld][%ld][%ld] \n",
//                _thd_id, wbatch_id, batch_slot, _thd_id, wplanner_id);

        // free/release batch_part
        quecc_pool.batch_part_release(batch_part, wplanner_id, _thd_id);
        INC_STATS(_thd_id,exec_mem_free_time[_thd_id],get_sys_clock() - quecc_prof_time);
    };

    inline SRC plan_batch(uint64_t batch_slot) ALWAYS_INLINE;
    inline SRC execute_batch(uint64_t batch_slot, uint64_t &idle_starttime, uint64_t * eq_comp_cnts, TxnManager * my_txn_man) ALWAYS_INLINE;

    inline SRC execute_batch_part(batch_partition * batch_part, uint64_t *eq_comp_cnts, TxnManager * my_txn_man, uint64_t wplanner_id)
    ALWAYS_INLINE{
        Array<exec_queue_entry> * exec_q;

        //        M_ASSERT_V(batch_part->batch_id == wbatch_id, "Batch part map slot [%ld][%ld][%ld],"
//                " wbatch_id=%ld, batch_part_batch_id = %ld\n",
//                   batch_slot,_thd_id, wplanner_id, wbatch_id, batch_part->batch_id);
//        double x = (double)(rand() % 1000000) / 1000000;
//        if (x < SAMPLING_FACTOR){

//#if DEBUG_QUECC
//        int eq_cnt UNUSED = 0;
//        if (batch_part->single_q){
//            eq_cnt = 1;
//            DEBUG_Q("ET_%ld: Got a PG %ld, wbatch_id = %ld, with %d EQs of size = %ld"
//                            "\n",
//                    _thd_id, wplanner_id, wbatch_id, eq_cnt, batch_part->exec_q->size());
//        }
//        else{
//            eq_cnt = batch_part->exec_qs->size();
//            DEBUG_Q("ET_%ld: Got a PG %ld, wbatch_id = %ld, with %d EQs"
//                            "\n",
//                    _thd_id, wplanner_id, wbatch_id, eq_cnt);
//            for (int y=0; y < eq_cnt; ++y){
//                if (!batch_part->single_q){
//                    DEBUG_Q("ET_%ld: --- EQ[%d], size = %ld\n", _thd_id, y,
//                            batch_part->exec_qs->get(y)->size());
//                }
//            }
//        }
//#endif

//        }

        volatile uint64_t batch_part_eq_cnt = 0;
        exec_queue_entry * exec_qe_ptr UNUSED = NULL;
        uint64_t w_exec_q_index = 0;
        volatile bool eq_switch = false;
        uint64_t quecc_prof_time =0;
        uint64_t quecc_txn_wait_starttime =0;
        RC rc = RCOK;

//        DEBUG_Q("ET_%ld: going to work on PG %ld, batch_id = %ld\n",_thd_id, wplanner_id, wbatch_id);

        memset(eq_comp_cnts,0, sizeof(uint64_t)*g_exec_qs_max_size);

        //select an execution queue to work on.
        if (batch_part->single_q){
            exec_q = batch_part->exec_q;
            batch_part_eq_cnt =1;
        }
        else {
            exec_q = batch_part->exec_qs->get(w_exec_q_index);
            batch_part_eq_cnt = batch_part->exec_qs->size();
        }

        while (true){
//            DEBUG_Q("ET_%ld: Got a pointer for map slot [%ld][%ld][%ld] is %ld, current_batch_id = %ld,"
//                            "batch partition exec_q size = %ld entries, single queue = %d, sub_queues_cnt = %ld"
//                            "\n",
//                    _thd_id, batch_slot,_thd_id, wplanner_id, ((uint64_t) exec_q), wbatch_id,
//                    exec_q->size(), batch_part->single_q, batch_part->sub_exec_qs_cnt);

#if ENABLE_EQ_SWITCH

            // Select an entry from selected exec_q
            if (exec_q->size() > 0){
                quecc_prof_time = get_sys_clock();
                if (exec_q->size() > eq_comp_cnts[w_exec_q_index]){
                    exec_qe_ptr = exec_q->get_ptr(eq_comp_cnts[w_exec_q_index]);
                }
                INC_STATS(_thd_id,exec_entry_deq_time[_thd_id],get_sys_clock() - quecc_prof_time);

            }
            else{
                batch_part_eq_cnt--;
                eq_switch = true;
                goto eq_done;
            }
            // execute selected entry
//            DEBUG_Q("ET_%ld: Processing an entry, batch_id=%ld, txn_id=%ld, planner_id = %ld\n",
//                            _thd_id, wbatch_id, exec_qe.txn_id, wplanner_id);

            quecc_prof_time = get_sys_clock();
            rc = my_txn_man->run_quecc_txn(exec_qe_ptr);
            INC_STATS(_thd_id,exec_txn_proc_time[_thd_id],get_sys_clock() - quecc_prof_time);

            while (rc == RCOK){
                if (quecc_txn_wait_starttime > 0){
                    INC_STATS(_thd_id,exec_txn_wait_time[_thd_id],get_sys_clock() - quecc_txn_wait_starttime);
                    quecc_txn_wait_starttime = 0;
                }
//                DEBUG_Q("ET_%ld: Processed an entry successfully, batch_id=%ld, txn_id=%ld, planner_id = %ld\n",
//                _thd_id, wbatch_id, exec_qe.txn_id, wplanner_id);
                eq_comp_cnts[w_exec_q_index]++;

                if (exec_q->size() == eq_comp_cnts[w_exec_q_index]){
                    batch_part_eq_cnt--;

//                DEBUG_Q("ET_%ld: completed full EQ for batch_id= %ld, EQs_cnt= %d, for planner = %ld,"
//                                " completed %ld out of %ld"
//                                " et_idle_time = %f\n",
//                        _thd_id,wbatch_id, eq_cnt, wplanner_id,
//                        eq_comp_cnts[w_exec_q_index], exec_q->size(),
//                        et_idle_time/BILLION
//                );
                    quecc_prof_time = get_sys_clock();
                    quecc_pool.exec_queue_release(exec_q, wplanner_id, _thd_id);
                    INC_STATS(_thd_id,exec_mem_free_time[_thd_id],get_sys_clock() - quecc_prof_time);

                    eq_switch = true;
                    break;
                }

                quecc_prof_time = get_sys_clock();
                exec_qe_ptr = exec_q->get_ptr(eq_comp_cnts[w_exec_q_index]);
                rc = my_txn_man->run_quecc_txn(exec_qe_ptr);
                INC_STATS(_thd_id,exec_txn_proc_time[_thd_id],get_sys_clock() - quecc_prof_time);
            }

            if (rc == WAIT){
                quecc_txn_wait_starttime = get_sys_clock();
                assert(rc == WAIT);


//                DEBUG_Q("ET_%ld: waiting on batch_id= %ld, EQs_cnt= %d, for planner = %ld,"
//                                    " completed %ld out of %ld"
//                                    " exec_idle_time = %f,  waiting for txn_id = %ld\n",
//                                _thd_id,wbatch_id, eq_cnt, wplanner_id,
//                                eq_comp_cnts[w_exec_q_index], exec_q->size(),
//                                et_idle_time/BILLION, exec_qe.txn_id
//                );

                eq_switch = true;
            }
            eq_done:
            quecc_prof_time = get_sys_clock();
            if (!batch_part->single_q && eq_switch){
//                uint64_t p_w_exec_q_index = w_exec_q_index;

                move_to_next_eq(batch_part, eq_comp_cnts, exec_q, w_exec_q_index);
//                if (p_w_exec_q_index != w_exec_q_index){
//                    DEBUG_Q("ET_%ld: switching EQ[%ld] to EQ[%ld]\n", _thd_id, p_w_exec_q_index, w_exec_q_index);
//                }
            }
            INC_STATS(_thd_id,exec_eq_swtich_time[_thd_id],get_sys_clock() - quecc_prof_time);

            eq_switch = false;

#else
            M_ASSERT_V(false, "Not supported anymore\n");
            for (uint64_t i = 0; i < exec_q->size(); ++i) {
//                exec_queue_entry exec_qe __attribute__ ((unused)) = exec_q->get(i);
                // Asserts are commented out
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

                // Use txnManager to execute transaction fragment
#if QUECC_DB_ACCESS
                rc = my_txn_man->run_quecc_txn(&exec_qe);
                if (!simulation->is_done()){
                    assert(rc == RCOK);
                }
                else {
                    break;
                }
#endif

//                uint64_t comp_cnt;
//                if (rc == RCOK){
//                    quecc_prof_time = get_sys_clock();
//                    comp_cnt = exec_qe.txn_ctx->completion_cnt.fetch_add(1);
//                    INC_STATS(_thd_id, exec_txn_ctx_update[_thd_id], get_sys_clock()-quecc_prof_time);
//                    INC_STATS(_thd_id, exec_txn_frag_cnt[_thd_id], 1);
//                }

            }
            // recycle exec_q
            quecc_mem_free_startts = get_sys_clock();
            quecc_pool.exec_queue_release(exec_q, wplanner_id, _thd_id);
            INC_STATS(_thd_id, exec_mem_free_time[_thd_id], get_sys_clock() - quecc_mem_free_startts);

            batch_part_eq_cnt--;
#endif

            if (batch_part_eq_cnt == 0){
                break;
            }
            if (simulation->is_done()){
                return BREAK;
//                M_ASSERT_V(false, "simulation is done before finishing my batch partition\n")
            }
        }

        return SUCCESS;
    };

    inline RC commit_batch(uint64_t batch_slot) ALWAYS_INLINE;
    inline RC commit_txn(priority_group * planner_pg, uint64_t txn_idx,
                                       std::list<uint64_t> * pending_txn, uint64_t &commit_cnt) ALWAYS_INLINE;
    inline void wt_release_accesses(transaction_context * context, bool cascading_abort, bool rollback) ALWAYS_INLINE;

    inline void move_to_next_eq(const batch_partition *batch_part, const uint64_t *eq_comp_cnts,
                                              Array<exec_queue_entry> *&exec_q, uint64_t &w_exec_q_index) const ALWAYS_INLINE{
        for (uint64_t i =0; i < batch_part->exec_qs->size(); ++i){
            if (i != w_exec_q_index && batch_part->exec_qs->get(i)->size() > eq_comp_cnts[i]){
                w_exec_q_index = i;
                exec_q = batch_part->exec_qs->get(i);
                return;
            }
        }
        // could not switch, using the same the same
        return;

//    w_exec_q_index = (w_exec_q_index + 1) % batch_part->exec_qs->size();
//    exec_q = batch_part->exec_qs->get(w_exec_q_index);
//    while (eq_comp_cnts[w_exec_q_index] == exec_q->size()){
//        w_exec_q_index = (w_exec_q_index+1) % batch_part->exec_qs->size();
//        exec_q = batch_part->exec_qs->get(w_exec_q_index);
//    }
    };
#endif

private:
    uint64_t _thd_txn_id;
    ts_t        _curr_ts;
    ts_t        get_next_ts();
    TxnManager * txn_man;
};

#endif
