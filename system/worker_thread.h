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

#include <tpcc.h>
#include <ycsb.h>
#include "global.h"
#include "work_queue.h"
#include "quecc_thread.h"
#include "txn.h"
#include "message.h"
#include "tpcc_query.h"

class Workload;
class Message;

class WorkerThread : public Thread {
public:
    RC run();
    RC run_normal_mode();
    RC run_fixed_mode();
    void setup();
    RC process(Message * msg);
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
    int stage =0; //0=plan, 1=exec, 2,commit
    inline SRC sync_on_planning_phase_end(uint64_t batch_slot) ALWAYS_INLINE{
        uint64_t sync_idlestarttime = 0;
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
                if (sync_idlestarttime ==0){
                    sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: plan_stage waiting for WT_* to SET done, batch_id = %ld\n", _thd_id,wbatch_id);
                }
                //TQ: no need to preeempt this wait
                if (simulation->is_done()){
                    INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
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
            if (sync_idlestarttime > 0){
                INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
            }
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
            *(work_queue.plan_next_stage[batch_slot]) = 1;
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

                if (sync_idlestarttime ==0){
                    sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: plan_stage waiting for WT_* to SET done, batch_id = %ld\n", _thd_id,wbatch_id);
                }
                //TQ: no need to preeempt this wait
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
                    return BREAK;
                }
            }

            if (sync_idlestarttime > 0){
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
            }

//            DEBUG_Q("WT_%ld: plan_stage - all WT_* are ready to move to next stage, batch_id = %ld\n", _thd_id,wbatch_id);
            // all other threads are ready to exit sync
            *(work_queue.plan_next_stage[batch_slot]) = 0;
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
            while(*(work_queue.plan_next_stage[batch_slot]) != 1){
                if (sync_idlestarttime ==0){
                    sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: plan_stage waiting for WT_0 to SET next_stage, batch_id = %ld\n", _thd_id,wbatch_id);
                }
                if (simulation->is_done()){
                    INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
                    return BREAK;
                }
                atomic_thread_fence(memory_order_acquire);
            };

            if (sync_idlestarttime > 0){
                INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
            }

//            DEBUG_Q("WT_%ld: plan_stage WT_0 has SET next_stage, batch_id = %ld\n", _thd_id,wbatch_id);

            work_queue.plan_sblocks[batch_slot][_thd_id].done = 0;
            atomic_thread_fence(memory_order_release);

            atomic_thread_fence(memory_order_acquire);
            while(*(work_queue.plan_next_stage[batch_slot]) != 0){
                if (sync_idlestarttime ==0){
                    sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: plan_stage waiting for WT_0 to RESET next_stage, batch_id = %ld\n", _thd_id,wbatch_id);
                }
                if (simulation->is_done()){
                    INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
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
            if (sync_idlestarttime > 0){
                INC_STATS(_thd_id,plan_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
            }
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

    inline SRC sync_on_execution_phase_end(uint64_t batch_slot) ALWAYS_INLINE{
        uint64_t sync_idlestarttime =0;
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
                if (sync_idlestarttime ==0){
                    sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: exec_stage waiting for WT_* to SET done, batch_id = %ld\n", _thd_id,wbatch_id);
                }
                //TQ: no need to preeempt this wait
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
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
            if (sync_idlestarttime > 0){
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
            }

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
            *(work_queue.exec_next_stage[batch_slot]) = 1;
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
                if (sync_idlestarttime ==0){
                    sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: commit_stage waiting for WT_* to SET done, batch_id = %ld\n", _thd_id,wbatch_id);
                }
                //TQ: no need to preeempt this wait
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
                    return BREAK;
                }
            }

            if (sync_idlestarttime > 0){
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
            }

            *(work_queue.exec_next_stage[batch_slot]) = 0;
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
            while (*work_queue.exec_next_stage[batch_slot] != 1){
                if (sync_idlestarttime ==0){
                    sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: exec_stage waiting for WT_0 to SET next_stage, batch_id = %ld\n", _thd_id,wbatch_id);
                }
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
                    return BREAK;
                }
                atomic_thread_fence(memory_order_acquire);
            }

            if (sync_idlestarttime > 0){
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
            }

            work_queue.exec_sblocks[batch_slot][_thd_id].done = 0;
            atomic_thread_fence(memory_order_release);

            atomic_thread_fence(memory_order_acquire);
            while (*work_queue.exec_next_stage[batch_slot] != 0){
                if (sync_idlestarttime ==0){
                    sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: exec_stage waiting for WT_0 to SET next_stage, batch_id = %ld\n", _thd_id,wbatch_id);
                }
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
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
            if (sync_idlestarttime > 0){
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
            }
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


#if DEBUG_QUECC
    void print_threads_status() const {// print phase status
//        for (UInt32 ii=0; ii < g_plan_thread_cnt; ++ii){
//            DEBUG_Q("ET_%ld: planner_%d active : %ld, wbatch_id=%ld\n", _thd_id, ii, plan_active[ii]->load(), wbatch_id);
//        }
//
//        for (UInt32 ii=0; ii < g_thread_cnt; ++ii){
//            DEBUG_Q("ET_%ld: exec_%d active : %ld, wbatch_id=%ld\n", _thd_id, ii, exec_active[ii]->load(), wbatch_id);
//            DEBUG_Q("ET_%ld: commit_%d active : %ld, wbatch_id=%ld\n", _thd_id, ii, commit_active[ii]->load(), wbatch_id);
//        }
    }
#endif

    inline SRC sync_on_commit_phase_end(uint64_t batch_slot) ALWAYS_INLINE{
        uint64_t sync_idlestarttime =0;
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
                if (sync_idlestarttime ==0){
                    sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: commit_stage waiting for WT_* to SET done, batch_id = %ld\n", _thd_id,wbatch_id);
                }
                //TQ: no need to preeempt this wait
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
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
            if (sync_idlestarttime > 0){
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
            }
//                DEBUG_Q("ET_%ld: all other ETs has finished their commit\n", _thd_id);

            //TODO(tq): remove this later
#if SYNC_MASTER_BATCH_CLEANUP
            // cleanup my batch part and allow planners waiting on me to
            for (uint64_t i = 0; i < g_plan_thread_cnt; ++i){
                for (uint64_t j = 0; j < g_thread_cnt; ++j) {
                    cleanup_batch_part(batch_slot, i,j);
                }
#if PIPELINED
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
            *(work_queue.commit_next_stage[batch_slot]) = 1;
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
                if (sync_idlestarttime ==0){
                    sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: commit_stage waiting for WT_* to SET done, batch_id = %ld\n", _thd_id,wbatch_id);
                }
                //TQ: no need to preeempt this wait
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
                    return BREAK;
                }
            }

            if (sync_idlestarttime > 0){
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
            }

            *(work_queue.commit_next_stage[batch_slot]) = 0;
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
//                DEBUG_Q("ET_%ld: going to wait for ET_0 to finalize the commit phase\n", _thd_id);
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
            while (*work_queue.commit_next_stage[batch_slot] != 1){
                if (sync_idlestarttime ==0){
                    sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: commit stage waiting for WT_0 to SET next_stage, batch_id = %ld\n", _thd_id,wbatch_id);
                }
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
                    return BREAK;
                }
            }

            if (sync_idlestarttime > 0){
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
            }

            work_queue.commit_sblocks[batch_slot][_thd_id].done = 0;
            atomic_thread_fence(memory_order_release);

            atomic_thread_fence(memory_order_acquire);
            while (*work_queue.commit_next_stage[batch_slot] != 0){
                if (sync_idlestarttime ==0){
                    sync_idlestarttime = get_sys_clock();
//                    DEBUG_Q("WT_%ld: commit stage waiting for WT_0 to SET next_stage, batch_id = %ld\n", _thd_id,wbatch_id);
                }
                if (simulation->is_done()){
                    INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
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
            if (sync_idlestarttime > 0){
                INC_STATS(_thd_id,exec_idle_time[_thd_id],get_sys_clock() - sync_idlestarttime);
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - sync_idlestarttime);
            }
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
        return SUCCESS;
    }

    inline SRC batch_cleanup(uint64_t batch_slot) ALWAYS_INLINE{

        //TQ: we need a sync after this cleanup
#if !SYNC_MASTER_BATCH_CLEANUP
// cleanup my batch part and allow planners waiting on me to
        for (uint64_t i = 0; i < g_plan_thread_cnt; ++i){
            cleanup_batch_part(batch_slot, i);
        }
#endif // -- #if SYNC_MASTER_BATCH_CLEANUP

        return SUCCESS;
    }

    inline SRC wait_for_batch_ready(uint64_t batch_slot, uint64_t wplanner_id,
                                    batch_partition *& batch_part) ALWAYS_INLINE;
    inline batch_partition * get_batch_part(uint64_t batch_slot, uint64_t wplanner_id) ALWAYS_INLINE{
        return get_batch_part(batch_slot,wplanner_id,_thd_id);
    }
    inline batch_partition * get_batch_part(uint64_t batch_slot, uint64_t wplanner_id, uint64_t et_id) ALWAYS_INLINE{
        batch_partition * ret;
#if BATCH_MAP_ORDER == BATCH_ET_PT
        ret = (batch_partition *)  work_queue.batch_map[batch_slot][et_id][wplanner_id].load();
#else
        ret = (batch_partition *)  work_queue.batch_map[batch_slot][wplanner_id][et_id].load();
#endif
        return  ret;
    }

    inline void cleanup_batch_part(uint64_t batch_slot, uint64_t wplanner_id) ALWAYS_INLINE{
        cleanup_batch_part(batch_slot, wplanner_id, _thd_id);
    }
    inline void cleanup_batch_part(uint64_t batch_slot, uint64_t wplanner_id, uint64_t et_id) ALWAYS_INLINE{

//        DEBUG_Q("ET_%ld: PG %ld is done moving to th next PG\n",_thd_id, wplanner_id);

        // reset batch_map_slot to zero after processing it
        // reset map slot to 0 to allow planners to use the slot
        batch_partition * batch_part = get_batch_part(batch_slot, wplanner_id, et_id);

        uint64_t desired = 0;
        uint64_t expected = (uint64_t) batch_part;
#if BATCH_MAP_ORDER == BATCH_ET_PT
        while(!work_queue.batch_map[batch_slot][_thd_id][wplanner_id].compare_exchange_strong(expected, desired)){
            DEBUG_Q("ET_%ld: failing to RESET map slot \n", _thd_id);
        }
#else
//        DEBUG_Q("ET_%ld: RESET batch map slot=%ld, PG=%ld, batch_id=%ld \n", _thd_id, batch_slot, wplanner_id, wbatch_id);
        if(!work_queue.batch_map[batch_slot][wplanner_id][et_id].compare_exchange_strong(expected, desired)){
            M_ASSERT_V(false, "ET_%ld: failing to RESET map slot \n", et_id);
        }
#endif
        uint64_t quecc_prof_time;
        if (batch_part->empty){
            quecc_prof_time = get_sys_clock();
            quecc_pool.batch_part_release(batch_part, wplanner_id, et_id);
            INC_STATS(_thd_id,exec_mem_free_time[et_id],get_sys_clock() - quecc_prof_time);
            return;
        }

        quecc_prof_time = get_sys_clock();
        if (!batch_part->single_q){
            // free batch_partition
            batch_part->exec_qs->clear();
            quecc_pool.exec_qs_release(batch_part->exec_qs, wplanner_id);
//            quecc_pool.exec_qs_status_release(batch_part->exec_qs_status, wplanner_id, _thd_id);
        }

//        DEBUG_Q("ET_%ld: For batch %ld , batch partition processing complete at map slot [%ld][%ld][%ld] \n",
//                _thd_id, wbatch_id, batch_slot, _thd_id, wplanner_id);

        // free/release batch_part
        quecc_pool.batch_part_release(batch_part, wplanner_id, et_id);
        INC_STATS(_thd_id,exec_mem_free_time[et_id],get_sys_clock() - quecc_prof_time);
    };

    inline SRC plan_batch(uint64_t batch_slot, TxnManager * my_txn_man) ALWAYS_INLINE;
    inline SRC execute_batch(uint64_t batch_slot, uint64_t * eq_comp_cnts, TxnManager * my_txn_man) ALWAYS_INLINE;
    inline batch_partition * get_curr_batch_part(uint64_t batch_slot) ALWAYS_INLINE{
        batch_partition * batch_part;
#if BATCH_MAP_ORDER == BATCH_ET_PT
        batch_part = (batch_partition *)  work_queue.batch_map[batch_slot][_thd_id][wplanner_id].load();
#else
        batch_part = (batch_partition *)  work_queue.batch_map[batch_slot][wplanner_id][_thd_id].load();
#endif
        M_ASSERT_V(batch_part, "WT_%ld: batch part pointer is zero, PG=%ld, batch_slot = %ld, batch_id = %ld!!\n",
                   _thd_id, wplanner_id, batch_slot, wbatch_id);
        return batch_part;
    };
    inline bool add_txn_dep(uint64_t txn_id, uint64_t d_txn_id, hash_table_tctx_t * tdg, priority_group * d_planner_pg) ALWAYS_INLINE{
        transaction_context * d_tctx = get_tctx_from_pg(d_txn_id, d_planner_pg);
        if (d_tctx != NULL){
            auto txn_search = tdg->find(txn_id);
            if (txn_search != tdg->end()){
                // found tdg entry
                // this txn has other dependencies
                txn_search->second->add_unique(d_tctx);
            }
            else{
                // a new dependency for this txn
                Array<transaction_context *> * txn_list;
                quecc_pool.txn_ctx_list_get_or_create(txn_list, _thd_id);
                txn_list->add(d_tctx);
                tdg->insert({txn_id, txn_list});
            }
            return true;
        }
        else{
            return false;
        }
    };

    inline void capture_txn_deps(uint64_t batch_slot, exec_queue_entry * entry, RC rc)
    ALWAYS_INLINE{
#if EXEC_BUILD_TXN_DEPS
        if (rc == RCOK){
            batch_partition * mypart = get_batch_part(batch_slot,wplanner_id,_thd_id);
            hash_table_tctx_t * mytdg = mypart->planner_pg->exec_tdg[_thd_id];
            uint64_t last_tid = entry->row->last_tid;
            if (last_tid == 0){
                //  if this is a read, and record has not been update before, so no dependency
                return;
            }
            if (last_tid == entry->txn_id){
                // I just wrote to this row, // get previous txn_id from context
                last_tid = entry->txn_ctx->prev_tid[entry->req_idx];
            }
            if (last_tid == 0){
                // if this is a write, we are the first to write to this record, so no dependency
                return;
            }
            int64_t cplan_id = (int64_t)wplanner_id;
            for (int64_t j = cplan_id; j >= 0; --j) {
                batch_partition * part = get_batch_part(batch_slot,j,_thd_id);
                if (add_txn_dep(entry->txn_id,last_tid,mytdg,part->planner_pg)){
//                    DEBUG_Q("ET_%ld: added dependency : entry_txnid=%ld, row_last_tid=%ld, PG=%ld, batch_id=%ld\n",
//                            _thd_id,entry->txn_id,last_tid, wplanner_id,wbatch_id);
                    break;
                }
            }
        }
#endif
    }

    inline SRC execute_batch_part(uint64_t batch_slot, uint64_t *eq_comp_cnts, TxnManager * my_txn_man)
    ALWAYS_INLINE{

        batch_partition * batch_part = get_curr_batch_part(batch_slot);
        Array<exec_queue_entry> * exec_q;

        volatile uint64_t batch_part_eq_cnt = 0;
        exec_queue_entry * exec_qe_ptr UNUSED = NULL;
        uint64_t w_exec_q_index = 0;
        volatile bool eq_switch = false;
        uint64_t quecc_prof_time =0;
        uint64_t quecc_txn_wait_starttime =0;
        RC rc = RCOK;

//        DEBUG_Q("ET_%ld: going to work on PG %ld, batch_id = %ld\n",_thd_id, wplanner_id, wbatch_id);

        memset(eq_comp_cnts,0, sizeof(uint64_t)*g_exec_qs_max_size);

        if (batch_part->empty){
//            DEBUG_Q("ET_%ld: going to work on PG %ld, batch_id = %ld\n",_thd_id, wplanner_id, wbatch_id);
            return SUCCESS;
        }

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
//            DEBUG_Q("ET_%ld: Got a pointer for map slot PG = [%ld] is %ld, current_batch_id = %ld,"
//                            "batch partition exec_q size = %ld entries, single queue = %d"
//                            "\n",
//                    _thd_id, wplanner_id, ((uint64_t) exec_q), wbatch_id,
//                    exec_q->size(), batch_part->single_q);

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
                // empty EQ
                batch_part_eq_cnt--;
                quecc_prof_time = get_sys_clock();
                quecc_pool.exec_queue_release(exec_q, wplanner_id, _thd_id);
                INC_STATS(_thd_id,exec_mem_free_time[_thd_id],get_sys_clock() - quecc_prof_time);
                eq_switch = true;
                goto eq_done;
            }
            // execute selected entry
//            DEBUG_Q("ET_%ld: Processing an entry, batch_id=%ld, txn_id=%ld, planner_id = %ld\n",
//                            _thd_id, wbatch_id, exec_qe_ptr->txn_id, wplanner_id);

            quecc_prof_time = get_sys_clock();
//            M_ASSERT_V(exec_qe_ptr->txn_ctx,"ET_%ld: invalid transaction context, batch_id=%ld\n", _thd_id, wbatch_id);
            rc = my_txn_man->run_quecc_txn(exec_qe_ptr);
            capture_txn_deps(batch_slot, exec_qe_ptr, rc);
            INC_STATS(_thd_id,exec_txn_proc_time[_thd_id],get_sys_clock() - quecc_prof_time);

            while (rc == RCOK){
                INC_STATS(_thd_id, exec_txn_frag_cnt[_thd_id], 1);
                if (quecc_txn_wait_starttime > 0){
                    INC_STATS(_thd_id,exec_txn_wait_time[_thd_id],get_sys_clock() - quecc_txn_wait_starttime);
                    quecc_txn_wait_starttime = 0;
                }
//                DEBUG_Q("ET_%ld: Processed an entry successfully, batch_id=%ld, txn_id=%ld, planner_id = %ld\n",
//                _thd_id, wbatch_id, exec_qe_ptr->txn_id, wplanner_id);
                eq_comp_cnts[w_exec_q_index]++;

                if (exec_q->size() == eq_comp_cnts[w_exec_q_index]){
                    batch_part_eq_cnt--;

//                    DEBUG_Q("ET_%ld: completed full EQ for batch_id= %ld, for planner = %ld,"
//                                    " completed %ld out of %ld"
//                                    "\n",
//                            _thd_id,wbatch_id, wplanner_id,
//                            eq_comp_cnts[w_exec_q_index], exec_q->size()
//                    );
                    quecc_prof_time = get_sys_clock();
                    quecc_pool.exec_queue_release(exec_q, wplanner_id, _thd_id);
                    INC_STATS(_thd_id,exec_mem_free_time[_thd_id],get_sys_clock() - quecc_prof_time);

                    eq_switch = true;
                    break;
                }

                quecc_prof_time = get_sys_clock();
                exec_qe_ptr = exec_q->get_ptr(eq_comp_cnts[w_exec_q_index]);
                rc = my_txn_man->run_quecc_txn(exec_qe_ptr);
                capture_txn_deps(batch_slot, exec_qe_ptr, rc);
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
    inline RC commit_txn(priority_group * planner_pg, uint64_t txn_idx) ALWAYS_INLINE;
    inline void finalize_txn_commit(transaction_context * tctx, RC rc) ALWAYS_INLINE{
        uint64_t commit_time = get_sys_clock(),e8,d8;
        uint64_t timespan_long = commit_time - tctx->starttime;

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
        wt_release_accesses(tctx,rc);
        e8 = TXN_READY_TO_COMMIT;
        d8 = TXN_COMMITTED;
        if (tctx->txn_state.compare_exchange_strong(e8,d8,memory_order_acq_rel)){
            M_ASSERT_V(false, "ET_%ld: trying to commit a transaction with invalid status\n", _thd_id);
        }
//                    DEBUG_Q("ET_%ld: committed transaction txn_id = %ld, batch_id = %ld\n",
//                            _thd_id, planner_pg->txn_ctxs[j].txn_id, wbatch_id);
    };

    inline void wt_release_accesses(transaction_context * context, RC rc) ALWAYS_INLINE{
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
        for (int i = 0; i < REQ_PER_QUERY; ++i) {
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
    };

    uint64_t wbatch_id = 0;
    uint64_t wplanner_id = 0;

    inline transaction_context * get_tctx_from_pg(uint64_t txnid, priority_group *planner_pg) ALWAYS_INLINE{
        transaction_context * d_tctx;
        uint64_t d_txn_ctx_idx = txnid-planner_pg->batch_starting_txn_id;

        if (d_txn_ctx_idx >= (g_batch_size/g_plan_thread_cnt)){
            return NULL;
        }
        else{
            d_tctx = &planner_pg->txn_ctxs[d_txn_ctx_idx];
//            DEBUG_Q("ET_%ld: d_txn_ctx_idx=%ld, txn_id=%ld, batch_starting_txn_id=%ld\n",
//                    _thd_id,d_txn_ctx_idx,txnid,planner_pg->batch_starting_txn_id);
            return d_tctx;
        }
    }

#if !PIPELINED

    uint64_t _planner_id;
    uint64_t query_cnt =0;

    inline uint32_t get_split(uint64_t key, Array<uint64_t> * ranges) ALWAYS_INLINE{
        for (uint32_t i = 0; i < ranges->size(); i++){
            if (key <= ranges->get(i)){
                return i;
            }
        }
        M_ASSERT_V(false, "could not assign to range key = %lu\n", key);
        return (uint32_t) ranges->size()-1;
    }

    uint64_t OPTIMIZE_OUT get_key_from_entry(exec_queue_entry * entry) {
#if WORKLOAD == YCSB
        ycsb_request *ycsb_req_tmp = (ycsb_request *) entry->req_buffer;
        return ycsb_req_tmp->key;
#else
        return mrange->get(r).rid;
#endif
    }

    inline void splitMRange(Array<exec_queue_entry> *& mrange, uint64_t key, uint64_t et_id) ALWAYS_INLINE{
        volatile uint64_t idx = get_split(key, exec_qs_ranges);
        volatile uint64_t tidx;
        volatile uint64_t nidx;
        volatile Array<exec_queue_entry> * texec_q = NULL;

        for (uint64_t r =0; r < mrange->size(); ++r){
            // TODO(tq): refactor this to respective benchmark implementation
            uint64_t lid = get_key_from_entry(mrange->get_ptr(r));
            tidx = get_split(lid, exec_qs_ranges);
            M_ASSERT_V(((Array<exec_queue_entry> volatile * )exec_queues->get(tidx)) == mrange, "PL_%ld: mismatch mrange and tidx_eq\n",_planner_id);
            nidx = get_split(lid, ((Array<uint64_t> *)exec_qs_ranges_tmp));

            texec_q =  ((Array<Array<exec_queue_entry> *> * )exec_queues_tmp)->get(nidx);
//            M_ASSERT_V(texec_q == nexec_q || texec_q == oexec_q , "PL_%ld: mismatch mrange and tidx_eq\n",_planner_id);
            // all entries must fall into one of the splits
#if DEBUG_QUECC
            if (!(nidx == tidx || nidx == (tidx+1))){
                DEBUG_Q("PL_%ld: nidx=%ld, tidx = %ld,lid=%ld,key=%ld\n",_planner_id, nidx, idx, lid,key);
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
            M_ASSERT_V(nidx == tidx || nidx == (tidx+1),"PL_%ld: nidx=%ld, tidx = %ld,lid=%ld,key=%ld\n",_planner_id, nidx, idx, lid,key);

            ((Array<exec_queue_entry> *)texec_q)->add(mrange->get(r));
        }
    }
    inline void checkMRange(Array<exec_queue_entry> *& mrange, uint64_t key, uint64_t et_id) ALWAYS_INLINE{
#if SPLIT_MERGE_ENABLED && SPLIT_STRATEGY == EAGER_SPLIT

        int max_tries = 64;
        int trial =0;

        volatile uint64_t c_range_start;
        volatile uint64_t c_range_end;
        volatile uint64_t idx = get_split(key, exec_qs_ranges);
        volatile uint64_t split_point;
        Array<exec_queue_entry> * nexec_q = NULL;
        Array<exec_queue_entry> * oexec_q = NULL;

        mrange = exec_queues->get(idx);

        uint64_t _prof_starttime =0;
        while (mrange->is_full()){
            if (_prof_starttime == 0){
                _prof_starttime  = get_sys_clock();
            }
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
            splitMRange(mrange,key,et_id);

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
            quecc_pool.exec_queue_release(mrange,_planner_id,_thd_id);
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
        if (_prof_starttime > 0){
            INC_STATS(_thd_id, plan_split_time[_planner_id], get_sys_clock()-_prof_starttime);
        }
#else
        M_ASSERT(false, "LAZY_SPLIT not supported in TPCC")
#endif
    }

    inline void plan_client_msg(Message *msg, transaction_context *txn_ctxs, TxnManager *my_txn_man) ALWAYS_INLINE{

// Query from client
//        DEBUG_Q("PT_%ld planning txn %ld, pbatch_cnt=%ld\n", _planner_id, planner_txn_id,pbatch_cnt);
        txn_prof_starttime = get_sys_clock();

        transaction_context *tctx = &txn_ctxs[pbatch_cnt];
        // reset transaction context

        tctx->txn_id = planner_txn_id;
        tctx->txn_state.store(TXN_INITIALIZED,memory_order_acq_rel);
        tctx->completion_cnt.store(0,memory_order_acq_rel);
        tctx->txn_comp_cnt.store(0,memory_order_acq_rel);
        tctx->starttime = get_sys_clock(); // record start time of transaction
        //TODO(tq): move to repective benchmark transaction manager implementation
#if WORKLOAD == TPCC
        tctx->o_id.store(-1);
#endif

#if !SERVER_GENERATE_QUERIES
        tctx->client_startts = ((ClientQueryMessage *) msg)->client_startts;
#endif

        // create execution entry, for now it will contain only one request
        // we need to reset the mutable values of tctx
        entry->txn_id = planner_txn_id;
        entry->txn_ctx = tctx;
#if ROW_ACCESS_IN_CTX
        // initializ undo_buffer if needed
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
//        uint8_t e8 = TXN_INITIALIZED;
//        uint8_t d8 = TXN_STARTED;

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

#if YCSB_INDEX_LOOKUP_PLAN
        ((YCSBTxnManager *)my_txn_man)->lookup_key(req_buff->key,entry);
#endif
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
        row_t * r_local;
        Array<exec_queue_entry> *mrange;
//    uint64_t idx;
        uint64_t rid;

        uint64_t e8 = TXN_INITIALIZED;
        uint64_t d8 = TXN_STARTED;
#if PIPLINED
        et_id = _planner_id;
#else
        et_id = _thd_id;
#endif
        switch (tpcc_msg->txn_type) {
            case TPCC_PAYMENT:
                if(!entry->txn_ctx->txn_state.compare_exchange_strong(e8,d8)){
                    assert(false);
                }
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

                break;
            case TPCC_NEW_ORDER:
                // plan read on warehouse record

                if(!entry->txn_ctx->txn_state.compare_exchange_strong(e8,d8)){
                    assert(false);
                }
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


                //plan update on district table
                tpcc_txn_man->neworder_lookup_d(tpcc_msg->w_id, tpcc_msg->d_id, r_local);
                rid = r_local->get_row_id();
                checkMRange(mrange, rid, et_id);
                tpcc_txn_man->plan_neworder_update_d(r_local,entry);
                mrange->add(*entry);

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
                    tpcc_txn_man->plan_neworder_insert_ol(ol_i_id,ol_supply_w_id,ol_quantity, ol_number, r_local, entry);
                    rid = entry->rid;
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
        pbatch_cnt++;

#if !INIT_QUERY_MSGS
        // Free message, as there is no need for it anymore
        Message::release_message(msg);
#endif

    }

    inline void do_batch_delivery(uint64_t batch_slot, priority_group * planner_pg) ALWAYS_INLINE{

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

        uint64_t _prof_starttime = get_sys_clock();
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
                    uint64_t et_id =  (rand() % g_thread_cnt);
//                    DEBUG_Q("PT_%ld: releasing empty to ET_%ld\n", _planner_id, et_id);
                    quecc_pool.exec_queue_release(exec_q_tmp,_planner_id, et_id);
                }
            }
        }
//        M_ASSERT_V(assignment.size() == g_thread_cnt, "PL_%ld: size mismatch of assignments to threads, assignment size = %ld, thread-cnt = %d\n",
//                   _planner_id, assignment.size(), g_thread_cnt);
        INC_STATS(_thd_id, plan_merge_time[_planner_id], get_sys_clock()-_prof_starttime);
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

#if DEBUG_QUECC
                // print sync blocks
                for (int i = 0; i < BATCH_MAP_LENGTH; ++i) {
                    for (UInt32 j = 0; j < g_thread_cnt; ++j) {
                        DEBUG_Q("WT_%ld: set_batch_map, batch_id=%ld, plan_sync: done[%d]=%ld, plan_next_stage=%ld\n", _thd_id,wbatch_id,j,work_queue.plan_sblocks[i][j].done, *work_queue.plan_next_stage[i]);
                    }
                    for (UInt32 j = 0; j < g_thread_cnt; ++j) {
                        DEBUG_Q("WT_%ld: set_batch_map, batch_id=%ld, exec_sync: done[%d]=%ld, exec_next_stage=%ld\n",_thd_id,wbatch_id,j,work_queue.exec_sblocks[i][j].done, *work_queue.exec_next_stage[i]);
                    }
                    for (UInt32 j = 0; j < g_thread_cnt; ++j) {
                        DEBUG_Q("WT_%ld: set_batch_map, batch_id=%ld, commit_sync: done[%d]=%ld, commit_next_stage=%ld\n",_thd_id,wbatch_id,j,work_queue.commit_sblocks[i][j].done, *work_queue.commit_next_stage[i]);
                    }
                }
#endif
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
        prof_starttime = get_sys_clock();
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

        INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock()-prof_starttime);
        INC_STATS(_thd_id, plan_batch_cnts[_planner_id], 1);
        INC_STATS(_thd_id, plan_batch_process_time[_planner_id], get_sys_clock() - batch_start_time);

        //reset batch_cnt for next time
        pbatch_cnt = 0;
    }

#if WORKLOAD == YCSB
    // create a bucket for each worker thread
    uint64_t bucket_size = g_synth_table_size / g_thread_cnt;
#elif WORKLOAD == TPCC
    // 9223372036854775807 = 2^63
    // FIXME(tq): Use a parameter to determine the maximum database size
//    uint64_t bucket_size = (9223372036854775807) / g_thread_cnt;
    uint64_t bucket_size = UINT64_MAX/NUM_WH;
#else
    uint64_t bucket_size = 0;
#endif
#endif // if !PIPELINED

#endif // if CC_ALG == QUECC

private:
    uint64_t _thd_txn_id;
    ts_t        _curr_ts;
    ts_t        get_next_ts();
    TxnManager * txn_man;
    uint64_t idle_starttime = 0;


#if CC_ALG == QUECC
    uint64_t planner_batch_size = g_batch_size/g_plan_thread_cnt;
#if !PIPELINED
    // for QueCC palnning

    // txn related
    uint64_t planner_txn_id = 0;
    uint64_t txn_prefix_base = 0x0010000000000000;
    uint64_t txn_prefix_planner_base = 0;

    // Batch related
    uint64_t pbatch_cnt = 0;
    bool force_batch_delivery = false;
//    uint64_t batch_starting_txn_id;

    exec_queue_entry *entry = (exec_queue_entry *) mem_allocator.align_alloc(sizeof(exec_queue_entry));
    uint64_t et_id = 0;
    boost::random::mt19937 plan_rng;
    boost::random::uniform_int_distribution<> * eq_idx_rand = new boost::random::uniform_int_distribution<>(0, g_thread_cnt-1);

    // measurements
    uint64_t batch_start_time = 0;
    uint64_t prof_starttime = 0;
    uint64_t txn_prof_starttime = 0;
    uint64_t plan_starttime = 0;

    // CAS related
    uint64_t expected = 0;
    uint64_t desired = 0;
    uint8_t expected8 = 0;
    uint8_t desired8 = 0;
//    uint64_t slot_num = 0;

    // For txn dependency tracking

    // create and and pre-allocate execution queues
    // For each mrange which will be assigned to an execution thread
    // there will be an array pointer.
    // When the batch is complete we will CAS the exec_q array to allow
    // execution threads to be
    Array<Array<exec_queue_entry> *> * exec_queues = new Array<Array<exec_queue_entry> *>();
    Array<uint64_t> * exec_qs_ranges = new Array<uint64_t>();

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
    volatile Array<uint64_t> * exec_qs_ranges_tmp = new Array<uint64_t>();
    volatile Array<uint64_t> * exec_qs_ranges_tmp_tmp = new Array<uint64_t>();
    volatile Array<Array<exec_queue_entry> *> * exec_queues_tmp;
    volatile Array<Array<exec_queue_entry> *> * exec_queues_tmp_tmp;
#endif
#if MERGE_STRATEGY == BALANCE_EQ_SIZE
    assign_ptr_min_heap_t assignment;
#elif MERGE_STRATEGY == RR
#endif
//#if NUMA_ENABLED
//    uint64_t * f_assign = (uint64_t *) mem_allocator.alloc_local(sizeof(uint64_t)*g_thread_cnt);
//#else
    uint64_t * f_assign = (uint64_t *) mem_allocator.alloc(sizeof(uint64_t)*g_thread_cnt);
//#endif
    boost::lockfree::spsc_queue<assign_entry *> * assign_entry_free_list =
            new boost::lockfree::spsc_queue<assign_entry *>(FREE_LIST_INITIAL_SIZE);

#endif

    inline void print_eqs_ranges_after_swap(uint64_t pt_id, uint64_t et_id) const ALWAYS_INLINE{
        uint64_t total_eq_entries = 0;
//#if SPLIT_MERGE_ENABLED
//        for (uint64_t i =0; i < ((Array<uint64_t> *)exec_qs_ranges_tmp)->size(); ++i){
//            DEBUG_Q("PL_%ld: old exec_qs_ranges[%lu] = %lu\n", _planner_id, i, ((Array<uint64_t> *)exec_qs_ranges_tmp)->get(i));
//        }
//
//        for (uint64_t i =0; i < ((Array<Array<exec_queue_entry> *> *)exec_queues_tmp)->size(); ++i){
//            DEBUG_Q("PL_%ld: old exec_queues[%lu] size = %lu, ptr = %lu, range= %lu\n",
//                    _planner_id, i, ((Array<Array<exec_queue_entry> *> *)exec_queues_tmp)->get(i)->size(),
//                    (uint64_t) (((Array<Array<exec_queue_entry> *> *)exec_queues_tmp)->get(i)),
//                    ((Array<uint64_t> *)exec_qs_ranges_tmp)->get(i));
//        }
//#endif
//        for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
//            DEBUG_Q("PL_%ld: new exec_qs_ranges[%lu] = %lu\n", _planner_id, i, exec_qs_ranges->get(i));
//        }
        for (uint64_t i =0; i < exec_queues->size(); ++i){
//            DEBUG_Q("PL_%ld: new exec_queues[%lu] size = %lu, ptr = %lu, range=%lu\n",
//                    _planner_id, i, exec_queues->get(i)->size(), (uint64_t) exec_queues->get(i), exec_qs_ranges->get(i));
            total_eq_entries += exec_queues->get(i)->size();
        }
        DEBUG_Q("WT_%ld: total eq entries = %ld, batch_id=%ld, et_id=%ld, PG=%ld\n",_thd_id, total_eq_entries, wbatch_id, et_id,pt_id);
    }

    void print_eqs_ranges_before_swap() const;
#endif // - if !PIPELINED
#endif // - if CC_ALG == QUECC


};

#endif
