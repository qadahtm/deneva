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
#if CC_ALG == CALVIN
    RC process_calvin_rtxn(Message * msg);
#endif
    RC process_rtxn_cont(Message * msg);
    RC process_log_msg(Message * msg);
    RC process_log_msg_rsp(Message * msg);
    RC process_log_flushed(Message * msg);
    RC init_phase();
    uint64_t get_next_txn_id();
    bool is_cc_new_timestamp();

#if CC_ALG == QUECC
    int stage =0; //0=plan, 1=exec, 2,commit
#if !PIPELINED
    SRC sync_on_planning_phase_end(uint64_t batch_slot);
#endif

#if SYNC_AFTER_PG
    SRC pg_sync(uint64_t batch_slot, uint64_t planner_id);
#endif //if SYNC_AFTER_PG

    SRC sync_on_execution_phase_end(uint64_t batch_slot);

    SRC sync_on_commit_phase_end(uint64_t batch_slot);

    SRC batch_cleanup(uint64_t batch_slot);

    SRC wait_for_batch_ready(uint64_t batch_slot, uint64_t wplanner_id,
                                    batch_partition *& batch_part);
    batch_partition * get_batch_part(uint64_t batch_slot, uint64_t wplanner_id);

    batch_partition * get_batch_part(uint64_t batch_slot, uint64_t wplanner_id, uint64_t et_id);

    void cleanup_batch_part(uint64_t batch_slot, uint64_t wplanner_id);

    void cleanup_batch_part(uint64_t batch_slot, uint64_t wplanner_id, uint64_t et_id);

    SRC plan_batch(uint64_t batch_slot, TxnManager * my_txn_man);

    SRC execute_batch(uint64_t batch_slot, uint64_t * eq_comp_cnts, TxnManager * my_txn_man);

    batch_partition * get_curr_batch_part(uint64_t batch_slot);


#if EXEC_BUILD_TXN_DEPS
    bool add_txn_dep(transaction_context * context, uint64_t d_txn_id, priority_group * d_planner_pg);
#endif

    void capture_txn_deps(uint64_t batch_slot, exec_queue_entry * entry, RC rc);

    SRC execute_batch_part(uint64_t batch_slot, uint64_t *eq_comp_cnts, TxnManager * my_txn_man);

    RC commit_batch(uint64_t batch_slot);

    RC commit_txn(priority_group * planner_pg, uint64_t txn_idx);

    RC commit_txn(transaction_context * tctx);

    void finalize_txn_commit(transaction_context * tctx, RC rc);

    void wt_release_accesses(transaction_context * context, RC rc);

    void move_to_next_eq(const batch_partition *batch_part, const uint64_t *eq_comp_cnts,
                                              Array<exec_queue_entry> *&exec_q, uint64_t &w_exec_q_index) const;

    uint64_t wbatch_id = 0;
    uint64_t wplanner_id = 0;

    transaction_context * get_tctx_from_pg(uint64_t txnid, priority_group *planner_pg);

#if !PIPELINED

    uint64_t _planner_id;
    uint64_t _worker_cluster_wide_id;
    uint64_t query_cnt =0;
#if YCSB_RANGE_PARITIONING
    inline ALWAYS_INLINE uint32_t get_split(uint64_t key, Array<uint64_t> * ranges){
        for (uint32_t i = 0; i < ranges->size(); i++){
            if (key <= ranges->get(i)){
                return i;
            }
        }
        M_ASSERT_V(false, "could not assign to range key = %lu\n", key);
        return (uint32_t) ranges->size()-1;
    }
#else
    inline ALWAYS_INLINE uint32_t get_split(uint64_t key, Array<uint64_t> * ranges){
//        return (uint32_t) key % g_cluster_worker_thread_cnt;
        return (uint32_t) (key % g_node_cnt);
    }
#endif
    uint64_t get_key_from_entry(exec_queue_entry * entry);

    void splitMRange(Array<exec_queue_entry> *& mrange, uint64_t et_id);

    void checkMRange(Array<exec_queue_entry> *& mrange, uint64_t key, uint64_t et_id);

    void plan_client_msg(Message *msg, transaction_context *txn_ctxs, TxnManager *my_txn_man);

    void do_batch_delivery(uint64_t batch_slot, priority_group * planner_pg);
    void do_batch_delivery_mpt(uint64_t batch_slot, priority_group * planner_pg);

#if WORKLOAD == YCSB
    // create a bucket for each worker thread
    uint64_t bucket_size = g_synth_table_size / g_cluster_worker_thread_cnt;
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
    uint64_t planner_batch_size = get_planner_batch_size();

    uint64_t get_planner_batch_size() {
//        return g_batch_size/g_cluster_worker_thread_cnt;
        assert(quecc_pool.planner_batch_size > 0);
        return quecc_pool.planner_batch_size;
    }

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
#if PROFILE_EXEC_TIMING
    uint64_t batch_start_time = 0;
    uint64_t prof_starttime = 0;
    uint64_t txn_prof_starttime = 0;
    uint64_t plan_starttime = 0;
#endif
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
#endif // - if !PIPELINED
#endif // - if CC_ALG == QUECC
};

#endif
