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

#ifndef _WORK_QUEUE_H_
#define _WORK_QUEUE_H_


#include "global.h"
#include "helper.h"
#include <queue>
#include <boost/lockfree/queue.hpp>
#include <boost/random.hpp>
#include <boost/atomic.hpp>
//#include "message.h"
#include "quecc_thread.h"

class BaseQuery;
class Workload;
class Message;

struct work_queue_entry {
  Message * msg;
  uint64_t batch_id;
  uint64_t txn_id;
  RemReqType rtype;
  uint64_t starttime;

};


struct CompareSchedEntry {
  bool operator()(const work_queue_entry* lhs, const work_queue_entry* rhs) {
    if(lhs->batch_id == rhs->batch_id)
      return lhs->starttime > rhs->starttime;
    return lhs->batch_id < rhs->batch_id;
  }
};
struct CompareWQEntry {
#if PRIORITY == PRIORITY_FCFS
  bool operator()(const work_queue_entry* lhs, const work_queue_entry* rhs) {
    return lhs->starttime < rhs->starttime;
  }
#elif PRIORITY == PRIORITY_ACTIVE
  bool operator()(const work_queue_entry* lhs, const work_queue_entry* rhs) {
    if(lhs->rtype == CL_QRY && rhs->rtype != CL_QRY)
      return true;
    if(rhs->rtype == CL_QRY && lhs->rtype != CL_QRY)
      return false;
    return lhs->starttime < rhs->starttime;
  }
#elif PRIORITY == PRIORITY_HOME
  bool operator()(const work_queue_entry* lhs, const work_queue_entry* rhs) {
    if(ISLOCAL(lhs->txn_id) && !ISLOCAL(rhs->txn_id))
      return true;
    if(ISLOCAL(rhs->txn_id) && !ISLOCAL(lhs->txn_id))
      return false;
    return lhs->starttime < rhs->starttime;
  }
#endif

};

class QWorkQueue {
public:
  void init();
  void enqueue(uint64_t thd_id,Message * msg,bool busy); 
  Message * dequeue(uint64_t thd_id);
  void sched_enqueue(uint64_t thd_id, Message * msg); 
  Message * sched_dequeue(uint64_t thd_id);
  void sequencer_enqueue(uint64_t thd_id, Message * msg);
  Message * sequencer_dequeue(uint64_t thd_id);

#if CC_ALG == QUECC || CC_ALG == LADS
    // TQ: QUECC
    uint64_t get_random_planner_id(uint64_t thd_id);
    void plan_enqueue(uint64_t thd_id, Message * msg);
    Message * plan_dequeue(uint64_t thd_id, uint64_t home_partition);
#endif

#if CC_ALG == QUECC
    // QueCC batch slot map
// Layout of the batch map is imporatant to avoid potential tharshing
#if BATCH_MAP_ORDER == BATCH_ET_PT
    volatile atomic<uint64_t> batch_map[BATCH_MAP_LENGTH][THREAD_CNT][PLAN_THREAD_CNT];
#else //BATCH_MAP_ORDER == BATCH_PT_ET
    volatile atomic<uint64_t> batch_map[BATCH_MAP_LENGTH][PLAN_THREAD_CNT][THREAD_CNT];
#endif
//    uint64_t gbatch_id = 0;
//    batch_partition batch_map[BATCH_MAP_LENGTH][THREAD_CNT][PLAN_THREAD_CNT];

    // Use to synchronize between ETs so that a priority group will need to be completed before any transaction
    // from the next one starts to process
    // Each cell in this map is increamed atomically by an ET
    // Each cell is reset by the commit thread.
#if COMMIT_BEHAVIOR == AFTER_PG_COMP
    volatile atomic<uint8_t> batch_map_comp_cnts[BATCH_MAP_LENGTH][PLAN_THREAD_CNT];
#elif COMMIT_BEHAVIOR == AFTER_BATCH_COMP
//    volatile atomic<uint16_t> batch_plan_comp_cnts[BATCH_MAP_LENGTH];
//    volatile atomic<uint16_t> batch_map_comp_cnts[BATCH_MAP_LENGTH];
//    volatile atomic<uint16_t> batch_commit_et_cnts[BATCH_MAP_LENGTH];
#if WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC || WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
    // need to make not to use stale values of these counts.
    atomic<uint16_t> * batch_plan_comp_cnts;
    atomic<uint16_t> * batch_map_comp_cnts;
    atomic<uint16_t> * batch_commit_et_cnts;
#elif WT_SYNC_METHOD == SYNC_BLOCK
    sync_block plan_sblocks[BATCH_MAP_LENGTH][PLAN_THREAD_CNT];
    sync_block exec_sblocks[BATCH_MAP_LENGTH][THREAD_CNT];
    sync_block commit_sblocks[BATCH_MAP_LENGTH][THREAD_CNT];
#if SYNC_AFTER_PG
    sync_block pg_sblocks[BATCH_MAP_LENGTH][PLAN_THREAD_CNT][THREAD_CNT];
#endif
#if NEXT_STAGE_ARRAY
    int64_t * plan_next_stage[BATCH_MAP_LENGTH][THREAD_CNT];
    int64_t * exec_next_stage[BATCH_MAP_LENGTH][THREAD_CNT];
    int64_t * commit_next_stage[BATCH_MAP_LENGTH][THREAD_CNT];
#if SYNC_AFTER_PG
    int64_t * pg_next_stage[BATCH_MAP_LENGTH][PLAN_THREAD_CNT][THREAD_CNT];
#endif
#else
    int64_t * plan_next_stage[BATCH_MAP_LENGTH];
    int64_t * exec_next_stage[BATCH_MAP_LENGTH];
    int64_t * commit_next_stage[BATCH_MAP_LENGTH];
#endif
#elif WT_SYNC_METHOD == CAS_GLOBAL_SC
    atomic<uint16_t> ** batch_plan_comp_status;
    atomic<uint16_t> ** batch_exec_comp_status;
    atomic<uint16_t> ** batch_commit_comp_status;

    atomic<uint16_t> ** batch_plan_sync_status;
    atomic<uint16_t> ** batch_exec_sync_status;
    atomic<uint16_t> ** batch_commit_sync_status;
#endif
#endif


    // A map for holding arrays of transaction contexts
#if BATCHING_MODE == SIZE_BASED
    priority_group batch_pg_map[BATCH_MAP_LENGTH][PLAN_THREAD_CNT];
#else
    volatile atomic<uint64_t> batch_pg_map[BATCH_MAP_LENGTH][PLAN_THREAD_CNT];
#endif



    // QueCC optimization to reuse and recycle execution queues
    // Use this instead of freeing memory and reallocating it
    // TODO(tq): refactor all free_lists to pool.h
//    boost::lockfree::queue<transaction_context *> ** txn_ctxs_free_list;
//    boost::lockfree::queue<priority_group *> ** pg_free_list;

#if QUECC_DEBUG
    atomic<int64_t> inflight_msg;
#endif
//------

#endif // #if CC_ALG == QUECC
  uint64_t get_cnt() {return get_wq_cnt() + get_rem_wq_cnt() + get_new_wq_cnt();}
  uint64_t get_wq_cnt() {return 0;}
  //uint64_t get_wq_cnt() {return work_queue.size();}
  uint64_t get_sched_wq_cnt() {return 0;}
  uint64_t get_rem_wq_cnt() {return 0;} 
  uint64_t get_new_wq_cnt() {return 0;}
  //uint64_t get_rem_wq_cnt() {return remote_op_queue.size();}
  //uint64_t get_new_wq_cnt() {return new_query_queue.size();}

#if ABORT_QUEUES
    AbortQueue ** abort_queues;
#endif
private:
  boost::lockfree::queue<work_queue_entry* > * work_queue;
  boost::lockfree::queue<work_queue_entry* > * new_txn_queue;
  boost::lockfree::queue<work_queue_entry* > * seq_queue;
  boost::lockfree::queue<work_queue_entry* > ** sched_queue;
#if CC_ALG == QUECC || CC_ALG == LADS
    // TQ: QueCC
  boost::lockfree::queue<work_queue_entry* > ** plan_queue;

    boost::random::mt19937 * rng;         // produces randomness out of thin air
    uint32_t max_planner_index = g_plan_thread_cnt-1;
#endif // #if CC_ALG == QUECC
  uint64_t sched_ptr;
  BaseQuery * last_sched_dq;
  uint64_t curr_epoch;
#if SINGLE_NODE && CC_ALG == CALVIN
    uint64_t query_seq_cnt =0;
#endif

};


#endif
