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

#include "work_queue.h"
#include "mem_alloc.h"
#include "query.h"
#include "message.h"
#include "client_query.h"
#include "abort_queue.h"
#include <boost/lockfree/queue.hpp>
#include <boost/random.hpp>

void QWorkQueue::init() {

  last_sched_dq = NULL;
  sched_ptr = 0;
  seq_queue = new boost::lockfree::queue<work_queue_entry* > (0);
  work_queue = new boost::lockfree::queue<work_queue_entry* > (0);
  new_txn_queue = new boost::lockfree::queue<work_queue_entry* >(0);
  sched_queue = new boost::lockfree::queue<work_queue_entry* > * [g_node_cnt];
  for ( uint64_t i = 0; i < g_node_cnt; i++) {
    sched_queue[i] = new boost::lockfree::queue<work_queue_entry* > (0);
  }
#if ABORT_QUEUES
  abort_queues = (AbortQueue **) malloc(sizeof(AbortQueue *)*g_thread_cnt);
  for (uint64_t i =0; i < g_thread_cnt; ++i){
    abort_queues[i] = new AbortQueue();
    abort_queues[i]->init();
  }
#endif
#if CC_ALG == QUECC
  // QUECC planners

  rng = new boost::random::mt19937[g_rem_thread_cnt];
  plan_queue = new boost::lockfree::queue<work_queue_entry* > * [g_plan_thread_cnt];
  for ( uint64_t i = 0; i < g_plan_thread_cnt; i++) {
    plan_queue[i] = new boost::lockfree::queue<work_queue_entry* > (0);
  }
  //TODO(tq): is this cache-aware?
#if BATCH_MAP_ORDER == BATCH_ET_PT
  for (uint64_t i=0; i < g_batch_map_length ; i++){
    for (uint64_t j=0; j < g_thread_cnt; j++){
      for (uint64_t k=0; k< g_plan_thread_cnt ; k++){
        (batch_map[i][j][k]).store(0);
      }
    }
  }
#else
  for (uint64_t i=0; i < g_batch_map_length ; i++){
    for (uint64_t j=0; j < g_plan_thread_cnt; j++){
      for (uint64_t k=0; k< g_thread_cnt ; k++){
        (batch_map[i][j][k]).store(0);
      }
    }
  }
#endif

#if ROW_ACCESS_TRACKING
#if ROLL_BACK
#if !ROW_ACCESS_IN_CTX
    uint64_t planner_batch_size = g_batch_size/g_plan_thread_cnt;
    for (uint64_t b =0; b < BATCH_MAP_LENGTH; ++b){
        for (uint64_t j= 0; j < g_plan_thread_cnt; ++j){
          priority_group * planner_pg = &batch_pg_map[b][j];
          for (uint64_t i=0; i < planner_batch_size; ++i){
            planner_pg->txn_ctxs[i].accesses = new Array<Access*>();
            planner_pg->txn_ctxs[i].accesses->init(MAX_ROW_PER_TXN);
            }
        }
    }
#endif

#endif
#endif //#if ROW_ACCESS_TRACKING

#if WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC || WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
  batch_plan_comp_cnts = (atomic<uint16_t> * ) mem_allocator.align_alloc(sizeof(atomic<uint16_t>)*BATCH_MAP_LENGTH);
  batch_map_comp_cnts = (atomic<uint16_t> * ) mem_allocator.align_alloc(sizeof(atomic<uint16_t>)*BATCH_MAP_LENGTH);
  batch_commit_et_cnts = (atomic<uint16_t> * ) mem_allocator.align_alloc(sizeof(atomic<uint16_t>)*BATCH_MAP_LENGTH);
#elif WT_SYNC_METHOD == CAS_GLOBAL_SC
  batch_plan_comp_status = (atomic<uint16_t> **) mem_allocator.align_alloc(sizeof(atomic<uint16_t>*)*BATCH_MAP_LENGTH);
  batch_exec_comp_status = (atomic<uint16_t> **) mem_allocator.align_alloc(sizeof(atomic<uint16_t>*)*BATCH_MAP_LENGTH);
  batch_commit_comp_status = (atomic<uint16_t> **) mem_allocator.align_alloc(sizeof(atomic<uint16_t>*)*BATCH_MAP_LENGTH);
  batch_plan_sync_status = (atomic<uint16_t> **) mem_allocator.align_alloc(sizeof(atomic<uint16_t>*)*BATCH_MAP_LENGTH);
  batch_exec_sync_status = (atomic<uint16_t> **) mem_allocator.align_alloc(sizeof(atomic<uint16_t>*)*BATCH_MAP_LENGTH);
  batch_commit_sync_status = (atomic<uint16_t> **) mem_allocator.align_alloc(sizeof(atomic<uint16_t>*)*BATCH_MAP_LENGTH);
#endif

  for (uint64_t i=0; i < g_batch_map_length ; i++){
#if COMMIT_BEHAVIOR == AFTER_BATCH_COMP
#if WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC || WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
      (batch_plan_comp_cnts[i]).store(0);
      (batch_map_comp_cnts[i]).store(0);
      (batch_commit_et_cnts[i]).store(0);
#elif WT_SYNC_METHOD == CAS_GLOBAL_SC
    batch_plan_comp_status[i] = (atomic<uint16_t> *) mem_allocator.align_alloc(sizeof(atomic<uint16_t>)*g_plan_thread_cnt);
    batch_plan_sync_status[i] = (atomic<uint16_t> *) mem_allocator.align_alloc(sizeof(atomic<uint16_t>)*g_plan_thread_cnt);
    batch_exec_comp_status[i] = (atomic<uint16_t> *) mem_allocator.align_alloc(sizeof(atomic<uint16_t>)*g_thread_cnt);
    batch_commit_comp_status[i] = (atomic<uint16_t> *) mem_allocator.align_alloc(sizeof(atomic<uint16_t>)*g_thread_cnt);
    batch_exec_sync_status[i] = (atomic<uint16_t> *) mem_allocator.align_alloc(sizeof(atomic<uint16_t>)*g_thread_cnt);
    batch_commit_sync_status[i] = (atomic<uint16_t> *) mem_allocator.align_alloc(sizeof(atomic<uint16_t>)*g_thread_cnt);

    for (uint64_t j=0; j < g_thread_cnt; j++){
      batch_exec_comp_status[i][j].store(0);
      batch_commit_comp_status[i][j].store(0);
      batch_exec_sync_status[i][j].store(0);
      batch_commit_sync_status[i][j].store(0);
    }
#endif // WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC
#endif
    for (uint64_t j=0; j < g_plan_thread_cnt; j++){
#if WT_SYNC_METHOD == CNT_ALWAYS_FETCH_ADD_SC || WT_SYNC_METHOD == CNT_FETCH_ADD_ACQ_REL
#if COMMIT_BEHAVIOR == AFTER_PG_COMP
      (batch_map_comp_cnts[i][j]).store(0);
#endif
#elif WT_SYNC_METHOD == CAS_GLOBAL_SC
      batch_plan_comp_status[i][j].store(0);
      batch_plan_sync_status[i][j].store(0);
#endif
      // pointer-based implementation of PG_MAP
      // with static allocation there is no need fo this
#if BATCHING_MODE != SIZE_BASED
      (batch_pg_map[i][j]).store(0);
#endif
    }
  }

  // QueCC execution queue free list
  // TODO(tq): refactor this and move it to QueCC pool
//  txn_ctxs_free_list = new boost::lockfree::queue<transaction_context *> * [g_plan_thread_cnt];
//  pg_free_list = new boost::lockfree::queue<priority_group *> * [g_plan_thread_cnt];
//  for ( uint64_t i = 0; i < g_plan_thread_cnt; i++) {
//    txn_ctxs_free_list[i] = new boost::lockfree::queue<transaction_context *> (FREE_LIST_INITIAL_SIZE);
//    pg_free_list[i] = new boost::lockfree::queue<priority_group *> (FREE_LIST_INITIAL_SIZE);
//  }

#if QUECC_DEBUG
  inflight_msg.store(0);
#endif
  DEBUG_Q("Initialized batch_map\n");

#endif // #if CC_ALG == QUECC
}


#if CC_ALG == QUECC || CC_ALG == LADS
uint64_t QWorkQueue::get_random_planner_id(uint64_t thd_id) {
  boost::random::uniform_int_distribution<> dist(0, max_planner_index);
  return (uint64_t) dist(rng[thd_id]);
}


void QWorkQueue::plan_enqueue(uint64_t thd_id, Message * msg){
    uint64_t planner_id = get_random_planner_id(thd_id);
    work_queue_entry * entry = (work_queue_entry*)mem_allocator.align_alloc(sizeof(work_queue_entry));
    entry->msg = msg;
    entry->rtype = msg->rtype;
    entry->txn_id = msg->txn_id;
    entry->batch_id = msg->batch_id;
    entry->starttime = get_sys_clock();
    assert(ISSERVER);
//    DEBUG_Q("Enqueue work for Planner(%ld) (%ld,%ld) %d\n",planner_id,entry->txn_id,entry->batch_id,entry->rtype);
    // insert into planner's queue
    while(!plan_queue[planner_id]->push(entry) && !simulation->is_done()) {}
    INC_STATS(thd_id,plan_txn_cnts[planner_id],1);
}
// need a mapping between thread ids and planner ids
Message * QWorkQueue::plan_dequeue(uint64_t thd_id, uint64_t home_partition) {
  assert(ISSERVER);
  Message * msg = NULL;
#if PROFILE_EXEC_TIMING
  uint64_t prof_starttime = 0;
//    DEBUG_Q("thread %ld, planner_%ld, poping from queue\n", thd_id, home_partition);
  prof_starttime = get_sys_clock();
#endif
#if SERVER_GENERATE_QUERIES
  if(ISSERVER) {
#if INIT_QUERY_MSGS
    msg = client_query_queue.get_next_query(home_partition, thd_id);
#else
    BaseQuery * m_query = NULL;
//    m_query = client_query_queue.get_next_query(home_partition,thd_id);
    m_query = client_query_queue.get_next_query(thd_id,thd_id);
    assert(m_query);
    if(m_query) {
//      DEBUG_Q("thread %ld, home partition = %ld, creating client query message\n", thd_id, home_partition);
      msg = Message::create_message((BaseQuery*)m_query,CL_QRY);
    }
#endif
  }
#else
    work_queue_entry * entry = NULL;
    bool valid = plan_queue[home_partition]->pop(entry);
    if(valid) {
    msg = entry->msg;
    assert(msg);
//    DEBUG_Q("Planner Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
    //DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d, 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
//    DEBUG_M("PlanQueue::dequeue work_queue_entry free\n");
    prof_starttime = get_sys_clock();
    mem_allocator.free(entry,sizeof(work_queue_entry));
    INC_STATS(thd_id, plan_queue_deq_free_mem_time[home_partition], get_sys_clock()-prof_starttime);
  }
#endif
#if PROFILE_EXEC_TIMING
  INC_STATS(thd_id, plan_queue_deq_pop_time[home_partition], get_sys_clock()-prof_starttime);
#endif
  return msg;

}

#endif // - if CC_ALG == QUECC

#if CC_ALG == CALVIN
void QWorkQueue::sequencer_enqueue(uint64_t thd_id, Message * msg) {
#if PROFILE_EXEC_TIMING
  uint64_t starttime = get_sys_clock();
#endif
  assert(msg);
  DEBUG_M("SeqQueue::enqueue work_queue_entry alloc\n");
  work_queue_entry * entry = (work_queue_entry*)mem_allocator.align_alloc(sizeof(work_queue_entry));
  entry->msg = msg;
  entry->rtype = msg->rtype;
  entry->txn_id = msg->txn_id;
  entry->batch_id = msg->batch_id;
  entry->starttime = get_sys_clock();
  assert(ISSERVER);

  DEBUG("Seq Enqueue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
  while(!seq_queue->push(entry) && !simulation->is_done()) {}
#if PROFILE_EXEC_TIMING
  INC_STATS(thd_id,seq_queue_enqueue_time,get_sys_clock() - starttime);
#endif
  INC_STATS(thd_id,seq_queue_enq_cnt,1);

}

Message * QWorkQueue::sequencer_dequeue(uint64_t thd_id) {
  assert(CC_ALG == CALVIN);

  assert(ISSERVER);
  Message * msg = NULL;
#if CC_ALG == CALVIN
  uint64_t starttime = get_sys_clock();
  work_queue_entry * entry = NULL;
  bool valid = seq_queue->pop(entry);

  uint64_t seq_id = query_seq_cnt % client_query_queue.size;
  query_seq_cnt++;

  if(valid) {
    msg = entry->msg;
    assert(msg);
    DEBUG("Seq Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
    uint64_t queue_time = get_sys_clock() - entry->starttime;
    INC_STATS(thd_id,seq_queue_wait_time,queue_time);
    INC_STATS(thd_id,seq_queue_cnt,1);
    //DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d, 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
    DEBUG_M("SeqQueue::dequeue work_queue_entry free\n");
    mem_allocator.free(entry,sizeof(work_queue_entry));
    INC_STATS(thd_id,seq_queue_dequeue_time,get_sys_clock() - starttime);
  }
  else{
#if SERVER_GENERATE_QUERIES
    if(ISSERVER) {
#if INIT_QUERY_MSGS
      msg = client_query_queue.get_next_query(seq_id, seq_id);
      assert(msg->rtype == CL_QRY);
#else
      BaseQuery * m_query = client_query_queue.get_next_query(seq_id,seq_id);
      if(m_query) {
        assert(m_query);
        msg = Message::create_message((BaseQuery*)m_query,CL_QRY);
      }
#endif

    }
#endif
  }
#endif // #if CC_ALG == CALVIN
  return msg;

}

void QWorkQueue::sched_enqueue(uint64_t thd_id, Message * msg) {
  assert(CC_ALG == CALVIN);
  assert(msg);
  assert(ISSERVERN(msg->return_node_id));
#if PROFILE_EXEC_TIMING
  uint64_t starttime = get_sys_clock();
#endif
  DEBUG_M("QWorkQueue::sched_enqueue work_queue_entry alloc\n");
  work_queue_entry * entry = (work_queue_entry*)mem_allocator.alloc(sizeof(work_queue_entry));
  entry->msg = msg;
  entry->rtype = msg->rtype;
  entry->txn_id = msg->txn_id;
  entry->batch_id = msg->batch_id;
  entry->starttime = get_sys_clock();

  DEBUG("Sched Enqueue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
#if PROFILE_EXEC_TIMING
  uint64_t mtx_time_start = get_sys_clock();
#endif
#if SINGLE_NODE
  while(!sched_queue[0]->push(entry) && !simulation->is_done()) {}
#else
  while(!sched_queue[msg->get_return_id()]->push(entry) && !simulation->is_done()) {}
#endif

#if PROFILE_EXEC_TIMING
  INC_STATS(thd_id,mtx[37],get_sys_clock() - mtx_time_start);
  INC_STATS(thd_id,sched_queue_enqueue_time,get_sys_clock() - starttime);
#endif
  INC_STATS(thd_id,sched_queue_enq_cnt,1);
}

Message * QWorkQueue::sched_dequeue(uint64_t thd_id) {
#if PROFILE_EXEC_TIMING
  uint64_t starttime = get_sys_clock();
#endif
  assert(CC_ALG == CALVIN);
  Message * msg = NULL;
  work_queue_entry * entry = NULL;

  bool valid = sched_queue[sched_ptr]->pop(entry);

  if(valid) {

    msg = entry->msg;
    DEBUG("Sched Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
#if PROFILE_EXEC_TIMING
    uint64_t queue_time = get_sys_clock() - entry->starttime;
    INC_STATS(thd_id,sched_queue_wait_time,queue_time);
#endif
    INC_STATS(thd_id,sched_queue_cnt,1);

    DEBUG_M("QWorkQueue::sched_enqueue work_queue_entry free\n");
    mem_allocator.free(entry,sizeof(work_queue_entry));

    if(msg->rtype == RDONE) {
      // Advance to next queue or next epoch
      DEBUG("Sched RDONE %ld %ld\n",sched_ptr,simulation->get_worker_epoch());
      assert(msg->get_batch_id() == simulation->get_worker_epoch());
#if SINGLE_NODE
      INC_STATS(thd_id,sched_epoch_cnt,1);
      INC_STATS(thd_id,sched_epoch_diff,get_sys_clock()-simulation->last_worker_epoch_time);
      simulation->next_worker_epoch();
#else
      if(sched_ptr == g_node_cnt - 1) {
        INC_STATS(thd_id,sched_epoch_cnt,1);
        INC_STATS(thd_id,sched_epoch_diff,get_sys_clock()-simulation->last_worker_epoch_time);
        simulation->next_worker_epoch();
      }
      sched_ptr = (sched_ptr + 1) % g_node_cnt;
#endif
      msg->release();
      msg = NULL;

    } else {
      simulation->inc_epoch_txn_cnt();
      DEBUG("Sched msg dequeue %ld (%ld,%ld) %ld\n",sched_ptr,msg->txn_id,msg->batch_id,simulation->get_worker_epoch());
      M_ASSERT_V(msg->batch_id == simulation->get_worker_epoch(), "WT_%ld: msg->batch_id = %ld, simulation->get_worker_epoch() = %ld ",
                 thd_id, msg->batch_id , simulation->get_worker_epoch());
    }
#if PROFILE_EXEC_TIMING
    INC_STATS(thd_id,sched_queue_dequeue_time,get_sys_clock() - starttime);
#endif
  }


  return msg;

}
#endif //#if CC_ALG == CALVIN

void QWorkQueue::enqueue(uint64_t thd_id, Message * msg,bool busy) {
  if (CC_ALG == QUECC && msg->rtype == CL_QRY){
    DEBUG_Q("With QueCC, enq. to workQ should not happen\n");
    assert(false);
  }
#if PROFILE_EXEC_TIMING
  uint64_t starttime = get_sys_clock();
#endif
  assert(msg);
  DEBUG_M("QWorkQueue::enqueue work_queue_entry alloc\n");
  work_queue_entry * entry = (work_queue_entry*)mem_allocator.align_alloc(sizeof(work_queue_entry));
  entry->msg = msg;
  entry->rtype = msg->rtype;
  entry->txn_id = msg->txn_id;
  entry->batch_id = msg->batch_id;
  entry->starttime = get_sys_clock();
  assert(ISSERVER || ISREPLICA);
  DEBUG("Work Enqueue (%ld,%ld) %d\n",entry->txn_id,entry->batch_id,entry->rtype);
#if PROFILE_EXEC_TIMING
  uint64_t mtx_wait_starttime = get_sys_clock();
#endif
  if(msg->rtype == CL_QRY) {
    while(!new_txn_queue->push(entry) && !simulation->is_done()) {}
  } else {
    while(!work_queue->push(entry) && !simulation->is_done()) {}
  }
#if PROFILE_EXEC_TIMING
  INC_STATS(thd_id,mtx[13],get_sys_clock() - mtx_wait_starttime);
#endif

  if(busy) {
    INC_STATS(thd_id,work_queue_conflict_cnt,1);
  }
#if PROFILE_EXEC_TIMING
  INC_STATS(thd_id,work_queue_enqueue_time,get_sys_clock() - starttime);
#endif
  INC_STATS(thd_id,work_queue_enq_cnt,1);
}

Message * QWorkQueue::dequeue(uint64_t thd_id) {
  assert(ISSERVER || ISREPLICA);
  Message * msg = NULL;
  work_queue_entry * entry = NULL;
#if PROFILE_EXEC_TIMING
  uint64_t mtx_wait_starttime = get_sys_clock();
  uint64_t starttime = get_sys_clock();
#endif
#if ABORT_QUEUES
  // process abort queue for this thread
  abort_queues[thd_id]->process(thd_id);
#endif
  bool valid = work_queue->pop(entry);
  if(!valid) {
#if CC_ALG != CALVIN
#if SERVER_GENERATE_QUERIES
    if(ISSERVER) {
#if INIT_QUERY_MSGS
      msg = client_query_queue.get_next_query(thd_id, thd_id);
      assert(msg->rtype == CL_QRY);
#else
      BaseQuery * m_query = client_query_queue.get_next_query(thd_id,thd_id);
      if(m_query) {
        assert(m_query);
        msg = Message::create_message((BaseQuery*)m_query,CL_QRY);
      }
#endif

    }
#else
    valid = new_txn_queue->pop(entry);
#endif
#else // if calvin
    valid = new_txn_queue->pop(entry);
#endif // if CC_ALG != CALVIN
  }
#if PROFILE_EXEC_TIMING
  INC_STATS(thd_id,mtx[14],get_sys_clock() - mtx_wait_starttime);
#endif
  if(valid) {
    msg = entry->msg;
    assert(msg);
    DEBUG("%ld WQdequeue %ld\n",thd_id,entry->txn_id);
    //printf("%ld WQdequeue %ld\n",thd_id,entry->txn_id);

#if PROFILE_EXEC_TIMING
    uint64_t queue_time = get_sys_clock() - entry->starttime;
    INC_STATS(thd_id,work_queue_wait_time,queue_time);
#endif

    INC_STATS(thd_id,work_queue_cnt,1);
    if(msg->rtype == CL_QRY) {
#if PROFILE_EXEC_TIMING
      INC_STATS(thd_id,work_queue_new_wait_time,queue_time);
#endif
      INC_STATS(thd_id,work_queue_new_cnt,1);
    } else {
#if PROFILE_EXEC_TIMING
      INC_STATS(thd_id,work_queue_old_wait_time,queue_time);
#endif
      INC_STATS(thd_id,work_queue_old_cnt,1);
    }
#if PROFILE_EXEC_TIMING
    msg->wq_time = queue_time;
    DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d, 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
#endif
    DEBUG("Work Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
    DEBUG_M("QWorkQueue::dequeue work_queue_entry free\n");
    mem_allocator.free(entry,sizeof(work_queue_entry));
#if PROFILE_EXEC_TIMING
    INC_STATS(thd_id,work_queue_dequeue_time,get_sys_clock() - starttime);
#endif
  }

#if SERVER_GENERATE_QUERIES
  if(msg && msg->rtype == CL_QRY) {
#if PROFILE_EXEC_TIMING
    INC_STATS(thd_id,work_queue_new_wait_time,get_sys_clock() - starttime);
#endif
    INC_STATS(thd_id,work_queue_new_cnt,1);
  }
#endif
  return msg;
}

