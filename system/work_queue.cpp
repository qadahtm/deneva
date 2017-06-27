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

  // QUECC planners

  rng = new boost::random::mt19937[g_rem_thread_cnt];
  plan_queue = new boost::lockfree::queue<work_queue_entry* > * [g_plan_thread_cnt];
  for ( uint64_t i = 0; i < g_plan_thread_cnt; i++) {
    plan_queue[i] = new boost::lockfree::queue<work_queue_entry* > (0);
  }
  //TODO(tq): is this cache-aware?
  for (uint64_t i=0; i < g_batch_map_length ; i++){
    for (uint64_t j=0; j < g_thread_cnt; j++){
      for (uint64_t k=0; k< g_plan_thread_cnt ; k++){
        (batch_map[i][j][k]).store(0);
      }
    }
  }

  //TODO(tq): is this cache-aware?
  for (uint64_t i=0; i < g_batch_map_length ; i++){
    for (uint64_t j=0; j < g_plan_thread_cnt; j++){
      (batch_map_comp_cnts[i][j]).store(0);
      (batch_pg_map[i][j]).store(0);
    }
  }

  // QueCC execution queue free list
  exec_queue_free_list = new boost::lockfree::queue<Array<exec_queue_entry> *> * [g_plan_thread_cnt];
  exec_qs_free_list = new boost::lockfree::queue<Array<Array<exec_queue_entry> *> *> * [g_plan_thread_cnt];
  for ( uint64_t i = 0; i < g_plan_thread_cnt; i++) {
    exec_queue_free_list[i] = new boost::lockfree::queue<Array<exec_queue_entry> *> (FREE_LIST_INITIAL_SIZE);
    exec_qs_free_list[i] = new boost::lockfree::queue<Array<Array<exec_queue_entry> *> *>(FREE_LIST_INITIAL_SIZE);
  }
  // completion queue
  completion_queue = new boost::lockfree::queue<transaction_context *>(0);

//  txn_ctxs_freelist = new boost::lockfree::queue<transaction_context *> ;
  txn_ctxs_free_list = new boost::lockfree::queue<transaction_context *> * [g_plan_thread_cnt];
  for ( uint64_t i = 0; i < g_plan_thread_cnt; i++) {
    txn_ctxs_free_list[i] = new boost::lockfree::queue<transaction_context *> (FREE_LIST_INITIAL_SIZE);
  }

#if QUECC_DEBUG
  inflight_msg.store(0);
#endif
  DEBUG_Q("Initialized batch_map\n");
}

uint64_t QWorkQueue::get_random_planner_id(uint64_t thd_id) {
  boost::random::uniform_int_distribution<> dist(0, max_planner_index);
  return (uint64_t) dist(rng[thd_id]);
}

void QWorkQueue::sequencer_enqueue(uint64_t thd_id, Message * msg) {
  uint64_t starttime = get_sys_clock();
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

  INC_STATS(thd_id,seq_queue_enqueue_time,get_sys_clock() - starttime);
  INC_STATS(thd_id,seq_queue_enq_cnt,1);

}

Message * QWorkQueue::sequencer_dequeue(uint64_t thd_id) {
  uint64_t starttime = get_sys_clock();
  assert(ISSERVER);
  Message * msg = NULL;
  work_queue_entry * entry = NULL;
  bool valid = seq_queue->pop(entry);

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

  return msg;

}

void QWorkQueue::sched_enqueue(uint64_t thd_id, Message * msg) {
  assert(CC_ALG == CALVIN);
  assert(msg);
  assert(ISSERVERN(msg->return_node_id));
  uint64_t starttime = get_sys_clock();

  DEBUG_M("QWorkQueue::sched_enqueue work_queue_entry alloc\n");
  work_queue_entry * entry = (work_queue_entry*)mem_allocator.alloc(sizeof(work_queue_entry));
  entry->msg = msg;
  entry->rtype = msg->rtype;
  entry->txn_id = msg->txn_id;
  entry->batch_id = msg->batch_id;
  entry->starttime = get_sys_clock();

  DEBUG("Sched Enqueue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
  uint64_t mtx_time_start = get_sys_clock();
  while(!sched_queue[msg->get_return_id()]->push(entry) && !simulation->is_done()) {}
  INC_STATS(thd_id,mtx[37],get_sys_clock() - mtx_time_start);

  INC_STATS(thd_id,sched_queue_enqueue_time,get_sys_clock() - starttime);
  INC_STATS(thd_id,sched_queue_enq_cnt,1);
}

Message * QWorkQueue::sched_dequeue(uint64_t thd_id) {
  uint64_t starttime = get_sys_clock();

  assert(CC_ALG == CALVIN);
  Message * msg = NULL;
  work_queue_entry * entry = NULL;

  bool valid = sched_queue[sched_ptr]->pop(entry);

  if(valid) {

    msg = entry->msg;
    DEBUG("Sched Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);

    uint64_t queue_time = get_sys_clock() - entry->starttime;
    INC_STATS(thd_id,sched_queue_wait_time,queue_time);
    INC_STATS(thd_id,sched_queue_cnt,1);

    DEBUG_M("QWorkQueue::sched_enqueue work_queue_entry free\n");
    mem_allocator.free(entry,sizeof(work_queue_entry));

    if(msg->rtype == RDONE) {
      // Advance to next queue or next epoch
      DEBUG("Sched RDONE %ld %ld\n",sched_ptr,simulation->get_worker_epoch());
      assert(msg->get_batch_id() == simulation->get_worker_epoch());
      if(sched_ptr == g_node_cnt - 1) {
        INC_STATS(thd_id,sched_epoch_cnt,1);
        INC_STATS(thd_id,sched_epoch_diff,get_sys_clock()-simulation->last_worker_epoch_time);
        simulation->next_worker_epoch();
      }
      sched_ptr = (sched_ptr + 1) % g_node_cnt;
      msg->release();
      msg = NULL;

    } else {
      simulation->inc_epoch_txn_cnt();
      DEBUG("Sched msg dequeue %ld (%ld,%ld) %ld\n",sched_ptr,msg->txn_id,msg->batch_id,simulation->get_worker_epoch());
      assert(msg->batch_id == simulation->get_worker_epoch());
    }

    INC_STATS(thd_id,sched_queue_dequeue_time,get_sys_clock() - starttime);
  }


  return msg;

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
Message * QWorkQueue::plan_dequeue(uint64_t thd_id, uint64_t planner_id) {
  assert(ISSERVER);
  Message * msg = NULL;

  uint64_t prof_starttime = 0;
//    DEBUG_Q("thread %ld, planner_%ld, poping from queue\n", thd_id, planner_id);
  prof_starttime = get_sys_clock();
#if SERVER_GENERATE_QUERIES
  if(ISSERVER) {
    BaseQuery * m_query = NULL;
#if CC_ALG == QUECC
    m_query = client_query_queue.get_next_query(planner_id,thd_id);
#else
    m_query = client_query_queue.get_next_query(thd_id,thd_id);
#endif
    if(m_query) {
      assert(m_query);
      msg = Message::create_message((BaseQuery*)m_query,CL_QRY);
    }
  }
#else
    work_queue_entry * entry = NULL;
    bool valid = plan_queue[planner_id]->pop(entry);
    if(valid) {
    msg = entry->msg;
    assert(msg);
//    DEBUG_Q("Planner Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
    //DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d, 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
//    DEBUG_M("PlanQueue::dequeue work_queue_entry free\n");
    prof_starttime = get_sys_clock();
    mem_allocator.free(entry,sizeof(work_queue_entry));
    INC_STATS(thd_id, plan_queue_deq_free_mem_time[planner_id], get_sys_clock()-prof_starttime);
  }
#endif

  INC_STATS(thd_id, plan_queue_deq_pop_time[planner_id], get_sys_clock()-prof_starttime);

  return msg;

}


void QWorkQueue::enqueue(uint64_t thd_id, Message * msg,bool busy) {
  if (CC_ALG == QUECC && msg->rtype == CL_QRY){
    DEBUG_Q("With QueCC, enq. to workQ should not happen\n");
    assert(false);
  }
  uint64_t starttime = get_sys_clock();
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

  uint64_t mtx_wait_starttime = get_sys_clock();
  if(msg->rtype == CL_QRY) {
    while(!new_txn_queue->push(entry) && !simulation->is_done()) {}
  } else {
    while(!work_queue->push(entry) && !simulation->is_done()) {}
  }
  INC_STATS(thd_id,mtx[13],get_sys_clock() - mtx_wait_starttime);

  if(busy) {
    INC_STATS(thd_id,work_queue_conflict_cnt,1);
  }
  INC_STATS(thd_id,work_queue_enqueue_time,get_sys_clock() - starttime);
  INC_STATS(thd_id,work_queue_enq_cnt,1);
}

Message * QWorkQueue::dequeue(uint64_t thd_id) {

  uint64_t starttime = get_sys_clock();
  assert(ISSERVER || ISREPLICA);
  Message * msg = NULL;
  work_queue_entry * entry = NULL;
  uint64_t mtx_wait_starttime = get_sys_clock();
  bool valid = work_queue->pop(entry);
  if(!valid) {
#if SERVER_GENERATE_QUERIES
    if(ISSERVER) {
      BaseQuery * m_query = client_query_queue.get_next_query(thd_id,thd_id);
      if(m_query) {
        assert(m_query);
        msg = Message::create_message((BaseQuery*)m_query,CL_QRY);
      }
    }
#else
    valid = new_txn_queue->pop(entry);
#endif
  }
  INC_STATS(thd_id,mtx[14],get_sys_clock() - mtx_wait_starttime);
  
  if(valid) {
    if (CC_ALG == QUECC){
      DEBUG_Q("With QueCC, deq. from workQ should not happen\n");
      assert(false);
    }

    msg = entry->msg;
    assert(msg);
    DEBUG("%ld WQdequeue %ld\n",thd_id,entry->txn_id);
    //printf("%ld WQdequeue %ld\n",thd_id,entry->txn_id);
    uint64_t queue_time = get_sys_clock() - entry->starttime;
    INC_STATS(thd_id,work_queue_wait_time,queue_time);
    INC_STATS(thd_id,work_queue_cnt,1);
    if(msg->rtype == CL_QRY) {
      INC_STATS(thd_id,work_queue_new_wait_time,queue_time);
      INC_STATS(thd_id,work_queue_new_cnt,1);
    } else {
      INC_STATS(thd_id,work_queue_old_wait_time,queue_time);
      INC_STATS(thd_id,work_queue_old_cnt,1);
    }
    msg->wq_time = queue_time;
    DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d, 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
    DEBUG("Work Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
    DEBUG_M("QWorkQueue::dequeue work_queue_entry free\n");
    mem_allocator.free(entry,sizeof(work_queue_entry));
    INC_STATS(thd_id,work_queue_dequeue_time,get_sys_clock() - starttime);
  }

#if SERVER_GENERATE_QUERIES
  if(msg && msg->rtype == CL_QRY) {
    INC_STATS(thd_id,work_queue_new_wait_time,get_sys_clock() - starttime);
    INC_STATS(thd_id,work_queue_new_cnt,1);
  }
#endif
  return msg;
}

