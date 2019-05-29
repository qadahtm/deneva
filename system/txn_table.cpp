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
#include "txn_table.h"
#include "tpcc_query.h"
#include "tpcc.h"
#include "ycsb_query.h"
#include "ycsb.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"
#include "row.h"
#include "pool.h"
#include "work_queue.h"
#include "message.h"

void TxnTable::init() {
  //pool_size = g_inflight_max * g_node_cnt * 2 + 1;
  pool_size = g_inflight_max + 1;
  DEBUG_M("TxnTable::init pool_node alloc\n");
  pool = (pool_node **) mem_allocator.align_alloc(sizeof(pool_node*) * pool_size);
  for(uint32_t i = 0; i < pool_size;i++) {
    pool[i] = (pool_node *) mem_allocator.align_alloc(sizeof(struct pool_node));
    pool[i]->head = NULL;
    pool[i]->tail = NULL;
    pool[i]->cnt = 0;
    pool[i]->modify = false;
    pool[i]->min_ts = UINT64_MAX;
  }
}

void TxnTable::dump() {
  for(uint64_t i = 0; i < pool_size;i++) {
    if(pool[i]->cnt  == 0)
      continue;
      txn_node_t t_node = pool[i]->head;

      while (t_node != NULL) {
        printf("TT (%ld,%ld)\n",t_node->txn_man->get_txn_id(),t_node->txn_man->get_batch_id()
            );
        t_node = t_node->next;
      }
      
  }
}

bool TxnTable::is_matching_txn_node(txn_node_t t_node, uint64_t txn_id, uint64_t batch_id){
  assert(t_node);
#if CC_ALG == CALVIN || CC_ALG == QUECC
//  DEBUG_Q("Matching based txn_id and batch_id\n");
    return (t_node->txn_man->get_txn_id() == txn_id && t_node->txn_man->get_batch_id() == batch_id); 
#else
    return (t_node->txn_man->get_txn_id() == txn_id); 
#endif
}

void TxnTable::update_min_ts(uint64_t thd_id, uint64_t txn_id,uint64_t batch_id,uint64_t ts){

  uint64_t pool_id = txn_id % pool_size;
  while(!ATOM_CAS(pool[pool_id]->modify,false,true)) { };
  if(ts < pool[pool_id]->min_ts)
    pool[pool_id]->min_ts = ts;
  ATOM_CAS(pool[pool_id]->modify,true,false);
}

TxnManager * TxnTable::get_transaction_manager(uint64_t thd_id, uint64_t txn_id,uint64_t batch_id){
  DEBUG("TxnTable::get_txn_manager %ld / %ld\n",txn_id,pool_size);
//    DEBUG_Q("WT_%lu: TxnTable::get_txn_manager %ld / %ld, get_cnt=%lu, release_cnt=%lu, active_cnt=%lu\n",
//            thd_id,txn_id,pool_size,get_cnt.fetch_add(1),rel_cnt.load(),active_cnt.fetch_add(1));
  uint64_t pool_id = txn_id % pool_size;
#if PROFILE_EXEC_TIMING
  uint64_t starttime = get_sys_clock();
  uint64_t mtx_starttime = starttime;
#endif

  // set modify bit for this pool: txn_id % pool_size
  while(!ATOM_CAS(pool[pool_id]->modify,false,true)) { };
#if PROFILE_EXEC_TIMING
  INC_STATS(thd_id,mtx[7],get_sys_clock()-mtx_starttime);
#endif
  txn_node_t t_node = pool[pool_id]->head;
  TxnManager * txn_man = NULL;
#if PROFILE_EXEC_TIMING
  uint64_t prof_starttime = get_sys_clock();
#endif
  while (t_node != NULL) {
    if(is_matching_txn_node(t_node,txn_id,batch_id)) {
      txn_man = t_node->txn_man;
      break;
    }
    t_node = t_node->next;
  }
#if PROFILE_EXEC_TIMING
  INC_STATS(thd_id,mtx[20],get_sys_clock()-prof_starttime);
#endif

  if(!txn_man) {
#if PROFILE_EXEC_TIMING
    prof_starttime = get_sys_clock();
#endif
    txn_table_pool.get(thd_id,t_node);
#if PROFILE_EXEC_TIMING
    INC_STATS(thd_id,mtx[21],get_sys_clock()-prof_starttime);
    prof_starttime = get_sys_clock();
#endif
    txn_man_pool.get(thd_id,txn_man);
#if PROFILE_EXEC_TIMING
    INC_STATS(thd_id,mtx[22],get_sys_clock()-prof_starttime);
    prof_starttime = get_sys_clock();
#endif
    txn_man->set_txn_id(txn_id);
    txn_man->set_batch_id(batch_id);
    t_node->txn_man = txn_man;
#if PROFILE_EXEC_TIMING
    txn_man->txn_stats.starttime = get_sys_clock();
    txn_man->txn_stats.restart_starttime = txn_man->txn_stats.starttime;
#endif
    LIST_PUT_TAIL(pool[pool_id]->head,pool[pool_id]->tail,t_node);
#if PROFILE_EXEC_TIMING
    INC_STATS(thd_id,mtx[23],get_sys_clock()-prof_starttime);
    prof_starttime = get_sys_clock();
#endif
    ++pool[pool_id]->cnt;
    if(pool[pool_id]->cnt > 1) {
      INC_STATS(thd_id,txn_table_cflt_cnt,1);
      INC_STATS(thd_id,txn_table_cflt_size,pool[pool_id]->cnt-1);
    }
    INC_STATS(thd_id,txn_table_new_cnt,1);
#if PROFILE_EXEC_TIMING
    INC_STATS(thd_id,mtx[24],get_sys_clock()-prof_starttime);
#endif
  }

#if CC_ALG == MVCC
  if(txn_man->get_timestamp() < pool[pool_id]->min_ts)
    pool[pool_id]->min_ts = txn_man->get_timestamp();
#endif


  // unset modify bit for this pool: txn_id % pool_size
  ATOM_CAS(pool[pool_id]->modify,true,false);
#if PROFILE_EXEC_TIMING
  INC_STATS(thd_id,txn_table_get_time,get_sys_clock() - starttime);
#endif
  INC_STATS(thd_id,txn_table_get_cnt,1);
  return txn_man;

}

void TxnTable::restart_txn(uint64_t thd_id, uint64_t txn_id,uint64_t batch_id){
  uint64_t pool_id = txn_id % pool_size;
  // set modify bit for this pool: txn_id % pool_size
  while(!ATOM_CAS(pool[pool_id]->modify,false,true)) { };

  txn_node_t t_node = pool[pool_id]->head;

  while (t_node != NULL) {
    if(is_matching_txn_node(t_node,txn_id,batch_id)) {
#if CC_ALG == CALVIN
    //TQ: Enqueueing this to the work_queue allows the transaction to execute without locks which is fine because
      // a transaction is restarted only if all of its lock requests are fullfilled.
    work_queue.enqueue(thd_id,Message::create_message(t_node->txn_man,RTXN),false);
#else
      if(IS_LOCAL(txn_id))
        work_queue.enqueue(thd_id,Message::create_message(t_node->txn_man,RTXN_CONT),false);
      else
        work_queue.enqueue(thd_id,Message::create_message(t_node->txn_man,RQRY_CONT),false);
#endif
      break;
    }
    t_node = t_node->next;
  }

  // unset modify bit for this pool: txn_id % pool_size
  ATOM_CAS(pool[pool_id]->modify,true,false);

}

void TxnTable::release_transaction_manager(uint64_t thd_id, uint64_t txn_id, uint64_t batch_id){
  uint64_t pool_id = txn_id % pool_size;
#if PROFILE_EXEC_TIMING
  uint64_t starttime = get_sys_clock();
  uint64_t mtx_starttime = starttime;
#endif
  // set modify bit for this pool: txn_id % pool_size
  while(!ATOM_CAS(pool[pool_id]->modify,false,true)) { };
#if DEBUG_QUECC
    rel_cnt.fetch_add(1);
    active_cnt.fetch_add(-1);
#endif
#if PROFILE_EXEC_TIMING
  INC_STATS(thd_id,mtx[8],get_sys_clock()-mtx_starttime);
#endif
  txn_node_t t_node = pool[pool_id]->head;
if (t_node == NULL){
  DEBUG_Q("t_node == NULL\n");
}
#if CC_ALG == MVCC
  uint64_t min_ts = UINT64_MAX;
  txn_node_t saved_t_node = NULL;
#endif
#if PROFILE_EXEC_TIMING
  uint64_t prof_starttime = get_sys_clock();
#endif
  while (t_node != NULL) {
    if(is_matching_txn_node(t_node,txn_id,batch_id)) {
      LIST_REMOVE_HT(t_node,pool[txn_id % pool_size]->head,pool[txn_id % pool_size]->tail);
      --pool[pool_id]->cnt;
#if CC_ALG == MVCC
    saved_t_node = t_node;
    t_node = t_node->next;
    continue;
#else
      break;
#endif
    }
#if CC_ALG == MVCC
    if(t_node->txn_man->get_timestamp() < min_ts)
      min_ts = t_node->txn_man->get_timestamp();
#endif
    t_node = t_node->next;
  }
#if PROFILE_EXEC_TIMING
  INC_STATS(thd_id,mtx[25],get_sys_clock()-prof_starttime);
  prof_starttime = get_sys_clock();
#endif

#if CC_ALG == MVCC
  t_node = saved_t_node;
  pool[pool_id]->min_ts = min_ts;
#endif

  // unset modify bit for this pool: txn_id % pool_size
  bool res __attribute__((unused)) = ATOM_CAS(pool[pool_id]->modify,true,false);
#if PROFILE_EXEC_TIMING
  prof_starttime = get_sys_clock();
#endif
  assert(t_node);
  assert(t_node->txn_man);

  txn_man_pool.put(thd_id,t_node->txn_man);

#if PROFILE_EXEC_TIMING
  INC_STATS(thd_id,mtx[26],get_sys_clock()-prof_starttime);
  prof_starttime = get_sys_clock();
#endif
  txn_table_pool.put(thd_id,t_node);
#if PROFILE_EXEC_TIMING
  INC_STATS(thd_id,mtx[27],get_sys_clock()-prof_starttime);
  INC_STATS(thd_id,txn_table_release_time,get_sys_clock() - starttime);
#endif
  INC_STATS(thd_id,txn_table_release_cnt,1);

}

uint64_t TxnTable::get_min_ts(uint64_t thd_id) {
#if PROFILE_EXEC_TIMING
  uint64_t starttime = get_sys_clock();
#endif
  uint64_t min_ts = UINT64_MAX;
  for(uint64_t i = 0 ; i < pool_size; i++) {
    uint64_t pool_min_ts = pool[i]->min_ts;
    if(pool_min_ts < min_ts)
      min_ts = pool_min_ts;
  }
#if PROFILE_EXEC_TIMING
  INC_STATS(thd_id,txn_table_min_ts_time,get_sys_clock() - starttime);
#endif
  return min_ts;

}

void TxnTable::free() {
    //TQ
    uint64_t txn_id;
    txn_node_t t_node;
    for(uint32_t i = 0; i < pool_size;i++) {
        t_node = pool[i]->head;

        while (t_node != NULL) {
            LIST_REMOVE_HT(t_node,pool[i]->head,pool[i]->tail);
            --pool[i]->cnt;

            txn_id = t_node->txn_man->get_txn_id();
            t_node->txn_man->release();
            // Releasing the txn manager.
            txn_man_pool.put(txn_id,t_node->txn_man);

            // Releasing the node associated with the txn_mann
            txn_table_pool.put(txn_id,t_node);

            t_node = t_node->next;
        }
        mem_allocator.free(pool[i],0);
    }
    mem_allocator.free(pool,0);
}

void TxnTable::cleanup() {
  // not thread-safe
  // must be called by main
  for (uint64_t j = 0; j < g_thread_cnt; ++j) {
    uint64_t thd_id = j;
    for (uint64_t i = 0; i < pool_size; ++i) {
      txn_node_t t_node = pool[i]->head;
      while (t_node != NULL) {
        LIST_REMOVE_HT(t_node,pool[i]->head,pool[i]->tail);
        --pool[i]->cnt;
//        t_node->txn_man->release();
        txn_man_pool.put(thd_id,t_node->txn_man);
        t_node = t_node->next;
      }

    }
  }

}

