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
#include "sequencer.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "pps_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "wl.h"
#include "helper.h"
#include "msg_queue.h"
#include "msg_thread.h"
#include "work_queue.h"
#include "message.h"
#include "stats.h"
#include <boost/lockfree/queue.hpp>
#if CC_ALG == CALVIN
void Sequencer::init(Workload * wl) {
  next_txn_id = 0;
  rsp_cnt = g_node_cnt + g_client_node_cnt;
  _wl = wl;
  last_time_batch = 0;
  wl_head = NULL;
  wl_tail = NULL;
  fill_queue = new boost::lockfree::queue<Message*, boost::lockfree::capacity<65526> > [g_node_cnt];
}

void Sequencer::free(){
    Message * msg;
    for(uint64_t j = 0; j < g_node_cnt; j++) {
        int fill_msg_cnt = 0;
        while (fill_queue[j].pop(msg)) {
#if WORKLOAD == YCSB
            YCSBClientQueryMessage* cl_msg = (YCSBClientQueryMessage*)msg;
#if !SINGLE_NODE
            for(uint64_t i = 0; i < cl_msg->requests.size(); i++) {
                DEBUG_M("Sequencer::free() ycsb_request free\n");
                mem_allocator.free(cl_msg->requests[i],sizeof(ycsb_request));
            }
            cl_msg->release();
#endif
#endif
//            Message::release_message(msg);
            fill_msg_cnt++;
        }
        printf("Fill cnt [%lu] = %d\n",j,fill_msg_cnt);
    }

    delete [] fill_queue;
    qlite_ll * en = wl_head;
    int n = 0;
    uint64_t m_cnt = 0;
    while (en != NULL){
        LIST_REMOVE_HT(en,wl_head,wl_tail);
//        printf("txn_left is = %u, size = %lu\n",en->txns_left,en->size);

        if (en->txns_left == 0) goto next_en;

        for (uint64_t i =0; i < en->size; i++){
            msg = en->list[i].msg;

            if (en->list[i].server_ack_cnt == 0) {
                continue;
            }

//            printf("server_ack_cnt for %lu is = %d\n",i,en->list[i].server_ack_cnt);
#if WORKLOAD == YCSB
            YCSBClientQueryMessage* cl_msg = (YCSBClientQueryMessage*)msg;
#if !SINGLE_NODE
            for(uint64_t k = 0; k < cl_msg->requests.size(); k++) {
                DEBUG_M("Sequencer::free() ycsb_request free\n");
                mem_allocator.free(cl_msg->requests[k],sizeof(ycsb_request));
            }
//            cl_msg->release();
            Message::release_message(msg);
#endif
#endif
        }

        m_cnt += en->size;
        mem_allocator.free(en->list,sizeof(qlite) * en->max_size);
        mem_allocator.free(en,sizeof(qlite_ll));

next_en:
        en = wl_head;
        n++;
    }
    printf("En count =%d, m_cnt=%lu\n",n,m_cnt);
}

// Assumes 1 thread does sequencer work
void Sequencer::process_ack(Message * msg, uint64_t thd_id) {
    qlite_ll * en = wl_head;
    while(en != NULL && en->epoch != msg->get_batch_id()) {
        en = en->next;
    }
    assert(en);
    qlite * wait_list = en->list;
    assert(wait_list != NULL);
    assert(en->txns_left > 0);

    uint64_t id = msg->get_txn_id() / g_node_cnt;
    uint64_t prof_stat = get_sys_clock();
    assert(wait_list[id].server_ack_cnt > 0);

    // Decrement the number of acks needed for this txn
    uint32_t query_acks_left = ATOM_SUB_FETCH(wait_list[id].server_ack_cnt, 1);

    if (wait_list[id].skew_startts == 0) {
        wait_list[id].skew_startts = get_sys_clock();
    }
    DEBUG_Q("Seq %d: ack_left for (%lu,%lu) is %d\n",g_node_id, msg->get_txn_id(),msg->get_batch_id(),query_acks_left);
    if (query_acks_left == 0) {
        en->txns_left--;
        ATOM_FETCH_ADD(total_txns_finished,1);
        auto amsg = (AckMessage *) msg;
        if (amsg->rc == Abort){
            INC_STATS(thd_id,seq_txn_abort_cnt,1);
        }
        else{
            INC_STATS(thd_id,seq_txn_cnt,1);
        }
        // free msg, queries
#if WORKLOAD == YCSB
        YCSBClientQueryMessage* cl_msg = (YCSBClientQueryMessage*)wait_list[id].msg;
#if !SINGLE_NODE && !SERVER_GENERATE_QUERIES
        // if SERVER_GENERATE_QUERIES: we should not clear query request pointers because they will be reused by transactions
        for(uint64_t i = 0; i < cl_msg->requests.size(); i++) {
            DEBUG_M("Sequencer::process_ack() ycsb_request free\n");
            mem_allocator.free(cl_msg->requests[i],sizeof(ycsb_request));
        }
#endif
#elif WORKLOAD == TPCC
        TPCCClientQueryMessage* cl_msg = (TPCCClientQueryMessage*)wait_list[id].msg;
#if !SINGLE_NODE && !SERVER_GENERATE_QUERIES
      if(cl_msg->txn_type == TPCC_NEW_ORDER) {
          for(uint64_t i = 0; i < cl_msg->items.size(); i++) {
              DEBUG_M("Sequencer::process_ack() items free\n");
              mem_allocator.free(cl_msg->items[i],sizeof(Item_no));
          }
      }
#endif
#elif WORKLOAD == PPS
      PPSClientQueryMessage* cl_msg = (PPSClientQueryMessage*)wait_list[id].msg;

#endif

#if WORKLOAD == PPS
        if ( WORKLOAD == PPS && CC_ALG == CALVIN && ((cl_msg->txn_type == PPS_GETPARTBYSUPPLIER) ||
              (cl_msg->txn_type == PPS_GETPARTBYPRODUCT) ||
              (cl_msg->txn_type == PPS_ORDERPRODUCT))
              && (cl_msg->recon || ((AckMessage*)msg)->rc == Abort)) {
          int abort_cnt = wait_list[id].abort_cnt;
          if (cl_msg->recon) {
              // Copy over part keys
              cl_msg->part_keys.copy( ((AckMessage*)msg)->part_keys);
              DEBUG("Finished RECON (%ld,%ld)\n",msg->get_txn_id(),msg->get_batch_id());
          }
          else {
              uint64_t timespan = get_sys_clock() - wait_list[id].seq_startts;
              if (warmup_done) {
                INC_STATS_ARR(0,start_abort_commit_latency, timespan);
              }
              cl_msg->part_keys.clear();
              DEBUG("Aborted (%ld,%ld)\n",msg->get_txn_id(),msg->get_batch_id());
              INC_STATS(0,total_txn_abort_cnt,1);
              abort_cnt++;
          }

          cl_msg->return_node_id = wait_list[id].client_id;
          wait_list[id].total_batch_time += en->batch_send_time - wait_list[id].seq_startts;
          // restart
          process_txn(cl_msg, thd_id, wait_list[id].seq_first_startts, wait_list[id].seq_startts, wait_list[id].total_batch_time, abort_cnt);
      }
      else {
#endif
        uint64_t curr_clock = get_sys_clock();
        uint64_t timespan = curr_clock - wait_list[id].seq_first_startts;
        uint64_t timespan2 = curr_clock - wait_list[id].seq_startts;
        uint64_t skew_timespan = get_sys_clock() - wait_list[id].skew_startts;
        wait_list[id].total_batch_time += en->batch_send_time - wait_list[id].seq_startts;
        if (warmup_done) {
            INC_STATS_ARR(0,first_start_commit_latency, timespan);
            INC_STATS_ARR(0,last_start_commit_latency, timespan2);
            INC_STATS_ARR(0,start_abort_commit_latency, timespan2);
        }
        if (wait_list[id].abort_cnt > 0) {
            INC_STATS(0,unique_txn_abort_cnt,1);
        }

        INC_STATS(0,lat_l_loc_msg_queue_time,wait_list[id].total_batch_time);
        INC_STATS(0,lat_l_loc_process_time,skew_timespan);

        INC_STATS(0,lat_short_work_queue_time,msg->lat_work_queue_time);
        INC_STATS(0,lat_short_msg_queue_time,msg->lat_msg_queue_time);
        INC_STATS(0,lat_short_cc_block_time,msg->lat_cc_block_time);
        INC_STATS(0,lat_short_cc_time,msg->lat_cc_time);
        INC_STATS(0,lat_short_process_time,msg->lat_process_time);

        cl_msg->release();

#if !SINGLE_NODE // no need to prepare/send message to client as there is no client
        if (msg->return_node_id != g_node_id) {
            /*
                if (msg->lat_network_time/BILLION > 1.0) {
                  printf("%ld %d %ld -> %d: %f %f\n",msg->txn_id, msg->rtype, msg->return_node_id,g_node_id ,msg->lat_network_time/BILLION, msg->lat_other_time/BILLION);
                }
                */
            INC_STATS(0,lat_short_network_time,msg->lat_network_time);
        }
        INC_STATS(0,lat_short_batch_time,wait_list[id].total_batch_time);

        PRINT_LATENCY("lat_l_seq %ld %ld %d %f %f %f\n"
        , msg->get_txn_id()
        , msg->get_batch_id()
        , wait_list[id].abort_cnt
        , (double) timespan / BILLION
        , (double) skew_timespan / BILLION
        , (double) wait_list[id].total_batch_time / BILLION
        );



        ClientResponseMessage * rsp_msg = (ClientResponseMessage*)Message::create_message(msg->get_txn_id(),CL_RSP);
#if ABORT_MODE
        rsp_msg->rc = amsg->rc;
#else
        rsp_msg->rc = Commit;
#endif
        DEBUG_Q("%s txn_id=%lu by sending client resposne message\n",(rsp_msg->rc == Commit)? "Commiting": "Aborting",msg->get_txn_id());
        assert(rsp_msg->rc == Commit || rsp_msg->rc == Abort);
        rsp_msg->client_startts = wait_list[id].client_startts;
        msg_queue.enqueue(thd_id,rsp_msg,wait_list[id].client_id);
#if COUNT_BASED_SIM_ENABLED && CC_ALG == CALVIN
        simulation->inc_txn_cnt(1);
#endif

#endif

#if WORKLOAD == PPS
        }
#endif

        INC_STATS(thd_id,seq_complete_cnt,1);

    }

    // If we have all acks for this batch, send qry responses to all clients
    if (en->txns_left == 0) {
        DEBUG("FINISHED BATCH %ld\n",en->epoch);
        LIST_REMOVE_HT(en,wl_head,wl_tail);
        mem_allocator.free(en->list,sizeof(qlite) * en->max_size);
        mem_allocator.free(en,sizeof(qlite_ll));

    }
    INC_STATS(thd_id,seq_ack_time,get_sys_clock() - prof_stat);
}

// Assumes 1 thread does sequencer work
void Sequencer::process_txn( Message * msg,uint64_t thd_id, uint64_t early_start, uint64_t last_start, uint64_t wait_time, uint32_t abort_cnt) {

    uint64_t starttime = get_sys_clock();
    DEBUG("SEQ Processing msg\n");
    qlite_ll * en = wl_tail;

    // LL is potentially a bottleneck here
    if(!en || en->epoch != simulation->get_seq_epoch()+1) {
      DEBUG("SEQ new wait list for epoch %ld\n",simulation->get_seq_epoch()+1);
      // First txn of new wait list
      en = (qlite_ll *) mem_allocator.alloc(sizeof(qlite_ll));
      en->epoch = simulation->get_seq_epoch()+1;
      //TQ: this may cause the sequencer to allocate a lot of memory and the process get killed due to OOM
//      en->max_size = 1000;
//      en->max_size = 200;
      en->max_size = 100;
      en->size = 0;
      en->txns_left = 0;
      en->list = (qlite *) mem_allocator.alloc(sizeof(qlite) * en->max_size);
      LIST_PUT_TAIL(wl_head,wl_tail,en)
    }
    if(en->size == en->max_size) {
      en->max_size *= 2;
      en->list = (qlite *) mem_allocator.realloc(en->list,sizeof(qlite) * en->max_size);
    }
#if !SINGLE_NODE
    txnid_t txn_id = g_node_id + g_node_cnt * next_txn_id;
    uint64_t id = txn_id / g_node_cnt;
#else
    txnid_t txn_id = next_txn_id;
    uint64_t id = txn_id;
#endif
    next_txn_id++;
    msg->batch_id = en->epoch;
    msg->txn_id = txn_id;
    assert(txn_id != UINT64_MAX);
    assert(txn_id >= 0);

#if WORKLOAD == YCSB
    std::set<uint64_t> participants = YCSBQuery::participants(msg,_wl);
#elif WORKLOAD == TPCC
    std::set<uint64_t> participants = TPCCQuery::participants(msg,_wl);
#elif WORKLOAD == PPS
    std::set<uint64_t> participants = PPSQuery::participants(msg,_wl);
#endif
    uint32_t server_ack_cnt = participants.size();
    assert(server_ack_cnt > 0);
#if !SINGLE_NODE
    assert(ISCLIENTN(msg->get_return_id()));
#endif
    en->list[id].client_id = msg->get_return_id();
    en->list[id].client_startts = ((ClientQueryMessage*)msg)->client_startts;
    //en->list[id].seq_startts = get_sys_clock();

    en->list[id].total_batch_time = wait_time;
    en->list[id].abort_cnt = abort_cnt;
    en->list[id].skew_startts = 0;
    en->list[id].server_ack_cnt = server_ack_cnt;
    en->list[id].msg = msg;
    en->size++;
    en->txns_left++;
    // Note: Modifying msg!
    msg->return_node_id = g_node_id;
    msg->lat_network_time = 0;
    msg->lat_other_time = 0;
#if CC_ALG == CALVIN && WORKLOAD == PPS
    PPSClientQueryMessage* cl_msg = (PPSClientQueryMessage*) msg;
    if (cl_msg->txn_type == PPS_GETPARTBYSUPPLIER ||
            cl_msg->txn_type == PPS_GETPARTBYPRODUCT ||
            cl_msg->txn_type == PPS_ORDERPRODUCT) {
        if (cl_msg->part_keys.size() == 0) {
            cl_msg->recon = true;
            en->list[id].seq_startts = get_sys_clock();
        }
        else {
            cl_msg->recon = false;
            en->list[id].seq_startts = last_time;
        }

    }
    else {
        cl_msg->recon = false;
        en->list[id].seq_startts = get_sys_clock();
    }
#else
    en->list[id].seq_startts = get_sys_clock();
#endif
    if (early_start == 0) {
        en->list[id].seq_first_startts = en->list[id].seq_startts;
    } else {
        en->list[id].seq_first_startts = early_start;
    }
    assert(en->size == en->txns_left);
    M_ASSERT_V(en->size <= ((uint64_t)g_inflight_max * g_node_cnt), "en->size=%lu\n", en->size);
    assert(en->size <= ((uint64_t)g_inflight_max * g_node_cnt));

    // Add new txn to fill queue
#if SINGLE_NODE
    M_ASSERT_V(participants.size() == 1, "SEQ: participant size = %ld\n", participants.size());
#endif
    for(auto participant = participants.begin(); participant != participants.end(); participant++) {
      DEBUG("SEQ adding (%ld,%ld) to fill queue (recon: %d)\n",msg->get_txn_id(),msg->get_batch_id(),((PPSClientQueryMessage*)msg)->recon);
      while(!fill_queue[*participant].push(msg) && !simulation->is_done()) {}
    }

	INC_STATS(thd_id,seq_process_cnt,1);
	INC_STATS(thd_id,seq_process_time,get_sys_clock() - starttime);
	ATOM_ADD(total_txns_received,1);

}


// Assumes 1 thread does sequencer work
void Sequencer::send_next_batch(uint64_t thd_id) {
  uint64_t prof_stat = get_sys_clock();
  qlite_ll * en = wl_tail;
  bool empty = true;
  if(en && en->epoch == simulation->get_seq_epoch()) {
    DEBUG_Q("SEND NEXT BATCH %ld [%ld,%ld] %ld\n",thd_id,simulation->get_seq_epoch(),en->epoch,en->size);
    empty = false;

    en->batch_send_time = prof_stat;
  }

  if (ISREPLICAN(g_node_id)){
      return;
  }

  assert(ISSERVERN(g_node_id));

  Message * msg;

#if SINGLE_NODE
    while(fill_queue[0].pop(msg)) {
        work_queue.sched_enqueue(thd_id,msg);
    }
    if(!empty) {
        DEBUG("Seq RDONE %ld\n",simulation->get_seq_epoch())
    }
    msg = Message::create_message(RDONE);
    msg->batch_id = simulation->get_seq_epoch();
    work_queue.sched_enqueue(thd_id,msg);
#else
    //Send batch to schedulers. This is done once the batch is agreed upon (e.g., after sending it to Zookeeper)
  for(uint64_t j = 0; j < g_node_cnt; j++) {
    while(fill_queue[j].pop(msg)) {
      if(j == g_node_id) {
//          DEBUG_Q("Seq assign local txn_id=%lu, batch_id=%lu\n",msg->txn_id,msg->batch_id);
          work_queue.sched_enqueue(thd_id,msg);
      } else {
//          DEBUG_Q("Seq sending to node=%lu, txn_id=%lu, batch_id=%lu\n",j,msg->txn_id,msg->batch_id);
        msg_queue.enqueue(thd_id,msg,j);
      }
    }
//    if(!empty) {
//      DEBUG_Q("Seq RDONE %ld\n",simulation->get_seq_epoch())
//    }
    msg = Message::create_message(RDONE);
    msg->batch_id = simulation->get_seq_epoch(); 
    if(j == g_node_id) {
      work_queue.sched_enqueue(thd_id,msg);
    } else {
      msg_queue.enqueue(thd_id,msg,j);
    }
  }
#endif
  if(last_time_batch > 0) {
    INC_STATS(thd_id,seq_batch_time,get_sys_clock() - last_time_batch);
  }
  last_time_batch = get_sys_clock();

	INC_STATS(thd_id,seq_batch_cnt,1);
  if(!empty) {
    INC_STATS(thd_id,seq_full_batch_cnt,1);
  }
  INC_STATS(thd_id,seq_prep_time,get_sys_clock() - prof_stat);
  next_txn_id = 0;
}

#endif //#if CC_ALG == CALVIN