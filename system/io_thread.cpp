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
#include "helper.h"
#include "manager.h"
#include "thread.h"
#include "io_thread.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "math.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "message.h"
#include "client_txn.h"
#include "work_queue.h"
#include "txn.h"

void InputThread::setup() {

    std::vector<Message *> *msgs;
    while (!simulation->is_setup_done()) {
        msgs = tport_man.recv_msg(get_thd_id());
        if (msgs == NULL)
            continue;
        while (!msgs->empty()) {
            Message *msg = msgs->front();
            if (msg->rtype == INIT_DONE) {
                printf("Received INIT_DONE from node %ld\n", msg->return_node_id);
                fflush(stdout);
                // TQ: simulation starts at the server at the receiver/input thread.
                simulation->process_setup_msg();
            } else {
                assert(ISSERVER || ISREPLICA);
//                printf("Received Msg %d from node %ld\n",msg->rtype,msg->return_node_id);
#if CC_ALG == CALVIN
                if(msg->rtype == CALVIN_ACK ||(msg->rtype == CL_QRY && ISCLIENTN(msg->get_return_id()))) {
                  work_queue.sequencer_enqueue(get_thd_id(),msg);
                  msgs->erase(msgs->begin());
                  continue;
                }
                if( msg->rtype == RDONE || msg->rtype == CL_QRY) {
                    M_ASSERT_V(ISSERVERN(msg->get_return_id()),
                               "return nid=%lu\n", msg->get_return_id())
//                  assert(ISSERVERN(msg->get_return_id()));
                  work_queue.sched_enqueue(get_thd_id(),msg);
                  msgs->erase(msgs->begin());
                  continue;
                }
#endif
#if CC_ALG == QUECC
//      DEBUG_Q("Enqueue to planning layer\n")
                if (msg->rtype == CL_QRY) {
                    uint64_t dplan_id = planner_msg_cnt % g_plan_thread_cnt;
                    planner_enq_msg_cnt[dplan_id]++;
                    work_queue.plan_enqueue(dplan_id, msg);
                    planner_msg_cnt++;
                    msgs->erase(msgs->begin());
                    continue;
                }

#endif // CC_ALG == QUECC
                work_queue.enqueue(get_thd_id(), msg, false);
            }
            msgs->erase(msgs->begin());
        }
        delete msgs;
    }
}

RC InputThread::run() {
    tsetup();
    printf("N_%u: Running InputThread %ld\n",g_node_id, _thd_id);
    fflush(stdout);
    if (ISCLIENT) {
        client_recv_loop();
    } else {
        server_recv_loop();
    }

    return FINISH;

}

RC InputThread::client_recv_loop() {
    DEBUG_Q("servers to track %d\n", g_servers_per_client);
    int rsp_cnts[g_servers_per_client];
    memset(rsp_cnts, 0, g_servers_per_client * sizeof(int));

    run_starttime = get_sys_clock();
    uint64_t return_node_offset;
    uint64_t inf;

    std::vector<Message *> *msgs;

    while (!simulation->is_done()) {
        heartbeat();
        uint64_t starttime = get_sys_clock();
        msgs = tport_man.recv_msg(get_thd_id());
        INC_STATS(_thd_id, mtx[28], get_sys_clock() - starttime);
        starttime = get_sys_clock();
        //while((m_query = work_queue.get_next_query(get_thd_id())) != NULL) {
        //Message * msg = work_queue.dequeue();
        if (msgs == NULL)
            continue;
        while (!msgs->empty()) {
            Message *msg = msgs->front();
            assert(msg->rtype == CL_RSP);
            return_node_offset = msg->return_node_id - g_server_start_node;
            assert(return_node_offset < g_servers_per_client);
            rsp_cnts[return_node_offset]++;
            auto rsp_msg = (ClientResponseMessage *) msg;
            if (rsp_msg->rc == Commit){
                INC_STATS(get_thd_id(), txn_cnt, 1);
            }
            else{
                assert(rsp_msg->rc == Abort);
                INC_STATS(get_thd_id(), total_txn_abort_cnt, 1);
            }
            uint64_t timespan = get_sys_clock() - ((ClientResponseMessage *) msg)->client_startts;
            INC_STATS(get_thd_id(), txn_run_time, timespan);
            if (warmup_done) {
                INC_STATS_ARR(get_thd_id(), client_client_latency, timespan);
            }
            //INC_STATS_ARR(get_thd_id(),all_lat,timespan);
            inf = client_man.dec_inflight(return_node_offset);
            // TQ: QueCC stats
//            client_man.inflight_msgs.fetch_sub(1);
//            stats.totals->quecc_txn_comp_cnt.fetch_add(1);
#if COUNT_BASED_SIM_ENABLED
            simulation->inc_txn_cnt(1);
#endif
            DEBUG("Recv %ld from %ld, %ld -- %f\n", ((ClientResponseMessage *) msg)->txn_id, msg->return_node_id, inf,
                  float(timespan)/BILLION);
            assert(inf >= 0);
            // delete message here
            msgs->erase(msgs->begin());

//            if (simulation->is_warmup_done()){
//                DEBUG_Q("Thread_%ld:Txn_%ld , batch %ld committed at node_offset %ld, commit_txn_cnt = %d, stat_txn_cnt = %ld, inf = %d \n",
//                        _thd_id, msg->txn_id, msg->batch_id, return_node_offset, rsp_cnts[return_node_offset],
//                        stats._stats[_thd_id]->txn_cnt, client_man.get_inflight(return_node_offset));
//            }
        }
        delete msgs;
        INC_STATS(_thd_id, mtx[29], get_sys_clock() - starttime);

    }

    printf("FINISH %ld:%ld\n", _node_id, _thd_id);
    fflush(stdout);
    return FINISH;
}

RC InputThread::server_recv_loop() {

    myrand rdm;
    rdm.init(get_thd_id());
    RC rc = RCOK;
    assert(rc == RCOK);
    uint64_t starttime;

    std::vector<Message *> *msgs;
#if CC_ALG == QUECC
    uint64_t quecc_starttime;
    // avoids enqueue to the same planner
    planner_msg_cnt = _thd_id;
    for (int i = 0; i < PLAN_THREAD_CNT; ++i) {
        planner_enq_msg_cnt[i] = 0;
    }
#endif

    while (!simulation->is_done()) {
        heartbeat();
        starttime = get_sys_clock();

        msgs = tport_man.recv_msg(get_thd_id());
        INC_STATS(_thd_id, mtx[28], get_sys_clock() - starttime);
        starttime = get_sys_clock();

        if (msgs == NULL)
            continue;
        while (!msgs->empty()) {
            Message *msg = msgs->front();
            if (msg->rtype == INIT_DONE) {
                msgs->erase(msgs->begin());
                continue;
            }

            if (ISREPLICAN(g_node_id) && !is_leader && msg->rtype == CL_QRY){
                // This  case is when a follower receives a client transaction
                DEBUG_Q("N_%d: I am a replica, got a client query. Should forward that to leader with id= %d\n",
                        g_node_id, g_node_id-(g_client_node_cnt+g_node_cnt));
                // FIXME(tq): forward to leader sequencer
                assert(false);
            }
#if CC_ALG == CALVIN
            if(msg->rtype == CALVIN_ACK ||(msg->rtype == CL_QRY && ISCLIENTN(msg->get_return_id()))) {
              work_queue.sequencer_enqueue(get_thd_id(),msg);
              msgs->erase(msgs->begin());
              continue;
            }
            if( msg->rtype == RDONE || msg->rtype == CL_QRY) {
              //TODO(tq): RDONE is sent by sequencers so eventually
              // this assertion should be removed especially when there is
              // a dynamic configuration for leaders/follower replicas
                assert(ISSERVERN(msg->get_return_id()));
              work_queue.sched_enqueue(get_thd_id(),msg);
              msgs->erase(msgs->begin());
              continue;
            }
#endif

#if CC_ALG == QUECC
//      DEBUG_Q("Enqueue to planning layer\n")

            if (msg->rtype == CL_QRY) {
                // assume 1 input thread
                // FIXME(tq): if there are > 1 input threads planner_msg_cnt risks a race condition and must inc atomically
                uint64_t dplan_id = planner_msg_cnt % g_plan_thread_cnt;
//                planner_enq_msg_cnt[dplan_id]++;
                work_queue.plan_enqueue(dplan_id, msg);
                planner_msg_cnt++;
#if DEBUG_QUECC & false
                work_queue.inflight_msg.fetch_add(1);
#endif
                msgs->erase(msgs->begin());
                continue;
            }

            if( msg->rtype == RDONE) {
                assert(ISSERVERN(msg->get_return_id()));
//                DEBUG_Q("RT_%lu: Received RDONE from node=%lu for batch_id=%lu\n",_thd_id, msg->return_node_id, msg->batch_id);
//                assert(((uint64_t)quecc_pool.last_commited_batch_id.load(memory_order_acq_rel)+1) == msg->batch_id);
                quecc_pool.batch_deps[ msg->batch_id % g_batch_map_length].fetch_add(-1,memory_order_acq_rel);
                Message::release_message(msg);
                msgs->erase(msgs->begin());

                continue;
            }


            if( msg->rtype == REMOTE_OP_ACK) {
                assert(ISSERVERN(msg->get_return_id()));


#if WORKLOAD == TPCC
                RemoteOpAckMessage * opack_msg = (RemoteOpAckMessage *) msg;
                DEBUG_Q("RT_%lu: Received REMOTE_OP_ACK from node=%lu for batch_id=%lu, planner_id=%lu, remote_et_id=%lu, txn_idx=%lu, o_id=%ld\n", _thd_id,
                        msg->return_node_id, msg->batch_id,opack_msg->planner_id,opack_msg->et_id, opack_msg->txn_idx, opack_msg->o_id);
                transaction_context * txn_ctx = &work_queue.batch_pg_map[msg->batch_id % g_batch_map_length][opack_msg->planner_id].txn_ctxs[opack_msg->txn_idx];
//                txn_ctx->o_id.store(opack_msg->o_id, memory_order_acq_rel);
                int64_t e = -1;
                while(!simulation->is_done() && !txn_ctx->o_id.compare_exchange_strong(e,opack_msg->o_id, memory_order_acq_rel)){}
//                if(!txn_ctx->o_id.compare_exchange_strong(e,opack_msg->o_id, memory_order_acq_rel)){
//                    M_ASSERT_V(false, "Receiver thread: we should have -1 but found %ld, batch_id=%lu, PT_%lu , txn_idx=%lu, recv o_id=%ld\n",
//                               txn_ctx->o_id.load(memory_order_acq_rel), msg->batch_id, opack_msg->planner_id, opack_msg->txn_idx, opack_msg->o_id);
//                }
//                txn_ctx->completion_cnt.fetch_add(1,memory_order_acq_rel);
#endif
                Message::release_message(msg);
                msgs->erase(msgs->begin());

                continue;
            }

            if (msg->rtype == REMOTE_EQ_SET) {
                quecc_starttime = get_sys_clock();

                auto eq_msg = (RemoteEQSetMessage *) msg;
                uint64_t batch_slot = eq_msg->batch_id % g_batch_map_length;
                batch_partition * batch_part;
                quecc_pool.batch_part_get_or_create(batch_part, eq_msg->planner_id, eq_msg->exec_id);

                if(eq_msg->eqs->size() == 0){
                    batch_part->empty = true;
                    DEBUG_Q("N_%u:RT_%lu: Received Remote EQ from node=%lu, Batch map[%lu][%lu][%lu] , EQ_size=%lu\n",g_node_id,_thd_id,
                            eq_msg->return_node_id, eq_msg->batch_id, eq_msg->planner_id, eq_msg->exec_id, 0L);
                }
                else{
                    batch_part->single_q = false;
                    batch_part->exec_qs = eq_msg->eqs;
                }

                batch_part->remote = true;
                batch_part->batch_id = eq_msg->batch_id;
                uint64_t expected = 0;
                uint64_t desired = (uint64_t) batch_part;

                INC_STATS(get_rthd_id(), rt_rplan_time[get_rthd_id()], get_sys_clock() - quecc_starttime);
                assert(eq_msg->planner_id < (g_plan_thread_cnt*g_node_cnt));
                assert(eq_msg->exec_id < (g_thread_cnt*g_node_cnt));
                assert(batch_slot < g_batch_map_length);
                COMPILER_MEMORY_FENCE;
                while((!work_queue.batch_map[batch_slot][eq_msg->planner_id][eq_msg->exec_id].compare_exchange_strong(expected, desired))){
                    if (simulation->is_done()){
                        break;
                    }
//                    DEBUG_Q("N_%u:RT_%lu: Could not install!! batch_part for Remote EQ from node=%lu, Batch_map[%lu][%lu][%lu]\n",g_node_id,_thd_id,
//                            eq_msg->return_node_id, eq_msg->batch_id , eq_msg->planner_id, eq_msg->exec_id);
                };

                Message::release_message(msg);
                msgs->erase(msgs->begin());
                continue;
            }

            if (msg->rtype == REMOTE_EQ) {
                quecc_starttime = get_sys_clock();

                RemoteEQMessage * eq_msg = (RemoteEQMessage *) msg;
                uint64_t batch_slot = eq_msg->batch_id % g_batch_map_length;
                batch_partition * batch_part;
                quecc_pool.batch_part_get_or_create(batch_part, eq_msg->planner_id, eq_msg->exec_id);

                if (eq_msg->exec_q == nullptr){
                    batch_part->empty = true;
//                    DEBUG_Q("N_%u:RT_%lu: Received Remote EQ from node=%lu, Batch map[%lu][%lu][%lu] , EQ_size=%lu\n",g_node_id,_thd_id,
//                            eq_msg->return_node_id, eq_msg->batch_id, eq_msg->planner_id, eq_msg->exec_id, 0L);
                }
                else{
//                    DEBUG_Q("N_%u:RT_%lu: Received Remote EQ from node=%lu, Batch map[%lu][%lu][%lu] , EQ_size=%lu\n",g_node_id,_thd_id,
//                            eq_msg->return_node_id, eq_msg->batch_id, eq_msg->planner_id, eq_msg->exec_id, eq_msg->exec_q->size());
                    batch_part->exec_q = eq_msg->exec_q;
#if WORKLOAD == TPCC
                    // setup txn contexts for remote
                    for (uint64_t i = 0; i < batch_part->exec_q->size(); ++i) {
                        exec_queue_entry * entry = batch_part->exec_q->get_ptr(i);
//                        DEBUG_Q("Setting up context for remote EQ from node=%lu, PT_%lu, batch_id=%lu, txn_idx=%lu\n",
//                                eq_msg->return_node_id, entry->planner_id, entry->batch_id, entry->txn_idx);
                        transaction_context * txn_ctx = &work_queue.batch_pg_map[entry->batch_id % g_batch_map_length][entry->planner_id].txn_ctxs[entry->txn_idx];
//                        txn_ctx->o_id.store(-1, memory_order_acq_rel);
                        entry->txn_ctx = txn_ctx;
                    }
#endif
                }

                batch_part->remote = true;
                batch_part->batch_id = eq_msg->batch_id;
                uint64_t expected = 0;
                uint64_t desired = (uint64_t) batch_part;
                // neet to spin if batch slot is not ready
//                DEBUG_Q("N_%u:RT_%lu: Going to Spin on MSG Remote EQ from node=%lu, PT_%lu, ET_%lu, batch_id=%lu\n",g_node_id,_thd_id,
//                        eq_msg->return_node_id, eq_msg->planner_id, eq_msg->exec_id, eq_msg->batch_id);
                INC_STATS(get_rthd_id(), rt_rplan_time[get_rthd_id()], get_sys_clock() - quecc_starttime);
                assert(eq_msg->planner_id < (g_plan_thread_cnt*g_node_cnt));
                assert(eq_msg->exec_id < (g_thread_cnt*g_node_cnt));
                assert(batch_slot < g_batch_map_length);
                COMPILER_MEMORY_FENCE;
                while((!work_queue.batch_map[batch_slot][eq_msg->planner_id][eq_msg->exec_id].compare_exchange_strong(expected, desired))){
                    if (simulation->is_done()){
                        break;
                    }
//                    DEBUG_Q("N_%u:RT_%lu: Could not install!! batch_part for Remote EQ from node=%lu, Batch_map[%lu][%lu][%lu]\n",g_node_id,_thd_id,
//                            eq_msg->return_node_id, eq_msg->batch_id , eq_msg->planner_id, eq_msg->exec_id);
                };
//                while(!simulation->is_done() &&(!work_queue.batch_map[batch_slot][eq_msg->planner_id][eq_msg->exec_id].compare_exchange_strong(expected, desired,memory_order_acq_rel))){};
//                DEBUG_Q("RT_%lu: DONE Spinning on MSG Remote EQ from node=%lu, PT_%lu, ET_%lu, batch_id=%lu\n", _thd_id,
//                        eq_msg->return_node_id, eq_msg->planner_id, eq_msg->exec_id, eq_msg->batch_id);

//                if(!work_queue.batch_map[batch_slot][eq_msg->planner_id][eq_msg->exec_id].compare_exchange_strong(expected, desired)){
//                    // this should not happen after spinning but can happen if simulation is done
//                    M_ASSERT_V(false, "Node_%u: For batch %lu : failing to SET map slot [%ld],  PG=[%ld], batch_map_val=%ld\n",
//                               g_node_id, eq_msg->batch_id, batch_slot, eq_msg->planner_id, work_queue.batch_map[batch_slot][eq_msg->planner_id][eq_msg->exec_id].load());
//                }

//                DEBUG_Q("N_%u:RT_%lu: Installed batch_part for Remote EQ from node=%lu, Batch_map[%lu][%lu][%lu]\n",g_node_id,_thd_id,
//                        eq_msg->return_node_id, eq_msg->batch_id , eq_msg->planner_id, eq_msg->exec_id);

                // cannot release message yet!! relase on cleanup ???
                //Actually we can release it here since the pointer of exec_q is already set
                Message::release_message(msg);
                msgs->erase(msgs->begin());
                continue;
            }


            if (msg->rtype == REMOTE_EQ_ACK) {
                quecc_starttime = get_sys_clock();

                RemoteEQAckMessage * req_ack = (RemoteEQAckMessage *) msg;
                DEBUG_Q("N_%u:RT_%lu: Received ACK for remote EQ from node=%lu, batch_map[%lu][%lu][%lu]\n",g_node_id,_thd_id,
                        req_ack->return_node_id, req_ack->batch_id, req_ack->planner_id, req_ack->exec_id);
                uint64_t batch_slot = req_ack->batch_id % g_batch_map_length;
                batch_partition * batch_part = (batch_partition *) work_queue.batch_map[batch_slot][req_ack->planner_id][QueCCPool::map_to_et_id(req_ack->exec_id)].load();
//                batch_partition * batch_part = (batch_partition *) work_queue.batch_map[batch_slot][req_ack->planner_id][QueCCPool::map_to_et_id(req_ack->exec_id)].load(memory_order_acq_rel);
                M_ASSERT_V(batch_part,"N_%u:RT_%lu: Received ACK for remote EQ from node=%lu, PT_%lu, CWET_%lu, ET_%lu batch_id=%lu\n",g_node_id,_thd_id,
                           req_ack->return_node_id, req_ack->planner_id, req_ack->exec_id, QueCCPool::map_to_et_id(req_ack->exec_id), req_ack->batch_id);

                //assume single EQ per batch part
//                assert(batch_part->single_q);
                assert(batch_part->batch_id == req_ack->batch_id);

                // update txn contexts 'locally'
                // For TPC-C, req_ack->update_contexts == false because operations are dependent and messages are sent
                // as each operation/fragment is executed
                // This final REQ-ACK is used to indicate that the remote EQ has been fully processed.
//                if (!batch_part->empty && req_ack->update_contexts){
                if (!batch_part->empty){
                    if (batch_part->single_q){
                        for (uint64_t j = 0; j < batch_part->exec_q->size(); ++j) {
#if WORKLOAD == TPCC
//                        if (batch_part->exec_q->get_ptr(j)->type == TPCC_PAYMENT_UPDATE_C){
                            TxnManager::check_commit_ready(batch_part->exec_q->get_ptr(j));
//                        }
#endif
#if WORKLOAD == YCSB
                            TxnManager::check_commit_ready(batch_part->exec_q->get_ptr(j));
#endif
                        }
                        // release EQs
                        DEBUG_Q("N_%u:RT_%lu: Releaseing during ET execution and RECV ACK SINGLE, ptr=%lu\n",g_node_id,_thd_id,(uint64_t) batch_part->exec_q);
                        quecc_pool.exec_queue_release(batch_part->exec_q, req_ack->planner_id, req_ack->exec_id);
                        assert(QueCCPool::get_plan_node(req_ack->planner_id) == g_node_id);
                        batch_part->planner_pg->eq_completed_rem_cnt.fetch_sub(1);
                    }
                    else{

                        DEBUG_Q("N_%u:RT_%lu:: batch map [%lu][%lu][%lu] , CW_ET_ID=%lu, eqs_size = %lu\n", g_node_id, _thd_id,
                        req_ack->batch_id, req_ack->planner_id,QueCCPool::map_to_et_id(req_ack->exec_id),req_ack->exec_id, batch_part->exec_qs->size() );
                        for (uint64_t i = 0; i < batch_part->exec_qs->size(); ++i) {
                            auto exec_q = batch_part->exec_qs->get(i);
                            for (uint64_t j = 0; j < exec_q->size(); ++j) {
#if WORKLOAD == TPCC
//                        if (exec_q->get_ptr(j)->type == TPCC_PAYMENT_UPDATE_C){
                            TxnManager::check_commit_ready(exec_q->get_ptr(j));
//                        }
#endif
#if WORKLOAD == YCSB
                                TxnManager::check_commit_ready(exec_q->get_ptr(j));
#endif
                            }
                            // release EQs
                            DEBUG_Q("N_%u:RT_%lu: Releaseing during ET execution and RECV ACK DONE, ptr=%lu\n",g_node_id,_thd_id, (uint64_t) exec_q);
                            quecc_pool.exec_queue_release(exec_q, req_ack->planner_id, req_ack->exec_id);
                            assert(QueCCPool::get_plan_node(req_ack->planner_id) == g_node_id);
                            batch_part->planner_pg->eq_completed_rem_cnt.fetch_sub(1);
                        }

                        quecc_pool.exec_qs_release(batch_part->exec_qs,req_ack->planner_id);
                    }
//                    DEBUG_Q("updated txn_ctx for %lu entries\n",batch_part->exec_q->size());
                }
//                // release batch part
                quecc_pool.batch_part_release(batch_part, req_ack->planner_id, req_ack->exec_id);

//                // reset or clean up
                uint64_t desired = 0;
                uint64_t expected = (uint64_t) batch_part;
#if BATCH_MAP_ORDER == BATCH_ET_PT
                while(!work_queue.batch_map[batch_slot][_thd_id][wplanner_id].compare_exchange_strong(expected, desired)){
                    DEBUG_Q("ET_%ld: failing to RESET map slot \n", _thd_id);
                }
#else
//                DEBUG_Q("N_%u:RT_%lu:: RESET batch map [%lu][%lu][%lu] CWET_id = %lu\n", g_node_id, _thd_id,
//                        req_ack->batch_id, req_ack->planner_id,QueCCPool::map_to_et_id(req_ack->exec_id),req_ack->exec_id );
//                assert(req_ack->planner_id < (g_plan_thread_cnt*g_node_cnt));
//                assert(req_ack->exec_id< (g_thread_cnt*g_node_cnt));

//                if(!work_queue.batch_map[batch_slot][req_ack->planner_id][QueCCPool::map_to_et_id(req_ack->exec_id)].compare_exchange_strong(expected, desired)){
//                    M_ASSERT_V(false, "ET_%ld: failing to RESET map slot for REMOTE EQ \n", req_ack->exec_id);
//                }
//                while(!work_queue.batch_map[batch_slot][req_ack->planner_id][QueCCPool::map_to_et_id(req_ack->exec_id)].compare_exchange_strong(expected, desired)){
                while(!work_queue.batch_map[batch_slot][req_ack->planner_id][req_ack->exec_id].compare_exchange_strong(expected, desired)){

                    if (simulation->is_done()){
                        break;
                    }
                }
#endif
                INC_STATS(get_rthd_id(), rt_rplan_time[get_rthd_id()], get_sys_clock() - quecc_starttime);
                Message::release_message(msg);
                msgs->erase(msgs->begin());
                continue;
            }
#endif // CC_ALG == QUECC
            work_queue.enqueue(get_thd_id(), msg, false);
            msgs->erase(msgs->begin());

        }
        delete msgs;
        INC_STATS(_thd_id, mtx[29], get_sys_clock() - starttime);

    }
#if CC_ALG == QUECC
    for (uint64_t i = 0; i < g_plan_thread_cnt ; ++i) {
        DEBUG_Q("Total messages enq. to planner[%lu] = %ld\n",i, planner_enq_msg_cnt[i]);
    }
    DEBUG_Q("Total messages enq. to planners = %ld\n", planner_msg_cnt);
#endif
    printf("FINISH %ld:%ld\n", _node_id, _thd_id);
    fflush(stdout);
    return FINISH;
}

void OutputThread::setup() {
    DEBUG_M("OutputThread::setup MessageThread alloc\n");
    messager = (MessageThread *) mem_allocator.alloc(sizeof(MessageThread));
    messager->init(_thd_id);
    while (!simulation->is_setup_done()) {
        messager->run();
    }
}

RC OutputThread::run() {

    tsetup();
    printf("N_%u: Running OutputThread %ld\n", g_node_id, _thd_id);
    fflush(stdout);
    while (!simulation->is_done()) {
        heartbeat();
        messager->run();
    }

    printf("FINISH %ld:%ld\n", _node_id, _thd_id);
    fflush(stdout);
    return FINISH;
}


