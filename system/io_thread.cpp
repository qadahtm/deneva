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
                  assert(ISSERVERN(msg->get_return_id()));
                  work_queue.sched_enqueue(get_thd_id(),msg);
                  msgs->erase(msgs->begin());
                  continue;
                }
#endif
#if CC_ALG == QUECC
//      DEBUG_Q("Enqueue to planning layer\n")
#if WORKLOAD == YCSB
                if (msg->rtype == CL_QRY) {
                    uint64_t dplan_id = planner_msg_cnt % g_plan_thread_cnt;
                    planner_enq_msg_cnt[dplan_id]++;
                    work_queue.plan_enqueue(dplan_id, msg);
                    planner_msg_cnt++;
                    msgs->erase(msgs->begin());
                    continue;
                }
#endif

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
    printf("Running InputThread %ld\n", _thd_id);

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
            INC_STATS(get_thd_id(), txn_cnt, 1);
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
//            simulation->inc_txn_cnt(1);

            DEBUG_Q("Recv %ld from %ld, %ld -- %f\n", ((ClientResponseMessage *) msg)->txn_id, msg->return_node_id, inf,
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
#if CC_ALG == CALVIN
            if(msg->rtype == CALVIN_ACK ||(msg->rtype == CL_QRY && ISCLIENTN(msg->get_return_id()))) {
              work_queue.sequencer_enqueue(get_thd_id(),msg);
              msgs->erase(msgs->begin());
              continue;
            }
            if( msg->rtype == RDONE || msg->rtype == CL_QRY) {
              assert(ISSERVERN(msg->get_return_id()));
              work_queue.sched_enqueue(get_thd_id(),msg);
              msgs->erase(msgs->begin());
              continue;
            }
#endif

#if CC_ALG == QUECC
//      DEBUG_Q("Enqueue to planning layer\n")
#if WORKLOAD == YCSB
            if (msg->rtype == CL_QRY) {
                // assume 1 input thread
                // FIXME(tq): if there are > 1 input threads planner_msg_cnt risks a race condition and must inc atomically
                uint64_t dplan_id = planner_msg_cnt % g_plan_thread_cnt;
                planner_enq_msg_cnt[dplan_id]++;
                work_queue.plan_enqueue(dplan_id, msg);
                planner_msg_cnt++;
#if DEBUG_QUECC & false
                work_queue.inflight_msg.fetch_add(1);
#endif
                msgs->erase(msgs->begin());
                continue;
            }
#endif

            if( msg->rtype == RDONE) {
                assert(ISSERVERN(msg->get_return_id()));
                DEBUG_Q("Received RDONE from node=%lu for batch_id=%lu\n", msg->return_node_id, msg->batch_id);
//                assert(((uint64_t)quecc_pool.last_commited_batch_id.load(memory_order_acq_rel)+1) == msg->batch_id);
                quecc_pool.batch_deps[ msg->batch_id % g_batch_map_length].fetch_add(-1,memory_order_acq_rel);
                Message::release_message(msg);
                msgs->erase(msgs->begin());

                continue;
            }

            if (msg->rtype == REMOTE_EQ) {
                RemoteEQMessage * eq_msg = (RemoteEQMessage *) msg;
                //spin here if other node is ahead of the currently executing batch on this node
//                while(quecc_pool.last_commited_batch_id.load(memory_order_acq_rel) < (eq_msg->batch_id-1)){}

                uint64_t batch_slot = eq_msg->batch_id % g_batch_map_length;
                batch_partition * batch_part;
                quecc_pool.batch_part_get_or_create(batch_part, eq_msg->planner_id, eq_msg->exec_id);

                if (eq_msg->exec_q == nullptr){
                    batch_part->empty = true;
//                    quecc_pool.exec_queue_release(eq_msg->exec_q, eq_msg->planner_id, eq_msg->exec_id);
                    DEBUG_Q("Received Remote EQ from node=%lu, PT_%lu, ET_%lu, batch_id=%lu, EQ_size=%lu\n",
                            eq_msg->return_node_id, eq_msg->planner_id, eq_msg->exec_id, eq_msg->batch_id, 0L);
                }
                else{
                    DEBUG_Q("Received Remote EQ from node=%lu, PT_%lu, ET_%lu, batch_id=%lu, EQ_size=%lu\n",
                            eq_msg->return_node_id, eq_msg->planner_id, eq_msg->exec_id, eq_msg->batch_id, eq_msg->exec_q->size());
                    batch_part->exec_q = eq_msg->exec_q;
                }

                batch_part->remote = true;

                uint64_t expected = 0;
                uint64_t desired = (uint64_t) batch_part;
                // neet to spin if batch slot is not ready
                while(!simulation->is_done() &&(!work_queue.batch_map[batch_slot][eq_msg->planner_id][eq_msg->exec_id].compare_exchange_strong(expected, desired))){};

//                if(!work_queue.batch_map[batch_slot][eq_msg->planner_id][eq_msg->exec_id].compare_exchange_strong(expected, desired)){
//                    // this should not happen after spinning but can happen if simulation is done
//                    M_ASSERT_V(false, "Node_%u: For batch %lu : failing to SET map slot [%ld],  PG=[%ld], batch_map_val=%ld\n",
//                               g_node_id, eq_msg->batch_id, batch_slot, eq_msg->planner_id, work_queue.batch_map[batch_slot][eq_msg->planner_id][eq_msg->exec_id].load());
//                }

//                DEBUG_Q("Installed batch_part for Remote EQ from node=%lu, Batch_map[%lu][%lu][%lu] EQ_size=%lu\n",
//                        eq_msg->return_node_id, eq_msg->batch_id , eq_msg->planner_id, eq_msg->exec_id, eq_msg->exec_q->size());

                // cannot release message yet!! relase on cleanup ???
                //Actually we can release it here since the pointer of exec_q is already set
                Message::release_message(msg);
                msgs->erase(msgs->begin());
                continue;
            }


            if (msg->rtype == REMOTE_EQ_ACK) {
                RemoteEQAckMessage * req_ack = (RemoteEQAckMessage *) msg;
                DEBUG_Q("Received ACK for remote EQ from node=%lu, PT_%lu, ET_%lu, batch_id=%lu\n",
                        req_ack->return_node_id, req_ack->planner_id, req_ack->exec_id, req_ack->batch_id);
                uint64_t batch_slot = req_ack->batch_id % g_batch_map_length;
                batch_partition * batch_part = (batch_partition *) work_queue.batch_map[batch_slot][req_ack->planner_id][req_ack->exec_id].load();
                assert(batch_part);
                //assume single EQ per batch part
                assert(batch_part->single_q);

                // update txn contexts 'locally'
                if (!batch_part->empty){
                    for (uint64_t j = 0; j < batch_part->exec_q->size(); ++j) {
//                        batch_part->exec_q->get_ptr(j)->txn_ctx->completion_cnt.fetch_add(1,memory_order_acq_rel);
                        TxnManager::check_commit_ready(batch_part->exec_q->get_ptr(j));
                    }
//                    DEBUG_Q("updated txn_ctx for %lu entries\n",batch_part->exec_q->size());
                }

//                // release EQs
                quecc_pool.exec_queue_release(batch_part->exec_q, req_ack->planner_id, req_ack->exec_id);
//
//                // release batch part
                quecc_pool.batch_part_release(batch_part, req_ack->planner_id, req_ack->exec_id);

//                // reset or clean up
//                uint64_t desired = 0;
//                uint64_t expected = (uint64_t) batch_part;
//#if BATCH_MAP_ORDER == BATCH_ET_PT
//                while(!work_queue.batch_map[batch_slot][_thd_id][wplanner_id].compare_exchange_strong(expected, desired)){
//                    DEBUG_Q("ET_%ld: failing to RESET map slot \n", _thd_id);
//                }
//#else
////        DEBUG_Q("ET_%ld: RESET batch map slot=%ld, PG=%ld, batch_id=%ld \n", _thd_id, batch_slot, wplanner_id, wbatch_id);
//                if(!work_queue.batch_map[batch_slot][req_ack->planner_id][req_ack->exec_id].compare_exchange_strong(expected, desired)){
//                    M_ASSERT_V(false, "ET_%ld: failing to RESET map slot for REMOTE EQ \n", req_ack->exec_id);
//                }
//#endif
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
    printf("Running OutputThread %ld\n", _thd_id);

    while (!simulation->is_done()) {
        heartbeat();
        messager->run();
    }

    printf("FINISH %ld:%ld\n", _node_id, _thd_id);
    fflush(stdout);
    return FINISH;
}


