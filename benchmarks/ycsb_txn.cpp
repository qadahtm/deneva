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
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"
#include "msg_queue.h"
#include "message.h"

#if WORKLOAD == YCSB
void YCSBTxnManager::init(uint64_t thd_id, Workload *h_wl) {
    TxnManager::init(thd_id, h_wl);
    _wl = (YCSBWorkload *) h_wl;
    reset();
}

void YCSBTxnManager::reset() {
    state = YCSB_0;
    next_record_id = 0;
    TxnManager::reset();
}

RC YCSBTxnManager::acquire_locks() {
    uint64_t starttime = get_sys_clock();
    assert(CC_ALG == CALVIN);
    YCSBQuery *ycsb_query = (YCSBQuery *) query;
    locking_done = false;
    RC rc = RCOK;
    incr_lr();
    assert(ycsb_query->requests.size() == g_req_per_query);
    assert(phase == CALVIN_RW_ANALYSIS);
    for (uint32_t rid = 0; rid < ycsb_query->requests.size(); rid++) {
        ycsb_request *req = ycsb_query->requests[rid];
        uint64_t part_id = _wl->key_to_part(req->key);
        DEBUG("LK Acquire (%ld,%ld) %d,%ld -> %ld\n", get_txn_id(), get_batch_id(), req->acctype, req->key,
              GET_NODE_ID(part_id));
        if (GET_NODE_ID(part_id) != g_node_id)
            continue;
        INDEX *index = _wl->the_index;
        itemid_t *item;
        item = index_read(index, req->key, part_id);
        row_t *row = ((row_t *) item->location);
        RC rc2 = get_lock(row, req->acctype);
        if (rc2 != RCOK) {
            rc = rc2;
        }
    }
    if (decr_lr() == 0) {
        if (ATOM_CAS(lock_ready, false, true))
            rc = RCOK;
    }
    txn_stats.wait_starttime = get_sys_clock();
    /*
    if(rc == WAIT && lock_ready_cnt == 0) {
      if(ATOM_CAS(lock_ready,false,true))
      //lock_ready = true;
        rc = RCOK;
    }
    */
    INC_STATS(get_thd_id(), calvin_sched_time, get_sys_clock() - starttime);
    locking_done = true;
    return rc;
}

RC YCSBTxnManager::run_hstore_txn(){
    RC rc = Commit;
    assert(CC_ALG == HSTORE);
    YCSBQuery *ycsb_query = (YCSBQuery *) query;

    for (uint64_t i = 0; i < ycsb_query->requests.size(); i++) {
        ycsb_request *req = ycsb_query->requests[i];
        uint64_t part_id = _wl->key_to_part(req->key);
        query->partitions_touched.add_unique(part_id);

        rc = run_ycsb_0(req, row);
        assert(rc == RCOK);

        rc = run_ycsb_1(req->acctype, row);
        assert(rc == RCOK);
    }

    release_locks(RCOK);
    commit_stats();
    // we always commit in YCSB
    return Commit;
}
#if CC_ALG == LADS || LADS_IN_QUECC
RC YCSBTxnManager::execute_lads_action(gdgcc::Action * action, uint64_t eid){
    RC rc = RCOK;
#if WORKLOAD == YCSB
    //extract the primary key from the action
    ycsb_request *req = action->req;

//    quecc_prof_time = get_sys_clock();
    // get pointer to record in row
    rc = run_ycsb_0(req, row);
    assert(rc == RCOK);
//    INC_STATS(get_thd_id(), exec_txn_index_lookup_time[get_thd_id()], get_sys_clock()-quecc_prof_time);

//    quecc_prof_time = get_sys_clock();
    // perfrom access
    rc = run_ycsb_1(req->acctype, row);
    assert(rc == RCOK);


//    uint32_t fid = action->getFuncId();
//    if(fid == 0) {  //read function
//        target_row->get_column(1);    //assume there are one column in each ycsb tuple
//    }else if(fid == 1){ //write function
//        std::string tmpstr = RandFunc::rand_string(10);
//        target_row->update(1, tmpstr); //TODO
//    }else{          //insert function
//        //TODO
//    }
#endif
    /*
     * temporal
     * */
    gdgcc::Action* action_temporal = action->next;
    if(action_temporal != nullptr) {
        action_temporal->subIndegreeByOne();
    }
    /*
     * logical
     * */
//    gdgcc::Action* action_logical = nullptr;
    int size = action->logical_dependency.size();
    for(int i=0; i<size; i++) {
        action->logical_dependency[i]->subIndegreeByOne();
    }

    check_commit_ready(action);
    return rc;
}
#endif // #if CC_ALG == LADS

RC YCSBTxnManager::run_txn() {
    RC rc = RCOK;
    assert(CC_ALG != CALVIN);

    if (IS_LOCAL(txn->txn_id) && state == YCSB_0 && next_record_id == 0) {
        DEBUG("Running txn %ld\n", txn->txn_id);
        //query->print();
        query->partitions_touched.add_unique(GET_PART_ID(0, g_node_id));
    }

    uint64_t starttime = get_sys_clock();

    while (rc == RCOK && !is_done()) {
        rc = run_txn_state();
    }

    uint64_t curr_time = get_sys_clock();
    txn_stats.process_time += curr_time - starttime;
    txn_stats.process_time_short += curr_time - starttime;
    txn_stats.wait_starttime = get_sys_clock();

#if !SINGLE_NODE
    if(IS_LOCAL(get_txn_id())) {
    if(is_done() && rc == RCOK)
      rc = start_commit();
    else if(rc == Abort)
      rc = start_abort();
  }
#else
#if CC_ALG == SILO
    if (is_done() && rc == RCOK){
        rc = validate_silo();
    }
#else
    if(is_done() && rc == RCOK)
        rc = start_commit();
    else if(rc == Abort)
        rc = start_abort();
#endif // #if CC_ALG == SILO
#endif
    return rc;

}

RC YCSBTxnManager::run_txn_post_wait() {
    get_row_post_wait(row);
    next_ycsb_state();
    return RCOK;
}

bool YCSBTxnManager::is_done() {
    return next_record_id == ((YCSBQuery *) query)->requests.size();
}

void YCSBTxnManager::next_ycsb_state() {
    switch (state) {
        case YCSB_0:
            state = YCSB_1;
            break;
        case YCSB_1:
            next_record_id++;
            if (!IS_LOCAL(txn->txn_id) || !is_done()) {
                state = YCSB_0;
            } else {
                state = YCSB_FIN;
            }
            break;
        case YCSB_FIN:
            break;
        default:
            assert(false);
    }
}

bool YCSBTxnManager::is_local_request(uint64_t idx) {
    return GET_NODE_ID(_wl->key_to_part(((YCSBQuery *) query)->requests[idx]->key)) == g_node_id;
}

RC YCSBTxnManager::send_remote_request() {
    YCSBQuery *ycsb_query = (YCSBQuery *) query;
    uint64_t dest_node_id = GET_NODE_ID(_wl->key_to_part(ycsb_query->requests[next_record_id]->key));
//    DEBUG_Q("dest_node_id=%lu for key=%lu\n",dest_node_id,ycsb_query->requests[next_record_id]->key);
//    ycsb_query->partitions_touched.add_unique(GET_PART_ID(0, dest_node_id));
    ycsb_query->partitions_touched.add_unique(_wl->key_to_part(ycsb_query->requests[next_record_id]->key));
    assert(ycsb_query->partitions_touched.size() > 1);
    msg_queue.enqueue(get_thd_id(), Message::create_message(this, RQRY), dest_node_id);
    return WAIT_REM;
}

RC YCSBTxnManager::send_remote_request(YCSBQuery *ycsb_query, uint64_t dest_node_id) {
//    YCSBQuery *ycsb_query = (YCSBQuery *) query;
//    uint64_t dest_node_id = GET_NODE_ID(_wl->key_to_part(ycsb_query->requests[next_record_id]->key));
//    DEBUG_Q("dest_node_id=%lu for key=%lu\n",dest_node_id,ycsb_query->requests[next_record_id]->key);
//    ycsb_query->partitions_touched.add_unique(GET_PART_ID(0, dest_node_id));
    ycsb_query->partitions_touched.add_unique(_wl->key_to_part(ycsb_query->requests[next_record_id]->key));
    assert(ycsb_query->partitions_touched.size() > 1);
    msg_queue.enqueue(get_thd_id(), Message::create_message(this, RQRY), dest_node_id);
    return WAIT_REM;
}

void YCSBTxnManager::copy_remote_requests(YCSBQueryMessage *msg) {
    YCSBQuery *ycsb_query = (YCSBQuery *) query;
    //msg->requests.init(ycsb_query->requests.size());
    uint64_t dest_node_id = GET_NODE_ID(_wl->key_to_part(ycsb_query->requests[next_record_id]->key));
    while (next_record_id < ycsb_query->requests.size() && !is_local_request(next_record_id) &&
           GET_NODE_ID(_wl->key_to_part(ycsb_query->requests[next_record_id]->key)) == dest_node_id) {
        YCSBQuery::copy_request_to_msg(ycsb_query, msg, next_record_id++);
    }
}

RC YCSBTxnManager::run_txn_state() {
    YCSBQuery *ycsb_query = (YCSBQuery *) query;
    ycsb_request *req = ycsb_query->requests[next_record_id];
#if !SINGLE_NODE
    uint64_t part_id = _wl->key_to_part(req->key);
    bool loc = GET_NODE_ID(part_id) == g_node_id;
#else
    bool loc = true;
#endif
    RC rc = RCOK;

    switch (state) {
        case YCSB_0 :
            if (loc) {
                rc = run_ycsb_0(req, row);
            } else {
                rc = send_remote_request();
            }
            break;
        case YCSB_1 :
            rc = run_ycsb_1(req->acctype, row);
            break;
        case YCSB_FIN :
            state = YCSB_FIN;
            break;
        default:
            assert(false);
    }

    if (rc == RCOK)
        next_ycsb_state();

    return rc;
}
/**
 * TQ: Finds the pointer to the value associated with a a key
 * @param req
 * @param row_local
 * @return
 */
inline RC YCSBTxnManager::run_ycsb_0(ycsb_request *req, row_t *&row_local) {
    RC rc = RCOK;
    int part_id = _wl->key_to_part(req->key);
#if !(CC_ALG == QUECC || CC_ALG == LADS)
    access_t type = req->acctype;
#endif
    itemid_t *m_item;

    m_item = index_read(_wl->the_index, req->key, part_id);

#if CC_ALG == QUECC || CC_ALG == LADS
//    // just access row, no need to go throught lock manager path
    row_local = ((row_t *) m_item->location);
#else
    row_t *row = ((row_t *) m_item->location);
    rc =    get_row(row, type, row_local);
#endif

    return rc;

}

inline RC YCSBTxnManager::run_ycsb_1(access_t acctype, row_t *row_local) {
    // TQ: implement read-modify-write
    uint8_t fval UNUSED =0;
    char *data = row_local->get_data();
    if (acctype == RD || acctype == SCAN) {

        //TQ: attribute unused cause GCC compiler not ot produce a warning as fval is no used
        // TQ: perform the actual read by
#if YCSB_DO_OPERATION
        for (int j = 0; j < row_local->tuple_size; ++j) {
            fval += data[j];
        }
#else
        // However this only reads 8 bytes of the data
        int fid = 0;
        fval = *(uint8_t  *) (&data[fid * 100]);
#endif
        //TODO(tq): we assume that isolation level is always set to serializable
#if CC_ALG != QUECC && (ISOLATION_LEVEL == READ_COMMITTED || ISOLATION_LEVEL == READ_UNCOMMITTED)
        // Release lock after read
        release_last_row_lock();
#endif

    } else {
        assert(acctype == WR);
#if YCSB_DO_OPERATION
        for (int j = 0; j < row_local->tuple_size; ++j) {
            fval += data[j];
        }
        // write value to the first byte
        data[0] = fval;
#else
        //TQ: here the we are zeroing the first 8 bytes
        // 100 below is the number of bytes for each field
        int fid = 0;
        *(uint64_t *) (&data[fid * 100]) = 0;
#endif
#if YCSB_ABORT_MODE
        if(data[0] == 'a')
          return RCOK;
#endif

#if CC_ALG != QUECC && ISOLATION_LEVEL == READ_UNCOMMITTED
        // Release lock after write
        release_last_row_lock();
#endif
    }
    return RCOK;
}

RC YCSBTxnManager::run_calvin_txn() {
    RC rc = RCOK;
    uint64_t starttime = get_sys_clock();
    YCSBQuery *ycsb_query = (YCSBQuery *) query;
    DEBUG("(%ld,%ld) Run calvin txn\n", txn->txn_id, txn->batch_id);
    while (!calvin_exec_phase_done() && rc == RCOK) {
        DEBUG("(%ld,%ld) phase %d\n", txn->txn_id, txn->batch_id, this->phase);
        switch (this->phase) {
            case CALVIN_RW_ANALYSIS:

                // Phase 1: Read/write set analysis
                calvin_expected_rsp_cnt = ycsb_query->get_participants(_wl);
#if YCSB_ABORT_MODE
            if(query->participant_nodes[g_node_id] == 1) {
              calvin_expected_rsp_cnt--;
            }
#else
                calvin_expected_rsp_cnt = 0;
#endif
                DEBUG("(%ld,%ld) expects %d responses;\n", txn->txn_id, txn->batch_id, calvin_expected_rsp_cnt);

                this->phase = CALVIN_LOC_RD;
                break;
            case CALVIN_LOC_RD:
                // Phase 2: Perform local reads
                DEBUG("(%ld,%ld) local reads\n", txn->txn_id, txn->batch_id);
                rc = run_ycsb();
                //release_read_locks(query);

                this->phase = CALVIN_SERVE_RD;
                break;
            case CALVIN_SERVE_RD:
                // Phase 3: Serve remote reads
                // If there is any abort logic, relevant reads need to be sent to all active nodes...

                // TQ: checks a if this node is a participant (a participant node performs read operations)
                if (query->participant_nodes[g_node_id] == 1) {
                    rc = send_remote_reads();
                }
                // TQ: checks if this node is an active node (an active node performs write operations)
                if (query->active_nodes[g_node_id] == 1) {
                    this->phase = CALVIN_COLLECT_RD;
                    if (calvin_collect_phase_done()) {
                        rc = RCOK;
                    } else {
                        DEBUG("(%ld,%ld) wait in collect phase; %d / %d rfwds received\n", txn->txn_id, txn->batch_id,
                              rsp_cnt, calvin_expected_rsp_cnt);
                        rc = WAIT;
                    }
                } else { // Done
                    rc = RCOK;
                    this->phase = CALVIN_DONE;
                }

                break;
            case CALVIN_COLLECT_RD:
                // Phase 4: Collect remote reads
                this->phase = CALVIN_EXEC_WR;
                break;
            case CALVIN_EXEC_WR:
                // Phase 5: Execute transaction / perform local writes
                DEBUG("(%ld,%ld) execute writes\n", txn->txn_id, txn->batch_id);
                rc = run_ycsb();
                this->phase = CALVIN_DONE;
                break;
            default:
                assert(false);
        }
    }
    uint64_t curr_time = get_sys_clock();
    txn_stats.process_time += curr_time - starttime;
    txn_stats.process_time_short += curr_time - starttime;
    txn_stats.wait_starttime = get_sys_clock();
    return rc;
}
#if CC_ALG == QUECC
RC YCSBTxnManager::run_quecc_txn(exec_queue_entry * exec_qe) {
    RC rc = RCOK;
//    uint64_t starttime = get_sys_clock();
//    uint64_t quecc_prof_time = 0;
    //TQ: dirty code: using a char buffer to store ycsb_request
#if WORKLOAD == YCSB

//    ycsb_request *req = (ycsb_request *) &exec_qe->req_buffer;
    ycsb_request *req = &exec_qe->req;

//    quecc_prof_time = get_sys_clock();
    // get pointer to record in row
#if YCSB_INDEX_LOOKUP_PLAN
    row = exec_qe->row;
#else
    rc = run_ycsb_0(req, row);
#if SINGLE_NODE
    exec_qe->row = row;
#endif
    assert(rc == RCOK);
#endif
//    INC_STATS(get_thd_id(), exec_txn_index_lookup_time[get_thd_id()], get_sys_clock()-quecc_prof_time);
    // backup row to rollback writes
    row_access_backup(exec_qe, req->acctype, row, _thd_id);
//    quecc_prof_time = get_sys_clock();
    // perfrom access
    rc = run_ycsb_1(req->acctype, row);
    assert(rc == RCOK);

    // we only update the transaction context locally
    if (update_context){
        check_commit_ready(exec_qe);
    }
#endif
//    INC_STATS(get_thd_id(), exec_txn_proc_time[get_thd_id()], get_sys_clock()-quecc_prof_time);

//    uint64_t curr_time = get_sys_clock();
//    txn_stats.process_time += curr_time - starttime;
//    txn_stats.process_time_short += curr_time - starttime;
//    txn_stats.wait_starttime = get_sys_clock();
    return rc;
}
#endif // #if CC_ALG == QUECC


RC YCSBTxnManager::run_ycsb() {
    RC rc = RCOK;
    assert(CC_ALG == CALVIN);
    YCSBQuery *ycsb_query = (YCSBQuery *) query;

    for (uint64_t i = 0; i < ycsb_query->requests.size(); i++) {
        ycsb_request *req = ycsb_query->requests[i];
        if (this->phase == CALVIN_LOC_RD && req->acctype == WR)
            continue;
        if (this->phase == CALVIN_EXEC_WR && req->acctype == RD)
            continue;

        uint64_t part_id = _wl->key_to_part(req->key);
        query->partitions_touched.add_unique(part_id);
//        DEBUG_Q("WT_%ld : accessing part %ld\n", get_thd_id(),part_id);
#if !SINGLE_NODE
        bool loc = GET_NODE_ID(part_id) == g_node_id;
        if (!loc)
            continue;
#endif
        rc = run_ycsb_0(req, row);
        assert(rc == RCOK);

        rc = run_ycsb_1(req->acctype, row);
        assert(rc == RCOK);
    }
    return rc;

}

#endif // #if WORKLOAD == YCSB