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

#include "helper.h"
#include "txn.h"
#include "row.h"
#include "wl.h"
#include "query.h"
#include "thread.h"
#include "mem_alloc.h"
#include "occ.h"
#include "row_occ.h"
#include "table.h"
#include "catalog.h"
#include "index_btree.h"
#include "index_hash.h"
#include "msg_queue.h"
#include "pool.h"
#include "message.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "pps_query.h"
#include "array.h"
#include "maat.h"


void TxnStats::init() {
    starttime = 0;
#if PROFILE_EXEC_TIMING
    wait_starttime = get_sys_clock();
#endif
    total_process_time = 0;
    process_time = 0;
    total_local_wait_time = 0;
    local_wait_time = 0;
    total_remote_wait_time = 0;
    remote_wait_time = 0;
    total_twopc_time = 0;
    twopc_time = 0;
    write_cnt = 0;
    abort_cnt = 0;

    total_work_queue_time = 0;
    work_queue_time = 0;
    total_cc_block_time = 0;
    cc_block_time = 0;
    total_cc_time = 0;
    cc_time = 0;
    total_work_queue_cnt = 0;
    work_queue_cnt = 0;
    total_msg_queue_time = 0;
    msg_queue_time = 0;
    total_abort_time = 0;

    clear_short();
}

void TxnStats::clear_short() {

    work_queue_time_short = 0;
    cc_block_time_short = 0;
    cc_time_short = 0;
    msg_queue_time_short = 0;
    process_time_short = 0;
    network_time_short = 0;
}

void TxnStats::reset() {
#if PROFILE_EXEC_TIMING
    wait_starttime = get_sys_clock();
#endif
    total_process_time += process_time;
    process_time = 0;
    total_local_wait_time += local_wait_time;
    local_wait_time = 0;
    total_remote_wait_time += remote_wait_time;
    remote_wait_time = 0;
    total_twopc_time += twopc_time;
    twopc_time = 0;
    write_cnt = 0;

    total_work_queue_time += work_queue_time;
    work_queue_time = 0;
    total_cc_block_time += cc_block_time;
    cc_block_time = 0;
    total_cc_time += cc_time;
    cc_time = 0;
    total_work_queue_cnt += work_queue_cnt;
    work_queue_cnt = 0;
    total_msg_queue_time += msg_queue_time;
    msg_queue_time = 0;

    clear_short();

}

void TxnStats::abort_stats(uint64_t thd_id) {
    total_process_time += process_time;
    total_local_wait_time += local_wait_time;
    total_remote_wait_time += remote_wait_time;
    total_twopc_time += twopc_time;
    total_work_queue_time += work_queue_time;
    total_msg_queue_time += msg_queue_time;
    total_cc_block_time += cc_block_time;
    total_cc_time += cc_time;
    total_work_queue_cnt += work_queue_cnt;
    assert(total_process_time >= process_time);

    INC_STATS(thd_id, lat_s_rem_work_queue_time, total_work_queue_time);
    INC_STATS(thd_id, lat_s_rem_msg_queue_time, total_msg_queue_time);
    INC_STATS(thd_id, lat_s_rem_cc_block_time, total_cc_block_time);
    INC_STATS(thd_id, lat_s_rem_cc_time, total_cc_time);
    INC_STATS(thd_id, lat_s_rem_process_time, total_process_time);
}

void TxnStats::commit_stats(uint64_t thd_id, uint64_t txn_id, uint64_t batch_id, uint64_t timespan_long,
                            uint64_t timespan_short) {
    total_process_time += process_time;
    total_local_wait_time += local_wait_time;
    total_remote_wait_time += remote_wait_time;
    total_twopc_time += twopc_time;
    total_work_queue_time += work_queue_time;
    total_msg_queue_time += msg_queue_time;
    total_cc_block_time += cc_block_time;
    total_cc_time += cc_time;
    total_work_queue_cnt += work_queue_cnt;
    assert(total_process_time >= process_time);

#if CC_ALG == CALVIN

    INC_STATS(thd_id, lat_s_loc_work_queue_time, work_queue_time);
    INC_STATS(thd_id, lat_s_loc_msg_queue_time, msg_queue_time);
    INC_STATS(thd_id, lat_s_loc_cc_block_time, cc_block_time);
    INC_STATS(thd_id, lat_s_loc_cc_time, cc_time);
    INC_STATS(thd_id, lat_s_loc_process_time, process_time);
    // latency from start of transaction at this node
    PRINT_LATENCY("lat_l %ld %ld %ld %f %f %f %f %f %f\n", txn_id, batch_id, total_work_queue_cnt,
                  (double) timespan_long / BILLION, (double) total_work_queue_time / BILLION,
                  (double) total_msg_queue_time / BILLION, (double) total_cc_block_time / BILLION,
                  (double) total_cc_time / BILLION, (double) total_process_time / BILLION
    );
#else
#if !SINGLE_NODE
    // latency from start of transaction
    if (IS_LOCAL(txn_id)) {
      INC_STATS(thd_id,lat_l_loc_work_queue_time,total_work_queue_time);
      INC_STATS(thd_id,lat_l_loc_msg_queue_time,total_msg_queue_time);
      INC_STATS(thd_id,lat_l_loc_cc_block_time,total_cc_block_time);
      INC_STATS(thd_id,lat_l_loc_cc_time,total_cc_time);
      INC_STATS(thd_id,lat_l_loc_process_time,total_process_time);
      INC_STATS(thd_id,lat_l_loc_abort_time,total_abort_time);

      INC_STATS(thd_id,lat_s_loc_work_queue_time,work_queue_time);
      INC_STATS(thd_id,lat_s_loc_msg_queue_time,msg_queue_time);
      INC_STATS(thd_id,lat_s_loc_cc_block_time,cc_block_time);
      INC_STATS(thd_id,lat_s_loc_cc_time,cc_time);
      INC_STATS(thd_id,lat_s_loc_process_time,process_time);

      INC_STATS(thd_id,lat_short_work_queue_time,work_queue_time_short);
      INC_STATS(thd_id,lat_short_msg_queue_time,msg_queue_time_short);
      INC_STATS(thd_id,lat_short_cc_block_time,cc_block_time_short);
      INC_STATS(thd_id,lat_short_cc_time,cc_time_short);
      INC_STATS(thd_id,lat_short_process_time,process_time_short);
      INC_STATS(thd_id,lat_short_network_time,network_time_short);


    }
    else {
      INC_STATS(thd_id,lat_l_rem_work_queue_time,total_work_queue_time);
      INC_STATS(thd_id,lat_l_rem_msg_queue_time,total_msg_queue_time);
      INC_STATS(thd_id,lat_l_rem_cc_block_time,total_cc_block_time);
      INC_STATS(thd_id,lat_l_rem_cc_time,total_cc_time);
      INC_STATS(thd_id,lat_l_rem_process_time,total_process_time);
    }

    if (IS_LOCAL(txn_id)) {
      PRINT_LATENCY("lat_s %ld %ld %f %f %f %f %f %f\n"
            , txn_id
            , work_queue_cnt
            , (double) timespan_short / BILLION
            , (double) work_queue_time / BILLION
            , (double) msg_queue_time / BILLION
            , (double) cc_block_time / BILLION
            , (double) cc_time / BILLION
            , (double) process_time / BILLION
            );
     /*
    PRINT_LATENCY("lat_l %ld %ld %ld %f %f %f %f %f %f %f\n"
            , txn_id
            , total_work_queue_cnt
            , abort_cnt
            , (double) timespan_long / BILLION
            , (double) total_work_queue_time / BILLION
            , (double) total_msg_queue_time / BILLION
            , (double) total_cc_block_time / BILLION
            , (double) total_cc_time / BILLION
            , (double) total_process_time / BILLION
            , (double) total_abort_time / BILLION
            );
            */
    }
    else {
      PRINT_LATENCY("lat_rs %ld %ld %f %f %f %f %f %f\n"
            , txn_id
            , work_queue_cnt
            , (double) timespan_short / BILLION
            , (double) total_work_queue_time / BILLION
            , (double) total_msg_queue_time / BILLION
            , (double) total_cc_block_time / BILLION
            , (double) total_cc_time / BILLION
            , (double) total_process_time / BILLION
            );
    }
    /*
    if (!IS_LOCAL(txn_id) || timespan_short < timespan_long) {
      // latency from most recent start or restart of transaction
      PRINT_LATENCY("lat_s %ld %ld %f %f %f %f %f %f\n"
            , txn_id
            , work_queue_cnt
            , (double) timespan_short / BILLION
            , (double) work_queue_time / BILLION
            , (double) msg_queue_time / BILLION
            , (double) cc_block_time / BILLION
            , (double) cc_time / BILLION
            , (double) process_time / BILLION
            );
    }
    */
#else
    //single node
#if PROFILE_EXEC_TIMING
    INC_STATS(thd_id,lat_l_loc_work_queue_time,total_work_queue_time);
    INC_STATS(thd_id,lat_l_loc_msg_queue_time,total_msg_queue_time);
    INC_STATS(thd_id,lat_l_loc_cc_block_time,total_cc_block_time);
    INC_STATS(thd_id,lat_l_loc_cc_time,total_cc_time);
    INC_STATS(thd_id,lat_l_loc_process_time,total_process_time);
    INC_STATS(thd_id,lat_l_loc_abort_time,total_abort_time);

    INC_STATS(thd_id,lat_s_loc_work_queue_time,work_queue_time);
    INC_STATS(thd_id,lat_s_loc_msg_queue_time,msg_queue_time);
    INC_STATS(thd_id,lat_s_loc_cc_block_time,cc_block_time);
    INC_STATS(thd_id,lat_s_loc_cc_time,cc_time);
    INC_STATS(thd_id,lat_s_loc_process_time,process_time);

    INC_STATS(thd_id,lat_short_work_queue_time,work_queue_time_short);
    INC_STATS(thd_id,lat_short_msg_queue_time,msg_queue_time_short);
    INC_STATS(thd_id,lat_short_cc_block_time,cc_block_time_short);
    INC_STATS(thd_id,lat_short_cc_time,cc_time_short);
    INC_STATS(thd_id,lat_short_process_time,process_time_short);
    INC_STATS(thd_id,lat_short_network_time,network_time_short);
#endif // #if PROFILE_EXEC_TIMING

#endif
#endif

    if (!IS_LOCAL(txn_id)) {
        return;
    }
#if PROFILE_EXEC_TIMING
    INC_STATS(thd_id, txn_total_process_time, total_process_time);
    INC_STATS(thd_id, txn_process_time, process_time);
    INC_STATS(thd_id, txn_total_local_wait_time, total_local_wait_time);
    INC_STATS(thd_id, txn_local_wait_time, local_wait_time);
    INC_STATS(thd_id, txn_total_remote_wait_time, total_remote_wait_time);
    INC_STATS(thd_id, txn_remote_wait_time, remote_wait_time);
    INC_STATS(thd_id, txn_total_twopc_time, total_twopc_time);
    INC_STATS(thd_id, txn_twopc_time, twopc_time);
#endif
    if (write_cnt > 0) {
        INC_STATS(thd_id, txn_write_cnt, 1);
    }
    if (abort_cnt > 0) {
        INC_STATS(thd_id, unique_txn_abort_cnt, 1);
    }

}


void Transaction::init() {
    timestamp = UINT64_MAX;
    start_timestamp = UINT64_MAX;
    end_timestamp = UINT64_MAX;
    txn_id = UINT64_MAX;
    batch_id = UINT64_MAX;
    DEBUG_M("Transaction::init array insert_rows\n");
    insert_rows.init(g_max_items_per_txn + 10);
    DEBUG_M("Transaction::reset array accesses\n");
    uint64_t r_size = MAX_ROW_PER_TXN;
    if (YCSB_LONG_READS_TXN_ENABLED){
        r_size = g_synth_table_size * YCSB_LONG_READS_DB_PERC + 4;
    }
    accesses.init(r_size);

    reset(0);
}

void Transaction::reset(uint64_t thd_id) {
    release_accesses(thd_id);
    accesses.clear();
    //release_inserts(thd_id);
    insert_rows.clear();
    write_cnt = 0;
    row_cnt = 0;
    twopc_state = START;
    rc = RCOK;
}

void Transaction::release_accesses(uint64_t thd_id) {
    for (uint64_t i = 0; i < accesses.size(); i++) {
        access_pool.put(thd_id, accesses[i]);
    }
}

void Transaction::release_inserts(uint64_t thd_id) {
    for (uint64_t i = 0; i < insert_rows.size(); i++) {
        row_t *row = insert_rows[i];
#if CC_ALG != MAAT && CC_ALG != OCC && CC_ALG != HSTORE
        DEBUG_M("TxnManager::cleanup row->manager free\n");
        mem_allocator.free(row->manager, 0);
#endif
        row->free_row();
        DEBUG_M("Transaction::release insert_rows free\n")
        row_pool.put(thd_id, row);
    }
}

void Transaction::release(uint64_t thd_id) {
    DEBUG_M("Transaction release\n");
    release_accesses(thd_id);
    DEBUG_M("Transaction::release array accesses free\n")
    accesses.release();
    release_inserts(thd_id);
    DEBUG_M("Transaction::release array insert_rows free\n")
    insert_rows.release();
}

void TxnManager::init(uint64_t thd_id, Workload *h_wl) {
#if PROFILE_EXEC_TIMING
    uint64_t prof_starttime = get_sys_clock();
#endif
    if (!txn) {
        DEBUG_M("Transaction alloc\n");
        txn_pool.get(thd_id, txn);

    }
#if PROFILE_EXEC_TIMING
    INC_STATS(get_thd_id(), mtx[15], get_sys_clock() - prof_starttime);
    prof_starttime = get_sys_clock();
#endif
    //txn->init();
    if (!query) {
        DEBUG_M("TxnManager::init Query alloc\n");
        qry_pool.get(thd_id, query);
    }
#if PROFILE_EXEC_TIMING
    INC_STATS(get_thd_id(), mtx[16], get_sys_clock() - prof_starttime);
#endif
//    query->init();
    //FIXME: calvin
    // this reset() call is commented out in the original Deneva code
    // However, if commented out, Calvin may consume more memory which leads it to be killed.
    // Current hack, uncomment for some calvin experiments
    // For Maat, to work, this needs to be commented out!!
    // TQ: this seems to be a bug but not sure how to resolve it
//    reset();
    sem_init(&rsp_mutex, 0, 1);
    return_id = UINT64_MAX;

    this->h_wl = h_wl;
#if CC_ALG == MAAT
    uncommitted_writes = new std::set<uint64_t>();
    uncommitted_writes_y = new std::set<uint64_t>();
    uncommitted_reads = new std::set<uint64_t>();
#endif
#if CC_ALG == CALVIN
    phase = CALVIN_RW_ANALYSIS;
    locking_done = false;
    uint64_t r_size = MAX_ROW_PER_TXN;
    if (YCSB_LONG_READS_TXN_ENABLED){
        r_size = g_synth_table_size * YCSB_LONG_READS_DB_PERC + 4;
    }
    calvin_locked_rows.init(r_size);
#endif

#if CC_ALG == TICTOC || CC_ALG == SILO || CC_ALG == MOCC_SILO
    _pre_abort = PRE_ABORT;

#if VALIDATION_LOCK == VALIDATION_LOCK_NO_WAIT
        _validation_no_wait = true;
#elif (VALIDATION_LOCK == VALIDATION_LOCK_WAIT)
        _validation_no_wait = false;
#else
        assert(false);
#endif

#endif


#if CC_ALG == TICTOC
    _max_wts = 0;
	_write_copy_ptr = (g_params["write_copy_form"] == "ptr");
	_atomic_timestamp = (g_params["atomic_timestamp"] == "true");
#elif CC_ALG == SILO
    _cur_tid = 0;
#elif CC_ALG == MOCC_SILO
    _cur_tid = 0;
#endif

#if CC_ALG == QUECC
    update_context = false;
#endif

#if ABORT_MODE
    dd_abort = false;
#endif
    txn_ready = true;
    twopl_wait_start = 0;

    txn_stats.init();
}

// reset after abort
void TxnManager::reset() {
    lock_ready = false;
    lock_ready_cnt = 0;
    locking_done = true;
    ready_part = 0;
    rsp_cnt = 0;
    aborted = false;
    return_id = UINT64_MAX;
    twopl_wait_start = 0;

    //ready = true;

    // MaaT
    greatest_write_timestamp = 0;
    greatest_read_timestamp = 0;
    commit_timestamp = 0;
#if CC_ALG == MAAT
    uncommitted_writes->clear();
    uncommitted_writes_y->clear();
    uncommitted_reads->clear();
#endif

#if CC_ALG == CALVIN
    phase = CALVIN_RW_ANALYSIS;
    locking_done = false;
    calvin_locked_rows.clear();
#endif

#if ABORT_MODE
    dd_abort = false;
#endif

    assert(txn);
    assert(query);
    txn->reset(get_thd_id());

    // Stats
    txn_stats.reset();

}

void
TxnManager::release() {
#if PROFILE_EXEC_TIMING
    uint64_t prof_starttime = get_sys_clock();
#endif
    if (query) qry_pool.put(get_thd_id(), query);
#if PROFILE_EXEC_TIMING
    INC_STATS(get_thd_id(), mtx[0], get_sys_clock() - prof_starttime);
#endif
    query = NULL;
#if PROFILE_EXEC_TIMING
    prof_starttime = get_sys_clock();
#endif
    if (txn) txn_pool.put(get_thd_id(), txn);
#if PROFILE_EXEC_TIMING
    INC_STATS(get_thd_id(), mtx[1], get_sys_clock() - prof_starttime);
#endif
    txn = NULL;

#if CC_ALG == MAAT
    delete uncommitted_writes;
    delete uncommitted_writes_y;
    delete uncommitted_reads;
#endif
#if CC_ALG == CALVIN
    calvin_locked_rows.release();
#endif
    txn_ready = true;
}

void TxnManager::reset_query() {
#if WORKLOAD == YCSB
    ((YCSBQuery *) query)->reset();
#elif WORKLOAD == TPCC
    ((TPCCQuery*)query)->reset();
#elif WORKLOAD == PPS
    ((PPSQuery*)query)->reset();
#endif
}

RC TxnManager::commit() {
    DEBUG("Commit %ld\n", get_txn_id());
    release_locks(RCOK);

#if CC_ALG == MAAT
    time_table.release(get_thd_id(), get_txn_id());
#endif
    commit_stats();
#if LOGGING
    LogRecord * record = logger.createRecord(get_txn_id(),L_NOTIFY,0,0);
    if(g_repl_cnt > 0) {
      msg_queue.enqueue(get_thd_id(),Message::create_message(record,LOG_MSG),g_node_id + g_node_cnt + g_client_node_cnt); 
    }
  logger.enqueueRecord(record);
  return WAIT;
#endif
    return Commit;
}

RC TxnManager::abort() {
    if (aborted)
        return Abort;
    DEBUG("Abort %ld\n", get_txn_id());
    txn->rc = Abort;
    INC_STATS(get_thd_id(), total_txn_abort_cnt, 1);
    txn_stats.abort_cnt++;
#if !SINGLE_NODE
    if (IS_LOCAL(get_txn_id())) {
        INC_STATS(get_thd_id(), local_txn_abort_cnt, 1);
#if ABORT_MODE
        if (dd_abort){
            INC_STATS(get_thd_id(), txn_dd_abort_cnt, 1);
        }
#endif
    } else {
        INC_STATS(get_thd_id(), remote_txn_abort_cnt, 1);
        txn_stats.abort_stats(get_thd_id());
    }
#else
    INC_STATS(get_thd_id(), local_txn_abort_cnt, 1);
#endif
    aborted = true;
    release_locks(Abort);
#if CC_ALG == MAAT
    //assert(time_table.get_state(get_txn_id()) == MAAT_ABORTED);
    time_table.release(get_thd_id(), get_txn_id());
#endif
#if !SINGLE_NODE
    uint64_t timespan = get_sys_clock() - txn_stats.restart_starttime;
    if (IS_LOCAL(get_txn_id()) && warmup_done) {
        INC_STATS_ARR(get_thd_id(), start_abort_commit_latency, timespan);
    }
#endif
    /*
    // latency from most recent start or restart of transaction
    PRINT_LATENCY("lat_s %ld %ld 0 %f %f %f %f %f %f 0.0\n"
            , get_txn_id()
            , txn_stats.work_queue_cnt
            , (double) timespan / BILLION
            , (double) txn_stats.work_queue_time / BILLION
            , (double) txn_stats.msg_queue_time / BILLION
            , (double) txn_stats.cc_block_time / BILLION
            , (double) txn_stats.cc_time / BILLION
            , (double) txn_stats.process_time / BILLION
            );
            */
    //commit_stats();
    return Abort;
}

RC TxnManager::start_abort() {
    txn->rc = Abort;
#if !SINGLE_NODE
    DEBUG("%ld start_abort\n", get_txn_id());
    if (query->partitions_touched.size() > 1) {
        send_finish_messages();
        abort();
        return Abort;
    }
#endif
    return abort();
}

RC TxnManager::start_commit() {
    RC rc = RCOK;
    DEBUG("%ld start_commit RO?%d\n", get_txn_id(), query->readonly());
#if SINGLE_NODE
    // TQ: being a multi-parition does not matter for most of the CC approaches since partitions are logical within a single node
        rc = validate();
        if (rc == RCOK)
            rc = commit();
        else
            start_abort();
#else
    if (is_multi_part()) {
        if (!query->readonly() || CC_ALG == OCC || CC_ALG == MAAT) {
            // send prepare messages
            send_prepare_messages();
            rc = WAIT_REM;
        } else {
            send_finish_messages();
            rsp_cnt = 0;
            rc = commit();
        }
    } else { // is not multi-part
        rc = validate();
        if (rc == RCOK)
            rc = commit();
        else
            start_abort();
    }
#endif

    return rc;
}

void TxnManager::send_prepare_messages() {
    rsp_cnt = query->partitions_touched.size() - 1;
    DEBUG("%ld Send PREPARE messages to %d\n", get_txn_id(), rsp_cnt);
    for (uint64_t i = 0; i < query->partitions_touched.size(); i++) {
        if (GET_NODE_ID(query->partitions_touched[i]) == g_node_id) {
            continue;
        }
        msg_queue.enqueue(get_thd_id(), Message::create_message(this, RPREPARE),
                          GET_NODE_ID(query->partitions_touched[i]));
    }
}

void TxnManager::send_finish_messages() {
    rsp_cnt = query->partitions_touched.size() - 1;
    assert(IS_LOCAL(get_txn_id()));
    DEBUG("%ld Send FINISH messages to %d\n", get_txn_id(), rsp_cnt);
    for (uint64_t i = 0; i < query->partitions_touched.size(); i++) {
        if (GET_NODE_ID(query->partitions_touched[i]) == g_node_id) {
            continue;
        }
        msg_queue.enqueue(get_thd_id(), Message::create_message(this, RFIN), GET_NODE_ID(query->partitions_touched[i]));
    }
}

int TxnManager::received_response(RC rc) {
    assert(txn->rc == RCOK || txn->rc == Abort);
    if (txn->rc == RCOK)
        txn->rc = rc;
#if CC_ALG == CALVIN
    ++rsp_cnt;
    DEBUG_Q("WT_%lu: recieved a response for (%lu,%lu), rsp_cnt=%d\n",get_thd_id(), get_txn_id(), get_batch_id(), rsp_cnt);
#else
    --rsp_cnt;
#endif
    return rsp_cnt;
}

bool TxnManager::waiting_for_response() {
    return rsp_cnt > 0;
}

bool TxnManager::is_multi_part() {
    return query->partitions_touched.size() > 1;
    //return query->partitions.size() > 1;
}

void TxnManager::commit_stats() {
#if PROFILE_EXEC_TIMING
    uint64_t commit_time = get_sys_clock();
    uint64_t timespan_short = commit_time - txn_stats.restart_starttime;
    uint64_t timespan_long = commit_time - txn_stats.starttime;
    uint64_t txn_id =  get_txn_id();
    uint64_t batch_id = get_batch_id();
#endif
    uint64_t ctid = get_thd_id();
    INC_STATS(ctid, total_txn_commit_cnt, 1);

#if !SINGLE_NODE
    if(!IS_LOCAL(get_txn_id()) && CC_ALG != CALVIN) {
        INC_STATS(ctid,remote_txn_commit_cnt,1);
#if PROFILE_EXEC_TIMING
//        txn_stats.commit_stats(get_thd_id(),get_txn_id(),get_batch_id(), timespan_long, timespan_short);
        txn_stats.commit_stats(ctid, txn_id, batch_id, timespan_long, timespan_short);
#endif
        return;
    }
#endif


    INC_STATS(ctid, txn_cnt, 1);
    INC_STATS(ctid, local_txn_commit_cnt, 1);
#if COUNT_BASED_SIM_ENABLED && CC_ALG != CALVIN
    simulation->inc_txn_cnt(1);
#endif
#if PROFILE_EXEC_TIMING
    INC_STATS(ctid, txn_run_time, timespan_long);
#endif
//    if(STRICT_PPT && PART_PER_TXN == 1) {
//        if (query->partitions_touched.size() != PART_PER_TXN){
//            for (uint64_t i =0; i <query->partitions_touched.size(); ++i){
//                DEBUG_Q("WT_%ld: touched partition : %ld\n",get_thd_id(), query->partitions_touched[i]);
//            }
//
//        }
//        M_ASSERT_V(query->partitions_touched.size() == PART_PER_TXN,
//                   "query->partitions_touched.size() = %ld\n",
//                   query->partitions_touched.size());
//    }

    if (query->partitions_touched.size() > 1) {
        INC_STATS(ctid, multi_part_txn_cnt, 1);
#if PROFILE_EXEC_TIMING
        INC_STATS(ctid, multi_part_txn_run_time, timespan_long);
#endif
    } else {
        INC_STATS(ctid, single_part_txn_cnt, 1);
#if PROFILE_EXEC_TIMING
        INC_STATS(ctid, single_part_txn_run_time, timespan_long);
#endif
    }
    /*if(cflt) {
      INC_STATS(get_thd_id(),cflt_cnt_txn,1);
    }*/
#if PROFILE_EXEC_TIMING
    txn_stats.commit_stats(ctid, txn_id, batch_id, timespan_long, timespan_short);
#endif
#if CC_ALG == CALVIN
    return;
#endif
#if PROFILE_EXEC_TIMING
    INC_STATS_ARR(ctid, start_abort_commit_latency, timespan_short);
    INC_STATS_ARR(ctid, last_start_commit_latency, timespan_short);
    INC_STATS_ARR(ctid, first_start_commit_latency, timespan_long);
#endif
    M_ASSERT_V(query->partitions_touched.size() > 0,"Committed %s txn=%lu \n", IS_LOCAL(get_txn_id())? "local" : "remote",get_txn_id());
    assert(query->partitions_touched.size() > 0);
    INC_STATS(ctid, parts_touched, query->partitions_touched.size());
    INC_STATS(ctid, part_cnt[query->partitions_touched.size() - 1], 1);
    for (uint64_t i = 0; i < query->partitions_touched.size(); i++) {
        INC_STATS(ctid, part_acc[query->partitions_touched[i]], 1);
    }
}

void TxnManager::register_thread(Thread *h_thd) {
    this->h_thd = h_thd;
    this->_thd_id = h_thd->_thd_id;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
    this->active_part = GET_PART_ID_FROM_IDX(get_thd_id());
#endif
}

void TxnManager::set_txn_id(txnid_t txn_id) {
    txn->txn_id = txn_id;
}

txnid_t TxnManager::get_txn_id() {
    return txn->txn_id;
}

Workload *TxnManager::get_wl() {
    return h_wl;
}

uint64_t TxnManager::get_thd_id() {
    if (h_thd)
//        return h_thd->get_thd_id();
        return _thd_id;
    else
        return 0;
}

BaseQuery *TxnManager::get_query() {
    return query;
}

void TxnManager::set_query(BaseQuery *qry) {
    query = qry;
}

void TxnManager::set_timestamp(ts_t timestamp) {
    txn->timestamp = timestamp;
}

ts_t TxnManager::get_timestamp() {
    return txn->timestamp;
}

void TxnManager::set_start_timestamp(uint64_t start_timestamp) {
    txn->start_timestamp = start_timestamp;
}

ts_t TxnManager::get_start_timestamp() {
    return txn->start_timestamp;
}

uint64_t TxnManager::incr_lr() {
    //ATOM_ADD(this->rsp_cnt,i);
    uint64_t result;
    sem_wait(&rsp_mutex);
    result = ++this->lock_ready_cnt;
    sem_post(&rsp_mutex);
    return result;
}

uint64_t TxnManager::decr_lr() {
    //ATOM_SUB(this->rsp_cnt,i);
    uint64_t result;
    sem_wait(&rsp_mutex);
    result = --this->lock_ready_cnt;
    sem_post(&rsp_mutex);
    return result;
}

uint64_t TxnManager::incr_rsp(int i) {
//    ATOM_ADD(this->rsp_cnt,i);
    return ATOM_ADD_FETCH(this->rsp_cnt, i);
//    uint64_t result;
//    sem_wait(&rsp_mutex);
//    result = ++this->rsp_cnt;
//    sem_post(&rsp_mutex);
//    return result;
}

uint64_t TxnManager::decr_rsp(int i) {
    //ATOM_SUB(this->rsp_cnt,i);
    uint64_t result;
    // TQ: thread can sleeps because spinning may be long
    sem_wait(&rsp_mutex);
    result = --this->rsp_cnt;
    sem_post(&rsp_mutex);
    return result;
}

void TxnManager::release_last_row_lock() {
    assert(txn->row_cnt > 0);
    row_t *orig_r = txn->accesses[txn->row_cnt - 1]->orig_row;
    access_t type = txn->accesses[txn->row_cnt - 1]->type;
    orig_r->return_row(RCOK, type, this, NULL);
    //txn->accesses[txn->row_cnt-1]->orig_row = NULL;
}

void TxnManager::cleanup_row(RC rc, uint64_t rid) {
    access_t type = txn->accesses[rid]->type;
    if (type == WR && rc == Abort && CC_ALG != MAAT) {
        type = XP;
    }

    // Handle calvin elsewhere
#if CC_ALG != CALVIN
#if ISOLATION_LEVEL != READ_UNCOMMITTED
    row_t * orig_r = txn->accesses[rid]->orig_row;

    if (ROLL_BACK && type == XP &&
                (CC_ALG == DL_DETECT ||
                CC_ALG == NO_WAIT ||
                CC_ALG == WAIT_DIE ||
      CC_ALG == HSTORE ||
      CC_ALG == HSTORE_SPEC
      ))
    {
//        uint64_t proftime = get_sys_clock();
        orig_r->return_row(rc,type, this, txn->accesses[rid]->orig_data);
        if (CC_ALG != CALVIN && ROLL_BACK && type == XP) {
//            INC_STATS(_thd_id, record_copy_time[_thd_id], get_sys_clock()-proftime);
            INC_STATS(_thd_id, record_recov_cnt[_thd_id],1);
        }
    } else {
#if ISOLATION_LEVEL == READ_COMMITTED
        if(type == WR) {
          orig_r->return_row(rc,type, this, txn->accesses[rid]->data);
        }
#else
        orig_r->return_row(rc,type, this, txn->accesses[rid]->data);
#endif
    }
#endif

#if ROLL_BACK && (CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC)
    if (type == WR) {
        uint64_t ctid = get_thd_id();
        //printf("free 10 %ld\n",get_txn_id());
        txn->accesses[rid]->orig_data->free_row();
        DEBUG_M("TxnManager::cleanup row_t free\n");
        row_pool.put(ctid,txn->accesses[rid]->orig_data);
        if(rc == RCOK) {
            INC_STATS(ctid,record_write_cnt,1);
            ++txn_stats.write_cnt;
        }
    }
#endif
#endif
    txn->accesses[rid]->data = NULL;

}

void TxnManager::cleanup(RC rc) {
#if CC_ALG == OCC && MODE == NORMAL_MODE
    occ_man.finish(rc, this);
#endif
#if PROFILE_EXEC_TIMING
    ts_t starttime = get_sys_clock();
#endif
    uint64_t row_cnt = txn->accesses.get_count();
    assert(txn->accesses.get_count() == txn->row_cnt);
    //assert((WORKLOAD == YCSB && row_cnt <= g_req_per_query) || (WORKLOAD == TPCC && row_cnt <= g_max_items_per_txn*2 + 3));


    DEBUG("Cleanup %ld %ld\n", get_txn_id(), row_cnt);
    for (int rid = row_cnt - 1; rid >= 0; rid--) {
        cleanup_row(rc, rid);
    }
#if CC_ALG == CALVIN
    // cleanup locked rows
    for (uint64_t i = 0; i < calvin_locked_rows.size(); i++) {
        row_t *row = calvin_locked_rows[i];
        row->return_row(rc, RD, this, row);
    }
#endif

    if (rc == Abort) {
        txn->release_inserts(get_thd_id());
        txn->insert_rows.clear();
#if PROFILE_EXEC_TIMING
        INC_STATS(get_thd_id(), abort_time, get_sys_clock() - starttime);
#endif
    }
}

RC TxnManager::get_lock(row_t *row, access_t type) {

    if (calvin_locked_rows.contains(row)) {
        return RCOK;
    }
    calvin_locked_rows.add(row);
    RC rc = row->get_lock(type, this);
    if (rc == WAIT) {
        INC_STATS(get_thd_id(), txn_wait_cnt, 1);
    }
    return rc;
}

#if CC_ALG == QUECC
void TxnManager::check_commit_ready(exec_queue_entry *entry) {
    uint64_t e8;
    uint64_t d8;
    do{
        e8 = entry->txn_ctx->completion_cnt.load(memory_order_acq_rel);
        d8 = e8 + 1;
    } while(!entry->txn_ctx->completion_cnt.compare_exchange_strong(e8,d8,memory_order_acq_rel));
//    DEBUG_Q("check_commit_ready: txn_id=%lu, completion_cnt = %lu, txn_comp_cnt=%lu\n",
//            entry->txn_ctx->txn_id,d8,entry->txn_ctx->txn_comp_cnt.load(memory_order_acq_rel));
//#if SINGLE_NODE
    if (d8 > (entry->txn_ctx->txn_comp_cnt.load(memory_order_acq_rel))) {
        DEBUG_Q("completion_cnt = %lu, txn_comp_cnt=%lu\n",d8,entry->txn_ctx->txn_comp_cnt.load(memory_order_acq_rel));
//            DEBUG_Q("Last entry in etxn_id=%ld, ctx_txn_id=%ld transaction comp_cnt = %lu, ctx txn_comp_cnt %lu, txn_ctx_ptr=%lu\n",
//                    entry->txn_id, entry->txn_ctx->txn_id, entry->txn_ctx->txn_comp_cnt.load(memory_order_acq_rel),
//                    entry->txn_ctx->txn_comp_cnt.load(),(uint64_t)entry->txn_ctx);
        // this is the last entry to be executed, we should be ready to commit
//                e8 = TXN_STARTED;
//                d8 = TXN_READY_TO_COMMIT;
//                if (!entry->txn_ctx->txn_state.compare_exchange_strong(e8, d8,memory_order_acq_rel)) {
//                    M_ASSERT_V(false,"ET_%ld: Invalid txn state = %ld, txn_id=%ld, ctx_txn_id=%lu\n",
//                               _thd_id,entry->txn_ctx->txn_state.load(), entry->txn_id, entry->txn_ctx->txn_id);
//                }
        assert(false);
    }
    else if (d8 == (entry->txn_ctx->txn_comp_cnt.load(memory_order_acq_rel))){
#if !SERVER_GENERATE_QUERIES && EARLY_CL_RESPONSE
        Message * rsp_msg = Message::create_message(CL_RSP);
        rsp_msg->txn_id = entry->txn_ctx->txn_id;
        rsp_msg->batch_id = entry->batch_id; // using batch_id from local, we can also use the one in the context
        ((ClientResponseMessage *) rsp_msg)->client_startts = entry->txn_ctx->client_startts;
        rsp_msg->lat_work_queue_time = 0;
        rsp_msg->lat_msg_queue_time = 0;
        rsp_msg->lat_cc_block_time = 0;
        rsp_msg->lat_cc_time = 0;
        rsp_msg->lat_process_time = 0;
        rsp_msg->lat_network_time = 0;
        rsp_msg->lat_other_time = 0;

        msg_queue.enqueue(0, rsp_msg, entry->txn_ctx->return_node_id);

        INC_STATS(0, txn_cnt, 1);
#endif
    }
//#endif
}

void TxnManager::row_access_backup(exec_queue_entry * entry, access_t type, row_t * row, uint64_t ctid){
#if ROW_ACCESS_TRACKING
#if ROLL_BACK
#if ROW_ACCESS_IN_CTX
    if (type == WR) {
        uint64_t rec_size = row->get_tuple_size();
#if WORKLOAD == YCSB
        // we store the prev_tid and set the last_tid to the current txn_id. This is used to roll back the transaction
        entry->txn_ctx->prev_tid[entry->req_idx] = row->last_tid;
        row->last_tid = entry->txn_id;
        uint64_t ri = entry->req_idx*rec_size;
#else
        M_ASSERT_V(rec_size <= MAX_TUPLE_SIZE, "resize is not valid: %lu\n",rec_size);
//        DEBUG_Q("WT_%lu: last_tid=%lu, txn_id=%lu, rid=%lu, req_idx=%lu\n",_thd_id,row->last_tid, entry->txn_id, entry->rid,entry->req_idx);
        // we store the prev_tid and set the last_tid to the current txn_id. This is used to roll back the transaction
        entry->txn_ctx->prev_tid[entry->req_idx] = row->last_tid;
        row->last_tid = entry->txn_id;
        uint64_t ri = entry->req_idx*MAX_TUPLE_SIZE;
#endif

//        DEBUG_Q("ET_%lu: ri=%lu, entry->req_idx=%lu, rec_size=%lu, rid=%lu, entry_type=%d\n",
//                _thd_id,ri,entry->req_idx,rec_size,entry->rid, entry->type);
#if PROFILE_EXEC_TIMING
        uint64_t proftime = get_sys_clock();
#endif
        memcpy(&entry->txn_ctx->undo_buffer_data[ri], row->data, rec_size);
#if PROFILE_EXEC_TIMING
        INC_STATS(_thd_id, record_copy_time[_thd_id], get_sys_clock()-proftime);
#endif
        INC_STATS(_thd_id, record_copy_cnt[_thd_id], 1);
        entry->txn_ctx->orig_rows[entry->req_idx] = row;
        entry->txn_ctx->a_types[entry->req_idx] = WR;
    }
    else{
        entry->txn_ctx->a_types[entry->req_idx] = RD;
    }

#else
    if (type == WR) {
        Access *access;
        access_pool.get(ctid, access);
        M_ASSERT_V(access, "got invalid access object\n");
        uint64_t part_id = row->get_part_id();
        DEBUG_M("TxnManager::get_row row_t alloc\n")
        row_pool.get(ctid,access->orig_data);
        access->orig_data->init_from_pool(row->get_table(), part_id,0, _thd_id);
//        access->orig_data->init(row->get_table(), part_id);
        access->prev_tid = row->last_tid;
        row->last_tid = entry->txn_id;
        uint64_t proftime = get_sys_clock();
        access->orig_data->copy(row);
        INC_STATS(_thd_id, record_copy_time[_thd_id], get_sys_clock()-proftime);
        assert(access->orig_data->get_schema() == row->get_schema());
        INC_STATS(_thd_id, record_copy_cnt[_thd_id], 1);

    // ARIES-style physiological logging
    //#if LOGGING
    //    //LogRecord * record = logger.createRecord(LRT_UPDATE,L_UPDATE,get_txn_id(),part_id,row->get_table()->get_table_id(),row->get_primary_key());
    //    LogRecord * record = logger.createRecord(get_txn_id(),L_UPDATE,row->get_table()->get_table_id(),row->get_primary_key());
    //    if(g_repl_cnt > 0) {
    //      msg_queue.enqueue(get_thd_id(),Message::create_message(record,LOG_MSG),g_node_id + g_node_cnt + g_client_node_cnt);
    //    }
    //    logger.enqueueRecord(record);
    //#endif

        access->thd_id = ctid;
        access->type = type;
        //TODO(tq): we should only use one
        access->orig_row = row;
        access->data = row;
        // Need to protect accesses and synchronize updates to it.
//        entry->txn_ctx->accesses->atomic_add(access);

        entry->txn_ctx->access_lock->lock();
        entry->txn_ctx->accesses->add(access);
        entry->txn_ctx->access_lock->unlock();

    }
#endif
#endif
#endif
}
#endif // #if CC_ALG == QUECC

RC  TxnManager::get_row(row_t *row, access_t type, row_t *&row_rtn) {
#if PROFILE_EXEC_TIMING
    uint64_t starttime = get_sys_clock();
    uint64_t timespan;
#endif
    RC rc = RCOK;
    uint64_t ctid = get_thd_id();
    DEBUG_M("TxnManager::get_row access alloc\n");
    Access *access;
    access_pool.get(ctid, access);
    //uint64_t row_cnt = txn->row_cnt;
    //assert(txn->accesses.get_count() - 1 == row_cnt);

    this->last_row = row;
    this->last_type = type;

    rc = row->get_row(type, this, access->data);

    if (rc == Abort || rc == WAIT) {
        row_rtn = NULL;
        DEBUG_M("TxnManager::get_row(abort) access free\n");
//        DEBUG_Q("ET_%ld: TxnManager::get_row(abort) access free, write_cnt = %ld\n", _thd_id, txn_stats.write_cnt);
        access_pool.put(get_thd_id(), access);
#if PROFILE_EXEC_TIMING
        timespan = get_sys_clock() - starttime;
        INC_STATS(ctid, txn_manager_time, timespan);
#endif
        INC_STATS(ctid, txn_conflict_cnt, 1);
        //cflt = true;
#if DEBUG_TIMELINE
        printf("CONFLICT %ld %ld\n",ctid,get_sys_clock());
#endif
        return rc;
    }
    access->type = type;
    access->orig_row = row;
#if CC_ALG == TICTOC
    access->wts = last_wts;
	access->rts = last_rts;
#elif CC_ALG == SILO
    access->tid = last_tid;
#elif CC_ALG == MOCC_SILO
    access->tid = last_tid;
#endif

#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC)
    if (type == WR) {

    //printf("alloc 10 %ld\n",get_txn_id());
    uint64_t part_id = row->get_part_id();
    DEBUG_M("TxnManager::get_row row_t alloc\n")
    row_pool.get(ctid,access->orig_data);
    access->orig_data->init(row->get_table(), part_id, 0);
#if PROFILE_EXEC_TIMING
        uint64_t proftime = get_sys_clock();
#endif
    access->orig_data->copy(row);
#if PROFILE_EXEC_TIMING
        INC_STATS(_thd_id, record_copy_time[_thd_id], get_sys_clock()-proftime);
#endif
        INC_STATS(_thd_id, record_copy_cnt[_thd_id],1);
    assert(access->orig_data->get_schema() == row->get_schema());

    // ARIES-style physiological logging
#if LOGGING
    //LogRecord * record = logger.createRecord(LRT_UPDATE,L_UPDATE,get_txn_id(),part_id,row->get_table()->get_table_id(),row->get_primary_key());
    LogRecord * record = logger.createRecord(get_txn_id(),L_UPDATE,row->get_table()->get_table_id(),row->get_primary_key());
    if(g_repl_cnt > 0) {
      msg_queue.enqueue(get_thd_id(),Message::create_message(record,LOG_MSG),g_node_id + g_node_cnt + g_client_node_cnt);
    }
    logger.enqueueRecord(record);
#endif

    }
#endif

    ++txn->row_cnt;
    if (type == WR)
        ++txn->write_cnt;

    txn->accesses.add(access);
#if PROFILE_EXEC_TIMING
    timespan = get_sys_clock() - starttime;
    INC_STATS(get_thd_id(), txn_manager_time, timespan);
#endif
    row_rtn = access->data;


    if (CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == CALVIN)
        assert(rc == RCOK);
    return rc;
}

RC TxnManager::get_row_post_wait(row_t *&row_rtn) {
    assert(CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC);
#if PROFILE_EXEC_TIMING
    uint64_t starttime = get_sys_clock();
#endif
    row_t *row = this->last_row;
    access_t type = this->last_type;
    assert(row != NULL);
    DEBUG_M("TxnManager::get_row_post_wait access alloc\n")
    Access *access;
    access_pool.get(get_thd_id(), access);

    row->get_row_post_wait(type, this, access->data);

    access->type = type;
    access->orig_row = row;
#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE)
    if (type == WR) {

      uint64_t part_id = row->get_part_id();
    //printf("alloc 10 %ld\n",get_txn_id());
    DEBUG_M("TxnManager::get_row_post_wait row_t alloc\n")
    row_pool.get(get_thd_id(),access->orig_data);
        access->orig_data->init(row->get_table(), part_id, 0);
#if PROFILE_EXEC_TIMING
        uint64_t proftime = get_sys_clock();
#endif
        access->orig_data->copy(row);
#if PROFILE_EXEC_TIMING
        INC_STATS(_thd_id, record_copy_time[_thd_id], get_sys_clock()-proftime);
#endif
        INC_STATS(_thd_id, record_copy_cnt[_thd_id], 1);
    }
#endif

    ++txn->row_cnt;
    if (type == WR)
        ++txn->write_cnt;


    txn->accesses.add(access);
#if PROFILE_EXEC_TIMING
    uint64_t timespan = get_sys_clock() - starttime;
    INC_STATS(get_thd_id(), txn_manager_time, timespan);
#endif
    this->last_row_rtn = access->data;
    row_rtn = access->data;
    return RCOK;

}

void TxnManager::insert_row(row_t *row, table_t *table) {
    if (CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC)
        return;
    assert(txn->insert_rows.size() < MAX_ROW_PER_TXN);
    txn->insert_rows.add(row);
}

itemid_t *
TxnManager::index_read(INDEX *index, idx_key_t key, int part_id) {
//    uint64_t starttime = get_sys_clock();

    itemid_t *item;
//    index->index_read(key, item, part_id, get_thd_id());
    index->index_read(key, item, part_id, 0);

//    uint64_t t = get_sys_clock() - starttime;
//    INC_STATS(get_thd_id(), txn_index_time, t);
    //txn_time_idx += t;

    return item;
}

itemid_t *
TxnManager::index_read(INDEX *index, idx_key_t key, int part_id, int count) {
#if PROFILE_EXEC_TIMING
    uint64_t starttime = get_sys_clock();
#endif
    itemid_t *item;
    index->index_read(key, count, item, part_id);
#if PROFILE_EXEC_TIMING
    uint64_t t = get_sys_clock() - starttime;
    INC_STATS(get_thd_id(), txn_index_time, t);
#endif
    //txn_time_idx += t;

    return item;
}


RC TxnManager::validate() {
#if MODE != NORMAL_MODE && MODE != FIXED_MODE
    return RCOK;
#endif
    if (CC_ALG != OCC && CC_ALG != MAAT) {
        return RCOK;
    }
    RC rc = RCOK;
#if PROFILE_EXEC_TIMING
    uint64_t starttime = get_sys_clock();
#endif
#if CC_ALG == OCC
    rc = occ_man.validate(this);
#endif
#if CC_ALG == MAAT
    rc = maat_man.validate(this);
    // Note: home node must be last to validate
    if (IS_LOCAL(get_txn_id()) && rc == RCOK) {
        rc = maat_man.find_bound(this);
    }
#endif
#if PROFILE_EXEC_TIMING
    INC_STATS(get_thd_id(), txn_validate_time, get_sys_clock() - starttime);
#endif
    return rc;
}

RC
TxnManager::send_remote_reads() {
    assert(CC_ALG == CALVIN);
    // we will send remote reads only in abort mode for YCSB
#if !ABORT_MODE && WORKLOAD == YCSB
    return RCOK;
#endif
    assert(query->active_nodes.size() == g_node_cnt);
    for (uint64_t i = 0; i < query->active_nodes.size(); i++) {
        if (i == g_node_id)
            continue;
        if (query->active_nodes[i] == 1) {
            DEBUG_Q("WT_%lu:(%ld,%ld) send_remote_read RFWD to %ld\n",get_thd_id(), get_txn_id(), get_batch_id(), i);
            msg_queue.enqueue(get_thd_id(), Message::create_message(this, RFWD), i);
        }
    }
    return RCOK;

}

bool TxnManager::calvin_exec_phase_done() {
    bool ready = (phase == CALVIN_DONE) && (get_rc() != WAIT);
    if (ready) {
        DEBUG("(%ld,%ld) calvin exec phase done!\n", txn->txn_id, txn->batch_id);
    }
    return ready;
}

bool TxnManager::calvin_collect_phase_done() {
    bool ready = (phase == CALVIN_COLLECT_RD) && (get_rsp_cnt() == calvin_expected_rsp_cnt);
    if (ready) {
        DEBUG_Q("(%ld,%ld) calvin collect phase done!\n", txn->txn_id, txn->batch_id);
    }
    return ready;
}

void TxnManager::release_locks(RC rc) {
#if PROFILE_EXEC_TIMING
    uint64_t starttime = get_sys_clock();
#endif
    cleanup(rc);
#if PROFILE_EXEC_TIMING
    uint64_t timespan = (get_sys_clock() - starttime);
    INC_STATS(get_thd_id(), txn_cleanup_time, timespan);
#endif
}
