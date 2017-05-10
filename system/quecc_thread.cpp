//
// Created by Thamir Qadah on 4/20/17.
//


#include "global.h"
#include "manager.h"
#include "thread.h"
#include "calvin_thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "math.h"
#include "helper.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "sequencer.h"
#include "logger.h"
#include "message.h"
#include "work_queue.h"
#include "quecc_thread.h"

void ExecutionThread::setup() {
}

RC ExecutionThread::run() {
    tsetup();

//    RC rc = RCOK;
//    TxnManager * txn_man;
//    uint64_t prof_starttime = get_sys_clock();
//    uint64_t idle_starttime = 0;
//

//    while(!simulation->is_done()) {
//        txn_man = NULL;



//        // for now, we will fix each executor thread to a specific queue
//        Message * msg = work_queue.sched_dequeue(_thd_id);
//
//        if(!msg) {
//            if(idle_starttime == 0)
//                idle_starttime = get_sys_clock();
//            continue;
//        }
//        if(idle_starttime > 0) {
//            INC_STATS(_thd_id,sched_idle_time,get_sys_clock() - idle_starttime);
//            idle_starttime = 0;
//        }
//
//        prof_starttime = get_sys_clock();
//        assert(msg->get_rtype() == CL_QRY);
//        assert(msg->get_txn_id() != UINT64_MAX);
//
//        txn_man = txn_table.get_transaction_manager(get_thd_id(),msg->get_txn_id(),msg->get_batch_id());
//        while(!txn_man->unset_ready()) { }
//        assert(ISSERVERN(msg->get_return_id()));
//        txn_man->txn_stats.starttime = get_sys_clock();
//
//        txn_man->txn_stats.lat_network_time_start = msg->lat_network_time;
//        txn_man->txn_stats.lat_other_time_start = msg->lat_other_time;
//
//        msg->copy_to_txn(txn_man);
//        txn_man->register_thread(this);
//        assert(ISSERVERN(txn_man->return_id));
//
//        INC_STATS(get_thd_id(),sched_txn_table_time,get_sys_clock() - prof_starttime);
//        prof_starttime = get_sys_clock();
//
//        rc = RCOK;
//        // Acquire locks
//        if (!txn_man->isRecon()) {
//            rc = txn_man->acquire_locks();
//        }
//
//        if(rc == RCOK) {
//            work_queue.enqueue(_thd_id,msg,false);
//        }
//        txn_man->set_ready();
//
//        INC_STATS(_thd_id,mtx[33],get_sys_clock() - prof_starttime);
//        prof_starttime = get_sys_clock();

//    }
    printf("FINISH %ld:%ld\n",_node_id,_thd_id);
    fflush(stdout);
    return FINISH;
}

void PlannerThread::setup() {
}

bool PlannerThread::is_batch_ready() {
//    bool ready = get_wall_clock() - simulation->last_seq_epoch_time >= g_seq_batch_time_limit;
//    return ready;
    return false;
}

uint32_t PlannerThread::get_bucket(uint64_t key) {
    uint64_t no_buckets = g_thread_cnt;
    for (uint64_t i = 0; i < no_buckets; i++){
        if (key >= (i*bucket_size) && key < ((i+1)*bucket_size)){
            return i;
        }
    }
    return 0;

}

RC PlannerThread::run() {
    tsetup();

    Message * msg;
    uint64_t idle_starttime = 0;
    uint64_t prof_starttime = 0;
    uint64_t batch_cnt = 0;
//    uint64_t mrange_cnt = g_thread_cnt;

    Array<uint64_t> mrange_cnts;
    mrange_cnts.init(g_thread_cnt);

    uint64_t txn_prefix_base = 0x0010000000000000;
    uint64_t txn_prefix_planner_base = (_planner_id * txn_prefix_base);

    assert(UINT64_MAX > (txn_prefix_planner_base+txn_prefix_base));

    DEBUG_Q("Planner_%ld thread started, txn_ids start at %ld \n", _planner_id, txn_prefix_planner_base);
    uint64_t planner_batch_size = g_batch_size/g_plan_thread_cnt;
    // max capcity, assume YCSB workload
    uint64_t exec_queue_capacity = planner_batch_size*10;
    // create and and pre-allocate execution queues
    // For each mrange which will be assigned to an execution thread
    // there will be an array pointer.
    // When the batch is complete we will CAS the exec_q array to allow
    // execution threads to be

    Array<Array<exec_queue_entry> *> * exec_queues = new Array<Array<exec_queue_entry> *>();
    exec_queues->init(g_thread_cnt);

    for (uint64_t i = 0; i < g_thread_cnt; i++) {
        mrange_cnts.add(0);
        Array<exec_queue_entry> * exec_q = new Array<exec_queue_entry>();
        exec_q->init(exec_queue_capacity);
        exec_queues->add(exec_q);
    }
    uint64_t total_access_cnt = 0;
    uint64_t  batch_id = 0;
    uint64_t planner_txn_id = txn_prefix_planner_base;


    while(!simulation->is_done()) {

//        prof_starttime = get_sys_clock();

        // TQ: this does nto work well for multiple planner threads
//        if(is_batch_ready()) {
//            simulation->advance_seq_epoch();
//            //last_batchtime = get_wall_clock();
//            seq_man.send_next_batch(_thd_id);
//        }

//        INC_STATS(_thd_id,mtx[30],get_sys_clock() - prof_starttime);
        prof_starttime = get_sys_clock();

        // dequeue for repective input_queue: there is an input queue for each planner
        // entries in the input queue is placed by the I/O thread
        // for now just dequeue and print notification
//        DEBUG_Q("Planner_%d is dequeuing\n", _planner_id);
        msg = work_queue.plan_dequeue(_thd_id, _planner_id);

        INC_STATS(_thd_id,mtx[31],get_sys_clock() - prof_starttime);
        prof_starttime = get_sys_clock();

        if(!msg) {
            if(idle_starttime == 0)
                idle_starttime = get_sys_clock();
            continue;
        }
        batch_cnt++;
        if (batch_cnt == planner_batch_size){
            DEBUG_Q("Batch complete\n")
            // a batch is ready to be delivered to the the execution threads.
            // we will have a batch queue for each of the planners and they are scanned in known order
            // by execution threads
            // We also have a batch queue for each of the executor that is mapped to bucket
            // for now, we will assign a bucket to each executor
            // All we need to do is to automically CAS the pointer to the address of the accumulated batch
            // and the execution threads who are spinning can start execution

            // Here major ranges have one-to-one mapping to worker threads
            for (uint64_t i = 0; i < g_thread_cnt; i++){
//                uint64_t exe_q_ptr = (uint64_t) work_queue.batch_map[_planner_id][i][batch_id];
//                DEBUG_Q("old value for ptr to  exec_q[%ld][%ld][%ld] is %ld\n", _planner_id, i, batch_id, exe_q_ptr);
//                DEBUG_Q("new value for ptr to  exec_q[%ld][%ld][%ld] is %ld\n", _planner_id, i, batch_id, (uint64_t) exec_queues->get(i));
                while (!ATOM_CAS(work_queue.batch_map[_planner_id][i][batch_id], 0, exec_queues->get(i))) {}
//                atomic_thread_fence(std::memory_order_release);
//                DEBUG_Q("Batch_%ld enqueue range_%ld\n", batch_id, i);
            }


            // reset execution queues for the new batch
            exec_queues = new Array<Array<exec_queue_entry> *>();
            exec_queues->init(g_thread_cnt);

            for (uint64_t i = 0; i < g_thread_cnt; i++) {
                Array<exec_queue_entry> * exec_q = new Array<exec_queue_entry>();
                exec_q->init(exec_queue_capacity);
                exec_queues->add(exec_q);
            }
            batch_id++;
            batch_cnt = 0;
        }

        if(idle_starttime > 0) {
            INC_STATS(_thd_id,seq_idle_time,get_sys_clock() - idle_starttime);
            idle_starttime = 0;
        }

        switch (msg->get_rtype()) {
            case CL_QRY: {
                // Query from client
//                DEBUG_Q("Planner_%d planning txn %ld\n", _planner_id,msg->txn_id);


#if WORKLOAD == YCSB
                // create transaction context
                transaction_context *tctx = (transaction_context *) mem_allocator.alloc(sizeof(transaction_context));
                tctx->txn_id = planner_txn_id;
                // Analyze read-write set
                /* We need to determine the ranges needed for each key
                 * We group keys that fall in the same range to be processed together
                 * TODO(tq): add repartitioning
                 */
                YCSBClientQueryMessage *ycsb_msg = ((YCSBClientQueryMessage *) msg);
                for (uint64_t j = 0; j < ycsb_msg->requests.size(); j++) {
                    ycsb_request *ycsb_req = ycsb_msg->requests.get(j);
                    uint64_t key = ycsb_req->key;
//                    DEBUG_Q("Planner_%d looking up bucket for key %ld\n", _planner_id, key);
                    uint64_t idx = get_bucket(key);
//                    DEBUG_Q("Planner_%d using bucket %ld for key %ld\n", _planner_id,idx, key);
                    uint64_t nval = mrange_cnts.get(idx) + 1;
                    mrange_cnts.set(idx, nval);
                    total_access_cnt++;

                    // create execution entry, for now it will contain only one request
                    exec_queue_entry *entry = (exec_queue_entry *) mem_allocator.alloc(sizeof(exec_queue_entry));
                    entry->tctx = tctx;
                    entry->batch_id = batch_id;
                    entry->mrange_id = idx;
                    entry->planner_id = _planner_id;
                    entry->starttime = get_sys_clock();
                    // for now, assume only one request per entry
                    entry->ycsb_req_index = j;

                    // TODO(tq): this should be removed if the buffer approach works
                    entry->ycsb_req_acctype = ycsb_req->acctype;
                    entry->ycsb_req_key = ycsb_req->key;
                    entry->ycsb_req_val[0] = ycsb_req->value;

                    // Dirty code
                    ycsb_request * req_buff = (ycsb_request *) &entry->req_buffer;
                    req_buff->acctype = ycsb_req->acctype;
                    req_buff->key = ycsb_req->key;
                    req_buff->value = ycsb_req->value;

                    std::memset(entry->dep_vector, 0, 10);

                    // add entry into range/bucket queue
                    Array<exec_queue_entry> *mrange = exec_queues->get(idx);
                    // entry is a sturct, need to double check if this works
                    mrange->add(*entry);
                }
                // increment for next ransaction
                planner_txn_id++;
#endif

//                seq_man.process_txn(msg,get_thd_id(),0,0,0,0);
                // Don't free message yet
//                break;
//            case CALVIN_ACK:
//                // Ack from server
//                DEBUG("SEQ process_ack (%ld,%ld) from %ld\n",msg->get_txn_id(),msg->get_batch_id(),msg->get_return_id());
//                seq_man.process_ack(msg,get_thd_id());
                // Free message here
//                msg->release();



                break;
            }
            default:
                assert(false);
        }

        INC_STATS(_thd_id,mtx[32],get_sys_clock() - prof_starttime);
        prof_starttime = get_sys_clock();
    }
    DEBUG_Q("Planner_%ld: Total access cnt = %ld\n", _planner_id, total_access_cnt);
    for (uint64_t i = 0; i < g_thread_cnt; i++) {
        DEBUG_Q("Planner_%ld: Access cnt for bucket_%ld = %ld\n",_planner_id, i, mrange_cnts.get(i));
    }
    printf("FINISH %ld:%ld\n",_node_id,_thd_id);
    fflush(stdout);
    return FINISH;

}