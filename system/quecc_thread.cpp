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

void PlannerThread::setup() {
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
//    uint64_t prof_starttime = 0;
    uint64_t batch_cnt = 0;

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

// implementtion using array class
    Array<Array<exec_queue_entry> *> * exec_queues = new Array<Array<exec_queue_entry> *>();
    exec_queues->init(g_thread_cnt);
    for (uint64_t i = 0; i < g_thread_cnt; i++) {
        Array<exec_queue_entry> * exec_q = new Array<exec_queue_entry>();
        exec_q->init(exec_queue_capacity);
        exec_queues->add(exec_q);
    }

//    exec_queue_entry ** exec_queues = (exec_queue_entry **) mem_allocator.alloc(sizeof(exec_queue_entry *)*g_thread_cnt);
//    uint64_t * b_mrange_cnts = (uint64_t *) mem_allocator.alloc(sizeof(uint64_t)*g_thread_cnt);
//    for (uint64_t i = 0; i < g_thread_cnt; i++) {
//        b_mrange_cnts[i] = 0;
//        exec_queue_entry * exec_q = (exec_queue_entry *) mem_allocator.alloc(sizeof(exec_queue_entry)*exec_queue_capacity);
//        exec_queues[i] = exec_q;
//    }

#if DEBUG_QUECC
    Array<uint64_t> mrange_cnts;
    mrange_cnts.init(g_thread_cnt);

    for (uint64_t i = 0; i < g_thread_cnt; i++) {
        mrange_cnts.add(0);
    }
    uint64_t total_msg_processed_cnt = 0;
    uint64_t total_access_cnt = 0;
#endif
    uint64_t  batch_id = 0;
    uint64_t planner_txn_id = txn_prefix_planner_base;
    uint64_t batch_start_time = 0;
    uint64_t prof_starttime = 0;
    uint64_t txn_prof_starttime = 0;

    while(!simulation->is_done()) {

        // dequeue for repective input_queue: there is an input queue for each planner
        // entries in the input queue is placed by the I/O thread
        // for now just dequeue and print notification
//        DEBUG_Q("Planner_%d is dequeuing\n", _planner_id);
        msg = work_queue.plan_dequeue(_thd_id, _planner_id);


        if(!msg) {
            if(idle_starttime == 0)
                idle_starttime = get_sys_clock();
            continue;
        }

        INC_STATS(_thd_id,plan_idle_time,get_sys_clock() - idle_starttime);


        // we have got a message, which is a transaction
        batch_cnt++;
        if (batch_start_time == 0){
            batch_start_time = get_sys_clock();
        }

        if (batch_cnt == planner_batch_size){ // || (get_sys_clock() - batch_start_time) >= BATCH_COMP_TIMEOUT){
//            DEBUG_Q("Batch complete\n")
            // a batch is ready to be delivered to the the execution threads.
            // we will have a batch queue for each of the planners and they are scanned in known order
            // by execution threads
            // We also have a batch queue for each of the executor that is mapped to bucket
            // for now, we will assign a bucket to each executor
            // All we need to do is to automically CAS the pointer to the address of the accumulated batch
            // and the execution threads who are spinning can start execution

            // Here major ranges have one-to-one mapping to worker threads
            for (uint64_t i = 0; i < g_thread_cnt; i++){
//                DEBUG_Q("old value for ptr to  exec_q[%ld][%ld][%ld] is %ld\n", _planner_id, i, batch_id, exe_q_ptr);
//                DEBUG_Q("new value for ptr to  exec_q[%ld][%ld][%ld] is %ld\n", _planner_id, i, batch_id, (uint64_t) exec_queues->get(i));
                uint64_t slot_num = (batch_id % g_batch_map_length);
//                while (!ATOM_CAS(work_queue.batch_map[_planner_id][i][slot_num], 0, exec_queues->get(i))) {}
//                DEBUG_Q("Batch %ld is ready! setting map slot [%ld][%ld][%ld]\n", batch_id, i, _planner_id, slot_num);
                Array<exec_queue_entry> * expected = (Array<exec_queue_entry> *) 0;
                while(!work_queue.batch_map[slot_num][i][_planner_id].compare_exchange_strong(
                        expected, exec_queues->get(i))){
                    DEBUG_Q("For batch %ld : failing to SET map slot [%ld][%ld][%ld]\n", batch_id, i, _planner_id, slot_num);
                }

//                atomic_thread_fence(std::memory_order_release);
                DEBUG_Q("Planner_%ld :Batch_%ld for range_%ld ready! b_slot = %ld\n", _planner_id, batch_id, i, slot_num);
            }


            // reset execution queues for the new batch
            // TODO(tq): we are allocating and initializing at every batch, can we use a memory pool and recycle

            // Array class implementation
            prof_starttime = get_sys_clock();
            exec_queues = new Array<Array<exec_queue_entry> *>();
            exec_queues->init(g_thread_cnt);
            for (uint64_t i = 0; i < g_thread_cnt; i++) {
                Array<exec_queue_entry> * exec_q = new Array<exec_queue_entry>();
                exec_q->init(exec_queue_capacity);
                exec_queues->add(exec_q);
            }
            INC_STATS(_thd_id, plan_mem_alloc_time, get_sys_clock()-prof_starttime);
// pointer-based implementation
//            exec_queues = (exec_queue_entry **) mem_allocator.alloc(sizeof(exec_queue_entry *)*g_thread_cnt);

//            uint64_t * b_mrange_cnts = (uint64_t *) mem_allocator.alloc(sizeof(uint64_t)*g_thread_cnt);
//            for (uint64_t i = 0; i < g_thread_cnt; i++) {
//                b_mrange_cnts[i] = 0;
//                exec_queue_entry * exec_q = (exec_queue_entry *) mem_allocator.alloc(sizeof(exec_queue_entry)*exec_queue_capacity);
//                exec_queues[i] = exec_q;
//            }

            INC_STATS(_thd_id, plan_batch_cnts[_planner_id], 1);
            INC_STATS(_thd_id, plan_batch_process_time, get_sys_clock() - batch_start_time);

            batch_id++;
            batch_cnt = 0;
            batch_start_time = 0;
        }

        switch (msg->get_rtype()) {
            case CL_QRY: {
                // Query from client
//                DEBUG_Q("Planner_%d planning txn %ld\n", _planner_id,msg->txn_id);


#if WORKLOAD == YCSB
                txn_prof_starttime = get_sys_clock();
                // create transaction context
                // TODO(tq): here also we are dynamically allocting memory, we should use a pool recycle
                transaction_context *tctx = (transaction_context *) mem_allocator.alloc(sizeof(transaction_context));
                tctx->txn_id = planner_txn_id;
                tctx->completion_cnt = 0;
                tctx->client_startts = ((ClientQueryMessage *) msg)->client_startts;
                tctx->batch_id = batch_id;

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

                    // TODO(tq): move to statitic module
#if DEBUG_QUECC
                    uint64_t nval = mrange_cnts.get(idx) + 1;
                    mrange_cnts.set(idx, nval);
                    total_access_cnt++;
#endif

                    // create execution entry, for now it will contain only one request
                    // we dont need to allocate memory here
                    prof_starttime = get_sys_clock();
                    exec_queue_entry *entry = (exec_queue_entry *) mem_allocator.alloc(sizeof(exec_queue_entry));
                    INC_STATS(_thd_id, plan_mem_alloc_time, get_sys_clock() - prof_starttime);
                    Array<exec_queue_entry> *mrange = exec_queues->get(idx);
                    // increment of batch mrange to use the next entry slot
//                    Array<exec_queue_entry> mmrange = *(exec_queues->get(idx));
//                    exec_queue_entry * entry = mmrange.add_ptr();

                    entry->txn_id = planner_txn_id;
                    entry->txn_ctx = tctx;
                    entry->req_id = j;
                    entry->batch_id = batch_id;
                    assert(msg->return_node_id != g_node_id);
                    entry->return_node_id = msg->return_node_id;

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

//                    std::memset(entry->dep_vector, 0, 10);

                    // add entry into range/bucket queue
                    // entry is a sturct, need to double check if this works
                    mrange->add(*entry);
                    mem_allocator.free(entry, sizeof(exec_queue_entry));
                }

#if DEBUG_QUECC
                total_msg_processed_cnt++;
#endif
                // Free message, as there is no need for it anymore
                msg->release();
                // increment for next ransaction
                planner_txn_id++;
#endif

                INC_STATS(_thd_id,plan_txn_process_time, get_sys_clock() - txn_prof_starttime);
                break;
            }
            default:
                assert(false);
        }

    }
#if DEBUG_QUECC
    DEBUG_Q("Planner_%ld: Total access cnt = %ld, and processed %ld msgs\n", _planner_id, total_access_cnt, total_msg_processed_cnt);
    for (uint64_t i = 0; i < g_thread_cnt; i++) {
        DEBUG_Q("Planner_%ld: Access cnt for bucket_%ld = %ld\n",_planner_id, i, mrange_cnts.get(i));
    }
#endif
    printf("FINISH %ld:%ld\n",_node_id,_thd_id);
    fflush(stdout);
    return FINISH;

}