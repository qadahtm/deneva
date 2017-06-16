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
#include <boost/heap/priority_queue.hpp>

void PlannerThread::setup() {
}

void SplitEntry::printEntries(uint64_t i, uint64_t planner_id) const{
//    if (children[0] != NULL){
//        children[0]->printEntries(i+1, planner_id);
//    }
//    else if (children[0] != NULL){
//        children[1]->printEntries(i+2, planner_id);
//    }
//    else {
    DEBUG_Q("Planner_%ld: mrange = %ld, exec_q size = %ld, from %ld to %ld\n", planner_id, i, exec_q->size(), range_start, range_end);
//    }
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

uint32_t PlannerThread::get_split(uint64_t key, uint32_t range_cnt, uint64_t range_start, uint64_t range_end) {
    uint64_t range_size = (range_end - range_start)/range_cnt;
    for (uint64_t i = 0; i < range_cnt; i++){
        if (key >= ((i*range_size)+range_start) && key < (((i+1)*range_size)+range_start)){
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
    uint64_t exec_queue_limit = planner_batch_size/g_thread_cnt;
    // max capcity, assume YCSB workload
//    uint64_t exec_queue_capacity = planner_batch_size*10;
    uint64_t exec_queue_capacity = 1024 * 32;
#if BATCHING_MODE == SIZE_BASED
    exec_queue_capacity = planner_batch_size * 10;
#endif

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

    // Pre-allocate execution queues to be used by planners and executors
    // this should eliminate the overhead of memory allocation
    uint64_t  exec_queue_pool_size = 10;
    for (uint64_t i = 0; i < g_thread_cnt; i++) {
        for (uint64_t j = 0; j < exec_queue_pool_size; j++){
            Array<exec_queue_entry> * exec_q = new Array<exec_queue_entry>();
            exec_q->init(exec_queue_capacity);
            while(!work_queue.exec_queue_free_list[i]->push(exec_q)){}
        }
    }


#if DEBUG_QUECC
    Array<uint64_t> mrange_cnts;
    mrange_cnts.init(g_thread_cnt);

    for (uint64_t i = 0; i < g_thread_cnt; i++) {
        mrange_cnts.add(0);
    }
    uint64_t total_msg_processed_cnt = 0;
    uint64_t total_access_cnt = 0;
#endif
    uint64_t batch_id = 0;
    uint64_t planner_txn_id = txn_prefix_planner_base;
    uint64_t batch_start_time = 0;
    uint64_t prof_starttime = 0;
    uint64_t txn_prof_starttime = 0;
    uint64_t plan_starttime = 0;
    bool force_batch_delivery = false;
    uint64_t idle_cnt = 0;


    while(!simulation->is_done()) {
        if (plan_starttime == 0 && simulation->is_warmup_done()){
            plan_starttime = get_sys_clock();
        }

        // dequeue for repective input_queue: there is an input queue for each planner
        // entries in the input queue is placed by the I/O thread
        // for now just dequeue and print notification
//        DEBUG_Q("Planner_%d is dequeuing\n", _planner_id);
        prof_starttime = get_sys_clock();
        msg = work_queue.plan_dequeue(_thd_id, _planner_id);
        INC_STATS(_thd_id, plan_queue_dequeue_time[_planner_id], get_sys_clock()-prof_starttime);

        if(!msg) {
            if(idle_starttime == 0){
                idle_starttime = get_sys_clock();
            }
            idle_cnt++;
            double idle_for =  ( (double) get_sys_clock() - idle_starttime);
            double batch_start_since = ( (double) get_sys_clock() - batch_start_time);
            if (idle_starttime > 0 && idle_cnt % (10 * MILLION) == 0){
                DEBUG_Q("Planner_%ld : we should force batch delivery with batch_cnt = %ld, total_idle_time = %f, idle_for=%f, batch_started_since=%f\n",
                        _planner_id,
                        batch_cnt,
                        stats._stats[_thd_id]->plan_idle_time[_planner_id]/BILLION,
                        idle_for/BILLION,
                        batch_start_since/BILLION)
            }
            // we have not recieved a transaction
                continue;
        }
        INC_STATS(_thd_id,plan_queue_deq_cnt[_planner_id],1);

        if (idle_starttime != 0){
            // plan_idle_time includes dequeue time, which should be subtracted from it
            INC_STATS(_thd_id,plan_idle_time[_planner_id],get_sys_clock() - idle_starttime);
            idle_starttime = 0;
        }

#if BATCHING_MODE == SIZE_BASED
        force_batch_delivery = (batch_cnt == planner_batch_size);
#else
    // Default to TIME_BASED
        force_batch_delivery = ((get_sys_clock() - batch_start_time) >= BATCH_COMP_TIMEOUT && batch_cnt > 0);
#endif

        if (force_batch_delivery){
            if (BATCHING_MODE == TIME_BASED) {
                INC_STATS(_thd_id, plan_time_batch_cnts[_planner_id], 1)
                force_batch_delivery = false;
            } else{
                INC_STATS(_thd_id, plan_size_batch_cnts[_planner_id], 1)
            }
//            DEBUG_Q("Batch complete\n")
            // a batch is ready to be delivered to the the execution threads.
            // we will have a batch queue for each of the planners and they are scanned in known order
            // by execution threads
            // We also have a batch queue for each of the executor that is mapped to bucket
            // for now, we will assign a bucket to each executor
            // All we need to do is to automically CAS the pointer to the address of the accumulated batch
            // and the execution threads who are spinning can start execution

            // Here major ranges have one-to-one mapping to worker threads
            prof_starttime = get_sys_clock();
            for (uint64_t i = 0; i < g_thread_cnt; i++){
//                DEBUG_Q("old value for ptr to  exec_q[%ld][%ld][%ld] is %ld\n", _planner_id, i, batch_id, exe_q_ptr);
//                DEBUG_Q("new value for ptr to  exec_q[%ld][%ld][%ld] is %ld\n", _planner_id, i, batch_id, (uint64_t) exec_queues->get(i));
                uint64_t slot_num = (batch_id % g_batch_map_length);

//                DEBUG_Q("Batch %ld is ready! setting map slot [%ld][%ld][%ld]\n", batch_id, i, _planner_id, slot_num);

                uint64_t expected = 0;
                Array<exec_queue_entry> * exec_q = exec_queues->get(i);
                // use batch_partition
                batch_partition * batch_part = (batch_partition *) mem_allocator.alloc(sizeof(batch_partition));
                batch_part->planner_id = _planner_id;
                batch_part->batch_id = batch_id;
                batch_part->exec_q = exec_q;
                batch_part->single_q = true;
                batch_part->batch_part_status.store(0);
                batch_part->exec_q_status.store(0);

                // Check if we need to split
                // 10 is the number of operations in each transaction, we are assuming YCSB here
                exec_queue_limit = (batch_cnt/g_thread_cnt) * 10 * EXECQ_CAP_FACTOR;

                if (exec_queue_limit < exec_q->size()){
                    DEBUG_Q("Planner_%ld : We need to split for range %ld, current size = %ld, limit = %ld,"
                                    " batch_cnt = %ld, planner_batch_size = %ld, batch_id = %ld"
                                    "\n",
                            _planner_id, i, exec_q->size(), exec_queue_limit, batch_cnt, planner_batch_size, batch_id);

                    batch_part->single_q = false;

                    boost::heap::priority_queue<SplitEntry, boost::heap::compare<SplitEntryCompareSize>> pq;

                    uint64_t split_rounds = 0;

                    SplitEntry * root = new SplitEntry();
                    root->exec_q = exec_q;
                    root->range_start = (i*bucket_size);
                    root->range_end = ((i+1)*bucket_size);
                    root->range_size = bucket_size;
                    SplitEntry * top_entry = root;

                    while (true){
                        //split current queue in half
//                        Array<exec_queue_entry> * cexec_q = exec_queues->get(i);
                        // Allocate memory for new exec_qs
                        Array<exec_queue_entry> ** nexec_qs = (Array<exec_queue_entry> **) mem_allocator.alloc(sizeof(Array<exec_queue_entry> *)*2);

                        if (work_queue.exec_queue_free_list[i]->pop(nexec_qs[0])){
                            nexec_qs[0]->clear();
                        }
                        else{
                            nexec_qs[0] = new Array<exec_queue_entry>();
                            nexec_qs[0]->init(exec_queue_capacity);
                        }

                        if (work_queue.exec_queue_free_list[i]->pop(nexec_qs[1])){
                            nexec_qs[1]->clear();
                        }
                        else{
                            nexec_qs[1] = new Array<exec_queue_entry>();
                            nexec_qs[1]->init(exec_queue_capacity);
                        }

                        //split current exec_q
                        for (uint64_t j = 0; j < top_entry->exec_q->size(); j++){
                            exec_queue_entry exec_qe = top_entry->exec_q->get(j);
                            ycsb_request *req = (ycsb_request *) &exec_qe.req_buffer;
//                        uint32_t get_split(uint64_t key, uint_32_t range_cnt, uint64_t range_start, uint64_t range_end);
                            uint32_t newrange = get_split(req->key, 2, top_entry->range_start, top_entry->range_end);
//                            min_range_cnts[newrange]++;
                            nexec_qs[newrange]->add(exec_qe);
                        }

                        //TODO(tq), use je_malloc instead of new?
                        SplitEntry * nsp_entries = new SplitEntry[2];
                        uint64_t range_size = ((top_entry->range_end - top_entry->range_start)/2);
                        nsp_entries[0].exec_q = nexec_qs[0];
                        nsp_entries[0].range_start = top_entry->range_start;
                        nsp_entries[0].range_end = range_size+top_entry->range_start;
                        nsp_entries[0].range_size = range_size;
                        nsp_entries[0].children[0] = NULL;
                        nsp_entries[0].children[1] = NULL;

                        nsp_entries[1].exec_q = nexec_qs[1];
                        nsp_entries[1].range_start = range_size+top_entry->range_start;
                        nsp_entries[1].range_end = top_entry->range_end;
                        nsp_entries[1].range_size = range_size;
                        nsp_entries[1].children[0] = NULL;
                        nsp_entries[1].children[1] = NULL;


                        pq.push(nsp_entries[0]);
                        pq.push(nsp_entries[1]);

                        // update top entry with new ranges
                        top_entry->children[0] = &nsp_entries[0];
                        top_entry->children[1] = &nsp_entries[1];
//                        top_entry->exec_q->release();

                        DEBUG_Q("Planner_%ld : Split exec_q of mrange(%ld), pq.size = %ld, split rounds = %ld, into 2 subranges, "
                                        "parent : size = %ld, rs = %ld, re = %ld, "
                                        "child_0 : size = %ld, rs = %ld, re = %ld, "
                                        "child_1 : size = %ld, rs = %ld, re = %ld"
                                        "\n",
                                _planner_id, i, pq.size(), split_rounds,
                                top_entry->exec_q->size(), top_entry->range_start, top_entry->range_end,
                                top_entry->children[0]->exec_q->size(), top_entry->children[0]->range_start, top_entry->children[0]->range_end,
                                top_entry->children[1]->exec_q->size(), top_entry->children[1]->range_start, top_entry->children[1]->range_end
                        );

                        // Recucle execution queue
//                        if (split_rounds != 0){
                            // skip the root, for now
                            while(!work_queue.exec_queue_free_list[i]->push(top_entry->exec_q)) {};
//                        }
                        top_entry->exec_q = NULL;

                        if(pq.top().exec_q->size() <= exec_queue_limit){

                            break;
                        }
                        else {
                            split_rounds++;
                            top_entry = new SplitEntry();
                            top_entry->exec_q = pq.top().exec_q;
                            top_entry->range_start = pq.top().range_start;
                            top_entry->range_end = pq.top().range_end;
                            top_entry->children[0] = pq.top().children[0];
                            top_entry->children[1] = pq.top().children[1];

                            pq.pop();
                        }

                    }

                    // Visualize the exec_queues
//                    DEBUG_Q("Planner_%ld : Visualizing splits of mrange %ld at batch_id: %ld\n", _planner_id, i, batch_id);
//                    boost::heap::priority_queue<SplitEntry, boost::heap::compare<SplitEntryCompareStartRange>> sorted;
//                    while (!pq.empty()){
//                        sorted.push(pq.top());
//                        pq.pop();
//                    }
//                    while (!sorted.empty()){
//                        sorted.top().printEntries(i, _planner_id);;
//                        sorted.pop();
//                    }

                    // Allocate memory for exec_qs
                    batch_part->sub_exec_qs_cnt = pq.size();
                    batch_part->exec_qs = (Array<exec_queue_entry> **) mem_allocator.alloc(
                            sizeof(Array<exec_queue_entry> *)*batch_part->sub_exec_qs_cnt);
                    batch_part->exec_qs_status = (atomic<uint64_t> *) mem_allocator.alloc(
                            sizeof(uint64_t)*batch_part->sub_exec_qs_cnt);

                    for (uint64_t j = 0; j < batch_part->sub_exec_qs_cnt; j++){
                        batch_part->exec_qs[j] = pq.top().exec_q;
                        pq.pop();
                        batch_part->exec_qs_status[j].store(AVAILABLE);
//                        uint64_t tmp_pq_size = pq.size();
//                        assert((tmp_pq_size-j-1) == (batch_part->sub_exec_qs_cnt-j-2));
                    }


//                    uint64_t j = 0;
//                    for (boost::heap::priority_queue<SplitEntry, boost::heap::compare<SplitEntryCompare>>::iterator it = pq.begin(); it != pq.end(); ++it){
//                        DEBUG_Q("Planner_%ld : of mrange(%ld), subrange_cnt(%ld) = %ld"
//                                        "\n",
//                                _planner_id, i,j, ((SplitEntry *) *it).exec_q->size()
//                        );
//                        j++;
//                    }
//                    mem_allocator.free(min_range_cnts, sizeof(uint64_t)*2);
                }


//                uint64_t desired = (uint64_t) exec_q;
                uint64_t desired = (uint64_t) batch_part;
                //TODO(tq): make sure we need these memory fences
                std::atomic_thread_fence(std::memory_order_seq_cst);
                while(work_queue.batch_map[slot_num][i][_planner_id].load() != 0 && !simulation->is_done()) {
                    if(idle_starttime == 0){
                        idle_starttime = get_sys_clock();
                    }
                    DEBUG_Q("SPIN!!! for batch %ld  Completed a lap up to map slot [%ld][%ld][%ld]\n", batch_id, slot_num, i, _planner_id);
                }

                // include idle time from spinning above
                if (idle_starttime != 0){
                    INC_STATS(_thd_id,plan_idle_time[_planner_id],get_sys_clock() - idle_starttime);
                    idle_starttime = 0;
                }
                while(!work_queue.batch_map[slot_num][i][_planner_id].compare_exchange_strong(
                        expected, desired)){
                    // this should not happen after spinning
                    M_ASSERT_V(false, "For batch %ld : failing to SET map slot [%ld][%ld][%ld]\n", batch_id, slot_num, i, _planner_id);
                }
                std::atomic_thread_fence(std::memory_order_seq_cst);
//                DEBUG_Q("Planner_%ld :Batch_%ld for range_%ld ready! b_slot = %ld\n", _planner_id, batch_id, i, slot_num);
            }

            // reset execution queues for the new batch
            prof_starttime = get_sys_clock();
            exec_queues->clear();
            for (uint64_t i = 0; i < g_thread_cnt; i++) {
                Array<exec_queue_entry> * exec_q = NULL;
                if (work_queue.exec_queue_free_list[i]->pop(exec_q)){
                    exec_q->clear();
//                    DEBUG_Q("Planner_%ld : Reusing exec_q\n", _planner_id);
                    INC_STATS(_thd_id, plan_reuse_exec_queue_cnt[_planner_id], 1);
                }
                else{
                    exec_q = new Array<exec_queue_entry>();
                    exec_q->init(exec_queue_capacity);
//                    DEBUG_Q("Planner_%ld: Allocating new exec_q\n", _planner_id);
                    INC_STATS(_thd_id, plan_alloc_exec_queue_cnt[_planner_id], 1);
                }
                exec_queues->add(exec_q);
            }
            INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock()-prof_starttime);
            INC_STATS(_thd_id, plan_batch_cnts[_planner_id], 1);
            INC_STATS(_thd_id, plan_batch_process_time[_planner_id], get_sys_clock() - batch_start_time);

            batch_id++;
            batch_cnt = 0;
            batch_start_time = 0;
            INC_STATS(_thd_id, plan_batch_delivery_time[_planner_id], get_sys_clock()-prof_starttime);
        }

        // we have got a message, which is a transaction
        batch_cnt++;
        if (batch_start_time == 0){
            batch_start_time = get_sys_clock();
        }

        switch (msg->get_rtype()) {
            case CL_QRY: {
                // Query from client
//                DEBUG_Q("Planner_%d planning txn %ld\n", _planner_id,msg->txn_id);


#if WORKLOAD == YCSB
                txn_prof_starttime = get_sys_clock();
                // create transaction context
                // TODO(tq): here also we are dynamically allocting memory, we should use a pool recycle
                prof_starttime = get_sys_clock();
                transaction_context *tctx = NULL;
//                if (!work_queue.txn_ctx_free_list[_planner_id]->pop(tctx)){
                tctx = (transaction_context *) mem_allocator.alloc(sizeof(transaction_context));
//                }
                INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock() - prof_starttime);
                tctx->txn_id = planner_txn_id;
//                tctx->completion_cnt = 0;
                tctx->completion_cnt.store(0);
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
                    INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock() - prof_starttime);
                    Array<exec_queue_entry> *mrange = exec_queues->get(idx);
                    // increment of batch mrange to use the next entry slot
                    entry->txn_id = planner_txn_id;
                    entry->txn_ctx = tctx;
                    entry->batch_id = batch_id;
#if !SERVER_GENERATE_QUERIES
                    assert(msg->return_node_id != g_node_id);
#endif
                    entry->return_node_id = msg->return_node_id;

                    // Dirty code
                    ycsb_request * req_buff = (ycsb_request *) &entry->req_buffer;
                    req_buff->acctype = ycsb_req->acctype;
                    req_buff->key = ycsb_req->key;
                    req_buff->value = ycsb_req->value;

                    // add entry into range/bucket queue
                    // entry is a sturct, need to double check if this works
                    mrange->add(*entry);
                    prof_starttime = get_sys_clock();
                    mem_allocator.free(entry, sizeof(exec_queue_entry));
                    INC_STATS(_thd_id, plan_mem_alloc_time[_planner_id], get_sys_clock() - prof_starttime);
                }

#if DEBUG_QUECC
                total_msg_processed_cnt++;
#endif
                INC_STATS(_thd_id,plan_txn_process_time[_planner_id], get_sys_clock() - txn_prof_starttime);
                // Free message, as there is no need for it anymore
                msg->release();
                // increment for next ransaction
                planner_txn_id++;
#endif
                break;
            }
            default:
                assert(false);
        }

    }
    INC_STATS(_thd_id, plan_total_time[_planner_id], get_sys_clock()-plan_starttime);
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