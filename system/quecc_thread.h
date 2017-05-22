//
// Created by Thamir Qadah on 4/20/17.
//

#ifndef _QUECC_THREAD_H
#define _QUECC_THREAD_H

#include "global.h"
#include "thread.h"
#include "message.h"
#include "array.h"

class Workload;

/**
 * TODO(tq): makes this more dynamic in terms of the number of fields and support TPCC
 * We will hardcode YCSB for now
 * Default YCSB specs:
 * - Each key is 8 bytes uint64_t
 * - Each value is 100 bytes
 * - We have 10 KV pairs
 * So for a single YCSB transaction, we have ~ 8*10 + 10*1000 + 8
 */
struct transaction_context {
    uint64_t txn_id;
//    uint64_t completion_cnt;
    boost::atomic<uint64_t> completion_cnt;
    uint64_t client_startts;
    uint64_t batch_id;
//    bool dep_vector[10][10];
//    uint64_t keys[10];
//    char vals[10000];
};

struct exec_queue_entry {
    transaction_context * txn_ctx;
    uint64_t txn_id;
//    uint64_t req_id;
//    uint64_t planner_id;
    uint64_t batch_id;
#if WORKLOAD == YCSB
    // Static allocation for request
    // Thiw should work after casting it to ycsb_request
    char req_buffer[1016];

//    access_t ycsb_req_acctype;
//    uint64_t ycsb_req_key;
    // This is different from ycsb_request spects where value is only 1 byte
    // here value consist of 10 fields and each is 100 bytes
//    char ycsb_req_val[1000];
    // Used to capture dependencies among data items within a transaction
//    uint64_t ycsb_req_index;
//    char dep_vector[10];
    uint64_t return_node_id;
#endif
};

class PlannerThread : public Thread {
public:
    RC run();
    void setup();
    uint64_t _planner_id;
    uint32_t get_bucket(uint64_t key);
#if WORKLOAD == YCSB
    // create a bucket for each worker thread
    uint64_t bucket_size = g_synth_table_size / g_thread_cnt;

#endif
private:
    uint64_t last_batchtime;
};

#endif //_QUECC_THREAD_H
