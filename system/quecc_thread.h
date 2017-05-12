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
    bool dep_vector[10][10];
    uint64_t keys[10];
    char vals[10000];

};

struct exec_queue_entry {
//    Message * msg;
    uint64_t batch_id;
    uint64_t planner_id;
    uint64_t mrange_id;
    uint64_t return_node_id;
    transaction_context * tctx;
//    RemReqType rtype;
    uint64_t starttime;
#if WORKLOAD == YCSB
    // Static allocation for request
    // Thiw should work after casting it to ycsb_request
    char req_buffer[1016];

    access_t ycsb_req_acctype;
    uint64_t ycsb_req_key;
    // This is different from ycsb_request spects where value is only 1 byte
    // here value consist of 10 fields and each is 100 bytes
    char ycsb_req_val[1000];
    // Used to capture dependencies among data items within a transaction
    uint64_t ycsb_req_index;
    char dep_vector[10];

#endif
};

struct subrange_entry {
    Array<exec_queue_entry* > * exec_queue;
    // for affinity
    Array<subrange_entry* > * subranges;
};

struct planner_entry {
    uint32_t planner_id;
    Array<subrange_entry* > * subranges;
};

struct mrange_entry {
    uint32_t mrange_id;
    Array<planner_entry *> * planner_queues;
};


struct batch_queue_entry {
    uint64_t batch_id;
    uint32_t planner_id;
    uint64_t starttime;
    Array<mrange_entry* > * mranges;
};

class ExecutionThread : public Thread {
public:
    RC run();
    void setup();
    uint32_t _executor_id;
private:
    TxnManager * m_txn;
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
    bool is_batch_ready();
    uint64_t last_batchtime;
};

#endif //_QUECC_THREAD_H
