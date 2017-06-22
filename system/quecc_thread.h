//
// Created by Thamir Qadah on 4/20/17.
//

#ifndef _QUECC_THREAD_H
#define _QUECC_THREAD_H

#include "global.h"
#include "thread.h"
#include "message.h"
#include "array.h"
//TQ: we will use standard lib version for now. We can use optimized implementation later
#include <unordered_map>
#include <vector>

class Workload;

typedef std::unordered_map<uint64_t, std::vector<uint64_t> *> hash_table_t;
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
    uint8_t txn_state;
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
//    uint64_t ycsb_req_index;
    uint64_t return_node_id;
#endif
};

struct batch_partition{
    uint64_t planner_id;
    uint64_t batch_id;
    atomic<uint64_t> batch_part_status;

    // A small optimization in case there is only a single exec_q
    // This optimization will avoid a cache miss
    bool single_q;
    Array<exec_queue_entry> * exec_q;
    atomic<uint64_t> exec_q_status;

    // Info. related to having multiple exec. queues
    uint64_t sub_exec_qs_cnt;
    atomic<uint64_t> exec_qs_comp_cnt;
    Array<exec_queue_entry> ** exec_qs;
    atomic<uint64_t> * exec_qs_status;
};


struct priority_group{
    uint64_t planner_id;
    uint64_t batch_id;
    uint64_t batch_txn_cnt;
    transaction_context * txn_ctxs;
    hash_table_t * txn_dep_graph;
    uint64_t batch_starting_txn_id;
};

struct ArrayCompare
{
    bool operator()(const Array<exec_queue_entry> &a1, const Array<exec_queue_entry> &a2) const
    {
        return a1.size() < a2.size();
    }
};

class SplitEntry {
public:
    Array<exec_queue_entry> * exec_q;
    SplitEntry * children[2];
    void printEntries(uint64_t i, uint64_t planner_id) const;
    uint64_t range_start;
    uint64_t range_end;
    uint64_t range_size;
};

struct SplitEntryCompareSize{
    bool operator()(const SplitEntry &e1, const SplitEntry &e2) const
    {
        return e1.exec_q->size() < e2.exec_q->size();
    }
};

struct SplitEntryCompareStartRange{
    bool operator()(const SplitEntry &e1, const SplitEntry &e2) const
    {
        return e1.range_start < e2.range_start;
    }
};

class PlannerThread : public Thread {
public:
    RC run();
    void setup();
    uint64_t _planner_id;
    uint32_t get_bucket(uint64_t key);
    uint32_t get_split(uint64_t key, uint32_t range_cnt, uint64_t range_start, uint64_t range_end);
#if WORKLOAD == YCSB
    // create a bucket for each worker thread
    uint64_t bucket_size = g_synth_table_size / g_thread_cnt;

#endif
private:
    uint64_t last_batchtime;

    uint64_t batch_id = 0;
    uint64_t planner_txn_id = 0;
//    uint64_t batch_starting_txn_id = txn_prefix_planner_base;
    uint64_t batch_start_time = 0;
    uint64_t prof_starttime = 0;
    uint64_t txn_prof_starttime = 0;
    uint64_t plan_starttime = 0;
    bool force_batch_delivery = false;
    uint64_t idle_cnt = 0;
    uint64_t expected = 0;
    uint64_t desired = 0;
    uint64_t slot_num = 0;

    hash_table_t access_table;
    hash_table_t * txn_dep_graph;

};

class CommitThread : public Thread {
public:
    RC run();
    void setup();
};

#endif //_QUECC_THREAD_H
