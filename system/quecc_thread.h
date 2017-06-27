//
// Created by Thamir Qadah on 4/20/17.
//

#ifndef _QUECC_THREAD_H
#define _QUECC_THREAD_H

#include "global.h"
#include "thread.h"
#include "message.h"
#include "array.h"
#include <boost/heap/priority_queue.hpp>
//TQ: we will use standard lib version for now. We can use optimized implementation later
#include <unordered_map>
#include <vector>
#include <boost/lockfree/spsc_queue.hpp>
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


struct priority_group{
    uint64_t planner_id;
    uint64_t batch_id;
    uint64_t batch_txn_cnt;
    transaction_context * txn_ctxs;
    hash_table_t * txn_dep_graph;
    uint64_t batch_starting_txn_id;
};

struct batch_partition{
    uint64_t planner_id;
    uint64_t batch_id;
    atomic<uint64_t> batch_part_status;
    priority_group * planner_pg;

    // A small optimization in case there is only a single exec_q
    // This optimization will avoid a cache miss
    bool single_q;
    Array<exec_queue_entry> * exec_q;
    atomic<uint64_t> exec_q_status;

    // Info. related to having multiple exec. queues
    uint64_t sub_exec_qs_cnt;
    atomic<uint64_t> exec_qs_comp_cnt;
//    Array<exec_queue_entry> ** exec_qs;
    Array<Array<exec_queue_entry> *> * exec_qs;
    atomic<uint64_t> * exec_qs_status;
};



struct ArrayCompare
{
    bool operator()(const Array<exec_queue_entry> &a1, const Array<exec_queue_entry> &a2) const
    {
        return a1.size() < a2.size();
    }
};

struct assign_entry{
    Array<Array<exec_queue_entry> *> * exec_qs;
    uint64_t curr_sum;
    uint64_t exec_thd_id;


};

inline void assign_entry_get_or_create(assign_entry *&a_entry, boost::lockfree::spsc_queue<assign_entry *> * assign_entry_free_list);
void assign_entry_init(assign_entry * &a_entry, uint64_t thd_id);
void assign_entry_add(assign_entry * a_entry, Array<exec_queue_entry> * exec_q);
void assign_entry_clear(assign_entry * &a_entry);
void assign_entry_release(assign_entry * &a_entry);




struct AssignEntryCompareSum{
    bool operator()(const uint64_t e1, const uint64_t e2) const
    {
        return ((assign_entry*)e1)->curr_sum > ((assign_entry*)e2)->curr_sum;
    }
};

//class SplitEntry {
//public:
//    Array<exec_queue_entry> * exec_q;
//    void printEntries(uint64_t i, uint64_t planner_id) const;
//    uint64_t range_start;
//    uint64_t range_end;
//    uint64_t range_size;
//
//    SplitEntry();
//    ~SplitEntry();
//};
//
//struct SplitEntryCompareSize{
//    bool operator()(const SplitEntry &e1, const SplitEntry &e2) const
//    {
//        return e1.exec_q->size() < e2.exec_q->size();
//    }
//};
//struct SplitEntryCompareStartRange{
//    bool operator()(const SplitEntry &e1, const SplitEntry &e2) const
//    {
//        return e1.range_start < e2.range_start;
//    }
//};

struct split_entry {
    Array<exec_queue_entry> * exec_q;
    void printEntries(uint64_t i, uint64_t planner_id) const;
    uint64_t range_start;
    uint64_t range_end;
    uint64_t range_size;
};

struct SplitEntryCompareSize{
    bool operator()(const uint64_t e1, const uint64_t e2) const
    {
        return ((split_entry*)e1)->exec_q->size() < ((split_entry*)e2)->exec_q->size();
    }
};

struct SplitEntryCompareStartRange{
    bool operator()(const uint64_t e1, const uint64_t e2) const
    {
        return ((split_entry*)e1)->range_start > ((split_entry*)e2)->range_start;
    }
};

void split_entry_get_or_create(split_entry *&s_entry, boost::lockfree::spsc_queue<split_entry *> *split_entry_free_list);

void split_entry_print(split_entry * ptr, uint64_t planner_id);

typedef boost::heap::priority_queue<uint64_t, boost::heap::compare<SplitEntryCompareSize>> split_max_heap_t;
typedef boost::heap::priority_queue<uint64_t, boost::heap::compare<SplitEntryCompareStartRange>> split_min_heap_t;
typedef boost::heap::priority_queue<uint64_t, boost::heap::compare<AssignEntryCompareSum>> assign_ptr_min_heap_t;

class PlannerThread : public Thread {
public:
    RC run();
    void setup();
    uint64_t _planner_id;
    uint32_t get_bucket(uint64_t key);
    uint32_t get_split(uint64_t key, uint32_t range_cnt, uint64_t range_start, uint64_t range_end);
    uint32_t get_split(uint64_t key, Array<uint64_t> * ranges);
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
#if BUILD_TXN_DEPS
    hash_table_t access_table;
    hash_table_t * txn_dep_graph;
#endif
};

class CommitThread : public Thread {
public:
    RC run();
    void setup();
};

#endif //_QUECC_THREAD_H
