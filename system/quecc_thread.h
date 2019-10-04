//
// Created by Thamir Qadah on 4/20/17.
//

#ifndef _QUECC_THREAD_H
#define _QUECC_THREAD_H

#include "global.h"
#include "thread.h"
#include "message.h"
#include "array.h"
#include "spinlock.h"
#include <boost/heap/priority_queue.hpp>
//TQ: we will use standard lib version for now. We can use optimized implementation later
#include <unordered_map>
#include <vector>
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <boost/random.hpp>
#include <boost/unordered_map.hpp>
#include <row.h>
#include "ycsb_query.h"

#if CC_ALG == QUECC
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
    uint64_t txn_id; // 8 bytes
#if PROFILE_EXEC_TIMING
    uint64_t starttime; // 8bytes
#endif
// this need to be reset on reuse
    // 8bytes
    volatile atomic<uint64_t> completion_cnt; // used at execution time to track operations that have completed
    // 8bytes
    volatile atomic<uint64_t> txn_comp_cnt; // used during planning to track the number of operations to be executed
    uint64_t commit_dep_per_node[NODE_CNT];
    uint64_t active_nodes[NODE_CNT];
#if EXEC_BUILD_TXN_DEPS
    // 8bytes
    volatile atomic<int64_t> commit_dep_cnt; // used during execution to track the number of dependent transactions
    // 8bytes
    std::vector<transaction_context*> * commit_deps;
    spinlock * depslock;
    bool should_abort;
    // prev. transaction id assocated with each data item accessed by this transaction
#if WORKLOAD == YCSB
    uint64_t prev_tid[REQ_PER_QUERY];
#else
    uint64_t prev_tid[MAX_ROW_PER_TXN];
#endif
#endif
#if !SERVER_GENERATE_QUERIES
    uint64_t client_startts;
    uint64_t return_node_id; //8
#endif
#if PARALLEL_COMMIT
    // 8 bytes
    volatile atomic<uint64_t> txn_state;
#else
    uint64_t txn_state;
#endif

#if WORKLOAD == TPCC
    volatile atomic<int64_t> o_id;
#if SINGLE_NODE
    double h_amount;
    uint64_t w_id;
    uint64_t d_id;
    uint64_t c_id;
    uint64_t c_w_id;
    uint64_t c_d_id;
    bool remote;
    uint64_t  ol_cnt;
    uint64_t  o_entry_d;
    uint64_t ol_quantity;
    uint64_t ol_i_id;
    uint64_t ol_supply_w_id;
    uint64_t  ol_number;
//    uint64_t ol_amount;
#endif
#endif

#if ROLL_BACK && ROW_ACCESS_TRACKING
#if ROW_ACCESS_IN_CTX
    bool undo_buffer_inialized;
    char * undo_buffer_data;
#if WORKLOAD == YCSB
    row_t * orig_rows[REQ_PER_QUERY];
    access_t a_types[REQ_PER_QUERY];
#else
    row_t * orig_rows[MAX_ROW_PER_TXN];
    access_t a_types[MAX_ROW_PER_TXN];;
#endif
#else
    // 8bytes
    spinlock * access_lock = NULL;
    // 8 bytes
    Array<Access*> * accesses;
#endif
#endif
} __attribute__((aligned));

#if TDG_ENTRY_TYPE == VECTOR_ENTRY
typedef boost::unordered::unordered_map<uint64_t, std::vector<uint64_t> *> hash_table_t;
#elif TDG_ENTRY_TYPE == ARRAY_ENTRY
typedef boost::unordered::unordered_map<uint64_t, Array<uint64_t> *> hash_table_t;
typedef boost::unordered::unordered_map<uint64_t, Array<transaction_context *> *> hash_table_tctx_t;
// Hashtable with size of buffers
typedef boost::unordered::unordered_map<uint64_t, boost::lockfree::queue<char *> *> row_data_pool_t;
#endif

enum tpcc_txn_frag_t{
    TPCC_PAYMENT_UPDATE_W=0,
    TPCC_PAYMENT_UPDATE_D, //1
    TPCC_PAYMENT_UPDATE_C,//2
    TPCC_PAYMENT_INSERT_H,//3
    TPCC_NEWORDER_READ_W,//4
    TPCC_NEWORDER_READ_C,//5
    TPCC_NEWORDER_UPDATE_D,//6
    TPCC_NEWORDER_INSERT_O,//7
    TPCC_NEWORDER_INSERT_NO,//8
    TPCC_NEWORDER_READ_I,//9
    TPCC_NEWORDER_UPDATE_S,//10
    TPCC_NEWORDER_INSERT_OL//11
};

struct exec_queue_entry {
    transaction_context * txn_ctx; // 8
//#if SINGLE_NODE
    row_t * row; // 8 bytes
//#endif
    uint64_t txn_id; //8
    uint64_t batch_id;
#if WORKLOAD == YCSB
     // 8 bytes for access_type
    // 8 bytes for key
    // 1 byte for value
//    char req_buffer[17];
    ycsb_request req;
#elif WORKLOAD == TPCC
    tpcc_txn_frag_t type; // 4 bytes
    uint64_t rid; // 8 bytes

#if SINGLE_NODE
    char padding[24]; // 64-(8+8+8+4+8+4)
#else
    uint64_t txn_idx; //8
    uint64_t planner_id;

//FIXME(tq): reduce the size of the entry (e.g., use union)
    // common txn input for both payment & new-order
    uint64_t w_id; // also used for d_w_id
    uint64_t d_w_id;
    uint64_t c_w_id;

    uint64_t d_id;
    uint64_t c_d_id;

    uint64_t c_id;

    double h_amount;
    char c_last[LASTNAME_LEN];
    bool by_last_name;

    //for neworder
//    bool remote;
    uint64_t  ol_cnt;
    uint64_t  o_entry_d;
    uint64_t ol_quantity;
    uint64_t ol_i_id;
    uint64_t ol_supply_w_id;
    uint64_t  ol_number;
    uint64_t ol_amount;
#endif
#endif
    int dep_nodes[NODE_CNT];
//#if ROW_ACCESS_IN_CTX
    uint64_t req_idx; // 8 bytes
//#endif
} __attribute__((aligned));

struct eq_et_meta_t {
    uint64_t et_id;
    bool read_only;
    Array<exec_queue_entry> * exec_q;
} __attribute__((aligned));;

typedef unordered_map<uint64_t, vector<eq_et_meta_t *> *> EQMap;

struct EQSizeLess{
    bool operator()(const Array<exec_queue_entry> *  e1, const Array<exec_queue_entry> *  e2) const
    {
        return e1->size() < e2->size();
    }
};
typedef boost::heap::priority_queue<Array<exec_queue_entry> *, boost::heap::compare<EQSizeLess>> eq_max_heap_t;

struct priority_group{
    uint64_t batch_starting_txn_id;
#if BATCHING_MODE == SIZE_BASED
    transaction_context txn_ctxs[PG_TXN_CTX_SIZE];
#else
    transaction_context * txn_ctxs;
#endif
    bool ready; // remote PGs can be sent out of order
    std::atomic<uint64_t> eq_completed_rem_cnt;

} __attribute__((aligned));;

struct batch_partition{
//    atomic<uint64_t> status; // TODO(tq) remove completely
    priority_group * planner_pg;
    uint64_t batch_id;
    bool empty;
    bool remote;
    // A small optimization in case there is only a single exec_q
    // This optimization will avoid a cache miss
    bool single_q;
    Array<exec_queue_entry> * exec_q;
    atomic<uint64_t> exec_q_status;

    // Info. related to having multiple exec. queues
    Array<Array<exec_queue_entry> *> * exec_qs;
    uint64_t curr_sum;
    uint64_t cwet_id;
    uint64_t cwpt_id;
} __attribute__((aligned));;



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

void assign_entry_get_or_create(assign_entry *&a_entry, boost::lockfree::spsc_queue<assign_entry *> * assign_entry_free_list);
void assign_entry_init(assign_entry * &a_entry, uint64_t thd_id);
void assign_entry_add(assign_entry * a_entry, Array<exec_queue_entry> * exec_q);
void assign_entry_clear(assign_entry * &a_entry);
//void assign_entry_release(assign_entry * &a_entry);

struct AssignEntryCompareSum{
    bool operator()(const assign_entry* e1, const assign_entry* e2) const
    {
        return e1->curr_sum > e2->curr_sum;
    }
};

struct BatchPartMaxSum{
    bool operator()(const batch_partition* e1, const batch_partition* e2) const
    {
        return e1->curr_sum > e2->curr_sum;
    }
};

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
typedef boost::heap::priority_queue<assign_entry*, boost::heap::compare<AssignEntryCompareSum>> assign_ptr_min_heap_t;
typedef boost::heap::priority_queue<batch_partition *, boost::heap::compare<BatchPartMaxSum>> batch_part_min_heap_t;

struct sync_block{
    int64_t done;
//    int64_t next_stage;
    char padding[56];
};

void txn_ctxs_get_or_create(transaction_context * &txn_ctxs, uint64_t length, uint64_t planner_id);
void txn_ctxs_release(transaction_context * &txn_ctxs, uint64_t planner_id);


class QueCCPool {
public:
    void init(Workload * wl, uint64_t size);

    // Planner EQS methods
    void exec_qs_get_or_create(Array<Array<exec_queue_entry> *> *&exec_qs, uint64_t planner_id);
    void exec_qs_release(Array<Array<exec_queue_entry> *> *&exec_qs, uint64_t planner_id);

    void exec_qs_status_get_or_create(atomic<uint8_t> *&execqs_status, uint64_t planner_id, uint64_t et_id);
    void exec_qs_status_release(atomic<uint8_t> *&execqs_status, uint64_t planner_id, uint64_t et_id);

    //EQ pool methods
    void exec_queue_get_or_create(Array<exec_queue_entry> *&exec_q, uint64_t planner_id, uint64_t et_id);
    void exec_queue_release(Array<exec_queue_entry> *&exec_q, uint64_t planner_id, uint64_t et_id);

    //Batch Partition methods
    void batch_part_get_or_create(batch_partition *&batch_p, uint64_t planner_id, uint64_t et_id);
    void batch_part_release(batch_partition *&batch_p, uint64_t planner_id, uint64_t et_id);

    // Txn Ctxs
    void txn_ctxs_get_or_create(transaction_context * &txn_ctxs, uint64_t planner_id);
    void txn_ctxs_release(transaction_context * &txn_ctxs, uint64_t planner_id);

    // PGs
    void pg_get_or_create(priority_group * &pg, uint64_t planner_id);
    void pg_release(priority_group * &pg, uint64_t planner_id);

    void databuff_get_or_create(char * &buf, uint64_t size, uint64_t thd_id);
    void databuff_release(char * &buf, uint64_t size, uint64_t thd_id);

    // TDG
#if TDG_ENTRY_TYPE == VECTOR_ENTRY
    void txn_list_get_or_create(std::vector<uint64_t> *& list, uint64_t planner_id){
        if (!vector_free_list[planner_id]->pop(list)){
            list = new std::vector<uint64_t>();
        }
    }
    void txn_list_release(std::vector<uint64_t> *& list, uint64_t planner_id){
        list->clear();
        while(!vector_free_list[planner_id]->push(list)){};
    }
#elif TDG_ENTRY_TYPE == ARRAY_ENTRY
    void txn_ctx_list_get_or_create(Array<transaction_context *> *& list, uint64_t thd_id){
        if (!tctx_ptr_free_list[thd_id]->pop(list)){
            list = (Array<transaction_context *> *) mem_allocator.alloc(sizeof(Array<transaction_context *>));
            list->init(TDG_ENTRY_LENGTH);
        }
    }
    void txn_ctx_list_release(Array<transaction_context *> *& list, uint64_t thd_id){
        list->clear();
        while(!tctx_ptr_free_list[thd_id]->push(list)){};
    }

    void txn_id_list_get_or_create(Array<uint64_t> *& list, uint64_t planner_id){
        if (!vector_free_list[planner_id]->pop(list)){
            list = (Array<uint64_t> *) mem_allocator.alloc(sizeof(Array<uint64_t>));
            list->init(TDG_ENTRY_LENGTH);
        }
    }
    void txn_id_list_release(Array<uint64_t> *& list, uint64_t planner_id){
        list->clear();
        while(!vector_free_list[planner_id]->push(list)){};
    }
#endif // #if TDG_ENTRY_TYPE == VECTOR_ENTRY
    //TODO(tq): implement free_all()
    void free_all();
    void print_stats(uint64_t batch_id);

    uint64_t exec_queue_capacity;
    uint64_t planner_batch_size;

    static uint64_t get_exec_node(uint64_t i);
    static uint64_t get_plan_node(uint64_t i);

    static uint64_t map_to_planner_id(uint64_t cwid);
    static uint64_t map_to_et_id(uint64_t cwid);
    static uint64_t map_g_et_to_et_id(uint64_t getid);
    static uint64_t map_to_cwplanner_id(uint64_t planner_id);
    static uint64_t map_to_cwexec_id(uint64_t et_id);

    struct timespec ts_req[THREAD_CNT];
    struct timespec ts_rem[THREAD_CNT];

    atomic<int32_t> batch_deps[BATCH_MAP_LENGTH];
    atomic<int64_t> last_commited_batch_id;

#if DEBUG_QUECC
    std::atomic<bool> eqset_lock;
    std::set<Array<exec_queue_entry> *> exec_q_set;
#endif

private:

    row_data_pool_t row_data_pool[THREAD_CNT*NODE_CNT];
    spinlock * row_data_pool_lock;
    boost::lockfree::queue<Array<Array<exec_queue_entry> *> *> ** exec_qs_free_list;
#if PIPELINED2
    boost::lockfree::queue<Array<exec_queue_entry> *> * exec_queue_free_list[(THREAD_CNT-PLAN_THREAD_CNT)*NODE_CNT][PLAN_THREAD_CNT*NODE_CNT];
    boost::lockfree::queue<batch_partition *> * batch_part_free_list[(THREAD_CNT-PLAN_THREAD_CNT)*NODE_CNT][PLAN_THREAD_CNT*NODE_CNT];
    boost::lockfree::queue<atomic<uint8_t> *> * exec_qs_status_free_list[(THREAD_CNT-PLAN_THREAD_CNT)*NODE_CNT][PLAN_THREAD_CNT*NODE_CNT];
#if TDG_ENTRY_TYPE == VECTOR_ENTRY
    boost::lockfree::queue<std::vector<uint64_t> *> * vector_free_list[PLAN_THREAD_CNT];
#elif TDG_ENTRY_TYPE == ARRAY_ENTRY
    boost::lockfree::queue<Array<uint64_t> *> * vector_free_list[PLAN_THREAD_CNT];
    boost::lockfree::queue<Array<transaction_context *> *> * tctx_ptr_free_list[(THREAD_CNT-PLAN_THREAD_CNT)*NODE_CNT];
#endif
#else
    boost::lockfree::queue<Array<exec_queue_entry> *> * exec_queue_free_list[THREAD_CNT*NODE_CNT][PLAN_THREAD_CNT*NODE_CNT];
    boost::lockfree::queue<batch_partition *> * batch_part_free_list[THREAD_CNT*NODE_CNT][PLAN_THREAD_CNT*NODE_CNT];
    boost::lockfree::queue<atomic<uint8_t> *> * exec_qs_status_free_list[THREAD_CNT*NODE_CNT][PLAN_THREAD_CNT*NODE_CNT];

#if TDG_ENTRY_TYPE == VECTOR_ENTRY
    boost::lockfree::queue<std::vector<uint64_t> *> * vector_free_list[PLAN_THREAD_CNT];
#elif TDG_ENTRY_TYPE == ARRAY_ENTRY
    boost::lockfree::queue<Array<uint64_t> *> * vector_free_list[PLAN_THREAD_CNT];
    boost::lockfree::queue<Array<transaction_context *> *> * tctx_ptr_free_list[THREAD_CNT*NODE_CNT];
#endif
#endif



    boost::lockfree::queue<transaction_context *> ** txn_ctxs_free_list;
    boost::lockfree::queue<priority_group *> ** pg_free_list;

    // Stats for debugging
    // TODO(tq): remove or use a macro to turn them off
#if DEBUG_QUECC
    atomic<uint64_t> batch_part_alloc_cnts[THREAD_CNT*NODE_CNT][PLAN_THREAD_CNT*NODE_CNT];
    atomic<uint64_t> batch_part_reuse_cnts[THREAD_CNT*NODE_CNT][PLAN_THREAD_CNT*NODE_CNT];
    atomic<uint64_t> batch_part_rel_cnts[THREAD_CNT*NODE_CNT][PLAN_THREAD_CNT*NODE_CNT];

    atomic<uint64_t> exec_q_alloc_cnts[THREAD_CNT*NODE_CNT][PLAN_THREAD_CNT*NODE_CNT];
    atomic<uint64_t> exec_q_reuse_cnts[THREAD_CNT*NODE_CNT][PLAN_THREAD_CNT*NODE_CNT];
    atomic<uint64_t> exec_q_rel_cnts[THREAD_CNT*NODE_CNT][PLAN_THREAD_CNT*NODE_CNT];

    atomic<uint64_t> exec_qs_alloc_cnts[PLAN_THREAD_CNT*NODE_CNT];
    atomic<uint64_t> exec_qs_reuse_cnts[PLAN_THREAD_CNT*NODE_CNT];
    atomic<uint64_t> exec_qs_rel_cnts[PLAN_THREAD_CNT*NODE_CNT];

    atomic<uint64_t> txn_ctxs_alloc_cnts[PLAN_THREAD_CNT*NODE_CNT];
    atomic<uint64_t> txn_ctxs_reuse_cnts[PLAN_THREAD_CNT*NODE_CNT];
    atomic<uint64_t> txn_ctxs_rel_cnts[PLAN_THREAD_CNT*NODE_CNT];

    atomic<uint64_t> pg_alloc_cnts[PLAN_THREAD_CNT*NODE_CNT];

    atomic<uint64_t> pg_reuse_cnts[PLAN_THREAD_CNT*NODE_CNT];
    atomic<uint64_t> pg_rel_cnts[PLAN_THREAD_CNT*NODE_CNT];
#endif
};


class PlannerThread : public Thread {
public:
    RC run();
    void setup();
    uint64_t _planner_id;
    uint32_t get_bucket(uint64_t key);

    uint32_t get_split(uint64_t key, Array<uint64_t> * ranges){
        for (uint32_t i = 0; i < ranges->size(); i++){
            if (key <= ranges->get(i)){
                return i;
            }
        }
        for (uint32_t i = 0; i < ranges->size(); i++){
            printf("could not assign to range key = %lu, range[%d]=%lu\n", key,i,ranges->get(i));
        }
        fflush(stdout);
        M_ASSERT_V(false, "could not assign to range key = %lu\n", key);
        return (uint32_t) ranges->size()-1;
    }

    uint64_t get_key_from_entry(exec_queue_entry * entry) {
#if WORKLOAD == YCSB
//        ycsb_request *ycsb_req_tmp = (ycsb_request *) entry->req_buffer;
//        return ycsb_req_tmp->key;
        return entry->req.key;
#else
        return entry->rid;
#endif
    }

    void splitMRange(Array<exec_queue_entry> *& mrange, uint64_t et_id){
        uint64_t tidx =0;
        uint64_t nidx =0;
        Array<exec_queue_entry> * texec_q = NULL;

        for (uint64_t r =0; r < mrange->size(); ++r){
            // TODO(tq): refactor this to respective benchmark implementation
            uint64_t lid = get_key_from_entry(mrange->get_ptr(r));
            tidx = get_split(lid, exec_qs_ranges);
//            M_ASSERT_V(((Array<exec_queue_entry> volatile * )exec_queues->get(tidx)) == mrange, "PL_%ld: mismatch mrange and tidx_eq\n",_planner_id);
            nidx = get_split(lid, ((Array<uint64_t> *)exec_qs_ranges_tmp));

            texec_q =  exec_queues_tmp->get(nidx);
//            M_ASSERT_V(texec_q == nexec_q || texec_q == oexec_q , "PL_%ld: mismatch mrange and tidx_eq\n",_planner_id);
            // all entries must fall into one of the splits
#if DEBUG_QUECC
            if (!(nidx == tidx || nidx == (tidx+1))){
//                DEBUG_Q("PL_%ld: nidx=%ld, tidx = %ld,lid=%ld,key=%ld\n",_planner_id, nidx, idx, lid,key);
                for (uint64_t i =0; i < ((Array<uint64_t> *)exec_qs_ranges_tmp)->size(); ++i){
                    DEBUG_Q("PL_%ld: old exec_qs_ranges[%lu] = %lu\n", _planner_id, i, ((Array<uint64_t> *)exec_qs_ranges_tmp)->get(i));
                }

//            for (uint64_t i =0; i < exec_queues_tmp->size(); ++i){
//                DEBUG_Q("PL_%ld: old exec_queues[%lu] size = %lu, ptr = %lu, range= %lu\n",
//                        _planner_id, i, exec_queues_tmp->get(i)->size(), (uint64_t) exec_queues_tmp->get(i), exec_qs_ranges_tmp->get(i));
//            }

                for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
                    DEBUG_Q("PL_%ld: new exec_qs_ranges[%lu] = %lu\n", _planner_id, i, exec_qs_ranges->get(i));
                }
//            for (uint64_t i =0; i < exec_queues->size(); ++i){
//                DEBUG_Q("PL_%ld: new exec_queues[%lu] size = %lu, ptr = %lu, range=%lu\n",
//                        _planner_id, i, exec_queues->get(i)->size(), (uint64_t) exec_queues->get(i), exec_qs_ranges->get(i));
//            }
            }
#endif
            M_ASSERT_V(nidx == tidx || nidx == (tidx+1),"PL_%ld: nidx=%ld, tidx = %ld,lid=%ld\n",_planner_id, nidx, tidx, lid);

            texec_q->add(mrange->get(r));
        }
    }

    void checkAndSplitRange(Array<exec_queue_entry> *& mrange, uint64_t key, uint64_t et_id,
                     Array<uint64_t> * &exec_qs_ranges,
                     Array<Array<exec_queue_entry> *> * &exec_queues,
                     Array<uint64_t> * &exec_qs_ranges_tmp,
                     Array<Array<exec_queue_entry> *> * &exec_queues_tmp){

        Array<uint64_t> * exec_qs_ranges_tmp_tmp;
        Array<Array<exec_queue_entry> *> * exec_queues_tmp_tmp;
#if SPLIT_MERGE_ENABLED && SPLIT_STRATEGY == EAGER_SPLIT

        int max_tries = 64; //TODO(tq): make this configurable
        int trial =0;
#if PROFILE_EXEC_TIMING
        uint64_t _prof_starttime =0;
#endif

        volatile uint64_t c_range_start;
        volatile uint64_t c_range_end;
        volatile uint64_t idx = get_split(key, exec_qs_ranges);
        volatile uint64_t split_point;
        Array<exec_queue_entry> * nexec_q = NULL;
        Array<exec_queue_entry> * oexec_q = NULL;

        mrange = exec_queues->get(idx);
        while (mrange->is_full()){
#if PROFILE_EXEC_TIMING
            if (_prof_starttime == 0){
                _prof_starttime  = get_sys_clock();
            }
#endif
            trial++;
            if (trial == max_tries){
                M_ASSERT_V(false, "Execeded max split tries\n");
            }

            // we need to split

//        M_ASSERT_V(idx == pidx, "idx mismatch after removal of empty queues; idx=%ld , pidx=%ld\n", idx, pidx);
//        idx = get_split(key, exec_qs_ranges);
//        mrange = exec_queues->get(idx);

            if (idx == 0){
                c_range_start = 0;
            }
            else{
                c_range_start = exec_qs_ranges->get(idx-1);
            }
            c_range_end = exec_qs_ranges->get(idx);

//        DEBUG_Q("Planner_%ld : Eagerly we need to split mrange ptr = %lu, key = %lu, current size = %ld,"
//                        " batch_id = %ld, c_range_start = %lu, c_range_end = %lu, split_point = %lu, trial=%d"
//                        "\n",
//                _planner_id, (uint64_t) mrange, key, mrange->size(), wbatch_id, c_range_start, c_range_end, split_point, trial);
#if EXPANDABLE_EQS
            // if we cannot split, we must expand this, otherwise, we fail
            if ((c_range_end-c_range_start) <= 1){
                // expand current EQ
                if (mrange->expand()){
                    assert(!mrange->is_full());
                    return;
                }
            }
#endif

            split_point = (c_range_end-c_range_start)/2;
            M_ASSERT_V(split_point, "PL_%ld: We are at a single record, and we cannot split anymore!, range_size = %ld, eq_size = %ld\n",
                       _planner_id, c_range_end-c_range_start, mrange->size());

            // compute new ranges
            ((Array<uint64_t> *)exec_qs_ranges_tmp)->clear();
            ((Array<Array<exec_queue_entry> *> *)exec_queues_tmp)->clear();
            M_ASSERT_V(exec_queues->size() == exec_qs_ranges->size(), "PL_%ld: Size mismatch : EQS(%lu) Ranges (%lu)\n",
                       _planner_id, exec_queues->size(), exec_qs_ranges->size());
            // if we have reached max number of ranges
            // drop ranges and EQs with zero entries
            if (exec_qs_ranges->is_full()){
                uint64_t add_cnt = 0;
                DEBUG_Q("WT_%lu: exec_qs_ranges is full with size = %lu\n",_thd_id, exec_qs_ranges->size());
                for (uint64_t j = 0; j < exec_qs_ranges->size(); ++j) {
                    if (j == idx-1 || j==idx || exec_queues->get(j)->size() > 0 || j == exec_qs_ranges->size()-1){
                        DEBUG_Q("WT_%lu: keeping range[%lu] %lu, has %lu eq entries\n",
                                _thd_id, j,exec_qs_ranges->get(j),exec_queues->get(j)->size());
                        ((Array<Array<exec_queue_entry> *> *)exec_queues_tmp)->add(exec_queues->get(j));
                        ((Array<uint64_t> *)exec_qs_ranges_tmp)->add(exec_qs_ranges->get(j));
                        add_cnt++;
                    }
                    else{
                        if (add_cnt < g_thread_cnt && (exec_qs_ranges->size()-j) < (g_thread_cnt)){
                            DEBUG_Q("WT_%lu: keeping an empty range[%lu] %lu, has %lu eq entries\n",
                                    _thd_id, j,exec_qs_ranges->get(j),exec_queues->get(j)->size());
                            ((Array<Array<exec_queue_entry> *> *)exec_queues_tmp)->add(exec_queues->get(j));
                            ((Array<uint64_t> *)exec_qs_ranges_tmp)->add(exec_qs_ranges->get(j));
                            add_cnt++;
                        }
                        else{
                            DEBUG_Q("WT_%lu: freeing range[%lu] %lu, has %lu eq entries\n",
                                    _thd_id, j,exec_qs_ranges->get(j),exec_queues->get(j)->size());
                            Array<exec_queue_entry> * tmp = exec_queues->get(j);
                            quecc_pool.exec_queue_release(tmp,0,0);
                        }
                    }
                }

                exec_queues_tmp_tmp = exec_queues;
                exec_qs_ranges_tmp_tmp = exec_qs_ranges;

                exec_queues = ((Array<Array<exec_queue_entry> *> * )exec_queues_tmp);
                exec_qs_ranges = ((Array<uint64_t> *)exec_qs_ranges_tmp);

                exec_queues_tmp = exec_queues_tmp_tmp;
                exec_qs_ranges_tmp = exec_qs_ranges_tmp_tmp;

                ((Array<uint64_t> *)exec_qs_ranges_tmp)->clear();
                ((Array<Array<exec_queue_entry> *> *)exec_queues_tmp)->clear();


                assert(exec_qs_ranges->size() >= g_thread_cnt);
                // recompute idx
                idx = get_split(key, exec_qs_ranges);
                if (idx == 0){
                    c_range_start = 0;
                }
                else{
                    c_range_start = exec_qs_ranges->get(idx-1);
                }
                c_range_end = exec_qs_ranges->get(idx);

#if EXPANDABLE_EQS
                // if we cannot split, we must expand this, otherwise, we fail
                if ((c_range_end-c_range_start) <= 1){
                    // expand current EQ
                    if (mrange->expand()){
                        assert(!mrange->is_full());
                        return;
                    }
                }
#endif

                split_point = (c_range_end-c_range_start)/2;
                M_ASSERT_V(split_point, "PL_%ld: We are at a single record, and we cannot split anymore!, range_size = %ld, eq_size = %ld\n",
                           _planner_id, c_range_end-c_range_start, mrange->size());
            }
            // update ranges
            // add two new and empty exec_queues
            for (uint64_t r=0; r < exec_qs_ranges->size(); ++r){
                if (r == idx){
                    // insert split
                    M_ASSERT_V(exec_qs_ranges->get(r) != split_point+c_range_start,
                               "PL_%ld: old range = %lu, new range = %lu",
                               _planner_id,exec_qs_ranges->get(r), split_point+c_range_start);
                    ((Array<uint64_t> *)exec_qs_ranges_tmp)->add(split_point+c_range_start);
#if MERGE_STRATEGY == RR
                    quecc_pool.exec_queue_get_or_create(oexec_q, _planner_id, r % g_thread_cnt);
                    quecc_pool.exec_queue_get_or_create(nexec_q, _planner_id, (r+1) % g_thread_cnt);
#else
                    quecc_pool.exec_queue_get_or_create(oexec_q, _planner_id, et_id);
                    quecc_pool.exec_queue_get_or_create(nexec_q, _planner_id, et_id);
#endif

//                M_ASSERT_V(oexec_q != mrange, "PL_%ld: oexec_q=%lu, nexec_q=%lu, mrange=%lu, trial=%d\n",
//                           _planner_id, (uint64_t) oexec_q, (uint64_t) nexec_q, (uint64_t) mrange, trial);

//                M_ASSERT_V(nexec_q != mrange, "PL_%ld: oexec_q=%lu, nexec_q=%lu, mrange=%lu, trial=%d\n",
//                           _planner_id, (uint64_t) oexec_q, (uint64_t) nexec_q, (uint64_t) mrange, trial);
//                assert(oexec_q->size() == 0);
//                assert(nexec_q->size() == 0);
                    ((Array<Array<exec_queue_entry> *> * )exec_queues_tmp)->add(oexec_q);
                    ((Array<Array<exec_queue_entry> *> * )exec_queues_tmp)->add(nexec_q);

                }
                else{
                    ((Array<Array<exec_queue_entry> *> * )exec_queues_tmp)->add(exec_queues->get(r));
                }
                ((Array<uint64_t> *)exec_qs_ranges_tmp)->add(exec_qs_ranges->get(r));
            }

            assert(exec_qs_ranges_tmp->size() == exec_qs_ranges->size()+1);

            // use new ranges to split current execq
            splitMRange(mrange,et_id);

//            if(exec_queues_tmp->get(idx)->size() == 0){
//                M_ASSERT_V(false,"PT_%ld: LEFT EQ is empty after split\n",_planner_id);
//            }
//
//            if (exec_queues_tmp->get(idx+1)->size() == 0){
//                M_ASSERT_V(false,"PT_%ld: RIGHT EQ is empty after split\n",_planner_id);
//            }
            // swap data structures
            exec_queues_tmp_tmp = exec_queues;
            exec_qs_ranges_tmp_tmp = exec_qs_ranges;

            exec_queues = ((Array<Array<exec_queue_entry> *> * )exec_queues_tmp);
            exec_qs_ranges = ((Array<uint64_t> *)exec_qs_ranges_tmp);

            exec_queues_tmp = exec_queues_tmp_tmp;
            exec_qs_ranges_tmp = exec_qs_ranges_tmp_tmp;

//        DEBUG_Q("Planner_%ld : After swapping New ranges size = %ld, old ranges size = %ld"
//                        "\n",
//                _planner_id, exec_qs_ranges->size(), exec_qs_ranges_tmp->size());

            // release current mrange
//            quecc_pool.exec_queue_release(mrange,_planner_id,RAND(g_plan_thread_cnt));
            quecc_pool.exec_queue_release(mrange,QueCCPool::map_to_planner_id(_planner_id),_thd_id);
//        DEBUG_Q("PL_%ld: key =%lu, nidx=%ld, idx=%ld, trial=%d\n", _planner_id, key, nidx, idx, trial);

            // use the new ranges to assign the new execution entry
            idx = get_split(key, exec_qs_ranges);
            mrange = exec_queues->get(idx);

//#if DEBUG_QUECC
//            for (uint64_t i =0; i < exec_qs_ranges_tmp->size(); ++i){
//                DEBUG_Q("PL_%ld: old exec_qs_ranges[%lu] = %lu\n", _planner_id, i, exec_qs_ranges_tmp->get(i));
//            }
//
//            for (uint64_t i =0; i < exec_queues_tmp->size(); ++i){
//                DEBUG_Q("PL_%ld: old exec_queues[%lu] size = %lu, ptr = %lu, range= %lu\n",
//                        _planner_id, i, exec_queues_tmp->get(i)->size(), (uint64_t) exec_queues_tmp->get(i), exec_qs_ranges_tmp->get(i));
//            }
//
//            for (uint64_t i =0; i < exec_qs_ranges->size(); ++i){
//                DEBUG_Q("PL_%ld: new exec_qs_ranges[%lu] = %lu\n", _planner_id, i, exec_qs_ranges->get(i));
//            }
//            for (uint64_t i =0; i < exec_queues->size(); ++i){
//                DEBUG_Q("PL_%ld: new exec_queues[%lu] size = %lu, ptr = %lu, range=%lu\n",
//                        _planner_id, i, exec_queues->get(i)->size(), (uint64_t) exec_queues->get(i), exec_qs_ranges->get(i));
//            }
//#endif

        }
#if PROFILE_EXEC_TIMING
        if (_prof_starttime > 0){
            INC_STATS(_thd_id, plan_split_time[_planner_id], get_sys_clock()-_prof_starttime);
        }
#endif

#else
        M_ASSERT(false, "LAZY_SPLIT not supported in TPCC")
#endif
    }

    void checkMRange(Array<exec_queue_entry> *&mrange, uint64_t key, uint64_t et_id);
    void plan_client_msg(Message *msg, priority_group * planner_pg);
    SRC do_batch_delivery(bool force_batch_delivery, priority_group * &planner_pg, transaction_context * &txn_ctxs);
#if DEBUG_QUECC
    void print_threads_status() const {// print phase status
//        for (UInt32 ii=0; ii < g_plan_thread_cnt; ++ii){
//            DEBUG_Q("PT_%ld: planner_%d active : %ld, pbatch_id=%ld\n", _planner_id, ii, plan_active[ii]->fetch_add(0), batch_id);
//        }
//
//        for (UInt32 ii=0; ii < g_thread_cnt; ++ii){
//            DEBUG_Q("PT_%ld: exec_%d active : %ld, pbatch_id=%ld\n", _planner_id, ii, exec_active[ii]->fetch_add(0), batch_id);
//            DEBUG_Q("PT_%ld: commit_%d active : %ld, pbatch_id=%ld\n", _planner_id, ii, commit_active[ii]->fetch_add(0), batch_id);
//        }
    }
#endif
#if WORKLOAD == YCSB
    // create a bucket for each worker thread
#if PIPELINED2
    uint64_t bucket_size = g_synth_table_size / (g_et_thd_cnt);
#else
    uint64_t bucket_size = g_synth_table_size / g_thread_cnt;
#endif

#elif WORKLOAD == TPCC
    // 9223372036854775807 = 2^63
    // FIXME(tq): Use a parameter to determine the maximum database size
//    uint64_t bucket_size = (9223372036854775807) / g_thread_cnt;
    uint64_t bucket_size = UINT64_MAX/NUM_WH;
#else
    uint64_t bucket_size = 0;
#endif

    RC run_fixed_mode();
    RC run_fixed_mode2();
    RC run_normal_mode();
private:
    // txn related
    uint64_t planner_txn_id = 0;
    uint64_t txn_prefix_base = 0x0010000000000000;
    uint64_t txn_prefix_planner_base = 0;
    TxnManager * my_txn_man;

    // Batch related
    uint64_t planner_batch_size = g_batch_size/g_plan_thread_cnt;
    uint64_t batch_id = 0;
    uint64_t batch_cnt = 0;
    bool force_batch_delivery = false;
    uint64_t batch_starting_txn_id;

    exec_queue_entry *entry = (exec_queue_entry *) mem_allocator.align_alloc(sizeof(exec_queue_entry));
    uint64_t et_id = 0;
    boost::random::mt19937 plan_rng;
#if PIPELINED2
    boost::random::uniform_int_distribution<> * eq_idx_rand = new boost::random::uniform_int_distribution<>(0, g_et_thd_cnt-1);
#else
    boost::random::uniform_int_distribution<> * eq_idx_rand = new boost::random::uniform_int_distribution<>(0, g_thread_cnt-1);
#endif
    // measurements
#if PROFILE_EXEC_TIMING
    uint64_t idle_starttime = 0;
    uint64_t batch_start_time = 0;
    uint64_t prof_starttime = 0;
    uint64_t txn_prof_starttime = 0;
    uint64_t plan_starttime = 0;
#endif
    // CAS related
    uint64_t expected = 0;
    uint64_t desired = 0;
    uint8_t expected8 = 0;
    uint8_t desired8 = 0;
    uint64_t slot_num = 0;

    // create and and pre-allocate execution queues
    // For each mrange which will be assigned to an execution thread
    // there will be an array pointer.
    // When the batch is complete we will CAS the exec_q array to allow
    // execution threads to be
    Array<Array<exec_queue_entry> *> * exec_queues = new Array<Array<exec_queue_entry> *>();
    Array<uint64_t> * exec_qs_ranges = new Array<uint64_t>();

#if SPLIT_MERGE_ENABLED

#if SPLIT_STRATEGY == LAZY_SPLIT
    split_max_heap_t pq_test;
    split_min_heap_t range_sorted;
    exec_queue_limit = (planner_batch_size/g_thread_cnt) * REQ_PER_QUERY * EXECQ_CAP_FACTOR;
    boost::lockfree::spsc_queue<split_entry *> * split_entry_free_list =
            new boost::lockfree::spsc_queue<split_entry *>(FREE_LIST_INITIAL_SIZE*10);
    split_entry ** nsp_entries = (split_entry **) mem_allocator.alloc(sizeof(split_entry*)*2);
    volatile bool ranges_stored = false;
     Array<exec_queue_entry> **nexec_qs = (Array<exec_queue_entry> **) mem_allocator.alloc(
            sizeof(Array<exec_queue_entry> *) * 2);

#elif SPLIT_STRATEGY == EAGER_SPLIT
    Array<uint64_t> * exec_qs_ranges_tmp = new Array<uint64_t>();
    Array<Array<exec_queue_entry> *> * exec_queues_tmp;
#endif
#if MERGE_STRATEGY == BALANCE_EQ_SIZE
    assign_ptr_min_heap_t assignment;
#elif MERGE_STRATEGY == RR
#endif
#if PIPELINED2
    uint64_t * f_assign = (uint64_t *) mem_allocator.alloc(sizeof(uint64_t)*g_et_thd_cnt);
#else
    uint64_t * f_assign = (uint64_t *) mem_allocator.alloc(sizeof(uint64_t)*g_thread_cnt);
#endif
    boost::lockfree::spsc_queue<assign_entry *> * assign_entry_free_list =
            new boost::lockfree::spsc_queue<assign_entry *>(FREE_LIST_INITIAL_SIZE);

#endif

};

#if CT_ENABLED
class CommitThread : public Thread {
public:
    RC run();
    void setup();
};
#endif //CT_ENABLED

#endif // if CC_ALG == QUECC

#endif //_QUECC_THREAD_H
