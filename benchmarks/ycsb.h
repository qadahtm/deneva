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

#ifndef _SYNTH_BM_H_
#define _SYNTH_BM_H_

#include "wl.h"
#include "txn.h"
#include "global.h"
#include "helper.h"

#if WORKLOAD == YCSB

class YCSBQuery;

class YCSBQueryMessage;

class ycsb_request;

enum YCSBRemTxnType {
    YCSB_0,
    YCSB_1,
    YCSB_FIN,
    YCSB_RDONE
};

inline uint64_t RAND(uint64_t max) {
    return rand() % max;
}

class YCSBWorkload : public Workload {
public :
    RC init();

    RC init_table();

    RC init_schema(const char *schema_file);

    RC get_txn_man(TxnManager *&txn_manager);

    int key_to_part(uint64_t key);

    RC resolve_txn_dependencies(Message* msg, int cid);

    INDEX *the_index;
    table_t *the_table;
#if NUMA_ENABLED
    struct c_thd_args_t{
        YCSBWorkload * wl;
        uint64_t thd_id;
    };
#endif
private:
    void init_table_parallel();
#if NUMA_ENABLED
    void *init_table_slice(uint64_t thd_id, int node);
    static void *threadInitTable(void *This) {
        c_thd_args_t * args = ((c_thd_args_t *) This);
        int node = (args->thd_id)/(CORE_CNT/NUMA_NODE_CNT);
        numa_set_preferred(node);
        DEBUG_Q("WorkloadThd_%ld: preferred node is %d\n", args->thd_id, numa_preferred());
        args->wl->init_table_slice(args->thd_id, node);
        mem_allocator.free(args, sizeof(c_thd_args_t));
        return NULL;
    }
#else
    void *init_table_slice();

    static void *threadInitTable(void *This) {
        ((YCSBWorkload *) This)->init_table_slice();
        return NULL;
    }
#endif



    pthread_mutex_t insert_lock;
    //  For parallel initialization
    static atomic<UInt32> next_tid;
};

class YCSBTxnManager : public TxnManager {
public:
    void init(uint64_t thd_id, Workload *h_wl);

    void reset();

    void partial_reset();

    RC acquire_locks();

    RC run_txn();

    RC run_txn_post_wait();

    RC run_calvin_txn();
#if CC_ALG == QUECC
    // For QueCC
    RC run_quecc_txn(exec_queue_entry * exec_qe);
#if YCSB_INDEX_LOOKUP_PLAN
    inline RC lookup_key(uint64_t key, exec_queue_entry *&entry) ALWAYS_INLINE {
        itemid_t *item;
        int part_id = _wl->key_to_part(key);
        INDEX *index = _wl->the_index;
        item = index_read(index, key,part_id);

        assert(item != NULL);
        entry->row = ((row_t *) item->location);
        assert(entry->row);
        return RCOK;
    };
#endif// if INDEX_LOOKUP_PLAN
#endif
    // For HStore
    RC run_hstore_txn();

    // For LADS
#if CC_ALG == LADS
    RC execute_lads_action(gdgcc::Action * action, int eid);
#endif
    void copy_remote_requests(YCSBQueryMessage *msg);

private:
    void next_ycsb_state();

    RC run_txn_state();

    RC run_ycsb_0(ycsb_request *req, row_t *&row_local);

    RC run_ycsb_1(access_t acctype, row_t *row_local);

    RC run_ycsb();

    bool is_done();

    bool is_local_request(uint64_t idx);

    RC send_remote_request();

    row_t *row;
    YCSBWorkload *_wl;
    YCSBRemTxnType state;
    uint64_t next_record_id;
};

#endif // #if WORKLOAD == YCSB

#endif
