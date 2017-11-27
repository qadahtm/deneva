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

#include "client_query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "pps_query.h"

/*************************************************/
//     class Query_queue
/*************************************************/
volatile atomic<uint64_t> Client_query_queue::next_tid;

void
Client_query_queue::init(Workload * h_wl) {
    _wl = h_wl;
    qlock = new spinlock();
#if SERVER_GENERATE_QUERIES
    if(ISCLIENT)
        return;

    M_ASSERT_V(g_part_cnt == 1 || g_part_cnt == g_thread_cnt,"part_cnt must be eitehr 1 or equal to thread_cnt\n");
//    size = g_part_cnt;
    size = g_thread_cnt;
#if CC_ALG == QUECC
    size = g_plan_thread_cnt;
#endif
#else
    size = g_servers_per_client;
#endif
    // allocate a set of queries for each thread
    query_cnt = new uint64_t * [size];
    size_t query_cnt_per_thread;
    if (g_part_cnt == 1){
        query_cnt_per_thread = (g_max_txn_per_part/g_thread_cnt) + 4;
    }
    else{
        query_cnt_per_thread = g_max_txn_per_part + 4;
    }

#if CREATE_TXN_FILE
    query_cnt_per_thread = g_max_txn_per_part + 4;
#endif

    for (UInt32 id = 0; id < size; id++) {
#if INIT_QUERY_MSGS
        std::vector<Message *> new_query_msgs(query_cnt_per_thread, NULL);
        queries_msgs.push_back(new_query_msgs);
#else

        std::vector<BaseQuery *> new_queries(query_cnt_per_thread, NULL);
        queries.push_back(new_queries);
#endif
        query_cnt[id] = (uint64_t *) mem_allocator.align_alloc(sizeof(uint64_t));
        *(query_cnt[id]) = 0;
    }
//    next_tid = 0;
    next_tid.store(0);

#if CREATE_TXN_FILE
    // single threaded generation to ensure output is not malformed
#if NUMA_ENABLED
    c_thd_args_t * args = (c_thd_args_t *) mem_allocator.alloc(sizeof(c_thd_args_t));
    args->context = this;
    args->thd_id = (0);
    initQueriesHelper(args);
#else
    initQueriesHelper(this);
#endif
#else
#if NUMA_ENABLED
    cpu_set_t cpus;
#endif
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_t * p_thds = new pthread_t[g_init_parallelism - 1];
    for (UInt32 i = 0; i < g_init_parallelism - 1; i++) {
#if NUMA_ENABLED
        CPU_ZERO(&cpus);
        CPU_SET((i+1), &cpus);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
        c_thd_args_t * args = (c_thd_args_t *) mem_allocator.alloc(sizeof(c_thd_args_t));
        args->context = this;
        args->thd_id = (i+1);
        pthread_create(&p_thds[i], NULL, initQueriesHelper, args);
#else
        pthread_create(&p_thds[i], NULL, initQueriesHelper, this);
#endif
        pthread_setname_np(p_thds[i], "clientquery");
    }
#if NUMA_ENABLED
    CPU_ZERO(&cpus);
    CPU_SET(0, &cpus);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
    c_thd_args_t * args = (c_thd_args_t *) mem_allocator.alloc(sizeof(c_thd_args_t));
    args->context = this;
    args->thd_id = 0;
    initQueriesHelper(args);
#else
    initQueriesHelper(this);
#endif

    for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
        pthread_join(p_thds[i], NULL);
    }
#endif

}

void *
Client_query_queue::initQueriesHelper(void * context) {
#if NUMA_ENABLED
    c_thd_args_t * args = (c_thd_args_t*) context;
    int node = (args->thd_id)/(CORE_CNT/NUMA_NODE_CNT);
    numa_set_preferred(node);
    DEBUG_Q("QueryInitThd_%ld: preferred node is %d\n", args->thd_id, numa_preferred());
    args->context->initQueriesParallel();
    mem_allocator.free(args, sizeof(c_thd_args_t));
#else
    ((Client_query_queue*)context)->initQueriesParallel();
#endif
    return NULL;
}

void
Client_query_queue::initQueriesParallel() {
//    UInt32 tid = ATOM_FETCH_ADD(next_tid, 1);
//    UInt32 tid;
    uint64_t tid;
    do{
        tid = next_tid.load();
    }while(!next_tid.compare_exchange_strong(tid,tid+1));

    uint64_t request_cnt;
    if (g_part_cnt == 1){
        request_cnt = (g_max_txn_per_part/g_thread_cnt) + 4;
    }
    else{
        request_cnt = g_max_txn_per_part + 4;
    }

    uint32_t final_request;

#if CREATE_TXN_FILE
    // TQ: single threaded generation
    request_cnt = g_max_txn_per_part + 4;
    final_request = request_cnt;
#else
//    if (tid == g_init_parallelism-1) {
//        final_request = request_cnt;
//    } else {
//        final_request = request_cnt / g_init_parallelism * (tid+1);
//    }

    if (g_part_cnt == 1){
        final_request = request_cnt;
    }
    else{
        if (tid < g_part_cnt){
            final_request = request_cnt;
        }
        else{
            return;
        }
    }

#endif

#if WORKLOAD == YCSB
    YCSBQueryGenerator * gen = new YCSBQueryGenerator;
    gen->init();
#elif WORKLOAD == TPCC
    TPCCQueryGenerator * gen = new TPCCQueryGenerator;
#elif WORKLOAD == PPS
    PPSQueryGenerator * gen = new PPSQueryGenerator;
#endif
#if SERVER_GENERATE_QUERIES

    if (g_part_cnt == 1){
        // single partition query generation
        int q_gen_cnt = 0;
        DEBUG_Q("gtid=%ld: Server-side generation for part %d ... ",tid, 0);
        DEBUG_Q("TID=%ld, Server-side generation - going to generate transactions from %lu to %u, total = %lu, for part %d\n",
                tid,(request_cnt / g_init_parallelism * tid), final_request, (final_request-(request_cnt / g_init_parallelism * tid)), 0);
        for (UInt32 query_id = 0; query_id < final_request; query_id ++) {
#if SINGLE_NODE
            BaseQuery * query = gen->create_query(_wl,0);
            M_ASSERT_V(g_part_cnt>=query->partitions.size(),"Invalid query generated\n");
#else
            BaseQuery * query = gen->create_query(_wl,g_node_id);
#endif
#if INIT_QUERY_MSGS
                Message * msg = Message::create_message(query,CL_QRY);
            queries_msgs[qslot][query_id] = msg;
            assert(msg->rtype == CL_QRY);
//            if ( query_id < 100 && qslot==1)
//                DEBUG_Q("added to qslot =%ld, query_id=%d\n", qslot, query_id);
#else
                queries[tid][query_id] = query;

#endif
                q_gen_cnt++;
            }
            DEBUG_Q("tid=%ld, Done .. gen %d, txns\n",tid,q_gen_cnt);
    }
    else{
        // multi-partition partition query generation
        M_ASSERT_V(false, "not supported for now\n");
        for ( UInt32 thread_id = 0; thread_id < g_part_cnt; thread_id ++) {
            int q_gen_cnt = 0;
            DEBUG_Q("gtid=%ld: Server-side generation for part %d ... ",tid, thread_id);
            DEBUG_Q("TID=%ld, Server-side generation - going to generate transactions from %lu to %u, total = %lu, for part %d\n",
                    tid,(request_cnt / g_init_parallelism * tid), final_request, (final_request-(request_cnt / g_init_parallelism * tid)), thread_id);
            for (UInt32 query_id = request_cnt / g_init_parallelism * tid; query_id < final_request; query_id ++) {
#if SINGLE_NODE
                BaseQuery * query = gen->create_query(_wl,thread_id);
                M_ASSERT_V(g_part_cnt>=query->partitions.size(),"Invalid query generated\n");
#else
                BaseQuery * query = gen->create_query(_wl,g_node_id);
#endif
//            uint64_t qslot;
#if WORKLOAD == YCSB
//            YCSBQuery * ycsb_query = (YCSBQuery*)query;
//            qslot = ((YCSBWorkload *) _wl)->key_to_part(ycsb_query->requests[0]->key);
//            qslot = tid;
//            assert(qslot == thread_id);
#else
                qslot = thread_id;
#endif
#if INIT_QUERY_MSGS
                Message * msg = Message::create_message(query,CL_QRY);
            queries_msgs[qslot][query_id] = msg;
            assert(msg->rtype == CL_QRY);
//            if ( query_id < 100 && qslot==1)
//                DEBUG_Q("added to qslot =%ld, query_id=%d\n", qslot, query_id);
#else
                queries[tid][query_id] = query;

#endif
                q_gen_cnt++;
            }
            DEBUG_Q("tid=%ld, Done .. gen %d, txns\n",tid,q_gen_cnt);
        }
    }
#else
M_ASSERT_V(false,"Not suppoprted\n");
//    UInt32 gq_cnt = 0;
//#if CREATE_TXN_FILE
//  DEBUG_WL("single threaded generation ...\n")
//  for ( UInt32 server_id = 0; server_id < g_servers_per_client; server_id ++) {
//    // SINGLE thread
////    for (UInt32 query_id = request_cnt / g_init_parallelism * tid; query_id < final_request; query_id ++) {
//    for (UInt32 query_id = 0; query_id < final_request; query_id ++) {
//      queries[server_id][query_id] = gen->create_query(_wl,server_id+g_server_start_node);
//      gq_cnt++;
//    }
//  }
//#else
//  for ( UInt32 server_id = 0; server_id < g_servers_per_client; server_id ++) {
//    for (UInt32 query_id = request_cnt / g_init_parallelism * tid; query_id < final_request; query_id ++) {
//      queries[server_id][query_id] = gen->create_query(_wl,server_id+g_server_start_node);
//      gq_cnt++;
//    }
//  }
//#endif
  DEBUG_WL("final_request = %d\n", final_request)
  DEBUG_WL("request_cnt = %lu\n", request_cnt)
  DEBUG_WL("g_init_parallelism = %d\n", g_init_parallelism)
  DEBUG_WL("g_servers_per_client = %d\n", g_servers_per_client)
  DEBUG_WL("Client: tid(%d): generated query count = %d\n", tid, gq_cnt);
#endif

}

bool Client_query_queue::done() {
    return false;
}

#if INIT_QUERY_MSGS
Message * Client_query_queue::get_next_query(uint64_t server_id, uint64_t thread_id) {
    M_ASSERT_V(server_id < size, "server_id = %ld, size = %ld\n", server_id, size);
    uint64_t query_id = __sync_fetch_and_add(query_cnt[server_id], 1);
    if (query_id > g_max_txn_per_part) {
        __sync_bool_compare_and_swap(query_cnt[server_id], query_id + 1, 0);
        query_id = __sync_fetch_and_add(query_cnt[server_id], 1);
    }
    // TODO(tq):FixME
//    Message *query = queries_msgs[server_id][query_id];
//    server_id = 0;
    Message *query = queries_msgs[server_id][query_id];
#if CC_ALG == CALVIN
    M_ASSERT_V(false, "flag init query msgs = true us not supported in calvin\n");
#endif
    M_ASSERT_V(query, "server_id=%ld, query_id=%ld\n", server_id, query_id);
    return query;
}
#else
BaseQuery * Client_query_queue::get_next_query(uint64_t server_id, uint64_t thread_id) {
    M_ASSERT_V(server_id < size, "server_id=%ld, size=%ld, thread_id=%ld\n",server_id, size, thread_id);
    assert(server_id < size);
    uint64_t query_id = __sync_fetch_and_add(query_cnt[server_id], 1);
    uint64_t max_txns_per_thread;
    if (g_part_cnt == 1){
        max_txns_per_thread = (g_max_txn_per_part/g_thread_cnt) + 4;
    }
    else{
        max_txns_per_thread = g_max_txn_per_part + 4;
    }
    if (query_id >= max_txns_per_thread) {

//        __sync_bool_compare_and_swap(query_cnt[server_id], query_id + 1, 0);
//        query_id = __sync_fetch_and_add(query_cnt[server_id], 1);
        *(query_cnt[server_id]) = 0;
        query_id = 0;
    }
    assert(query_id < max_txns_per_thread);
    BaseQuery *query = queries[server_id][query_id];
    M_ASSERT_V(query, "could not get next query???\n");
    return query;
}
#endif


