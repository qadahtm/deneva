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

void
Client_query_queue::init(Workload * h_wl) {
    _wl = h_wl;


#if SERVER_GENERATE_QUERIES
    if(ISCLIENT)
        return;
//    size = g_thread_cnt;
    size = g_part_cnt;
#else
    size = g_servers_per_client;
#endif
#if CC_ALG == QUECC
//    size = g_plan_thread_cnt;
#endif
    query_cnt = new uint64_t * [size];
    for (UInt32 id = 0; id < size; id++) {
#if INIT_QUERY_MSGS
        std::vector<Message *> new_query_msgs(g_max_txn_per_part + 4, NULL);
        queries_msgs.push_back(new_query_msgs);
#else
        std::vector<BaseQuery *> new_queries(g_max_txn_per_part + 4, NULL);
        queries.push_back(new_queries);
#endif
        query_cnt[id] = (uint64_t *) mem_allocator.align_alloc(sizeof(uint64_t));
    }
    next_tid = 0;

#if CREATE_TXN_FILE
    // single threaded generation to ensure output is not malformed
  initQueriesHelper(this);
#else
    pthread_t * p_thds = new pthread_t[g_init_parallelism - 1];
    for (UInt32 i = 0; i < g_init_parallelism - 1; i++) {
        pthread_create(&p_thds[i], NULL, initQueriesHelper, this);
        pthread_setname_np(p_thds[i], "clientquery");
    }

    initQueriesHelper(this);

    for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
        pthread_join(p_thds[i], NULL);
    }
#endif

}

void *
Client_query_queue::initQueriesHelper(void * context) {
    ((Client_query_queue*)context)->initQueriesParallel();
    return NULL;
}

void
Client_query_queue::initQueriesParallel() {
    UInt32 tid = ATOM_FETCH_ADD(next_tid, 1);
    uint64_t request_cnt;
    request_cnt = g_max_txn_per_part + 4;

    uint32_t final_request;

#if CREATE_TXN_FILE
    // TQ: single threaded generation
  final_request = request_cnt;
#else
    if (tid == g_init_parallelism-1) {
        final_request = request_cnt;
    } else {
        final_request = request_cnt / g_init_parallelism * (tid+1);
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
//    UInt32 thd_cnt = g_thread_cnt;
    UInt32 thd_cnt = g_part_cnt;

//#if CC_ALG == QUECC
//    DEBUG_WL("QueCC server-side generation ...\n")
//    thd_cnt = g_plan_thread_cnt;
//#endif
#if WORKLOAD == YCSB
    M_ASSERT_V(g_thread_cnt == g_part_cnt, "mismatch thd_cnt=%d and part_cnt = %d\n", thd_cnt, g_part_cnt)
#endif
    for ( UInt32 thread_id = 0; thread_id < thd_cnt; thread_id ++) {
        DEBUG_Q("Server-side generation for part %d ... ", thread_id);
        for (UInt32 query_id = request_cnt / g_init_parallelism * tid; query_id < final_request; query_id ++) {
#if SINGLE_NODE
            BaseQuery * query = gen->create_query(_wl,thread_id);
#else
            BaseQuery * query = gen->create_query(_wl,g_node_id);
#endif
            uint64_t qslot;
#if WORKLOAD == YCSB
            YCSBQuery * ycsb_query = (YCSBQuery*)query;
            qslot = ((YCSBWorkload *) _wl)->key_to_part(ycsb_query->requests[0]->key);
            assert(qslot == thread_id);
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
            queries[qslot][query_id] = query;

#endif
        }
        DEBUG_Q("Done\n");
    }
#else

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
    assert(server_id < size);
    uint64_t query_id = __sync_fetch_and_add(query_cnt[server_id], 1);
    if (query_id > g_max_txn_per_part) {
        __sync_bool_compare_and_swap(query_cnt[server_id], query_id + 1, 0);
        query_id = __sync_fetch_and_add(query_cnt[server_id], 1);
    }
    BaseQuery *query = queries[server_id][query_id];
    return query;
}
#endif


