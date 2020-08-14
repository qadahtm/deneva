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

#ifndef _GLOBAL_H_
#define _GLOBAL_H_

#define __STDC_LIMIT_MACROS
#include <stdint.h>
#include <unistd.h>
#include <cstddef>
#include <cstdlib>
#include <cassert>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <string.h>
#include <typeinfo>
#include <list>
#include <map>
#include <set>
#include <queue>
#include <string>
#include <vector>
#include <sstream>
#include <time.h> 
#include <sys/time.h>
#include <math.h>
#include <atomic>

#include "pthread.h"
#include "config.h"
#include "stats.h"
//#include "work_queue.h"
#include "pool.h"
#include "txn_table.h"
#include "logger.h"
#include "sim_manager.h"
#include "smanager.h"
//#include "maat.h"

#ifdef __APPLE__
#include "pthread_mac.h"
#endif

using namespace std;

class mem_alloc;
class Stats;
class SimManager;
class Manager;
class Query_queue;
class OptCC;
class Maat;
class Transport;
class Remote_query;
class TxnManPool;
class TxnPool;
class AccessPool;
class TxnTablePool;
class MsgPool;
class RowPool;
class QryPool;
class TxnTable;
class QWorkQueue;
class AbortQueue;
class MessageQueue;
class Client_query_queue;
class Client_txn;
class Sequencer;
class Logger;
class TimeTable;
// for Hstore
class Plock;
// for QueCC
//#if CC_ALG == QUECC
class QueCCPool;
#define PG_TXN_CTX_SIZE (BATCH_SIZE/(PLAN_THREAD_CNT*NODE_CNT))
//#endif
//#if CC_ALG == LADS
namespace gdgcc {
	class ConfigInfo;
	class SyncWorker;
	class ActionDependencyGraph;
	class ActionBuffer;
	class DepGraph;
}
//#endif

typedef uint32_t UInt32;
typedef int32_t SInt32;
typedef uint64_t UInt64;
typedef int64_t SInt64;

typedef uint64_t ts_t; // time stamp type

/******************************************/
// Global Data Structure 
/******************************************/
extern mem_alloc mem_allocator;
extern Stats stats;
extern SimManager * simulation;
extern Manager glob_manager;
extern Query_queue query_queue;
extern Client_query_queue client_query_queue;
extern OptCC occ_man;
extern Maat maat_man;
extern Transport tport_man;
extern TxnManPool txn_man_pool;
extern TxnPool txn_pool;
extern AccessPool access_pool;
extern TxnTablePool txn_table_pool;
extern MsgPool msg_pool;
extern RowPool row_pool;
extern QryPool qry_pool;
extern TxnTable txn_table;
extern QWorkQueue work_queue;
extern AbortQueue abort_queue;
extern MessageQueue msg_queue;
extern Client_txn client_man;
extern Sequencer seq_man;
extern Logger logger;
extern TimeTable time_table;
extern RIDMgr rid_man;

// For Hstore
#if CC_ALG == HSTORE
extern Plock part_lock_man;
#endif
// for QueCC
#if CC_ALG == QUECC
extern QueCCPool quecc_pool;
#endif
#if CC_ALG == LADS || LADS_IN_QUECC
extern gdgcc::ConfigInfo* configinfo;
extern gdgcc::SyncWorker* sync_worker;
extern gdgcc::ActionDependencyGraph** dgraphs;
extern gdgcc::ActionBuffer* action_allocator;
extern gdgcc::DepGraph * global_dgraph;
#endif

extern bool volatile warmup_done;
extern bool volatile enable_thread_mem_pool;
extern pthread_barrier_t warmup_bar;
/******************************************/
// Client Global Params 
/******************************************/
extern UInt32 g_client_thread_cnt;
extern UInt32 g_client_rem_thread_cnt;
extern UInt32 g_client_send_thread_cnt;
extern UInt32 g_client_node_cnt;
extern UInt32 g_servers_per_client;
extern UInt32 g_clients_per_server;
extern UInt32 g_server_start_node;

/******************************************/
// Global Parameter
/******************************************/
extern volatile UInt64 g_row_id;
extern bool g_part_alloc;
extern bool g_mem_pad;
extern bool g_prt_lat_distr;
extern UInt32 g_node_id;
extern UInt32 g_node_cnt;
extern UInt32 g_part_cnt;
extern UInt32 g_virtual_part_cnt;
extern UInt32 g_core_cnt;
extern UInt32 g_total_node_cnt;
extern UInt32 g_total_thread_cnt;
extern UInt32 g_total_client_thread_cnt;
extern UInt32 g_this_thread_cnt;
extern UInt32 g_this_rem_thread_cnt;
extern UInt32 g_this_send_thread_cnt;
extern UInt32 g_this_total_thread_cnt;
extern UInt32 g_thread_cnt;
#if CC_ALG == QUECC
extern UInt32 g_et_thd_cnt;
extern UInt32 g_cluster_worker_thread_cnt;
extern UInt32 g_cluster_planner_thread_cnt;
#endif
extern UInt32 g_abort_thread_cnt;
extern UInt32 g_logger_thread_cnt;
extern UInt32 g_send_thread_cnt;
extern UInt32 g_rem_thread_cnt;
extern ts_t g_abort_penalty; 
extern ts_t g_abort_penalty_max; 
extern bool g_central_man;
extern UInt32 g_ts_alloc;
extern bool g_key_order;
extern bool g_ts_batch_alloc;
extern UInt32 g_ts_batch_num;
extern int32_t g_inflight_max;
extern uint64_t g_msg_size;
extern uint64_t g_log_buf_max;
extern uint64_t g_log_flush_timeout;

extern UInt32 g_max_txn_per_part;
extern int32_t g_load_per_server;

extern bool g_hw_migrate;
extern UInt32 g_network_delay;
extern UInt64 g_done_timer;
extern UInt64 g_batch_time_limit;
extern UInt64 g_seq_batch_time_limit;
extern UInt64 g_prog_timer;
extern UInt64 g_warmup_timer;
extern UInt64 g_msg_time_limit;

// MVCC
extern UInt64 g_max_read_req;
extern UInt64 g_max_pre_req;
extern UInt64 g_his_recycle_len;

// YCSB
extern UInt32 g_cc_alg;
extern ts_t g_query_intvl;
extern UInt32 g_part_per_txn;
extern double g_perc_multi_part;
extern double g_txn_read_perc;
extern double g_txn_write_perc;
extern double g_tup_read_perc;
extern double g_tup_write_perc;
extern double g_zipf_theta;
extern double g_data_perc;
extern double g_access_perc;
extern UInt64 g_synth_table_size;
extern UInt32 g_req_per_query;
extern bool g_strict_ppt;
extern UInt32 g_field_per_tuple;
extern UInt32 g_init_parallelism;
extern double g_mpr;
extern double g_mpitem;

// TPCC
extern UInt32 g_num_wh;
extern double g_perc_payment;
extern bool g_wh_update;
extern char * output_file;
extern char * input_file;
extern char * txn_file;
extern UInt32 g_max_items;
extern UInt32 g_dist_per_wh;
extern UInt32 g_cust_per_dist;
extern UInt32 g_max_items_per_txn;

// PPS (Product-Part-Supplier)
extern UInt32 g_max_parts_per;
extern UInt32 g_max_part_key;
extern UInt32 g_max_product_key;
extern UInt32 g_max_supplier_key;
extern double g_perc_getparts;
extern double g_perc_getproducts;
extern double g_perc_getsuppliers;
extern double g_perc_getpartbyproduct;
extern double g_perc_getpartbysupplier;
extern double g_perc_orderproduct;
extern double g_perc_updateproductpart;
extern double g_perc_updatepart;

// CALVIN
extern UInt32 g_seq_thread_cnt;

#if REPLICATION_ENABLED
extern bool is_leader;
#endif

// QUECC
#if CC_ALG == QUECC || CC_ALG == LADS
extern const UInt32 g_plan_thread_cnt;
extern const UInt32 g_cluster_plan_thread_cnt;
extern UInt32 g_batch_size;
extern UInt32 g_exec_qs_max_size;
// for circular array buffer for batch completeiton time
extern const UInt32 g_batch_map_length;
#endif
// Replication
extern UInt32 g_repl_type;
extern UInt32 g_repl_cnt;

enum RC { RCOK=0, Commit, Abort, WAIT, WAIT_REM, ERROR, FINISH, NONE, Committed, Aborted };

#if CC_ALG == QUECC
enum SRC { SUCCESS=0, BREAK, BATCH_READY, BATCH_WAIT, FATAL_ERROR };
#endif


enum RemReqType {INIT_DONE=0,
    RLK, //1
    RULK, //2
    CL_QRY,//3 //TQ: Message type for Client Query or transaction
    RQRY,//4 //TQ: Remote Query
    RQRY_CONT,//5
    RFIN,//6
    RLK_RSP,//7
    RULK_RSP,//8
    RQRY_RSP,//9
    RACK,//10
    RACK_PREP,//11
    RACK_FIN,//12
    RTXN,//13
    RTXN_CONT,//14
    RINIT,//15
    RPREPARE,//16
    RPASS,//17
    RFWD,//18
    RDONE,//19
    CL_RSP,//20 //TQ: Client response
//#if CC_ALG == QUECC
	REMOTE_EQ,//21
	REMOTE_EQ_SET,//21
	REMOTE_EQ_ACK,//22
	REMOTE_OP_ACK,//23
//#endif
    LOG_MSG,//24
    LOG_MSG_RSP,//25
    LOG_FLUSHED,//26
    CALVIN_ACK,//27
    NO_MSG};//28

// Calvin
enum CALVIN_PHASE {CALVIN_RW_ANALYSIS=0,CALVIN_LOC_RD,CALVIN_SERVE_RD,CALVIN_COLLECT_RD,CALVIN_EXEC_WR,CALVIN_DONE};

/* Thread */
typedef uint64_t txnid_t;

/* Txn */
typedef uint64_t txn_t;

/* Table and Row */
typedef uint64_t rid_t; // row id
typedef uint64_t pgid_t; // page id



/* INDEX */
enum latch_t {LATCH_EX, LATCH_SH, LATCH_NONE};
// accessing type determines the latch type on nodes
enum idx_acc_t {INDEX_INSERT, INDEX_READ, INDEX_NONE};
typedef uint64_t idx_key_t; // key id for index
typedef uint64_t (*func_ptr)(idx_key_t);	// part_id func_ptr(index_key);

/* general concurrency control */
enum access_t {RD, WR, XP, SCAN};
/* LOCK */
enum lock_t {LOCK_EX = 0, LOCK_SH, LOCK_NONE };
/* TIMESTAMP */
enum TsType {R_REQ = 0, W_REQ, P_REQ, XP_REQ}; 

#define GET_THREAD_ID(id)	(id % g_thread_cnt)
#define GET_NODE_ID(id)	(id % g_node_cnt)
#define GET_PART_ID(t,n)	(n) 
#define GET_PART_ID_FROM_IDX(idx)	(g_node_id + idx * g_node_cnt) 
#define GET_PART_ID_IDX(p)	(p / g_node_cnt) 
#define ISSERVER (g_node_id < g_node_cnt)
#define ISSERVERN(id) (id < g_node_cnt)
#define ISCLIENT (g_node_id >= g_node_cnt && g_node_id < g_node_cnt + g_client_node_cnt)
#define ISREPLICA (g_node_id >= g_node_cnt + g_client_node_cnt && g_node_id < g_node_cnt + g_client_node_cnt + g_repl_cnt * g_node_cnt)
#define ISREPLICAN(id) (id >= g_node_cnt + g_client_node_cnt && id < g_node_cnt + g_client_node_cnt + g_repl_cnt * g_node_cnt)
#define ISCLIENTN(id) (id >= g_node_cnt && id < g_node_cnt + g_client_node_cnt)
#define IS_LOCAL(tid) (tid % g_node_cnt == g_node_id || CC_ALG == CALVIN || CC_ALG == QUECC)
#define IS_REMOTE(tid) (tid % g_node_cnt != g_node_id || CC_ALG == CALVIN)
#define IS_LOCAL_KEY(key) (key % g_node_cnt == g_node_id)

/*
#define GET_THREAD_ID(id)	(id % g_thread_cnt)
#define GET_NODE_ID(id)	(id / g_thread_cnt)
#define GET_PART_ID(t,n)	(n*g_thread_cnt + t) 
*/

#define MSG(str, args...) { \
	printf("[%s : %d] " str, __FILE__, __LINE__, args); } \
//	printf(args); }

// principal index structure. The workload may decide to use a different 
// index structure for specific purposes. (e.g. non-primary key access should use hash)
#if (INDEX_STRUCT == IDX_BTREE)
#define INDEX   index_btree
#elif (INDEX_STRUCT == IDX_HASH_SIMPLE)
#define INDEX   IndexHashSimple
#else  // IDX_HASH
#define INDEX   IndexHash
#endif

/************************************************/
// constants
/************************************************/
#ifndef UINT64_MAX
#define UINT64_MAX 		18446744073709551615UL
#endif // UINT64_MAX

#endif

/************************************************/
// constants for QueCC
/************************************************/

// Batch partition state
#define AVAILABLE   0
#define WORKING     1
#define COMPLETED   2

// Transaction states
#define TXN_INITIALIZED 0
#define TXN_STARTED 1
#define TXN_READY_TO_COMMIT 2
#define TXN_READY_TO_ABORT 3
#define TXN_COMMITTED  4
#define TXN_ABORTED  5