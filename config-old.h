#ifndef _CONFIG_H_
#define _CONFIG_H_

/***********************************************/
// Simulation + Hardware
/***********************************************/
#define NODE_CNT 2
//#define THREAD_CNT 2 // For calvin use smaller worker thread count since sequencer + scheduler occupy 2 threads already
#define THREAD_CNT 4
#define REM_THREAD_CNT 2//THREAD_CNT
#define SEND_THREAD_CNT 2//THREAD_CNT
#define CORE_CNT 8
#define NUMA_NODE_CNT 2
// PART_CNT should be at least NODE_CNT
//#define PART_CNT (NODE_CNT*THREAD_CNT)
#define PART_CNT NODE_CNT


// TQ: since we have 20 cores per node on halstead
// With a single node used for client requests,
// it is better to assign 5 threads for each client per node.
// Assigning more threads for each client processes
// seems to lower the number of transactions submitted
// to the server
#define CLIENT_NODE_CNT 1
#define CLIENT_THREAD_CNT 4
#define CLIENT_REM_THREAD_CNT 2
#define CLIENT_SEND_THREAD_CNT 2
#define CLIENT_RUNTIME false

#define LOAD_METHOD LOAD_MAX
#define LOAD_PER_SERVER 100

// Replication
#define REPLICA_CNT 0
// AA (Active-Active), AP (Active-Passive)
#define REPL_TYPE AP

// each transaction only accesses only 1 virtual partition. But the lock/ts manager and index are not aware of such partitioning. VIRTUAL_PART_CNT describes the request distribution and is only used to generate queries. For HSTORE, VIRTUAL_PART_CNT should be the same as PART_CNT.
#define VIRTUAL_PART_CNT    PART_CNT
#define PAGE_SIZE         4096
#define CL_SIZE           64
//#define CPU_FREQ          2.0 // FOR GS32
//#define CPU_FREQ          2.5 //2.4//2.6 // FOR M64/M128
//#define CPU_FREQ            2.5// FOR m4.2xlarge
#define CPU_FREQ            3.0// FOR c4.2xlarge
// enable hardware migration.
#define HW_MIGRATE          false

// # of transactions to run for warmup
#define WARMUP            0
// YCSB or TPCC or PPS
#define WORKLOAD TPCC
// print the transaction latency distribution
#define PRT_LAT_DISTR false
#define STATS_ENABLE        true
#define PROG_STATS          false
#define TIME_ENABLE         true //STATS_ENABLE
#define ASSERT_ENABLED      true
#define NUMA_ENABLED        false
#define PROFILE_EXEC_TIMING      false

#define FIN_BY_TIME true
// Max allowed number of transactions and also controls the pool size of the transaction table
#define MAX_TXN_IN_FLIGHT BATCH_SIZE*64 // we need more inflight txns for QueCC
//#define MAX_TXN_IN_FLIGHT BATCH_SIZE*2
// TQ: this allows servers to generate transactions and avoid client-server communication overhead
// However, it have only been tested with a single server node.
// Also, there is no need to run client processes when this flag is enabled
#define SERVER_GENERATE_QUERIES false



/***********************************************/
// Memory System
/***********************************************/
// Three different memory allocation methods are supported.
// 1. default libc malloc
// 2. per-thread malloc. each thread has a private local memory
//    pool
// 3. per-partition malloc. each partition has its own memory pool
//    which is mapped to a unique tile on the chip.
#define MEM_ALLIGN          8

// [THREAD_ALLOC]
#define THREAD_ALLOC        false
#define THREAD_ARENA_SIZE     (1UL << 22)
#define MEM_PAD           true

// [PART_ALLOC]
#define PART_ALLOC          true
#define MEM_SIZE          (1UL << 30)
#define NO_FREE           false

//#define N_MALLOC

/***********************************************/
// Message Passing
/***********************************************/
#define TPORT_TYPE TCP
#define TPORT_PORT 17000
#define SET_AFFINITY true
#define SET_AFFINITY_AFTER_INIT false
#define TPORT_TYPE TCP
#define TPORT_PORT 17000

#define MAX_TPORT_NAME 128
#define MSG_SIZE 128 // in bytes
#define HEADER_SIZE sizeof(uint32_t)*2 // in bits
#define MSG_TIMEOUT 5000000000UL // in ns
#define NETWORK_TEST false
#define NETWORK_DELAY_TEST false
#define NETWORK_DELAY 0UL

#define MAX_QUEUE_LEN NODE_CNT * 2

#define PRIORITY_WORK_QUEUE false
#define PRIORITY PRIORITY_ACTIVE
#define MSG_SIZE_MAX 4096*128
//#define MSG_SIZE_MAX 4096*16
#define MSG_TIME_LIMIT 0

/***********************************************/
// Concurrency Control
/***********************************************/
// WAIT_DIE, NO_WAIT, TIMESTAMP, MVCC,OCC, CALVIN, MAAT, QUECC, DUMMY_CC, HSTORE, SILO, LADS
#define CC_ALG QUECC
#define ISOLATION_LEVEL SERIALIZABLE
#define YCSB_ABORT_MODE false

// all transactions acquire tuples according to the primary key order.
#define KEY_ORDER         false
// transaction roll back changes after abort
#define ROLL_BACK         true
// per-row lock/ts management or central lock/ts management
#define CENTRAL_MAN         false
#define BUCKET_CNT          31
#define ABORT_PENALTY 10 * 1000000UL   // in ns.
#define ABORT_PENALTY_MAX 5 * 100 * 1000000UL   // in ns.
#define BACKOFF true
// [ INDEX ]
#define ENABLE_LATCH        false
#define CENTRAL_INDEX       false
#define CENTRAL_MANAGER       false
#define INDEX_STRUCT        IDX_HASH
#define BTREE_ORDER         16

// [STORAGE]
#define INSERT_RID_BATCH_SIZE   2*1024
#define TPCC_SCHISM     false

// [TIMESTAMP]
#define TS_TWR            false
#define TS_ALLOC          TS_CAS
#define TS_BATCH_ALLOC        false
#define TS_BATCH_NUM        1
// [MVCC]
// when read/write history is longer than HIS_RECYCLE_LEN
// the history should be recycled.
#define HIS_RECYCLE_LEN       10
#define MAX_PRE_REQ         MAX_TXN_IN_FLIGHT * NODE_CNT//1024
#define MAX_READ_REQ        MAX_TXN_IN_FLIGHT * NODE_CNT//1024
#define MIN_TS_INTVL        10 * 1000000UL // 10ms
// [OCC]
#define MAX_WRITE_SET       10
#define PER_ROW_VALID       false
// [VLL]
#define TXN_QUEUE_SIZE_LIMIT    THREAD_CNT
// [CALVIN]
#define SEQ_THREAD_CNT 4

// [TICTOC, SILO, MOCC_SILO]
#define VALIDATION_LOCK				VALIDATION_LOCK_WAIT // no-wait or waiting
#define PRE_ABORT					true
#define ATOMIC_WORD					true

#define VALIDATION_LOCK_NO_WAIT   1
#define VALIDATION_LOCK_WAIT      2

#define ZERO_SEED_RAND_WL false

// [QUECC]
// PART_CNT for QUECC is based on the total number of working threads to match other approaches e.g. HSTORE
// Planner thread cnt should be greater than or equal to part_cnt
#define PLAN_THREAD_CNT THREAD_CNT
// This relates to MAX_TXN_IN_FLIGHT if we are doing a Cient-server deployment,
// For server-only deployment, this can be set to any number
// batch size must be divisible by thread_cnt and partition cnt for YCSB
// batch size must be divisible by thread_cnt for TPCC
//#define BATCH_SIZE 5*56*6*3*6 // ~30K
//#define BATCH_SIZE 8192
//#define BATCH_SIZE 8 // testing
#define BATCH_SIZE 10368
//#define BATCH_SIZE 10368*(4*NODE_CNT)
//#define BATCH_SIZE 10080
//#define BATCH_SIZE 5040
//#define BATCH_SIZE 13440
//#define BATCH_SIZE 40320*2 //lcm(2,3,4,5,6,8,9,10,12,14,15,16,18,20,24,28,32,36,48,56,64,72,96,112,128)
//#define BATCH_SIZE 40320*4 //lcm(2,3,4,5,6,8,9,10,12,14,15,16,18,20,24,28,32,36,48,56,64,72,96,112,128)
//#define BATCH_SIZE 100000
//#define BATCH_SIZE 10368
//#define BATCH_SIZE 2*3*5*7*31*2*2*2*2*2*3 // = 624960 ~ 600K txns per batch
#define BATCH_MAP_LENGTH 1//2//16//100//300//1024 // width of map is PLAN_THREAD_CNT
//#define BATCH_MAP_LENGTH 4 // width of map is PLAN_THREAD_CNT
#define BATCH_MAP_ORDER BATCH_PT_ET
#define BATCH_ET_PT     1
#define BATCH_PT_ET     2
#define BATCH_COMP_TIMEOUT 1 * 5 * MILLION // 5ms

#define CLBUF_RANDOM false

//TQ: since RIDs used for EQs are logical, we can collocate/distribute records
//TPC-C benchmark tuning
#define DISTRIBUTE_DIST_RECS false // put true for single part exps
#define COLOC_STOCK_WH true // put false for single part exps
#define COLOC_CUST_WH true // put false for single part exps
#define PLAN_NO_DIST_UPDATE_FIRST false
#define PLAN_NO_DIST_DEPS_LAST false
#define PIPELINED false

#define INIT_QUERY_MSGS false

// Controls the batching decitions in the planning phase
#define BATCHING_MODE SIZE_BASED
#define TIME_BASED 1
// IMPORTATN: For Size-based batching, BATCH_SIZE must be divisable by PLAN_THREAD_CNT
#define SIZE_BASED 2
// Split and merge config. parameters
#define SPLIT_MERGE_ENABLED true
#define SPLIT_STRATEGY  EAGER_SPLIT
// Eager split means that as soon as we go over the threashold we split the EQ
#define EAGER_SPLIT 1
// In lazy splot, we perform the splitting when we are have buffered enough operations for a batch
#define LAZY_SPLIT  2

#define MERGE_STRATEGY BALANCE_EQ_SIZE
#define BALANCE_EQ_SIZE 1
#define GREEDY_RANGE_LOCALITY 2
#define RR  3

#define QUECC_DB_ACCESS true
#define SYNC_MASTER_BATCH_CLEANUP false
#define SYNC_MASTER_RR false
#define TDG_ENTRY_TYPE  ARRAY_ENTRY
#define ARRAY_ENTRY     0
#define VECTOR_ENTRY    1

// this does not work in distributed settings
#define YCSB_INDEX_LOOKUP_PLAN false

#define CT_ENABLED false
#define EXEC_BUILD_TXN_DEPS false
#define TDG_ENTRY_LENGTH 1000
#define FREE_LIST_INITIAL_SIZE 100
#define EQ_INIT_CAP 1000
// Controls execution queue split behavior.
#define EXECQ_CAP_FACTOR 10
#define EXECQ_EXPAND_FACTOR 1
#define EXPANDABLE_EQS false
#define MIN_EXECQ_SIZE 10
#define EXEC_QS_MAX_SIZE 1024//PLAN_THREAD_`CNT*THREAD_CNT*2

#define ROW_ACCESS_TRACKING false
#define ROW_ACCESS_IN_CTX  true
#define ENABLE_EQ_SWITCH true
#define PARALLEL_COMMIT true
#define FIXED_COMMIT_THREAD_CNT true
#define COMMIT_THREAD_CNT 1
#define TXN_CNT_COMMIT_THRESHOLD (THREAD_CNT*PLAN_THREAD_CNT)*2


//#define WT_SYNC_METHOD CNT_ALWAYS_FETCH_ADD_SC
#define WT_SYNC_METHOD SYNC_BLOCK
#define CNT_ALWAYS_FETCH_ADD_SC 0
#define CAS_GLOBAL_SC    1
#define CAS_GLOBAL_ACQ_REL    2
#define CNT_FETCH_ADD_ACQ_REL    3
#define SYNC_BLOCK    4
#define SENSE_BARRIER  5

#define SYNC_AFTER_PG   false
#define NEXT_STAGE_ARRAY true

#define ATOMIC_PG_STATUS true

#define SAMPLING_FACTOR 0.0001

#define RANDOM_PLAN_DEQ false
// used for building histogram for planning
#define HIST_BUCKET_CNT 100

// Commit behavior configuration. This controls the commit during execution.
// This should be used along with the CT_ENABLED parameter, and it is only activated if
// the CT_ENABLED = TRUE
// AFTER_PG_COMP: means that ETs will wait for a PG to complete before proceeding
// AFTER_BATCH_COMP: means that ETs will wait for a batch to complete before proceeding.

#define AFTER_PG_COMP       0
#define AFTER_BATCH_COMP    1
#define IMMEDIATE           2
#define COMMIT_BEHAVIOR     AFTER_BATCH_COMP

#define SINGLE_NODE false
#define ABORT_THREAD true // if this is false, ABORT_QUEUES should be true to handle aborts
#define ABORT_QUEUES false

//
#define LADS_IN_QUECC false

// LADS
#define LADS_ACTION_BUFFER_SIZE 1024*20
#define LADS_CHAR_LEN 10
#define LADS_CHAR_CNT 1

#define LADS_READ_FUNC 0
#define LADS_WRITE_FUNC 1

#define ACTION_BUF_SIZE BATCH_SIZE*REQ_PER_QUERY

/***********************************************/
// Logging
/***********************************************/
#define LOG_COMMAND         false
#define LOG_REDO          false
#define LOGGING false
#define LOG_BUF_MAX 10
#define LOG_BUF_TIMEOUT 10 * 1000000UL // 10ms

/***********************************************/
// Benchmark
/***********************************************/
#define SIM_FULL_ROW true

// max number of rows touched per transaction
#define MAX_ROW_PER_TXN       64
#define QUERY_INTVL         1UL
/**
 * TQ: MAX_TXN_PER_PART indicate the maximum number of "unique" transactions instances to be
 * generated per partition during client setup, which will be stored in an in-memory vector.
 * During the run phase, client worker threads will take one transaction at a time and send it to the server
 * If this number is exhausted during the run, client threads will loop over from the start.
 */
//#define MAX_TXN_PER_PART    500000
//#define MAX_TXN_PER_PART    15625
#define MAX_TXN_PER_PART    32 //debugging
//#define MAX_TXN_PER_PART    (100000/PART_CNT)
//#define MAX_TXN_PER_PART    (BATCH_SIZE/PLAN_THREAD_CNT) // ensures that batch_size == tital number of transactions
//#define MAX_TXN_PER_PART    (BATCH_SIZE/PART_CNT) // ensures that batch_size == tital number of transactions
//#define MAX_TXN_PER_PART    (0.1/PART_CNT) * MILLION
#define FIRST_PART_LOCAL      true
#define MAX_TUPLE_SIZE        1024 // in bytes
#define GEN_BY_MPR false
// ==== [YCSB] ====
// SKEW_METHOD:
//    ZIPF: use ZIPF_THETA distribution
//    HOT: use ACCESS_PERC of the accesses go to DATA_PERC of the data
#define SKEW_METHOD ZIPF
#define DATA_PERC 100
//#define ACCESS_PERC 0.03
#define ACCESS_PERC 100
#define INIT_PARALLELISM    4//THREAD_CNT
#define RANGE_PARITIONING true
//#define SYNTH_TABLE_SIZE 1024
//#define SYNTH_TABLE_SIZE  65536*NODE_CNT
//#define SYNTH_TABLE_SIZE   10*1048576
//#define SYNTH_TABLE_SIZE 16777216 // 16M recs
#define SYNTH_TABLE_SIZE 16783200*NODE_CNT // ~16M recs so that it is divisiable by different part_cnt values
//#define SYNTH_TABLE_SIZE 1191*13440 // ~16M recs so that it is divisiable by different part_cnt values
//#define SYNTH_TABLE_SIZE 416*BATCH_SIZE // ~16M recs so that it is divisiable by different part_cnt values
//#define SYNTH_TABLE_SIZE 16777152 // 16GB ~16M with 1K recs so that it is divisiable by different batch sizes values
//#define SYNTH_TABLE_SIZE 167771520 // 16GB ~16M with 100B recs so that it is divisiable by different batch sizes values
#define ZIPF_THETA 0.0//0.3 0.0 -> Uniform
#define WRITE_PERC 0.5
#define TXN_WRITE_PERC WRITE_PERC
#define TUP_WRITE_PERC WRITE_PERC
#define SCAN_PERC           0
#define SCAN_LEN          20
// We should be able to control multi-partition transactions using this.
// Setting this to PART_CNT means that all transactions will access all partitions
#define PART_PER_TXN PART_CNT
#define PERC_MULTI_PART MPR // Use MPR config parameter
#define REQ_PER_QUERY 10
#define FIELD_PER_TUPLE       10
#define YCSB_DO_OPERATION true
// Use this to only generate transactions
#define CREATE_TXN_FILE false
#define STRICT_PPT true
// Pick partitions according to Zipfian distribution
#define PART_ZIPF false
#define RECORD_ZIPF false // false: partitions are uniformally selected, records are zipf with a partition
// ==== [TPCC] ====
// For large warehouse count, the tables do not fit in memory
// small tpcc schemas shrink the table size.
#define TPCC_SMALL          true
#define MAX_ITEMS_SMALL 10000
#define CUST_PER_DIST_SMALL 2000
#define MAX_ITEMS_NORM 100000
#define CUST_PER_DIST_NORM 3000
#define MAX_ITEMS_PER_TXN 15
// Some of the transactions read the data but never use them.
// If TPCC_ACCESS_ALL == fales, then these parts of the transactions
// are not modeled.
#define TPCC_ACCESS_ALL       false
#define WH_UPDATE         true
//#define NUM_WH (4*NODE_CNT)
#define NUM_WH (THREAD_CNT*NODE_CNT)
// % of transactions that access multiple partitions
#define MPR 0.15 // used for TPCC and YCSB
#define MPIR 0.01
#define MPR_NEWORDER      20 // In %
enum TPCCTable {TPCC_WAREHOUSE,
	TPCC_DISTRICT,
	TPCC_CUSTOMER,
	TPCC_HISTORY,
	TPCC_NEWORDER,
	TPCC_ORDER,
	TPCC_ORDERLINE,
	TPCC_ITEM,
	TPCC_STOCK};
enum TPCCTxnType {TPCC_ALL,
	TPCC_PAYMENT,
	TPCC_NEW_ORDER,
	TPCC_ORDER_STATUS,
	TPCC_DELIVERY,
	TPCC_STOCK_LEVEL};
extern TPCCTxnType          g_tpcc_txn_type;

//#define TXN_TYPE          TPCC_ALL
#define PERC_PAYMENT 0.0 // percentage of payment transactions in the workload
#define TPCC_PAYMENT_H_INSERT_ENABLED true
#define FIRSTNAME_MINLEN      8
#define FIRSTNAME_LEN         16
#define LASTNAME_LEN        16

#define DIST_PER_WH       10
#define NEWORDER_INSERT_ENABLED false
#define PAYMENT_INSERT_ENABLED false

// PPS (Product-Part-Supplier)
#define MAX_PPS_PARTS_PER 10
#define MAX_PPS_PART_KEY 10000
#define MAX_PPS_PRODUCT_KEY 1000
#define MAX_PPS_SUPPLIER_KEY 1000
#define MAX_PPS_PART_PER_PRODUCT 10
#define MAX_PPS_PART_PER_SUPPLIER 10
#define MAX_PPS_PART_PER_PRODUCT_KEY 10
#define MAX_PPS_PART_PER_SUPPLIER_KEY 10

#define PERC_PPS_GETPART 0.00
#define PERC_PPS_GETSUPPLIER 0.00
#define PERC_PPS_GETPRODUCT 0.0
#define PERC_PPS_GETPARTBYSUPPLIER 0.0
#define PERC_PPS_GETPARTBYPRODUCT 0.2
#define PERC_PPS_ORDERPRODUCT 0.6
#define PERC_PPS_UPDATEPRODUCTPART 0.2
#define PERC_PPS_UPDATEPART 0.0

enum PPSTxnType {PPS_ALL = 0,
	PPS_GETPART,
	PPS_GETSUPPLIER,
	PPS_GETPRODUCT,
	PPS_GETPARTBYSUPPLIER,
	PPS_GETPARTBYPRODUCT,
	PPS_ORDERPRODUCT,
	PPS_UPDATEPRODUCTPART,
	PPS_UPDATEPART
};

/***********************************************/
// DEBUG info
/***********************************************/
#define WL_VERB           true
#define IDX_VERB          false
#define VERB_ALLOC          true

#define DEBUG_LOCK          false
#define DEBUG_TIMESTAMP       false
#define DEBUG_SYNTH         false
#define DEBUG_ASSERT        false
#define DEBUG_DISTR false
#define DEBUG_ALLOC false
#define DEBUG_RACE false
#define DEBUG_TIMELINE        false
#define DEBUG_BREAKDOWN       false
#define DEBUG_LATENCY       false

// For QueCC
#define DEBUG_QUECC false
// FOr Workload Debugging
#define DEBUG_WLOAD false

/***********************************************/
// MODES
/***********************************************/
// QRY Only do query operations, no 2PC
// TWOPC Only do 2PC, no query work
// SIMPLE Immediately send OK back to client
// NOCC Don't do CC
// NORMAL normal operation
// FIXED_MODE : runs a fixed number of transactions through the system, and computes the throughput based
// on the total. Currently, only QUECC is supported.
// TODO(tq): support other CC_ALGs
#define MODE NORMAL_MODE
//#define MODE FIXED_MODE
//#define SIM_BATCH_CNT (100*MAX_TXN_PER_PART)/BATCH_SIZE
//#define SIM_BATCH_CNT 1000/BATCH_SIZE
#define SIM_BATCH_CNT 1000
//#define SIM_BATCH_CNT 12000
//#define SIM_TXN_CNT 1024*1024*60 // 60 Million transactions per server
//#define SIM_TXN_CNT_WARMUP 1024*1024*30 // 60 Million transactions per server
#define SIM_TXN_CNT BATCH_SIZE*2 // 60 Million transactions per server
#define SIM_TXN_CNT_WARMUP BATCH_SIZE*1 // 60 Million transactions per server
#define COUNT_BASED_SIM_ENABLED false

/***********************************************/
// Constant
/***********************************************/
// INDEX_STRUCT
#define IDX_HASH          1
#define IDX_BTREE         2
#define IDX_HASH_SIMPLE   3

// WORKLOAD
#define YCSB            1
#define TPCC            2
#define PPS             3
#define TEST            4
// Concurrency Control Algorithm
#define DUMMY_CC         -1
#define NO_WAIT           1
#define WAIT_DIE          2
#define DL_DETECT         3
#define TIMESTAMP         4
#define MVCC            5
#define HSTORE            6
#define HSTORE_SPEC           7
#define OCC             8
#define VLL             9
#define CALVIN      10
#define MAAT      11
#define WDL           12
#define QUECC 13
#define SILO 14
#define MOCC_SILO 15
#define LADS 16
#define TICTOC 17
// TIMESTAMP allocation method.
#define TS_MUTEX          1
#define TS_CAS            2
#define TS_HW           3
#define TS_CLOCK          4
// MODES
// NORMAL < NOCC < QRY_ONLY < SETUP < SIMPLE
#define NORMAL_MODE 1
#define NOCC_MODE 2
#define QRY_ONLY_MODE 3
#define SETUP_MODE 4
#define SIMPLE_MODE 5
#define FIXED_MODE 6
// SKEW METHODS
#define ZIPF 1
#define HOT 2
// PRIORITY WORK QUEUE
#define PRIORITY_FCFS 1
#define PRIORITY_ACTIVE 2
#define PRIORITY_HOME 3
// Replication
#define AA 1
#define AP 2
// Load
#define LOAD_MAX 1
#define LOAD_RATE 2
// Transport
#define TCP 1
#define IPC 2
// Isolation levels
#define SERIALIZABLE 1
#define READ_COMMITTED 2
#define READ_UNCOMMITTED 3
#define NOLOCK 4

// Stats and timeout
#define BILLION 1000000000UL // in ns => 1 second
#define MILLION 1000000UL // in ns => 1 second
#define STAT_ARR_SIZE 1024
#define PROG_TIMER 10 * BILLION // in s
#define BATCH_TIMER 0
#define SEQ_BATCH_TIMER 5 * 1 * MILLION // ~5ms -- same as CALVIN paper
#define DONE_TIMER 1 * 60 * BILLION // ~1 minutes
#define WARMUP_TIMER 1 * 60 * BILLION // ~1 minutes
//#define DONE_TIMER 1 * 10 * BILLION // debugging
//#define WARMUP_TIMER 1 * 1 * BILLION // debugging

#define SEED 0
#define SHMEM_ENV false
#define ENVIRONMENT_EC2 false

#endif