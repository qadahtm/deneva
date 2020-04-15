![](https://github.com/msadoghi/ExpoDB-Platform/workflows/BuildAndTest/badge.svg)

SETUP
-----------------------------
== Dependencies ==
nanomsg (nanomsg.org)
jemalloc (www.canonware.com/jemalloc)


HOW TO RUN
-----------------------------

Create a file called ifconfig.txt with IP addresses for the servers and clients, one per line.

$ make
$ ./rundb -nid[unique server ID] &
$ ./runcl -nid[unique client ID] &

DBMS BENCHMARK
------------------------------

== General Features ==

  ddbms is a OLTP database benchmark with the following features.
  
  1. Seven different concurrency control algorithms are supported.
	NO\_WAIT[1]		: no wait two phase locking
	WAIT\_DIE[1]		: wait and die two phase locking
	TIMESTAMP[1]	: basic T/O
	MVCC[2]			: multi-version T/O
	OCC[3]			: optimistic concurrency control (MaaT)
	CALVIN[4]		: Calvin

  [1] Phlip Bernstein, Nathan Goodman, "Concurrency Control in Distributed Database Systems", Computing Surveys, June 1981
  [2] Phlip Bernstein, Nathan Goodman, "Multiversion Concurrency Control -- Theory and Algorithms", Transactions on Database Systems, Dec 1983
  [3] H. Mahmoud et al, "MaaT: Effective and Scalable Coordination of Distributed Transactions in the Cloud", VLDB 2014
  [4] A. Thomson et al, "Calvin: Fast Distributed Transactions for Partitioned Database Systems", SIGMOD 2012
	
  2. Three benchmarks are supported. 
    2.1 YCSB[5]
		2.2 TPCC[6] 
			Only Payment and New Order transactions are modeled. 
		2.2 Product-Parts-Supplier (PPS)
	
  [5] B. Cooper et al, "Benchmarking Cloud Serving Systems with YCSB", SoCC 201
  [6] http://www.tpc.org/tpcc/ 

== Config File ==

dbms benchmark has the following parameters in the config file. Parameters with a * sign should not be changed.

-  CORE\_CNT		: number of cores modeled in the system.
-  NODE\_CNT		: number of computation nodes modeled in the system
-  PART\_CNT		: number of logical partitions in the system; Should equal CORE\_CNT * NODE\_CNT
- THREAD\_CNT	: number of worker threads 
-  REM\_THREAD\_CNT	: number of message receiver threads 
-  SEND\_THREAD\_CNT	: number of message sender threads 
-  TPORT\_TYPE : communication protocol (TCP, IPC)
-  ENVIRONMENT\_EC2 : true if running on Amazon EC2

-  PAGE\_SIZE		: memory page size
-  CL\_SIZE		: cache line size

-  DONE\_TIMER : number of nanoseconds to run experiment
-  WARMUP\_TIMER		: number of nanoseconds to run for warmup
-  SEQ\_BATCH\_TIMER : number of nanoseconds between CALVIN batches
-  WORKLOAD		: workload supported (TPCC, YCSB, or PPS)
-  CC\_ALG		: concurrency control algorithm (WAIT\_DIE, NO\_WAIT, TIMESTAMP, MVCC, CALVIN, or MAAT)
-  ISOLATION\_LEVEL : system isolation level (SERIALIZABLE, READ\_COMMITTED, READ\_UNCOMMITTED, or NOLOCK)
  
-  THREAD\_ALLOC	: per thread allocator. 
-  \* MEM\_PAD		: enable memory padding to avoid false sharing.
-  MEM\_ALLIGN	: allocated blocks are alligned to MEM\_ALLIGN bytes

-  \* ROLL\_BACK		: roll back the modifications if a transaction aborts.
  
-  ENABLE\_LATCH  : enable latching in btree index
-  \* CENTRAL\_INDEX : centralized index structure
-  \* CENTRAL\_MANAGER	: centralized lock/timestamp manager
-  INDEX\_STRCT	: data structure for index. 
-  BTREE\_ORDER	: fanout of each B-tree node

-  TS\_TWR		: enable Thomas Write Rule (TWR) in TIMESTAMP
-  HIS\_RECYCLE\_LEN	: in MVCC, history will be recycled if they are too long.
-  MAX\_WRITE\_SET	: the max size of a write set in OCC.

-  MAX\_ROW\_PER\_TXN	: max number of rows touched per transaction.
-  QUERY\_INTVL	: the rate at which database queries come
-  MAX\_TXN\_PER\_PART	: maximum transactions to run per partition.
  
  // for YCSB Benchmark
-  SYNTH\_TABLE\_SIZE	: table size
-  ZIPF\_THETA	: theta in zipfian distribution (rows accessed follow zipfian distribution)
-  READ\_PERC		:
-  WRITE\_PERC	:
-  SCAN\_PERC		: percentage of read/write/scan queries. they should add up to 1.
-  SCAN\_LEN		: number of rows touched per scan query.
-  PART\_PER\_TXN	: number of logical partitions to touch per transaction
-  PERC\_MULTI\_PART	: percentage of multi-partition transactions
-  REQ\_PER\_QUERY	: number of queries per transaction
  FIRST\_PART\_LOCAL	: with this being true, the first touched partition is always the local partition.
  
  // for TPCC Benchmark
-  NUM\_WH		: number of warehouses being modeled.
-  PERC\_PAYMENT	: percentage of payment transactions.
-  DIST\_PER\_WH	: number of districts in one warehouse
-  MAX\_ITEMS		: number of items modeled.
-  CUST\_PER\_DIST	: number of customers per district
-  ORD\_PER\_DIST	: number of orders per district
-  FIRSTNAME\_LEN	: length of first name
-  MIDDLE\_LEN	: length of middle name
-  LASTNAME\_LEN	: length of last name

  // for PPS Benchmark
-	MAX\_PPS\_PART\_KEY : number of parts in the parts table
-	MAX\_PPS\_PRODUCT\_KEY :  number of products in the products table
-	MAX\_PPS\_SUPPLIER\_KEY :  number of suppliers in the suppliers table
-	MAX\_PPS\_PARTS\_PER : number of parts per product or supplier
