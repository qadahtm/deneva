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

#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"
#include "lads.h"

#if WORKLOAD == YCSB

atomic<UInt32> YCSBWorkload::next_tid;

RC YCSBWorkload::init() {
	Workload::init();
//	next_tid = 0;
	next_tid.store(0);
	char * cpath = getenv("SCHEMA_PATH");
	string path;
	if (cpath == NULL) 
		path = "./benchmarks/YCSB_schema.txt";
	else { 
		path = string(cpath);
		path += "YCSB_schema.txt";
		//path += "/tests/apps/dbms/YCSB_schema.txt";
	}
  printf("Initializing schema... ");
  fflush(stdout);
	init_schema( path.c_str() );
  printf("Done\n");
	
  printf("Initializing table... ");
  fflush(stdout);
	init_table_parallel();
  printf("Done\n");
  fflush(stdout);
//	init_table();
	return RCOK;
}

RC YCSBWorkload::init_schema(const char * schema_file) {
	Workload::init_schema(schema_file);
	the_table = tables["MAIN_TABLE"]; 	
	the_index = indexes["MAIN_INDEX"];
	return RCOK;
}
#if CC_ALG == LADS || LADS_IN_QUECC
RC YCSBWorkload::resolve_txn_dependencies(Message* msg, transaction_context * tctx, uint64_t cid){
	YCSBClientQueryMessage* ycsb_msg = (YCSBClientQueryMessage *) msg;

	//there are no logical dependency in YCSB
	gdgcc::Action* tmpAction = nullptr;
	for(uint64_t i=0; i<ycsb_msg->requests.size(); i++) {
		action_allocator->get(cid, tmpAction);
		ycsb_request* req = ycsb_msg->requests.get(i);
		if (req->acctype == RD){
			tmpAction->setFuncId(LADS_READ_FUNC);
		}
		else if (req->acctype == WR){
			tmpAction->setFuncId(LADS_WRITE_FUNC);
		}
		else{
			M_ASSERT_V(false, "Only read and write functions are supported in LADS\n")
		}
        tmpAction->setKey(req->key);
        tmpAction->req->copy(req);

#if LADS_IN_QUECC
		global_dgraph->addAction(cid, req->key, tmpAction);
		tctx->txn_comp_cnt.fetch_add(1,memory_order_acq_rel);
		tmpAction->setTxnId(tctx->txn_id);
		tmpAction->setTxnContext(tctx);
#else
		dgraphs[cid]->addActionToGraph(req->key, tmpAction);
#endif
	}
	return RCOK;
}
#endif

int 
YCSBWorkload::key_to_part(uint64_t key) {
#if RANGE_PARITIONING
	uint64_t rows_per_part = g_synth_table_size / g_part_cnt;
	return (key / rows_per_part) % g_part_cnt;
#else
	//TQ: it is assumed that NODE_CNT==PART_CNT
	// for QUECC, we further partition within a single node
#if CC_ALG == QUECC
	return key % (g_cluster_worker_thread_cnt);
#else
	return key % g_part_cnt;
#endif
#endif
}

RC YCSBWorkload::init_table() {
	RC rc;
    uint64_t total_row = 0;
    while (true) {
    	for (UInt32 part_id = 0; part_id < g_part_cnt; part_id ++) {
            if (total_row > g_synth_table_size)
                goto ins_done;
            // Assumes striping of partitions to nodes
            if(g_part_cnt % g_node_cnt != g_node_id) {
              total_row++;
              continue;
            }
            row_t * new_row = NULL;
			uint64_t row_id = rid_man.next_rid(0);
            rc = the_table->get_new_row(new_row, part_id, row_id); 
            // insertion of last row may fail after the table_size
            // is updated. So never access the last record in a table
			assert(rc == RCOK);
//			uint64_t value = rand();
			uint64_t primary_key = total_row;
			new_row->set_primary_key(primary_key);
            new_row->set_value(0, &primary_key);
			Catalog * schema = the_table->get_schema();
			for (UInt32 fid = 0; fid < schema->get_field_cnt(); fid ++) {
				int field_size = schema->get_field_size(fid);
				char value[field_size];
				for (int i = 0; i < field_size; i++) 
					value[i] = (char)rand() % (1<<8) ;
				new_row->set_value(fid, value);
			}
            itemid_t * m_item = (itemid_t *) mem_allocator.alloc( sizeof(itemid_t));
			assert(m_item != NULL);
            m_item->type = DT_row;
            m_item->location = new_row;
            m_item->valid = true;
            uint64_t idx_key = primary_key;
            rc = the_index->index_insert(idx_key, m_item, part_id);
            assert(rc == RCOK);
            total_row ++;
        }
    }
ins_done:
    printf("[YCSB] Table \"MAIN_TABLE\" initialized.\n");
    return RCOK;

}

// init table in parallel
void YCSBWorkload::init_table_parallel() {
	enable_thread_mem_pool = true;
	pthread_t * p_thds = new pthread_t[g_init_parallelism - 1];
#if NUMA_ENABLED
	cpu_set_t cpus;
#endif
	pthread_attr_t attr;
	pthread_attr_init(&attr);

	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) {
#if NUMA_ENABLED
		CPU_ZERO(&cpus);
		CPU_SET((i+1), &cpus);
		pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
		c_thd_args_t * args = (c_thd_args_t *) mem_allocator.alloc(sizeof(c_thd_args_t));
		args->wl = this;
		args->thd_id = (i+1);
		pthread_create(&p_thds[i], NULL, threadInitTable, args);
		pthread_setname_np(p_thds[i], "wl");
#else
		pthread_create(&p_thds[i], NULL, threadInitTable, this);
		pthread_setname_np(p_thds[i], "wl");
#endif

	}
#if NUMA_ENABLED
	CPU_ZERO(&cpus);
	CPU_SET(0, &cpus);
	pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpus);
	c_thd_args_t * args = (c_thd_args_t *) mem_allocator.alloc(sizeof(c_thd_args_t));
	args->wl = this;
	args->thd_id = 0;
	threadInitTable(args);
#else
	threadInitTable(this);
#endif


	for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
		int rc = pthread_join(p_thds[i], NULL);
		//printf("thread %d complete\n", i);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
	}
	enable_thread_mem_pool = false;
}
#if NUMA_ENABLED
void *YCSBWorkload::init_table_slice(uint64_t thd_id, int node) {
	UInt32 tid = thd_id;
	RC rc;
//	assert(g_synth_table_size % g_init_parallelism == 0);
	M_ASSERT_V(tid < g_init_parallelism, "tid = %d, g_init_parallelism = %d\n", tid, g_init_parallelism);
	uint64_t key_cnt = 0;

	uint64_t slice_size = g_synth_table_size / g_init_parallelism;

	printf("Thd %d: slice size = %ld, inserting from %ld, to %ld\n", tid, slice_size, slice_size * tid, (slice_size * (tid+1))-1);
	uint64_t slice_end = slice_size * (tid + 1);
	if (tid == g_init_parallelism-1){
		slice_end = g_synth_table_size;
	}
	for (uint64_t key = slice_size * tid;
		 key < slice_end;
		 key++
			) {
#if !SERVER_GENERATE_QUERIES
		if(GET_NODE_ID(key_to_part(key)) != g_node_id) {
		  ++key;
		  continue;
		}
#endif

		++key_cnt;
		if (key_cnt % 500000 == 0) {
			printf("Thd %d inserted %ld keys %f\n", tid, key_cnt, simulation->seconds_from_start(get_sys_clock()));
		}
//		printf("tid=%d. key=%ld\n", tid, key);
		row_t *new_row = NULL;
		uint64_t row_id = rid_man.next_rid((uint64_t)tid);
		int part_id = key_to_part(key); // % g_part_cnt;
		rc = the_table->get_new_row(new_row, part_id, row_id);
		assert(rc == RCOK);
		uint64_t primary_key = key;
		new_row->set_primary_key(primary_key);
#if SIM_FULL_ROW
		new_row->set_value(0, &primary_key,sizeof(uint64_t));

		Catalog * schema = the_table->get_schema();
		for (UInt32 fid = 0; fid < schema->get_field_cnt(); fid ++) {
//			int field_size = schema->get_field_size(fid);
//			char value[field_size];
//			for (int i = 0; i < field_size; i++)
//				value[i] = (char)rand() % (1<<8) ;
			char value[6] = "hello";
			new_row->set_value(fid, value,sizeof(value));
		}
#endif
		itemid_t *m_item = (itemid_t *) mem_allocator.alloc(sizeof(itemid_t));
		assert(m_item != NULL);
		m_item->type = DT_row;
		m_item->location = new_row;
		m_item->valid = true;
		uint64_t idx_key = primary_key;

//		DEBUG_WL("Thread_%d: inserting key = %ld, part_id=%d\n", tid, idx_key, part_id);
		rc = the_index->index_insert(idx_key, m_item, part_id);
		assert(rc == RCOK);
//    key += g_part_cnt;

	}
	printf("Thd %d inserted %ld keys\n", tid, key_cnt);
	return NULL;
}
#else
void *YCSBWorkload::init_table_slice() {
//	UInt32 tid = ATOM_FETCH_ADD(next_tid, 1);
//	UInt32 tid = next_tid.fetch_add(1);
	UInt32 ntid;
	UInt32 tid;
	do{
		tid = next_tid.load();
		ntid = tid +1;
	}while(!next_tid.compare_exchange_strong(tid, ntid));


	RC rc;
//	assert(g_synth_table_size % g_init_parallelism == 0);
	M_ASSERT_V(tid < g_init_parallelism, "tid = %d, g_init_parallelism = %d\n", tid, g_init_parallelism);
	uint64_t key_cnt = 0;
//	while ((UInt32) ATOM_FETCH_ADD(next_tid, 0) < g_init_parallelism) {}
//	assert((UInt32) ATOM_FETCH_ADD(next_tid, 0) == g_init_parallelism);

//	while ((UInt32) next_tid.load() < g_init_parallelism) {}
//	assert((UInt32) next_tid.load() == g_init_parallelism);

//	uint64_t slice_size = g_synth_table_size / g_init_parallelism;
	uint64_t node_slice_size = g_synth_table_size / g_part_cnt;
	uint64_t thd_slice_size = node_slice_size / g_init_parallelism;

	uint64_t slice_start = (g_node_id * node_slice_size) + (tid * thd_slice_size);
	uint64_t slice_end = (g_node_id * node_slice_size) + ((tid+1) * thd_slice_size);

	assert((slice_end-slice_start) == thd_slice_size);

	printf("N_%u:Thd %d: slice size = %ld, inserting from %ld, to %ld\n",g_node_id, tid, thd_slice_size, slice_start, (slice_end)-1);

//	if (tid == g_init_parallelism-1){
//		slice_end = g_synth_table_size;
//	}
	for (uint64_t key = slice_start;
		 key < slice_end;
		 key++
			) {
#if !SERVER_GENERATE_QUERIES
#if RANGE_PARITIONING
		if (GET_NODE_ID(key_to_part(key)) != g_node_id) {
			++key;
			continue;
		}
#else
		if (GET_NODE_ID(key_to_part(key)) != g_node_id) {
			++key;
			continue;
		}
#endif // #if RANGE_PARITIONING
#endif

		++key_cnt;
		if (key_cnt % 500000 == 0) {
			printf("Thd %d inserted %ld keys %f\n", tid, key_cnt, simulation->seconds_from_start(get_sys_clock()));
		}
//		printf("tid=%d. key=%ld\n", tid, key);
		row_t *new_row = NULL;
		uint64_t row_id = rid_man.next_rid((uint64_t)tid);
		int part_id = key_to_part(key); // % g_part_cnt;
		rc = the_table->get_new_row(new_row, part_id, row_id);
		assert(rc == RCOK);
		uint64_t primary_key = key;
		new_row->set_primary_key(primary_key);
#if SIM_FULL_ROW
        new_row->set_value(0, &primary_key,sizeof(uint64_t));

        Catalog * schema = the_table->get_schema();
        for (UInt32 fid = 0; fid < schema->get_field_cnt(); fid ++) {
//			int field_size = schema->get_field_size(fid);
//			char value[field_size];
//			for (int i = 0; i < field_size; i++)
//				value[i] = (char)rand() % (1<<8) ;
            char value[6] = "hello";
            new_row->set_value(fid, value,sizeof(value));
        }
#endif
		itemid_t *m_item = (itemid_t *) mem_allocator.alloc(sizeof(itemid_t));
		assert(m_item != NULL);
		m_item->type = DT_row;
		m_item->location = new_row;
		m_item->valid = true;
		uint64_t idx_key = primary_key;

//		DEBUG_WL("Thread_%d: inserting key = %ld, part_id=%d\n", tid, idx_key, part_id);
		rc = the_index->index_insert(idx_key, m_item, part_id);
		assert(rc == RCOK);
//    key += g_part_cnt;

	}
	printf("Thd %d inserted %ld keys\n", tid, key_cnt);
	return NULL;
}
#endif

RC YCSBWorkload::get_txn_man(TxnManager *& txn_manager){
  DEBUG_M("YCSBWorkload::get_txn_man YCSBTxnManager alloc\n");
	txn_manager = (YCSBTxnManager *)
		mem_allocator.align_alloc( sizeof(YCSBTxnManager));
	new(txn_manager) YCSBTxnManager();
	//txn_manager->init(this); 
	return RCOK;
}

#endif // #if WORKLOAD == YCSB