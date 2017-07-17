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
#include "ycsb.h"
#include "tpcc.h"
#include "pps.h"
#include "thread.h"
#include "worker_thread.h"
#include "calvin_thread.h"
#include "abort_thread.h"
#include "io_thread.h"
#include "log_thread.h"
#include "manager.h"
#include "math.h"
#include "query.h"
#include "occ.h"
#include "transport.h"
#include "msg_queue.h"
#include "ycsb_query.h"
#include "sequencer.h"
#include "logger.h"
#include "sim_manager.h"
#include "abort_queue.h"
#include "work_queue.h"
#include "maat.h"
#include "client_query.h"
#include "quecc_thread.h"
#include <jemalloc/jemalloc.h>

void network_test();
void network_test_recv();
void * run_thread(void *);


WorkerThread * worker_thds;
InputThread * input_thds;
OutputThread * output_thds;
AbortThread * abort_thds;
LogThread * log_thds;
#if CC_ALG == CALVIN
CalvinLockThread * calvin_lock_thds;
CalvinSequencerThread * calvin_seq_thds;
#endif

#if CC_ALG == QUECC
PlannerThread * planner_thds;
#if CT_ENABLED
CommitThread * commit_thds;
#endif
#endif

// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char* argv[])
{
	// 0. initialize global data structure
	parser(argc, argv);
#if SEED != 0
  uint64_t seed = SEED + g_node_id;
#else
	uint64_t seed = get_sys_clock();
#endif
	srand(seed);
	printf("Random seed: %ld\n",seed);

#if CC_ALG == QUECC && BATCHING_MODE == SIZE_BASED
    M_ASSERT_V(g_batch_size % g_plan_thread_cnt == 0, "(BATCH_SIZE) remainder PLAN_THREAD_CNT != 0,"
            " please configure them proparly, remainder = %d", g_batch_size % g_plan_thread_cnt)
#endif
	int64_t starttime;
	int64_t endtime;
  starttime = get_server_clock();
  printf("Initializing stats... ");
  fflush(stdout);
	stats.init(g_total_thread_cnt);
  printf("Done\n");
  printf("Initializing global manager... ");
  fflush(stdout);
	glob_manager.init();
  printf("Done\n");
  printf("Initializing transport manager... ");
  fflush(stdout);
	tport_man.init();
  printf("Done\n");
  fflush(stdout);
  printf("Initializing simulation... ");
  fflush(stdout);
  simulation = new SimManager;
  simulation->init();
  printf("Done\n");
  fflush(stdout);
	Workload * m_wl;
	switch (WORKLOAD) {
		case YCSB :
			m_wl = new YCSBWorkload; break;
		case TPCC :
			m_wl = new TPCCWorkload; break;
		case PPS :
			m_wl = new PPSWorkload; break;
		default:
			assert(false);
	}
	m_wl->init();
	printf("Workload initialized!\n");
  fflush(stdout);
#if NETWORK_TEST
	tport_man.init(g_node_id,m_wl);
	sleep(3);
	if(g_node_id == 0)
		network_test();
	else if(g_node_id == 1)
		network_test_recv();

	return 0;
#endif


  printf("Initializing work queue... ");
  fflush(stdout);
  work_queue.init();
  printf("Done\n");
  printf("Initializing abort queue... ");
  fflush(stdout);
  abort_queue.init();
  printf("Done\n");
  printf("Initializing message queue... ");
  fflush(stdout);
  msg_queue.init();
  printf("Done\n");
  printf("Initializing transaction manager pool... ");
  fflush(stdout);
  txn_man_pool.init(m_wl,0);
  printf("Done\n");
  printf("Initializing transaction pool... ");
  fflush(stdout);
  txn_pool.init(m_wl,0);
  printf("Done\n");
  printf("Initializing row pool... ");
  fflush(stdout);
  row_pool.init(m_wl,0);
  printf("Done\n");
  printf("Initializing access pool... ");
  fflush(stdout);
  access_pool.init(m_wl,0);
  printf("Done\n");
  printf("Initializing txn node table pool... ");
  fflush(stdout);
  txn_table_pool.init(m_wl,0);
  printf("Done\n");
  printf("Initializing query pool... ");
  fflush(stdout);
  qry_pool.init(m_wl,0);
  printf("Done\n");
  printf("Initializing msg pool... ");
  fflush(stdout);
  msg_pool.init(m_wl,0);
  printf("Done\n");
  printf("Initializing transaction table... ");
  fflush(stdout);
  txn_table.init();
  printf("Done\n");
#if CC_ALG == CALVIN
  printf("Initializing sequencer... ");
  fflush(stdout);
  seq_man.init(m_wl);
  printf("Done\n");
#endif
#if CC_ALG == MAAT
  printf("Initializing Time Table... ");
  fflush(stdout);
  time_table.init();
  printf("Done\n");
  printf("Initializing MaaT manager... ");
  fflush(stdout);
	maat_man.init();
  printf("Done\n");
#endif
#if LOGGING
  printf("Initializing logger... ");
  fflush(stdout);
  logger.init("logfile.log");
  printf("Done\n");
#endif
    stats.printProcInfo();
#if SERVER_GENERATE_QUERIES
  printf("Initializing client query queue... ");
  fflush(stdout);
  client_query_queue.init(m_wl);
  printf("Done\n");
  fflush(stdout);
#endif
    stats.printProcInfo();
#if CC_ALG == QUECC
    printf("Initializing QueCC pool... ");
    fflush(stdout);
    quecc_pool.init(m_wl,0);
    printf("Done\n");
    stats.printProcInfo();
#endif

	// 2. spawn multiple threads
	uint64_t thd_cnt = g_thread_cnt;
	uint64_t wthd_cnt = thd_cnt;
	uint64_t rthd_cnt = g_rem_thread_cnt;
	uint64_t sthd_cnt = g_send_thread_cnt;
    uint64_t all_thd_cnt = thd_cnt + rthd_cnt + sthd_cnt + g_abort_thread_cnt;

#if LOGGING
    all_thd_cnt += 1; // logger thread
#endif
#if CC_ALG == CALVIN
    all_thd_cnt += 2; // sequencer + scheduler thread
#endif

#if CC_ALG == QUECC
    all_thd_cnt += g_plan_thread_cnt;
    all_thd_cnt -= 1; // to remove abort thread for QueCC but there is a logger
#if CT_ENABLED
    all_thd_cnt += 1; // add commit thread
#endif

#endif

#if CC_ALG == DUMMY_CC
    all_thd_cnt -= 1; // remove abort thread
#endif

#if SERVER_GENERATE_QUERIES
    // remove io threads
    all_thd_cnt -= (rthd_cnt + sthd_cnt);
#endif

    DEBUG_Q("all_thd_cnt (%ld) ==  g_this_total_thread_cnt (%d)\n", all_thd_cnt, g_this_total_thread_cnt);
    assert(all_thd_cnt == g_this_total_thread_cnt);

    pthread_t * p_thds;
    pthread_attr_t attr;
    uint64_t id = 0;

#if SET_AFFINITY
    uint64_t cpu_cnt = 0;
    cpu_set_t cpus;
    CPU_ZERO(&cpus);
    CPU_SET(cpu_cnt, &cpus);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpus);
    cpu_cnt++;
#endif

#if MODE == FIXED_MODE
    worker_thds = new WorkerThread[wthd_cnt];

#if CC_ALG == QUECC
    planner_thds = new PlannerThread[g_plan_thread_cnt];

    wthd_cnt = g_plan_thread_cnt;
    starttime = get_server_clock();

    printf("Warm Up Start: PT Initialization Time = %ld, warmup = %d, warmup_done = %d\n",
           starttime, simulation->warmup, simulation->is_warmup_done());
    fflush(stdout);

    // Initialize barrier for PTs
    if (pthread_barrier_init( &warmup_bar, NULL, wthd_cnt)){
        M_ASSERT_V(false, "barriar init failed\n");
    }

    p_thds =
            (pthread_t *) malloc(sizeof(pthread_t) * (g_plan_thread_cnt));
    pthread_attr_init(&attr);

    starttime = get_server_clock();
    for (uint64_t j = 0; j < wthd_cnt; j++) {
#if SET_AFFINITY
        CPU_ZERO(&cpus);
        CPU_SET(cpu_cnt, &cpus);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
        cpu_cnt++;
#endif
        planner_thds[j].init(id,g_node_id,m_wl);
        planner_thds[j]._planner_id = j;
        pthread_create(&p_thds[id++], &attr, run_thread, (void *)&planner_thds[j]);
        pthread_setname_np(p_thds[id-1], "s_planner");
    }
    // Wait untill all PTs are done for all batches

    for (uint64_t i = 0; i < wthd_cnt ; i++){
        pthread_join(p_thds[i], NULL);
    }

    // destory PT barrier and create ET barrier, this is needed when the numbers of PTs and ETs are not equal
    if (pthread_barrier_destroy(&warmup_bar)){
        M_ASSERT_V(false, "barriar destroy failed\n");
    }
    free(p_thds);
#endif

    wthd_cnt = g_thread_cnt;
    if (pthread_barrier_init( &warmup_bar, NULL, wthd_cnt)){
        M_ASSERT_V(false, "barriar init failed\n");
    }
    p_thds = (pthread_t *) malloc(sizeof(pthread_t) * (wthd_cnt));
    pthread_attr_init(&attr);
    id = 0;
    cpu_cnt = 1; // restart from cpu 1

    for (uint64_t i = 0; i < wthd_cnt; i++) {
#if SET_AFFINITY
        CPU_ZERO(&cpus);
        CPU_SET(cpu_cnt, &cpus);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
        cpu_cnt++;
#endif
        assert(id >= 0 && id < wthd_cnt);
        worker_thds[i].init(id,g_node_id,m_wl);
        pthread_create(&p_thds[id++], &attr, run_thread, (void *)&worker_thds[i]);
        pthread_setname_np(p_thds[id-1], "s_worker");
    }


    // Wait until all ETs are done
    for (uint64_t i = 0; i < wthd_cnt ; i++){
        pthread_join(p_thds[i], NULL);
    }

    simulation->run_starttime = starttime;
    ATOM_CAS(simulation->warmup_end_time,0,get_sys_clock());
    ATOM_CAS(simulation->warmup,false,true);

    endtime = get_server_clock();

    stats.totals->clear();
    for(uint64_t i = 0; i < g_total_thread_cnt; i++){
        stats.totals->combine(stats._stats[i]);
    }

    printf("Warm Up Done !! in total Time = %ld secs, processed %ld txns, starting measured stage\n",
           (endtime - starttime)/BILLION, stats.totals->txn_cnt);
    fflush(stdout);


    if (STATS_ENABLE){
        stats.print(true);
    }
    printf("\n");
    fflush(stdout);

    // reset stats
    for (uint64_t i =0; i < g_total_thread_cnt; ++i){
        stats._stats[i]->clear();
    }
#if CC_ALG == QUECC
    // destory last barrier
    if (pthread_barrier_destroy(&warmup_bar)){
        M_ASSERT_V(false, "barriar destroy failed\n");
    }

    // Reset PG map and batch map

    // TODO(tq): Refactor this into a cleanup function

    for (uint64_t i =0; i < g_batch_map_length; ++i){
        for (uint64_t j=0; j< g_plan_thread_cnt; ++j){
            work_queue.batch_pg_map[i][j].status.store(0);
        }
    }
#if BATCH_MAP_ORDER == BATCH_ET_PT
    for (uint64_t i=0; i < g_batch_map_length ; i++){
        for (uint64_t j=0; j < g_thread_cnt; j++){
            for (uint64_t k=0; k< g_plan_thread_cnt ; k++){
                (work_queue.batch_map[i][j][k]).store(0);
            }
        }
    }
#else
    for (uint64_t i=0; i < g_batch_map_length ; i++){
        for (uint64_t j=0; j < g_plan_thread_cnt; j++){
            for (uint64_t k=0; k< g_thread_cnt; k++){
                (work_queue.batch_map[i][j][k]).store(0);
            }
        }
    }
#endif

    quecc_pool.print_stats();

    wthd_cnt = g_plan_thread_cnt;

    // create a new barrier for PT
    if (pthread_barrier_init( &warmup_bar, NULL, wthd_cnt)){
        M_ASSERT_V(false, "barriar init failed\n");
    }
    // Run PTs first
    free(p_thds);
    p_thds =
    (pthread_t *) malloc(sizeof(pthread_t) * (wthd_cnt));
    pthread_attr_init(&attr);
    id = 0;
    cpu_cnt = 1; // restart from cpu 1

    starttime = get_server_clock();
    for (uint64_t j = 0; j < wthd_cnt; j++) {
#if SET_AFFINITY
      CPU_ZERO(&cpus);
      CPU_SET(cpu_cnt, &cpus);
      pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
      cpu_cnt++;
#endif
        planner_thds[j].init(id,g_node_id,m_wl);
        planner_thds[j]._planner_id = j;
        pthread_create(&p_thds[id++], &attr, run_thread, (void *)&planner_thds[j]);
        pthread_setname_np(p_thds[id-1], "s_planner");
    }
    // Wait untill all PTs are done for all batches

    for (uint64_t i = 0; i < wthd_cnt ; i++){
        pthread_join(p_thds[i], NULL);
    }

    endtime = get_server_clock();
    float pt_total_runtime = (float)(endtime - starttime);

    if (pthread_barrier_destroy(&warmup_bar)){
        M_ASSERT_V(false, "barriar destroy failed\n");
    }
#endif

    // Run ETs
    wthd_cnt = g_thread_cnt;
    if (pthread_barrier_init( &warmup_bar, NULL, wthd_cnt)){
        M_ASSERT_V(false, "barriar init failed\n");
    }
    free(p_thds);
    p_thds =
            (pthread_t *) malloc(sizeof(pthread_t) * (wthd_cnt));
    pthread_attr_init(&attr);

    id = 0;
    cpu_cnt = 1; // restart from cpu 1
    starttime = get_server_clock();
      for (uint64_t i = 0; i < wthd_cnt; i++) {
    #if SET_AFFINITY
          CPU_ZERO(&cpus);
          CPU_SET(cpu_cnt, &cpus);
          pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
          cpu_cnt++;
    #endif
          assert(id >= 0 && id < wthd_cnt);
          worker_thds[i].init(id,g_node_id,m_wl);
          pthread_create(&p_thds[id++], &attr, run_thread, (void *)&worker_thds[i]);
          pthread_setname_np(p_thds[id-1], "s_worker");
        }


    // Wait until all ETs are done
    for (uint64_t i = 0; i < wthd_cnt ; i++){
        pthread_join(p_thds[i], NULL);
    }

    // Exit and print status.
#if CC_ALG == QUECC
	endtime = get_server_clock();
    float et_total_runtime = (float)(endtime - starttime);
    fflush(stdout);
    float total_runtime =  et_total_runtime + pt_total_runtime;
    stats.totals->clear();
    for(uint64_t i = 0; i < g_total_thread_cnt; i++){
        stats.totals->combine(stats._stats[i]);
    }
    float pt_total_runtime_sec = (pt_total_runtime / BILLION);
    float et_total_runtime_sec = (et_total_runtime / BILLION);
    uint64_t txn_cnt = (g_batch_size*g_batch_map_length);
    float pt_tput = (float) txn_cnt/ (pt_total_runtime_sec);
    float et_tput = (float) txn_cnt / (et_total_runtime_sec);
    float tput = (float) txn_cnt / (total_runtime / BILLION);


    printf("PASS!,SimTime=%f,total_time=%f,pt_time=%f,et_time=%f,total_tput=%f,pt_tput=%f,et_tput=%f, txn_cnt=%ld\n",
           total_runtime / BILLION,
           pt_total_runtime_sec+et_total_runtime_sec,
           pt_total_runtime_sec,
           et_total_runtime_sec,
           tput, pt_tput, et_tput,
//           stats.totals->txn_cnt
           txn_cnt
    );
#else
    endtime = get_server_clock();
    printf("PASS!,SimTime=%f\n",
           ((float)endtime-starttime) / BILLION);
#endif
    if (STATS_ENABLE){
        stats.print(false);
    }
    printf("\n");
    fflush(stdout);
    // Free things
    m_wl->index_delete_all();

#if CC_ALG == QUECC
    quecc_pool.print_stats();
#endif
    return 0;
#else

    p_thds =
    (pthread_t *) malloc(sizeof(pthread_t) * (all_thd_cnt));
    pthread_attr_init(&attr);

    worker_thds = new WorkerThread[wthd_cnt];
    input_thds = new InputThread[rthd_cnt];
    output_thds = new OutputThread[sthd_cnt];
    abort_thds = new AbortThread[1];
    log_thds = new LogThread[1];
#if CC_ALG == CALVIN
    calvin_lock_thds = new CalvinLockThread[1];
    calvin_seq_thds = new CalvinSequencerThread[1];
#endif

#if CC_ALG == QUECC
    planner_thds = new PlannerThread[g_plan_thread_cnt];
#if CT_ENABLED
    commit_thds = new CommitThread[1];
#endif // CT_ENABLED

#endif
	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	//if (WORKLOAD != TEST) {
	//	query_queue.init(m_wl);
	//}
#if CC_ALG == OCC
    printf("Initializing occ lock manager... ");
    occ_man.init();
    printf("Done\n");
#endif

    /*
    printf("Initializing threads... ");
    fflush(stdout);
      for (uint32_t i = 0; i < all_thd_cnt; i++)
          m_thds[i].init(i, g_node_id, m_wl);
    printf("Done\n");
    fflush(stdout);
    */

    endtime = get_server_clock();
    printf("Initialization Time = %ld\n", endtime - starttime);
    fflush(stdout);
    warmup_done = true;
    pthread_barrier_init( &warmup_bar, NULL, all_thd_cnt);

  // spawn and run txns again.
  starttime = get_server_clock();
  simulation->run_starttime = starttime;

  for (uint64_t i = 0; i < wthd_cnt; i++) {
#if SET_AFFINITY
      CPU_ZERO(&cpus);
      CPU_SET(cpu_cnt, &cpus);
      pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
      cpu_cnt++;
#endif
      assert(id >= 0 && id < wthd_cnt);
      worker_thds[i].init(id,g_node_id,m_wl);
      pthread_create(&p_thds[id++], &attr, run_thread, (void *)&worker_thds[i]);
      pthread_setname_np(p_thds[id-1], "s_worker");
	}

#if !SERVER_GENERATE_QUERIES
	for (uint64_t j = 0; j < rthd_cnt ; j++) {
#if SET_AFFINITY
      CPU_ZERO(&cpus);
      CPU_SET(cpu_cnt, &cpus);
      pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
      cpu_cnt++;
#endif
	    assert(id >= wthd_cnt && id < wthd_cnt + rthd_cnt);
	    input_thds[j].init(id,g_node_id,m_wl);
	    pthread_create(&p_thds[id++], &attr, run_thread, (void *)&input_thds[j]);
        pthread_setname_np(p_thds[id-1], "s_receiver");
	}


	for (uint64_t j = 0; j < sthd_cnt; j++) {
#if SET_AFFINITY
        CPU_ZERO(&cpus);
      CPU_SET(cpu_cnt, &cpus);
      pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
      cpu_cnt++;
#endif
	    assert(id >= wthd_cnt + rthd_cnt && id < wthd_cnt + rthd_cnt + sthd_cnt);
	    output_thds[j].init(id,g_node_id,m_wl);
	    pthread_create(&p_thds[id++], &attr, run_thread, (void *)&output_thds[j]);
        pthread_setname_np(p_thds[id-1], "s_sender");
	  }
#endif

#if LOGGING
    #if SET_AFFINITY
      CPU_ZERO(&cpus);
      CPU_SET(cpu_cnt, &cpus);
      pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
      cpu_cnt++;
    #endif
    log_thds[0].init(id,g_node_id,m_wl);
    pthread_create(&p_thds[id++], &attr, run_thread, (void *)&log_thds[0]);
    pthread_setname_np(p_thds[id-1], "s_logger");
#endif

#if CC_ALG != CALVIN && CC_ALG != QUECC && CC_ALG != DUMMY_CC
#if SET_AFFINITY
      CPU_ZERO(&cpus);
      CPU_SET(cpu_cnt, &cpus);
      pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
      cpu_cnt++;
    #endif
  abort_thds[0].init(id,g_node_id,m_wl);
  pthread_create(&p_thds[id++], &attr, run_thread, (void *)&abort_thds[0]);
    pthread_setname_np(p_thds[id-1], "aborter");
#endif

#if CC_ALG == CALVIN
#if SET_AFFINITY
    CPU_ZERO(&cpus);
    CPU_SET(cpu_cnt, &cpus);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
    cpu_cnt++;
#endif
    calvin_lock_thds[0].init(id,g_node_id,m_wl);
    pthread_create(&p_thds[id++], &attr, run_thread, (void *)&calvin_lock_thds[0]);
    pthread_setname_np(p_thds[id-1], "calvin_sched");

#if SET_AFFINITY
	CPU_ZERO(&cpus);
    CPU_SET(cpu_cnt, &cpus);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
    cpu_cnt++;
#endif

  calvin_seq_thds[0].init(id,g_node_id,m_wl);
  pthread_create(&p_thds[id++], &attr, run_thread, (void *)&calvin_seq_thds[0]);
    pthread_setname_np(p_thds[id-1], "calvin_seq");
#endif

    // creating threads for QUECC
#if CC_ALG == QUECC
    DEBUG_Q("Initilizing Quecc threads\n");
    for (uint64_t j = 0; j < g_plan_thread_cnt; j++) {
#if SET_AFFINITY
      CPU_ZERO(&cpus);
      CPU_SET(cpu_cnt, &cpus);
      pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
      cpu_cnt++;
#endif
        planner_thds[j].init(id,g_node_id,m_wl);
        planner_thds[j]._planner_id = j;
        pthread_create(&p_thds[id++], &attr, run_thread, (void *)&planner_thds[j]);
        pthread_setname_np(p_thds[id-1], "s_planner");
    }
#if CT_ENABLED
// Initialize and start commit threads
#if SET_AFFINITY
    CPU_ZERO(&cpus);
    CPU_SET(cpu_cnt, &cpus);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
    cpu_cnt++;
#endif
    commit_thds[0].init(id,g_node_id,m_wl);
    pthread_create(&p_thds[id++], &attr, run_thread, (void *)&commit_thds[0]);
    pthread_setname_np(p_thds[id-1], "s_commit");
#endif // #if CT_ENABLED
    DEBUG_Q("DONE: Initilizing Quecc threads\n");
    DEBUG_Q("total thread count = %ld, ids = %ld\n", all_thd_cnt, id);
#endif

#if SERVER_GENERATE_QUERIES
    //TQ: start simulation, there is no need to wait anymore
//    simulation->process_setup_msg();
    simulation->set_setup_done();
#endif

	for (uint64_t i = 0; i < all_thd_cnt ; i++) 
		pthread_join(p_thds[i], NULL);

	endtime = get_server_clock();
	
  fflush(stdout);
  printf("PASS! SimTime = %f\n", (float)(endtime - starttime) / BILLION);
  if (STATS_ENABLE)
    stats.print(false);
  //malloc_stats_print(NULL, NULL, NULL);
  printf("\n");
  fflush(stdout);
  // Free things
	//tport_man.shutdown();
  m_wl->index_delete_all();

  /*
  txn_table.delete_all();
  txn_pool.free_all();
  access_pool.free_all();
  txn_table_pool.free_all();
  msg_pool.free_all();
  qry_pool.free_all();
  */

    //JEMALLOC
//    je_malloc_stats_print(NULL, NULL, NULL);
//    je_mallctl("prof.dump", NULL, NULL, NULL, 0);
	return 0;
#endif
}

void * run_thread(void * id) {
    Thread * thd = (Thread *) id;
	thd->run();
	return NULL;
}

void network_test() {

      /*
	ts_t start;
	ts_t end;
	ts_t time;
	int bytes;
  float total = 0;
	for (int i = 0; i < 4; ++i) {
		time = 0;
		int num_bytes = (int) pow(10,i);
		printf("Network Bytes: %d\nns: ", num_bytes);
		for(int j = 0;j < 1000; j++) {
			start = get_sys_clock();
			tport_man.simple_send_msg(num_bytes);
			while((bytes = tport_man.simple_recv_msg()) == 0) {}
			end = get_sys_clock();
			assert(bytes == num_bytes);
			time = end-start;
      total += time;
			//printf("%lu\n",time);
		}
		printf("Avg(s): %f\n",total/BILLION/1000);
    fflush(stdout);
		//time = time/1000;
		//printf("Network Bytes: %d, s: %f\n",i,time/BILLION);
		//printf("Network Bytes: %d, ns: %.3f\n",i,time);
		
	}
      */

}

void network_test_recv() {
  /*
	int bytes;
	while(1) {
		if( (bytes = tport_man.simple_recv_msg()) > 0)
			tport_man.simple_send_msg(bytes);
	}
  */
}
