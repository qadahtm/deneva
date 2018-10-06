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

#include <jemalloc/jemalloc.h>
#include "global.h"
#include "manager.h"
#include "thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "math.h"
#include "helper.h"
#include "msg_queue.h"
#include "message.h"

void Thread::heartbeat() {
    /*
  #if TIME_ENABLE
    uint64_t now_time = get_sys_clock();
  #else
    uint64_t now_time = get_wall_clock();
  #endif
    if (now_time - heartbeat_time >= g_prog_timer) {
      printf("Heartbeat %ld %f\n",_thd_id,simulation->seconds_from_start(now_time));
      heartbeat_time = now_time;
    }
    */

}

void Thread::send_init_done_to_all_nodes() {
    for (uint64_t i = 0; i < g_total_node_cnt; i++) {
        if (i != g_node_id) {
            printf("Send INIT_DONE to %ld\n", i);
            msg_queue.enqueue(get_thd_id(), Message::create_message(INIT_DONE), i);
        }
    }
}

void Thread::init(uint64_t thd_id, uint64_t node_id, Workload *workload) {
    _thd_id = thd_id;
    _node_id = node_id;
    _wl = workload;
    rdm.init(_thd_id);
}

uint64_t Thread::get_thd_id() { return _thd_id; }

uint64_t Thread::get_node_id() { return _node_id; }

void Thread::tsetup() {
    printf("Setup %ld:%ld\n", _node_id, _thd_id);
    fflush(stdout);
    pthread_barrier_wait(&warmup_bar);

    setup();

    printf("Running %ld:%ld\n", _node_id, _thd_id);
    fflush(stdout);
    pthread_barrier_wait(&warmup_bar);

#if SET_AFFINITY_AFTER_INIT
#ifndef N_MALLOC
    uint64_t arena = _thd_id;
    je_mallctl("thread.arena", NULL, NULL, &arena, sizeof(uint64_t));
#endif
#if NUMA_ENABLED
    int node = _thd_id/(CORE_CNT/NUMA_NODE_CNT);
    numa_set_preferred(node);
    DEBUG_Q("ET_%ld: preferred node is %d\n", _thd_id, numa_preferred());
#endif
    cpu_set_t cpus;
    CPU_ZERO(&cpus);
    CPU_SET(_thd_id, &cpus);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpus);
#endif

//	// Force FIFO scheduling
//	struct sched_param param;
//	param.sched_priority = 1;
//	int rc = pthread_setschedparam(pthread_self(), SCHED_FIFO, &param);
//	printf("ET_%lu: forcing FIFO scheduling rc = %d, EINVAL=%d,EPERM=%d,ESRCH=%d,ENOTSUP=%d\n",_thd_id,rc, EINVAL, EPERM,ESRCH, ENOTSUP);

#if TIME_ENABLE
    run_starttime = get_sys_clock();
#else
    //  run_starttime = get_wall_clock();
      run_starttime = get_server_clock();
#endif
    simulation->set_starttime(run_starttime);
    prog_time = run_starttime;
    heartbeat_time = run_starttime;
    pthread_barrier_wait(&warmup_bar);


}

void Thread::progress_stats() {
#if PROG_STATS
    if (get_thd_id() == 0) {
#if TIME_ENABLE
        uint64_t now_time = get_sys_clock();
#else
        //      uint64_t now_time = get_wall_clock();
              uint64_t now_time = get_server_clock();
#endif
        if (now_time - prog_time >= g_prog_timer) {
            prog_time = now_time;
            SET_STATS(get_thd_id(), total_runtime, prog_time - simulation->run_starttime);

            if (ISCLIENT) {
                stats.print_client(true);
            } else {
                stats.print(true);
            }
        }
    }
#endif // -if PROG_STATS
}
