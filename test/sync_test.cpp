//
// Created by Thamir Qadah on 12/10/17.
//

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <vector>

#include <sys/time.h>
#include <pthread.h>
#include <numa.h>
#include <cassert>
#include <sys/resource.h>

#include <sched.h>
#include <errno.h>
#include <atomic>

unsigned long long microsecpassed(struct timeval* t) {
    struct timeval now, diff;
    gettimeofday(&now, NULL);
    timersub(&now, t, &diff);
    return (diff.tv_sec * 1000 * 1000)  + diff.tv_usec;
}

#if defined(__i386__)

inline unsigned long long rdtsc() {
  unsigned int lo, hi;
  __asm__ volatile (
     "cpuid \n"
     "rdtsc"
   : "=a"(lo), "=d"(hi) /* outputs */
   : "a"(0)             /* inputs */
   : "%ebx", "%ecx");     /* clobbers*/
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}
#elif 0
static inline unsigned long long rdtsc(void) {
    unsigned long long hi, lo;
    __asm__ __volatile__(
            "xorl %%eax, %%eax;\n\t"
            "push %%ebx;"
            "cpuid\n\t"
            ::
            :"%eax", "%ebx", "%ecx", "%edx");
    __asm__ __volatile__(
            "rdtsc;"
            : "=a" (lo),  "=d" (hi)
            ::);
    __asm__ __volatile__(
            "xorl %%eax, %%eax; cpuid;"
            "pop %%ebx;"
            ::
            :"%eax", "%ebx", "%ecx", "%edx");

    return (unsigned long long)hi << 32 | lo;
}

#elif 0
static inline unsigned long long rdtsc(void)
{
  unsigned long long int x;
     __asm__ volatile (".byte 0x0f, 0x31" : "=A" (x));
     return x;
}
#elif defined(__x86_64__)
static inline unsigned long long rdtsc(void)
{
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}

#elif defined(__powerpc__)


static __inline__ unsigned long long rdtsc(void)
{
  unsigned long long int result=0;
  unsigned long int upper, lower,tmp;
  __asm__ volatile(
                "0:                  \n"
                "\tmftbu   %0           \n"
                "\tmftb    %1           \n"
                "\tmftbu   %2           \n"
                "\tcmpw    %2,%0        \n"
                "\tbne     0b         \n"
                : "=r"(upper),"=r"(lower),"=r"(tmp)
                );
  result = upper;
  result = result<<32;
  result = result|lower;

  return(result);
}

#endif


//#define THD_CNT 20
//#define THD_CNT 1
#define CPU_FREQ 2.0
#define CL_SIZE  64
#define BILLION 1000000000

#define COPY_TRIAL_CNT 20000*3

#define US_SLEEP true

//#define COPY_TRIAL_CNT 10
#define NUMA_NODE_CNT 0
//#define DB_SIZE 1024*64

static std::vector<size_t> thd_cnts = {4,8,12,16,20,24,28,32};
//static std::vector<size_t> thd_cnts = {8};

static pthread_barrier_t start_bar;


typedef unsigned long long ullong;

struct args_t {
    int thd_id;
    int total_thd_cnt;
};

struct stat_t{
    int64_t sync_cnt;
    ullong total_ticks;
    char padding[48];
};

struct done_t{
    int64_t done;
    char padding[56];
} __attribute__((aligned(64)));

struct next_stage_t{
    int64_t next_stage;
    char padding[56];
} __attribute__((aligned(64)));

// global variables
//static stat_t * stats[THD_CNT];
static stat_t ** stats_aligned;

static args_t ** sync_args_arr;

volatile done_t * done;
volatile next_stage_t * next_stage;

void * mthread_contention_task(void * ctx){
    int thd_id = ((args_t *) ctx)->thd_id;
    int total_thd_cnt =  ((args_t *) ctx)->total_thd_cnt;
    ullong ticka, tickb;

    cpu_set_t cpus;
    CPU_ZERO(&cpus);
    CPU_SET(thd_id, &cpus);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpus);

    //sync barrier
    pthread_barrier_wait( &start_bar );

    // Force FIFO scheduling
//    struct sched_param param;
//    param.sched_priority = 1;
//    int rc = pthread_setschedparam(pthread_self(), SCHED_FIFO, &param);
//    printf("ET_%d: rc = %d, EINVAL=%d,EPERM=%d,ESRCH=%d,ENOTSUP=%d\n",thd_id,rc, EINVAL, EPERM,ESRCH, ENOTSUP);


    volatile int64_t sum=0;
    for (int i = 0; i < COPY_TRIAL_CNT; ++i) {

#if US_SLEEP
        usleep(1);
#endif

        ticka = rdtsc();
        // sync
        done[thd_id].done = 1;
        if (thd_id == 0){
            // master
            while (true){
                sum=0;
                for (int j = 0; j < total_thd_cnt; ++j) {
                    sum += done[j].done;
                }
                if (sum == total_thd_cnt) break;
            }

            done[thd_id].done = 0;

            // let other threads procced
            for (int j = 0; j < total_thd_cnt; ++j) {
                next_stage[j].next_stage = 1;
            }
            // check if all other threads have seen next_stage value
            while (true){
                sum = 0;
                for (int j = 0; j < total_thd_cnt; ++j) {
                    sum += done[j].done;
                }
                if (sum == 0) break;
            }

            for (int j = 0; j < total_thd_cnt; ++j) {
                next_stage[j].next_stage = 0;
            }
        }
        else{
            //others
            while(next_stage[thd_id].next_stage != 1){
                // spin
            }

            done[thd_id].done = 0;

            while(next_stage[thd_id].next_stage != 0){
                // spin
            }
        }
        tickb = rdtsc();
        stats_aligned[thd_id]->total_ticks += (tickb-ticka);
    }
//    printf("thread %d is done\n",thd_id);
    return NULL;
}

void mem_contention_test_driver(const int thd_cnt){
//    printf("Starting with thread_cnt: %d\n", thd_cnt);
    pthread_t * p_thds = (pthread_t *) malloc(sizeof(pthread_t)*thd_cnt);
    done = (done_t *) malloc(sizeof(done_t)*thd_cnt);
    memset((void *)done,0,sizeof(done_t)*thd_cnt);
    next_stage = (next_stage_t *) malloc(sizeof(next_stage_t)*thd_cnt);
    memset((void *)next_stage,0,sizeof(next_stage_t)*thd_cnt);

    sync_args_arr = (args_t **) malloc(sizeof(args_t *)*thd_cnt);
    stats_aligned = (stat_t **) malloc(sizeof(stat_t*)*thd_cnt);

    if (sync_args_arr == nullptr) return;

    if (pthread_barrier_init( &start_bar, NULL, thd_cnt)){
        printf( "barriar init failed\n");
        assert(false);
    }
    for (int i = 0; i < thd_cnt; i++) {
        stats_aligned[i] = (stat_t *) malloc(sizeof(stat_t));
        memset(stats_aligned[i],0,sizeof(stat_t));
        sync_args_arr[i] = (args_t *) malloc(sizeof(args_t));
        sync_args_arr[i]->thd_id = i;
        sync_args_arr[i]->total_thd_cnt = thd_cnt;
//        args_arr[i]->rec_size_i = rec_size_idx;
//        args_arr[i]->rec_cnt_i = rec_cnt_idx;
        pthread_create(&p_thds[i], NULL, mthread_contention_task, sync_args_arr[i]);
        pthread_setname_np(p_thds[i], "sync_thd");
    }

    for (int i = 0; i < thd_cnt; i++) {
        pthread_join(p_thds[i], NULL);
    }

    for (int i = 0; i < thd_cnt; i++) {
        free(sync_args_arr[i]);
        free(stats_aligned[i]);
    }
//    printf("done with %d\n", thd_cnt);
    pthread_barrier_destroy(&start_bar);
    free((void *)done);
    free((void *)next_stage);
    free(sync_args_arr);
    free(stats_aligned);
    free(p_thds);
}

int main(int argc, char** argv) {

    printf("sync_data={\n");
    printf("'thd_cnt':[");
    for (unsigned int i = 0; i < thd_cnts.size(); ++i) {
        if (i > 0) printf(",");
        printf("%lu",thd_cnts[i]);
    }
    printf("],\n");
    printf("'sync_time':[");
    for (unsigned int i = 0; i < thd_cnts.size(); ++i) {
        if (i > 0) printf(",");

        mem_contention_test_driver(thd_cnts[i]);
        ullong  total_ticks = 0;
        for (size_t j = 0; j < thd_cnts[i]; ++j) {
            total_ticks += stats_aligned[j]->total_ticks;
        }
        double total_time = (((double)total_ticks)/CPU_FREQ)/BILLION;
        double time_per_thd = total_time/thd_cnts[i];
//        printf("total_thd_cnt=%lu,total_time=%f,time_per_thd=%f\n",thd_cnts[i],total_time,time_per_thd);
        printf("%f",time_per_thd);
    }
    printf("]\n");
    printf("}\n");
    return 0;
}
