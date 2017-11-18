//
// Created by Thamir Qadah on 11/8/17.
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


#define THD_CNT 10
//#define THD_CNT 1
#define R_SIZE 4096
//#define R_SIZE 2048
//#define R_SIZE 100
//#define B_SIZE 1000
//#define B_SIZE 800
//#define B_SIZE 400
//#define B_SIZE 200
//#define B_SIZE 100
#define B_SIZE 1
#define CPU_FREQ 2.4
#define CL_SIZE  64
#define BILLION 1000000000
#define COPY_TRIAL_CNT 100
//#define COPY_TRIAL_CNT 10
#define NUMA_NODE_CNT 0
//#define DB_SIZE 1024*64

#define SINGLE_DB_SIM false
#define DO_MEMSET false


#define REC_PTR false
#define WARMUP true

//static std::vector<int> rec_cnts = {1,10,100,200,400,500}; // multithreaded
//static std::vector<int> rec_cnts = {1,10,100,200,400,500}; // multithreaded
//static std::vector<int> rec_cnts = {10,10,10,10,10,10,10,10,10,10}; // multithreaded
//static std::vector<int> rec_cnts = {1}; // multithreaded
static std::vector<size_t> rec_cnts = {10}; // multithreaded
//static std::vector<int> rec_cnts = {100,100,100,100,100}; // multithreaded
//static std::vector<int> rec_cnts = {1,10,100,1000,2000,4000,5000,6000,7000,8000,10000,12000};

static std::vector<size_t> rec_sizes = {100,1024,4096}; // multithreaded
//static std::vector<int> rec_sizes = {1024}; // multithreaded
//static std::vector<int> rec_sizes = {100}; // multithreaded
//static std::vector<int> rec_sizes = {100,4096}; // multithreaded

//static std::vector<int> batch_sizes = {1,100,200,400,800,1000,2000,4000,8000,10000,20000,40000}; // multithreaded
static std::vector<size_t> batch_sizes = {1,100,200,400,800,1000,10000,100000}; // multithreaded
//static std::vector<int> batch_sizes = {1,100}; // multithreaded

static pthread_barrier_t start_bar;


typedef unsigned long long ullong;

struct args_t {
    int thd_id;
    int batch_i;
    int rec_cnt_i;
    int rec_size_i;
};

struct record_t{
    char data[R_SIZE];
};

struct stat_t{
    int64_t copy_cnt;
    ullong copy_ticks;
    ullong  bytes_copied;
    char padding[40];
};

// global variables
#if REC_PTR
static record_t * db[B_SIZE];
static record_t * dest_db[B_SIZE];
#else
static record_t * db;
static record_t * dest_db;
#endif
#define SIM_SINGLE_DB true
#if SIM_SINGLE_DB
static char* src_db;
#endif
static stat_t * stats[THD_CNT];
static stat_t *** stats_aligned;

static args_t * args_arr[THD_CNT];

void * test_copy(void * context) {

    return NULL;
}

#if REC_PTR
void * malloc_src(void * context) {
    args_t * args = (args_t *) context;
    int part = (B_SIZE/THD_CNT);
#if NUMA_NODE_CNT > 0
    numa_set_preferred(0);
#endif
    for (int i = 0; i < part; ++i){
        int ri = (args->thd_id * part) +i;
//        printf("ET_%d: testing alloc on rec %d\n", args->thd_id, ri);
        db[ri] = (record_t*) malloc(sizeof(record_t));
    }
    return NULL;
}

void * malloc_dest(void * context) {
    args_t * args = (args_t *) context;
    int part = (B_SIZE/THD_CNT);
#if NUMA_NODE_CNT > 0
    numa_set_preferred(0);
#endif
    for (int i = 0; i < part; ++i){
        int ri = (args->thd_id * part) +i;
//        printf("ET_%d: testing alloc on rec %d\n", args->thd_id, ri);
        dest_db[ri] = (record_t*) malloc(sizeof(record_t));
    }
    return NULL;
}
#endif

void * parallel_copy(void * context) {
    args_t * args = (args_t *) context;
    int part = (B_SIZE/THD_CNT);
    ullong ticka, tickb;
#if NUMA_NODE_CNT > 0
    numa_set_preferred(0);
#endif
    for (int i = 0; i < part; ++i){
        int ri = (args->thd_id * part) +i;
        ticka = rdtsc();
#if REC_PTR
        memcpy(dest_db[ri],db[ri], sizeof(record_t));
#else
        memcpy(&dest_db[ri],&db[ri], sizeof(record_t));
#endif
        tickb = rdtsc();
        stats[args->thd_id]->copy_ticks += (tickb-ticka);
        stats[args->thd_id]->copy_cnt += 1;
    }
    return NULL;
}

void full_dbtest(){
    ullong res_src_malloc;
    ullong res_dest_malloc;
    ullong res_copying;
    ullong ticka, tickb;
    cpu_set_t cpus;
    pthread_attr_t attr;

    for (int i = 0; i < THD_CNT; i++) {
        args_arr[i] = (args_t *) malloc(sizeof(args_t));
        stats[i] = (stat_t *) malloc(sizeof(stat_t));
        memset(stats[i],0, sizeof(stat_t));
    }

    pthread_attr_init(&attr);
    pthread_t * p_thds = new pthread_t[THD_CNT];
#if REC_PTR
    // malloc source data
    printf("Strating malloc src ...\n");
    ticka = rdtsc();
    for (int i = 0; i < THD_CNT; i++) {
        CPU_ZERO(&cpus);
        CPU_SET((i), &cpus);
        args_arr[i]->thd_id = (i);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
        pthread_create(&p_thds[i], &attr, malloc_src, args_arr[i]);
        pthread_setname_np(p_thds[i], "src_malloc");
    }

    for (int i = 0; i < THD_CNT; i++) {
        pthread_join(p_thds[i], NULL);
    }
    tickb = rdtsc();
    res_src_malloc = tickb - ticka;

    // malloc dest buffers in parallel
    printf("Strating malloc dest ...\n");
    ticka = rdtsc();
    for (int i = 0; i < THD_CNT; i++) {
        CPU_ZERO(&cpus);
        CPU_SET((i), &cpus);
        args_arr[i]->thd_id = (i);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
        pthread_create(&p_thds[i], &attr, malloc_dest, args_arr[i]);
        pthread_setname_np(p_thds[i], "dest_malloc");
    }

    for (int i = 0; i < THD_CNT; i++) {
        pthread_join(p_thds[i], NULL);
    }
    tickb = rdtsc();
    res_dest_malloc = tickb - ticka;
#else
    ticka = rdtsc();
    db = (record_t *) malloc(sizeof(record_t)*B_SIZE);
    tickb = rdtsc();
    res_src_malloc = tickb - ticka;

    ticka = rdtsc();
    dest_db = (record_t *) malloc(sizeof(record_t)*B_SIZE);
    tickb = rdtsc();
    res_dest_malloc = tickb - ticka;
#endif
    printf("src_malloc_ticks = %llu, time=%f\n", res_src_malloc,(res_src_malloc/CPU_FREQ)/BILLION);
    printf("dest_malloc_ticks = %llu, time=%f\n", res_dest_malloc, (res_dest_malloc/CPU_FREQ)/BILLION);
    // copy

    int64_t total_copy_cnt = 0;
    ullong total_copy_ticks = 0;
#if WARMUP
    printf("Strating copying test - warmup...\n");
    ticka = rdtsc();
    for (int i = 0; i < THD_CNT; i++) {
        CPU_ZERO(&cpus);
        CPU_SET((i), &cpus);
        args_arr[i]->thd_id = (i);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
        pthread_create(&p_thds[i], &attr, parallel_copy, args_arr[i]);
        pthread_setname_np(p_thds[i], "copy_thd");
    }

    for (int i = 0; i < THD_CNT; i++) {
        pthread_join(p_thds[i], NULL);
    }
    tickb = rdtsc();
    res_copying = tickb - ticka;

    for (int i = 0; i < THD_CNT; ++i) {
        printf("warmup - ET_%d: copied %ld records in %llu\n", i, stats[i]->copy_cnt, stats[i]->copy_ticks);
        total_copy_cnt += stats[i]->copy_cnt;
        total_copy_ticks += stats[i]->copy_ticks;
        stats[i]->copy_cnt = 0;
        stats[i]->copy_ticks =0;
    }
    printf("warmup - copying_ticks = %llu, time=%f, avg_copy_ticks=%llu, total_copy_cnt=%ld, total_copy_ticks = %llu\n",
           res_copying, (res_copying/CPU_FREQ)/BILLION, (total_copy_ticks/total_copy_cnt), total_copy_cnt, total_copy_ticks);
#endif
    printf("Strating copying test ...\n");
    ticka = rdtsc();
    for (int i = 0; i < THD_CNT; i++) {
        CPU_ZERO(&cpus);
        CPU_SET((i), &cpus);
        args_arr[i]->thd_id = (i);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
        pthread_create(&p_thds[i], &attr, parallel_copy, args_arr[i]);
        pthread_setname_np(p_thds[i], "copy_thd");
    }

    for (int i = 0; i < THD_CNT; i++) {
        pthread_join(p_thds[i], NULL);
    }
    tickb = rdtsc();
    res_copying = tickb - ticka;

    total_copy_cnt = 0;
    total_copy_ticks = 0;
    for (int i = 0; i < THD_CNT; ++i) {
        printf("ET_%d: copied %ld records in %llu\n", i, stats[i]->copy_cnt, stats[i]->copy_ticks);
        total_copy_cnt += stats[i]->copy_cnt;
        total_copy_ticks += stats[i]->copy_ticks;
        stats[i]->copy_cnt = 0;
        stats[i]->copy_ticks =0;
    }
    printf("copying_ticks = %llu, time=%f, avg_copy_ticks=%llu, total_copy_cnt=%ld, total_copy_ticks = %llu\n",
           res_copying, (res_copying/CPU_FREQ)/BILLION, (total_copy_ticks/total_copy_cnt), total_copy_cnt, total_copy_ticks);

}

void single_thead_test(const int rec_size){
    ullong trial_cnt = 1024*1024;
    ullong ticka, tickb;
    cpu_set_t cpus;
#if NUMA_NODE_CNT > 0
    numa_set_preferred(0);
#endif
    CPU_ZERO(&cpus);
    CPU_SET(0, &cpus);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpus);

    char * src_rec = (char *) malloc(rec_size);
    memset(src_rec,1, rec_size);
    char * dest_rec = (char *) malloc(rec_size);
    memset(src_rec,0, rec_size);

    ticka = rdtsc();
    for (ullong i = 0; i < trial_cnt; ++i) {
        memcpy(dest_rec,src_rec, rec_size);
    }
    tickb = rdtsc();
    ullong res_copying = tickb - ticka;
    ullong total_copy_cnt = trial_cnt;
    printf("'rec_size=%d,copying_ticks=%llu,time=%f,avg_copy_ticks=%llu,avg_copy_time(ns)=%f,total_copy_cnt=%llu'",
           rec_size,res_copying, (res_copying/CPU_FREQ)/BILLION, (res_copying/total_copy_cnt), ((((double)res_copying)/total_copy_cnt)/CPU_FREQ), total_copy_cnt);
}

void single_thead_test_cnt(const int rec_cnt, int thd_id){
    ullong trial_cnt = COPY_TRIAL_CNT;
    ullong ticka, tickb;
    cpu_set_t cpus;
#if NUMA_NODE_CNT > 0
    int node = (thd_id)/(THD_CNT/NUMA_NODE_CNT);
    numa_set_preferred(node);
#endif
    CPU_ZERO(&cpus);
    CPU_SET(thd_id, &cpus);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpus);


    record_t * src_rec = (record_t *) malloc(sizeof(record_t)*rec_cnt);
    memset(src_rec,1, sizeof(record_t)*rec_cnt);
    record_t * dest_rec = (record_t *) malloc(sizeof(record_t)*rec_cnt);
    memset(src_rec,0, sizeof(record_t)*rec_cnt);

    for (ullong i = 0; i < trial_cnt; ++i) {
        for (int j = 0; j < rec_cnt; ++j) {
            memcpy(&dest_rec[j],&src_rec[j], sizeof(record_t));
        }
    }

    ticka = rdtsc();
    for (ullong i = 0; i < trial_cnt; ++i) {
        for (int j = 0; j < rec_cnt; ++j) {
            memcpy(&dest_rec[j],&src_rec[j], sizeof(record_t));
        }
    }
    tickb = rdtsc();
    ullong res_copying = tickb - ticka;
    ullong total_copy_cnt = trial_cnt*rec_cnt;
    printf("'rec_cnt=%d,copying_ticks=%llu,time=%f,avg_copy_ticks=%llu,avg_copy_time(ns)=%f,total_copy_cnt=%llu'",
           rec_cnt,res_copying, (res_copying/CPU_FREQ)/BILLION, (res_copying/total_copy_cnt), ((((double)res_copying)/total_copy_cnt)/CPU_FREQ), total_copy_cnt);
}

void single_thread_test_driver(){
    std::vector<int> rec_sizes = {20,100,512,1024,2048,4096,8192};
    printf("[");
    for (unsigned int i = 0; i < rec_sizes.size(); ++i) {
        single_thead_test(rec_sizes[i]);
        if (i < rec_sizes.size()-1) printf(",\n");
    }
    printf("]\n");
}

void single_thread_test_cnt_driver(){
    printf("[");
    for (unsigned int i = 0; i < rec_cnts.size(); ++i) {
        single_thead_test_cnt(rec_cnts[i], i);
        if (i < rec_sizes.size()-1) printf(",\n");
    }
    printf("]\n");
}

void mthread_test_cnt(int thd_id, int bsize_idx, int rec_size_idx,int rec_cnt_idx){
    ullong trial_cnt = COPY_TRIAL_CNT;
    ullong ticka, tickb;

    size_t rec_cnt = rec_cnts[rec_cnt_idx];
    size_t rec_size = rec_sizes[rec_size_idx];
    assert(rec_cnt>0);
    assert(rec_size>0);
    ullong batch_size = batch_sizes[bsize_idx];
#if !SINGLE_DB_SIM
    size_t buff_size = rec_cnt*rec_size*batch_size;
    char * per_thd_db = (char *) malloc(buff_size);
    if (per_thd_db == NULL){
        assert(false);
    }
#if DO_MEMSET
    memset(per_thd_db,1,buff_size);
#endif
#endif
//
    size_t d_buff_size = rec_cnt*rec_size;
//    printf("d_buff_size=%lu\n", d_buff_size);
    size_t db_size = rec_cnt*rec_size*batch_size;

    char * dest_ctx = (char *) malloc(d_buff_size);
    if (dest_ctx == NULL){
        exit(1);
    }
    memset(dest_ctx,0,d_buff_size);

//    char * dest_rec_one = (char *) malloc(rec_size);
//    memset(dest_rec_one,2,rec_size);


    // warmpup -- not measured
    for (ullong i = 0; i < trial_cnt; ++i) {
        for (int k = 0; k < batch_size; ++k) {
            for (int j = 0; j < rec_cnt; ++j) {
                size_t ri = k*j*rec_size;
                size_t dri = j*rec_size;
#if SINGLE_DB_SIM
                memcpy(&dest_ctx[dri],&src_db[ri], rec_size);
#else
                memcpy(&dest_ctx[dri],&per_thd_db[ri], rec_size);
#endif
            }
        }
    }

    ticka = rdtsc();
    for (ullong i = 0; i < trial_cnt; ++i) {
        for (int k = 0; k < batch_size; ++k) {
            for (int j = 0; j < rec_cnt; ++j) {
                size_t ri = k*j*rec_size;
                size_t dri = j*rec_size;
#if SINGLE_DB_SIM
                memcpy(&dest_ctx[dri],&src_db[ri], rec_size);
#else
                memcpy(&dest_ctx[dri],&per_thd_db[ri], rec_size);
#endif
            }
        }
    }
    tickb = rdtsc();
    ullong res_copying = tickb - ticka;
    ullong total_copy_cnt = trial_cnt*rec_cnt*batch_size;
//    printf("'rec_cnt=%d,copying_ticks=%llu,time=%f,avg_copy_ticks=%llu,avg_copy_time(ns)=%f,total_copy_cnt=%llu'",
//           rec_cnt,res_copying, (res_copying/CPU_FREQ)/BILLION, (res_copying/total_copy_cnt), ((((double)res_copying)/total_copy_cnt)/CPU_FREQ), total_copy_cnt);
    stats_aligned[thd_id][rec_cnt_idx]->copy_ticks = res_copying;
    stats_aligned[thd_id][rec_cnt_idx]->copy_cnt = total_copy_cnt;
    stats_aligned[thd_id][rec_cnt_idx]->bytes_copied = rec_size*(trial_cnt*rec_cnt*batch_size);

    free(dest_ctx);
#if !SINGLE_DB_SIM
    free(per_thd_db);
#endif
}


void * mthread_test_cnt_task(void * ctx){
    int thd_id = ((args_t *) ctx)->thd_id;
    int bsize_i = ((args_t *) ctx)->batch_i;
    int rsize_i = ((args_t *) ctx)->rec_size_i;
    int rcnt_i = ((args_t *) ctx)->rec_cnt_i;
    cpu_set_t cpus;
#if NUMA_NODE_CNT > 0
    int node = (thd_id)/(THD_CNT/NUMA_NODE_CNT);
    numa_set_preferred(node);
#endif
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
//    int pthread_setschedparam(pthread_t thread, int policy,
//                              const struct sched_param *param);
    mthread_test_cnt(thd_id, bsize_i,rsize_i,rcnt_i );
//    printf("ET_%d: done with task for rec_cnt=%lu, and rec_size=%lu\n",thd_id,rec_cnts[rcnt_i],rec_sizes[rsize_i]);
    return NULL;
}

void mthread_test_cnt_driver(int batch_size_idx, int rec_size_idx, int rec_cnt_idx){

#if SINGLE_DB_SIM
    size_t buff_size = rec_cnts[rec_cnt_idx]*rec_sizes[rec_size_idx]*batch_sizes[batch_size_idx];
    src_db = (char *) malloc(buff_size);
    if (src_db == NULL){
        exit(1);
    }
#if DO_MEMSET
    memset(src_db,1,buff_size);
#endif
#endif

//    printf("db_size in bytes =%lu,db_ptr=%lu\n",buff_size, (size_t) src_db);
    pthread_t * p_thds = new pthread_t[THD_CNT];
    if (pthread_barrier_init( &start_bar, NULL, THD_CNT)){
        printf( "barriar init failed\n");
        assert(false);
    }
    for (int i = 0; i < THD_CNT; i++) {
        args_arr[i] = (args_t *) malloc(sizeof(args_t));
        args_arr[i]->thd_id = i;
        args_arr[i]->batch_i = batch_size_idx;
        args_arr[i]->rec_size_i = rec_size_idx;
        args_arr[i]->rec_cnt_i = rec_cnt_idx;
        pthread_create(&p_thds[i], NULL, mthread_test_cnt_task, args_arr[i]);
        pthread_setname_np(p_thds[i], "copy_thd");
    }
    for (int i = 0; i < THD_CNT; i++) {
        pthread_join(p_thds[i], NULL);
    }

    for (int i = 0; i < THD_CNT; i++) {
        free(args_arr[i]);
    }

    free(src_db);
    pthread_barrier_destroy(&start_bar);
    delete p_thds;
}

void init_stats(){
    stats_aligned = (stat_t ***) malloc(sizeof(stat_t*)*THD_CNT);
    int aligned_size = sizeof(stat_t) + CL_SIZE - (sizeof(stat_t)% CL_SIZE);
    for (int i = 0; i < THD_CNT; ++i) {
        stats_aligned[i] = (stat_t **) malloc(sizeof(stat_t*)*rec_cnts.size());
        for (unsigned int j = 0; j < rec_cnts.size(); ++j) {
            stats_aligned[i][j] = (stat_t *) malloc(aligned_size);
            memset(stats_aligned[i][j],0, aligned_size);
        }
    }
}

void print_stats(int batch_size_idx, int rec_size_idx, int rec_cnt_idx){
    ullong agg_copy_cnt = 0;
    ullong total_copy_ticks = 0;
    for (unsigned int i = 0; i < THD_CNT; ++i) {
        for (unsigned int j = 0; j < rec_cnts.size(); ++j) {
            ullong res_copying = stats_aligned[i][j]->copy_ticks;
            ullong total_copy_cnt = stats_aligned[i][j]->copy_cnt;
            agg_copy_cnt+= total_copy_cnt;
            total_copy_ticks += res_copying;
            assert(total_copy_cnt > 0);
//            printf("'thd_id=%d,rec_cnt=%lu,copying_ticks=%llu,time=%f,avg_copy_ticks=%llu,avg_copy_time(ns)=%f,total_copy_cnt=%llu,total_bytes_copied=%llu',\n",
//                   i,rec_cnts[j],res_copying, (res_copying/CPU_FREQ)/BILLION, (res_copying/total_copy_cnt),
//                   ((((double)res_copying)/total_copy_cnt)/CPU_FREQ), total_copy_cnt, stats_aligned[i][j]->bytes_copied);
        }
    }

    printf("batch_size=%lu,rec_size=%lu,rec_cnt=%lu,ticks_per_copy=%llu,ns_per_copy=%f\n",
           batch_sizes[batch_size_idx],rec_sizes[rec_size_idx],rec_cnts[rec_cnt_idx],
           total_copy_ticks/agg_copy_cnt, ((double) total_copy_ticks/agg_copy_cnt)/CPU_FREQ);

//    int aligned_size = sizeof(stat_t) + CL_SIZE - (sizeof(stat_t)% CL_SIZE);
    for (unsigned int i = 0; i < THD_CNT; ++i) {
        for (unsigned int j = 0; j < rec_cnts.size(); ++j) {
            free(stats_aligned[i][j]);
        }
        free(stats_aligned[i]);
    }
    free(stats_aligned);

}

int main(int argc, char** argv) {
//    full_dbtest();
//    single_thread_test_driver();
//    single_thread_test_cnt_driver();


    for (int j = 0; j < rec_sizes.size(); ++j) {
        for (unsigned int i = 0; i < batch_sizes.size(); ++i) {
            for (int k = 0; k < rec_cnts.size(); ++k) {
                init_stats();
                mthread_test_cnt_driver(i,j,k);
                print_stats(i,j,k);
//                printf("batch_size=%lu,rec_size=%lu,rec_cnt=%lu\n",batch_sizes[i],rec_sizes[j],rec_cnts[k]);
            }
        }
    }


//    exit(0);
    return 0;
}
