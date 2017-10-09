//
// Created by Thamir Qadah on 7/26/17.
// Original code is from LADS
//

#ifndef DENEVA_LADS_H
#define DENEVA_LADS_H

#include <numa.h>
#include <ycsb_query.h>
#include "util/lads_spin_lock.h"
#include "util/lads_logging.h"
#include "wl.h"

#include<unordered_map>
#include<atomic>
#include<condition_variable>
#include<mutex>
#include<chrono>
#include<time.h>
#include <spinlock.h>

namespace gdgcc {

    class ConfigInfo;
    class EnvInfo;
    class Log;

/**************** Start Action.h ************************/
/*
Action is the basic structure in DGCC.
each action belongs to a transaction and is responsible for a part of work.
PS: I like STL template!!! But, there will be many many bugs when malloc is also used...
*/
    class Action {
    private:
        /*
        Transaction ID: a transaction is divided into a set of record actions.
                        Each action is responsible for a piece of work.
        Function ID:    the work of each action is finished by function.
                        In this version, we assume it is pre-defined.
        Function Name:  The name coresponding to the predefined function.
                        In the next version, we will add self-defined function.
        Key ID:         with the key, record action can track the tuple that should be modified.

        */
        uint32_t _txnId;
        uint32_t _funcId;
        uint64_t _key;
        char *_funcName;

        Spinlock *_indegree_lock;
        uint32_t _indegree; //the action can be executed only when indegree = 0

        /*
        Define the parameter structure in record action.
        In order to make use of malloc(or self defined malloc function), we do not use
        any STL templates.
        */
        char **_parameters_char;
        uint32_t _parameters_char_cnt;

        int *_parameters_int;
        uint32_t _parameters_int_cnt;

        double *_parameters_double;
        uint32_t _parameters_double_cnt;

        void **_parameters_tuple;
        uint32_t _parameters_tuple_cnt;

//    vector<char*> *_parameters_char;
//    vector<int> *_parameters_int;
//    vector<double> *_parameters_double;
//    vector<void*> *_parameters_tuple;
//    uint32_t _parameters_cnt[4];

        //deprecated!
        uint32_t type;

    public:
        /*
        linked list is used for to organize the graph structure
        */
        Action *next; //temporal dependency
        vector<Action *> logical_dependency;
        //Action **logical_dependency;
        int cid;
#if WORKLOAD == YCSB
        ycsb_request* req;
#endif

        /*
        this function must be called after the object is malloced
        only "new" will invoke the construtor
        */

        void renew(); //IMPORTANT function

        void init(ConfigInfo *configinfo, EnvInfo *envinfo);

        uint32_t getTxnId();

        void setTxnId(uint32_t tid);

        uint32_t getFuncId();

        void setFuncId(uint32_t fid);

        uint64_t getKey();

        void setKey(uint64_t key);

        char *getFuncName();

        void setFuncName(char *fname);

        void addIndegree(uint32_t cnt);

        void setIndegree(uint32_t degree);

        uint32_t getIndegree();

        void subIndegreeByOne();

        char **getParametersChar();

        char *getParametersChar(uint32_t indice);

        void setParametersChar(char *para, uint32_t indice);

        int *getParametersInt();

        int getParametersInt(uint32_t indice);

        void setParametersInt(int para, uint32_t indice);

        double *getParametersDouble();

        double getParametersDouble(uint32_t indice);

        void setParameterssDouble(double para, uint32_t indice);


        void **getParametersTuple();

        void *getParametersTuple(uint32_t indice);

        void setParametersTuple(void *para, uint32_t indice);

        uint32_t getParameterCnt(uint32_t type);

        void setParameterCnt(uint32_t type, uint32_t cnt);
    };


/*
organize record action as linked list.
Each tuple(record) will maintain a actionqueue, we wrap it
in the structure of TupleActionLinkedList
*/
    class ActionQueue {
    private:
//	void init(uint32_t wnumber);
//	uint32_t workernumber;
        Action *_head;
        Action *_curpos;

//        Spinlock lock = new Spinlock();
        spinlock * lock = new spinlock();

    public:
        ActionQueue();

        void init();

        Action *getHead();

        void setHead(Action *action);

        Action *getCurpos();
//    void setCurpos(Action* action);

        bool pushback(Action *action);

        Action *fetch();
    };


/*
in the database, each tuple maintains a linkedlist and
all record actions coresponding to that tuple are maintained in its linkedlist.
@IMPORTANT: this data structure is maintained as a column in each tuple!
*/
    class TupleActionLinkedList {
    private:
        uint64_t _key;
        uint32_t _machineId;    //which machine is this tuple located on
        uint32_t _numaId;       //which numa node this tuple located on
        bool _isnuma;           //the system is deployed on numa architecture or not

        ActionQueue *_actqueue;

        ConfigInfo *_configinfo;
    public:
        TupleActionLinkedList();

        TupleActionLinkedList(uint64_t key, ConfigInfo *configinfo);

        void init(uint64_t key, ConfigInfo *configinfo);

        void clear();

        uint64_t getKey();

        void setKey(uint64_t key);

        uint32_t getMachineId();

        void setMachineId(uint32_t m_id);

        uint32_t getNumaId();

        void setNumaId(uint32_t nid);

        bool isNuma();

        ActionQueue *getActionQueue();

        void initActionQueue();

        bool pushbackAction(Action *action);

        Action *fetchAction();

        void pushfrontAction(Action *action);

        void setConfigInfo(ConfigInfo *configinfo);

        ConfigInfo *getConfigInfo();
    };


/*
the class ActiveTupleList is monitoring the active tuple set in each round.
with such kind of information, it is easier for the sytem to do load balancing during the execution

each site/machine only invoke on ActiveTupleList object
important: use new to initialize this object!!! not use malloc
*/
    class ActiveTupleList {
    private:
        ConfigInfo *_configinfo;
        uint32_t _part_cnt;  //this info is extracted from configinfo
        uint32_t _cur_part_indice;

        bool *_activeTupleBitMap;

        //vector<TupleActionLinkedList*> _activetuplesVector;
        TupleActionLinkedList ***_activetuplesVector;
        uint32_t *_active_cnt_on_part;
        Spinlock *_spinlock;

    public:
        ActiveTupleList();

        ActiveTupleList(ConfigInfo *configinfo);
        void clear();

        ConfigInfo *getConfigInfo();

        uint32_t getPartitionCnt();

        uint32_t getCurrentPartIndice();

        bool isActive(uint64_t key);

        bool isActive(TupleActionLinkedList *actionlist);

        bool markToActive(TupleActionLinkedList *actionlist);

        /*
        @IMPORTANT: should add "get" functions to return
        executable sets
        Retuen Value: the number of active tuple in the partiton
        executablevector: the vector entrance
        */

        // uint32_t getExecutableSet(TupleActionLinkedList** executablevector, uint32_t part_id);
        TupleActionLinkedList **getExecutableSet(uint32_t part_id);

        uint32_t getExecutableSetCnt(uint32_t part_id);

        //unordered_map<uint32_t, bool>* getActiveTupleMap();
        //vector<list<TupleActionLinkedList*>* > getActiveTupleVector();
        //list<TupleActionLinkedList*>* getActiveTuplesByPartitionId(uint32_t partid);
    };


/*
 * this class is not used anymore
 */
    class DSSiteVertex {
    public:
        void init(uint32_t nodeid, uint32_t wnumber);

        uint32_t workernumber;
        ActionQueue **actqueue;
    };

/**************** End Action.h ************************/

/**************** Start  ConfigInfo.h ************************/
#ifdef __GNUC__
#define DEPRECATED(func) func __attribute__ ((deprecated))
#elif defined(_MSC_VER)
    #define DEPRECATED(func) __declspec(deprecated) func
#else
#pragma message("WARNING: You need to implement DEPRECATED for this compiler")
#define DEPRECATED(func) func
#endif

#define NUMADISABLED 999
#define RET_ERROR   123321
#define PCHAR   444
#define PINT    555
#define PDOUBLE 666
#define PTUPLE  777


#define DGCC_DEBUG


    class ConfigInfo{
    public:
        uint32_t worker_thread_cnt; //how many constructors/workers are used on each machine
        uint32_t partition_cnt;

        uint32_t machine_id;

        uint32_t unordered_map_bucket_cnt;


        /*
        action parameter configuration!
        */
        uint32_t Conf_charLength;
        uint32_t Conf_charCnt;
        uint32_t Conf_intCnt;
        uint32_t Conf_doubleCnt;
        uint32_t Conf_tupleCnt;

        uint32_t Conf_DATASIZE;


        /*
        define the size of each action buffer
        */
        uint32_t Conf_ActionBuffer_Size;

        /*
        define the number of executable sets, by default,
        the number equals to the number for worker thread: worker_thread_cnt;
        */
        uint32_t Conf_ExecutableSet_Cnt;


        /*
         * define the batch size of each round in DGCC
         * */
        uint32_t Conf_DGCC_Batch_size;

        /*
         * define txn manager params
         * */
//        int Txn_Queue_Size;  //the maximal size of each txn queue


        /*YCSB Workload Parameters*/
//        double  YCSBWorkload_Write_Percentage;
//        double  YCSBWorkload_Cross_Ratio;
//        int     YCSBWorkload_Req_Per_Txn;
//        double  YCSBWorkload_Theta;
//        uint32_t YCSBWorkload_Data_Size;
    };

/*
save the environment information(hardware information)
*/
    class EnvInfo {
    public:
        bool isnuma;
        uint32_t numaId;
    };

/**************** End ConfigInfo.h ************************/

/**************** Start syshelper.h ************************/
    static bool NUMAEnabled() UNUSED;
    static bool NUMAEnabled()
    {
        if(numa_available() < 0) {
//            Log::info("The system does not support NUMA APIs");
            return false;
        }
        return true;
    }

    static uint32_t getNumaId() UNUSED;
    static uint32_t getNumaId(){
        return 0;
    }
/**************** End syshelper.h ************************/


/**************** Start ActionBuffer.h ************************/
/*
 * "Action" data structure is frequently used.
 * In order to avoid to invoke malloc/free syscalls frequently, a action buffer
 * is maintained that recycle the actions and reuse them in the further
 * */
    class ActionBuffer {
    private:
        ConfigInfo* _configinfo;
        EnvInfo* _envinfo;
        uint64_t alloc_cnt=0;
        uint64_t reuse_cnt=0;
        uint64_t get_cnt=0;
        uint64_t put_cnt=0;

        boost::lockfree::queue<Action *> * action_free_list[PLAN_THREAD_CNT];

    public:
        ActionBuffer();
        void init(ConfigInfo* configinfo, EnvInfo* envinfo);

        ConfigInfo* getConfigInfo();
        EnvInfo* getEnvInfo();

        void get(int cid, Action* &ret);
        void put(int cid, Action* act);
    };
/**************** End ActionBuffer.h ************************/

    /**************** Start ActionDependencyGraph.h ************************/

    class ActionDependencyGraph {
    private:
        /*
        each tupleinkedlist is responsible for mointoring record actions
        on each tuple.
        */
        ConfigInfo *_configinfo;
        ActiveTupleList *_activeTupleList;
        TupleActionLinkedList** _actionlist_hashmap;
    public:
        UInt32 _id;

        ActionDependencyGraph(UInt32 id);
        ActionDependencyGraph(ConfigInfo* configinfo, UInt32 id);

        void init();
        void clear();

        ConfigInfo* getConfigInfo();
        void setConfigInfo(ConfigInfo* ConfigInfo);

        ActiveTupleList* getActiveTupleList();
        void setActiveTupleList(ActiveTupleList* activetuplelist);

        TupleActionLinkedList* getTupleActionLinkedListByKey(uint64_t key);


        /*
        when transaction is decomposed into actions, system re-organize them
        into graphs, and then do the execution according to the dependency graphs
        */
        bool addActionToGraph(uint64_t key, Action *action);

//    uint32_t getExecutableSetByPartId(TupleActionLinkedList** executableset, uint32_t part_id);
        TupleActionLinkedList** getExecutableSetByPartId(uint32_t part_id);
        uint32_t getExecutableSetCntByPartId(uint32_t part_id);

    };

    /**************** End ActionDependencyGraph.h ************************/


    class SyncWorker {
    public:
        std::mutex*                 construction_mutex;
        std::mutex*                 construction_lock_mutex;
        std::mutex*                 execution_mutex;
        std::mutex*                 execution_lock_mutex;

        spinlock *                  construction_slock;
        spinlock *                  execution_slock;

        std::condition_variable*    construction_cv;
        std::condition_variable*    execution_cv;
        bool* execution_continue;
//        atomic_bool batch_ready;
        atomic_bool const_phase;
        atomic_bool const_wakeup;
        atomic_bool exec_wakeup;

        struct timespec t1;
        struct timespec t2;

        atomic<uint32_t>     c_finish_cnt;
        atomic<uint32_t>     e_finish_cnt;

        ConfigInfo*                 configinfo;

        SyncWorker();
        SyncWorker(ConfigInfo* configinfo);
        ~SyncWorker();

        void constructor_wait(int cid);
        void    executor_wait(int eid,int dgraph_cnt, ActionDependencyGraph**  dgraphs);
        void executor_wait_begin(int eid);  //first time to start executor
    };


    class Constructor : public Thread {
    public:

        Constructor();
        Constructor(ConfigInfo* configinfo, int id, Workload* wload);
        ~Constructor();

        RC run();
        void setup();

        void set_config_info(ConfigInfo* configinfo);


        //@Important:the following three functions must be invoked after construction
//        void setTxnManager(TxnManager* mgr);
        void setDependencyGraph(ActionDependencyGraph* graph);
        void setSyncWorker(SyncWorker* syncworker);

        //bind thread to a fixed CPU to avoid frequent context switching
        void bind_cpu();
        void run_one_queue();
        void set_cid(int id);

        ActionBuffer* getActionAllocator();
        void    setActionAllocator(ActionBuffer* ab);

        Workload* get_workload();
        void      set_workload(Workload* wload);

    private:
        ConfigInfo*     configinfo;
        int             cid;
        uint32_t        batchsize;      //get the info from configinfo
        ActionDependencyGraph* dgraph;
        SyncWorker*     syncworker;
        ActionBuffer*   action_allocator;
        Workload*       workload;
    };//end of Constructor definition



    class Executor : public Thread {
    private:
        ConfigInfo*     configinfo;
        int             eid;
        uint32_t        batchsize;
        ActionDependencyGraph**      dgraphs;
        int             partid;
        SyncWorker*     syncworker;
        Workload*       workload;
        TxnManager*     txn_man;

    public:
        Executor();
        Executor(ConfigInfo* configinfo, int id, Workload* wload);
        ~Executor();

        RC run();
        void setup();

        void set_eid(int id);

        //@Important:the following three functions must be invoked after construction
        void    setDependencyGraphQueue(ActionDependencyGraph** graphs);
        void    setSyncWorker(SyncWorker* syncworker);

        Workload* get_workload();
        void     set_workload(Workload* wload);


        void    bind_cpu();

        void    run_one_queue();
        void    do_work(TupleActionLinkedList** wset, uint32_t size);
        bool    do_work_on_tuple(TupleActionLinkedList* tset);
    };




    class DGCCSite {
    private:
        ConfigInfo*     configinfo;
        EnvInfo*        envinfo;
        int             siteid;
        int             workercnt;


        //by default: the number of constructor equals to that of executor
        Constructor**    constructors;
        Executor**       executors;
        SyncWorker*     syncworker;

        //each site maintains multiple dependency graphs that are constructed in parallel
        ActionDependencyGraph** dgraphs;

        Workload* workload;


    public:
        DGCCSite(ConfigInfo* configinfo, Workload* wload);
        DGCCSite();
        ~DGCCSite();

        ConfigInfo* getConfigInfo();
        void        setConfigInfo(ConfigInfo* configinfo);

        EnvInfo*    getEnvInfo();
        void        setEnvInfo(EnvInfo* envinfo);

        int         getSiteId();
        void        setSiteId(int id);

        int         getWorkerCnt();
        void        setWorkerCnt(int cnt);

        Workload*   get_workload();
        void        set_workload(Workload* wload);

        ActionDependencyGraph**  getDependencyGraph();
        ActionDependencyGraph*   getDependencyGraph(int indice);
        void  setDependencyGraph(ActionDependencyGraph** graph);

        void        startwork();
    };



/*
//TODO(tq): see if we can enable TPCC workload using this peiece of code


class TPCCWorkload : public  Workload {
public:
        void executeAction(Action* action);
        void resolveTxn(BaseTxn* txn, ActionDependencyGraph* graph, ActionBuffer* allocator, int workerid);

		static uint32_t warehouseKey(uint32_t w_id)
		{
			return w_id;
		}
		static uint32_t itemKey(uint32_t i_id)
		{
			return i_id;
		}
		static uint32_t districtKey(uint32_t d_id, uint32_t d_w_id)
		{
			return DISTRICT + d_id;
		}
		static uint32_t customerKey(uint32_t c_id, uint32_t c_d_id, uint32_t c_w_id)
		{
			return districtKey(c_d_id, c_w_id) * CUSTOMER + c_id;
		}
		static uint32_t historyKey(uint32_t c_id, uint32_t c_d_id, uint32_t c_w_id)
		{
			return customerKey(c_id, c_d_id, c_w_id);
		}
		static uint32_t orderlineKey(uint32_t w_id, uint32_t d_id, uint32_t o_id, uint32_t o_number)
		{
			return orderKey(w_id, d_id, o_id)*MAX_CNT + o_number;
		}
		static uint32_t orderKey(uint32_t w_id, uint32_t d_id, uint32_t o_id)
		{
			return districtKey(d_id, w_id)*CUSTOMER + o_id;
		}
		static uint32_t neworderKey(uint32_t d_id, uint32_t w_id, uint32_t o_id)
		{
			return orderKey(w_id, d_id, o_id);
		}
		static uint32_t stockKey(uint32_t s_i_id, uint32_t s_w_id)
		{
			return s_w_id * ITEM + s_i_id;
		}
};

*/


}
#endif //DENEVA_LADS_H
