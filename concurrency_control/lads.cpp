//
// Created by Thamir Qadah on 7/26/17.
//
#include <message.h>
#include <ycsb_query.h>
#include <thread.h>
#include "global.h"
#include "wl.h"
#include "lads.h"
#include "txn.h"
#include "work_queue.h"
#include "ycsb.h"

using namespace std;
using namespace gdgcc;

class Thread;

namespace gdgcc {
/**************** Start of Action.cpp ************************/
/*
implementation of TupleActionLinkedList
*/
    TupleActionLinkedList::TupleActionLinkedList() {
        this->_actqueue = nullptr;
        this->_configinfo = nullptr;

        this->_key = 0;
        this->_machineId = 0;
        this->_numaId = 0;
        this->_isnuma = false;

        //  DEBUG_Q("System creates TupleActiveLinkedList. But it needs to be initialized!!!");
    }

    TupleActionLinkedList::TupleActionLinkedList(uint64_t key, ConfigInfo *configinfo) {
        this->_key = key;
        this->_machineId = configinfo->machine_id;
        this->_isnuma = NUMAEnabled();
        if (isNuma()) {
            this->_numaId = getNumaId();
        } else {
            this->_numaId = 0;
        }

        this->_configinfo = configinfo;
        //this->_actqueue = static_cast<ActionQueue*>(gdgccMallocWrapper(sizeof(ActionQueue)));
        //TODO:need to replace the "NEW" with more efficient malloc function
        this->_actqueue = new ActionQueue();
    }

    void TupleActionLinkedList::init(uint64_t key, ConfigInfo *configinfo) {
        this->_key = key;
        this->_machineId = configinfo->machine_id;
        this->_isnuma = NUMAEnabled();
        if (isNuma()) {
            this->_numaId = getNumaId();
        } else {
            this->_numaId = 0;
        }

        this->_configinfo = configinfo;
        //this->_actqueue = static_cast<ActionQueue*>(gdgccMallocWrapper(sizeof(ActionQueue)));
        //TODO:need to replace the "New" with more efficient malloc function
        this->_actqueue = new ActionQueue();
    }

    void TupleActionLinkedList::clear() {
        this->_actqueue->init();
    }


    uint64_t TupleActionLinkedList::getKey() {
        return this->_key;
    }

    void TupleActionLinkedList::setKey(uint64_t key) {
        this->_key = key;
    }

    uint32_t TupleActionLinkedList::getMachineId() {
        return this->_machineId;
    }

    void TupleActionLinkedList::setMachineId(uint32_t mid) {
        this->_machineId = mid;
    }

    uint32_t TupleActionLinkedList::getNumaId() {
        if (!isNuma()) { //if numa is not enabled
//            DEBUG_Q("NUMA is not supported by the system!");
        }
        return this->_numaId;
    }

    void TupleActionLinkedList::setNumaId(uint32_t nid) {
        if (!isNuma()) {
//            DEBUG_Q("NUMA is not supported by the system! CANNOT set NUMA ID!!!");
            this->_numaId = NUMADISABLED;
        } else {
            this->_numaId = nid;
        }
    }

    bool TupleActionLinkedList::isNuma() {
        return this->_isnuma;
    }

    ActionQueue *TupleActionLinkedList::getActionQueue() {
        return this->_actqueue;
    }

    void TupleActionLinkedList::initActionQueue() {
        if (this->_actqueue == nullptr) {
            //this->_actqueue = static_cast<ActionQueue*>(gdgccMallocWrapper(sizeof(ActionQueue)));
            //TODO: need to replace "NEW" with more efficient malloc function
            this->_actqueue = new ActionQueue();
        } else {
//            DEBUG_Q("the ActionQueue is already initialized during the construction!!!");
        }
    }

    bool TupleActionLinkedList::pushbackAction(Action *action) {
        if (this->_actqueue == nullptr) {
//            DEBUG_Q("ActionQueue is nullptr! Insert action failed!!!");
            return false;
        }
        this->_actqueue->pushback(action);
        return true;
    }

    void TupleActionLinkedList::pushfrontAction(Action *action) {
        if (this->_actqueue == nullptr) {
//            DEBUG_Q("ActionQueue is nullptr! Push front fails!");
            return;
        }
        this->_actqueue->setHead(action);
    }

    Action *TupleActionLinkedList::fetchAction() {
        if (this->_actqueue == nullptr) {
//            DEBUG_Q("ActionQueue is nullptr! Fetch action failed!!!");
            return nullptr;
        }
        return this->_actqueue->fetch();
    }

    ConfigInfo *TupleActionLinkedList::getConfigInfo() {
        return this->_configinfo;
    }


    void TupleActionLinkedList::setConfigInfo(ConfigInfo *configinfo) {
        this->_configinfo = configinfo;
    }


//=======================================================================================

/*
implementation of ActiveTupleList
*/
    ActiveTupleList::ActiveTupleList() {
        this->_configinfo = nullptr;
        this->_part_cnt = 0;
        this->_cur_part_indice = 0;
        this->_spinlock = nullptr;
        //   DEBUG_Q("System creates ActiveTupleList. But it needs to be initialized!!!");
    }

    ActiveTupleList::ActiveTupleList(ConfigInfo *configinfo) {
        this->_configinfo = configinfo;
        this->_part_cnt = configinfo->partition_cnt;
//    DEBUG_Q("ActiveTupleList::part_cnt = %d", _part_cnt);
        this->_cur_part_indice = 0;

        this->_activeTupleBitMap = new bool[configinfo->Conf_DATASIZE + 1];
        memset(_activeTupleBitMap, 0, sizeof(bool) * (configinfo->Conf_DATASIZE + 1));
//    for(int i=0; i< configinfo->Conf_DATASIZE + 1; i++) {
//        _activeTupleBitMap[i] = false;
//    }

        this->_activetuplesVector = new TupleActionLinkedList **[_part_cnt];
        for (uint32_t i = 0; i < _part_cnt; i++) {
            _activetuplesVector[i] = new TupleActionLinkedList *[configinfo->Conf_DATASIZE];
        }

        this->_active_cnt_on_part = new uint32_t[_part_cnt];
        for (uint32_t i = 0; i < _part_cnt; i++) {
            _active_cnt_on_part[i] = 0;
        }
        // this->_activeTupleMap = new unordered_map<uint32_t, bool>();
        // _activeTupleMap->reserve(configinfo->unordered_map_bucket_cnt);

        this->_spinlock = new Spinlock();
//        DEBUG_Q("System creates ActiveTupleList.");
    }

    void ActiveTupleList::clear() {
        memset(_activeTupleBitMap, 0, sizeof(bool)*(_configinfo->Conf_DATASIZE + 1));
        for(uint32_t i=0; i<_part_cnt; i++) {
            for (uint32_t j=0; j < _configinfo->Conf_DATASIZE; j++){
                _activetuplesVector[i][j] = nullptr;
            }
            _active_cnt_on_part[i] = 0;
        }
    }

    ConfigInfo *ActiveTupleList::getConfigInfo() {
        return this->_configinfo;
    }

    uint32_t ActiveTupleList::getPartitionCnt() {
        return this->_part_cnt;
    }

    uint32_t ActiveTupleList::getCurrentPartIndice() {
        return this->_cur_part_indice;
    }

    bool ActiveTupleList::isActive(uint64_t key) {
        return this->_activeTupleBitMap[key];
    }

    bool ActiveTupleList::isActive(TupleActionLinkedList *actionlist) {
        if (actionlist == nullptr) {
            //DEBUG_Q("input actionlist is nullptr!!!");
            return false;
        }
        return this->_activeTupleBitMap[actionlist->getKey()];
    }

    bool ActiveTupleList::markToActive(TupleActionLinkedList *actionlist) {
        if (actionlist == nullptr) {
            //DEBUG_Q("input actionlist is nullptr!!!");
            return false;
        }
        if (!_activeTupleBitMap[actionlist->getKey()]) {
            _spinlock->lock();
            _activeTupleBitMap[actionlist->getKey()] = true;
            _activetuplesVector[_cur_part_indice][_active_cnt_on_part[_cur_part_indice]] = actionlist;
            _active_cnt_on_part[_cur_part_indice]++;
            _cur_part_indice = (_cur_part_indice + 1) % _part_cnt;
            _spinlock->unlock();
        }
        return true;
    }

    TupleActionLinkedList **ActiveTupleList::getExecutableSet(uint32_t part_id) {
        return this->_activetuplesVector[part_id];
    }

    uint32_t ActiveTupleList::getExecutableSetCnt(uint32_t part_id) {
        return this->_active_cnt_on_part[part_id];
    }

//=============================================================================================


/*
implementation of ActionQueue
*/
    ActionQueue::ActionQueue() {
        this->_head = nullptr;
        this->_curpos = nullptr;
    }

    void ActionQueue::init() {
        this->_head = nullptr;
        this->_curpos = nullptr;
    }

    Action *ActionQueue::getHead() {
        return this->_head;
    }

    void ActionQueue::setHead(Action *action) {
        lock->lock();
        if (_head == nullptr){
            _head = action;
        }
        else{
            action->next = this->_head->next;
            this->_head = action;
        }

//        if (this->_head->next != nullptr) {
//            action->next = this->_head->next;
//            this->_head = action;
//        } else {
//            this->_head = action;
//            action->next = nullptr;
//        }
        lock->unlock();
    }

    Action *ActionQueue::getCurpos() {
        return this->_curpos;
    }

    bool ActionQueue::pushback(Action *action) {
        lock->lock();
        if (_head == nullptr){
            assert(this->_curpos == nullptr);
            _head = action;
            _curpos = action;
        }
        else{
            assert(_head != nullptr && _curpos != nullptr);
            action->addIndegree(1);// add temporal dependency
            if (_curpos != nullptr){
                _curpos->next = action;
                _curpos = action;
            }
//            else{
//
//            }

        }

//        if(this->_head == nullptr && this->_curpos == nullptr) {
//            this->_head = action;
//            this->_curpos = action;
//        }else{
//            assert(this->_head != nullptr);
//            action->addIndegree(1);// add temporal dependency
//            this->_curpos->next = action;
//            this->_curpos = action;
//
//        }
        lock->unlock();
        return true;
    }

    Action *ActionQueue::fetch() {
        lock->lock();
        if (this->_head == nullptr){
            lock->unlock();
            return nullptr;
        }
        Action *ret = this->_head;
        this->_head = this->_head->next;
        lock->unlock();
        return ret;
    }


//==========================================================================================

/*
implementation of Action
*/

//Init function must be called when Action object is allocated.
    void Action::init(ConfigInfo *configinfo, EnvInfo *envinfo) {
        this->next = nullptr;
        this->_txnId = 0;
        this->_funcId = 0;
        this->_key = 0;

        this->_indegree = 0;
        this->_indegree_lock = new Spinlock();

        //TODO: need to replace "NEW" with more efficient malloc function
        //this->_funcName = static_cast<char*>(gdgccMallocWrapper(configinfo->Conf_charLength * sizeof(char), envinfo));
        this->_funcName = new char[configinfo->Conf_charLength];

        if (configinfo->Conf_charCnt > 0) {
            this->_parameters_char_cnt = configinfo->Conf_charCnt;
            //this->_parameters_char = static_cast<char**>(gdgccMallocWrapper(configinfo->Conf_charCnt * sizeof(char*), envinfo));
            this->_parameters_char = new char *[configinfo->Conf_charCnt];
            for (uint32_t i = 0; i < configinfo->Conf_charCnt; i++) {
                // _parameters_char[i] = static_cast<char*>(gdgccMallocWrapper(configinfo->Conf_charLength * sizeof(char), envinfo));
                _parameters_char[i] = new char[configinfo->Conf_charLength];
            }
        }

        if (configinfo->Conf_intCnt > 0) {
            //this->_parameters_int = static_cast<int*>(gdgccMallocWrapper(configinfo->Conf_intCnt * sizeof(int), envinfo));
            this->_parameters_int = new int[configinfo->Conf_intCnt];
            this->_parameters_int_cnt = configinfo->Conf_intCnt;
        }

        if (configinfo->Conf_doubleCnt > 0) {
            //this->_parameters_double = static_cast<double*>(gdgccMallocWrapper(configinfo->Conf_doubleCnt * sizeof(double), envinfo));
            this->_parameters_double = new double[configinfo->Conf_doubleCnt];
            this->_parameters_double_cnt = configinfo->Conf_doubleCnt;
        }

        if (configinfo->Conf_tupleCnt > 0) {
            //this->_parameters_tuple = static_cast<void**>(gdgccMallocWrapper(configinfo->Conf_tupleCnt * sizeof(void*), envinfo));
            this->_parameters_tuple = new void *[configinfo->Conf_tupleCnt];
            this->_parameters_tuple_cnt = configinfo->Conf_tupleCnt;
        }
#if WORKLOAD == YCSB
        //TODO(tq): when to free this??
        this->req = (ycsb_request*) mem_allocator.alloc(sizeof(ycsb_request));
#endif
        //@deprecated member
        this->type = 0;
    }


    void Action::renew() {
        this->_txnId = 0;
        this->_funcId = 0;
        this->_funcName = nullptr;
        this->_key = 0;
        this->_indegree = 0;
        this->next = nullptr;
        logical_dependency.clear();
#if WORKLOAD == YCSB
        memset(this->req, 0, sizeof(ycsb_request));
#endif
    }

    uint32_t Action::getTxnId() {
        return this->_txnId;
    }

    void Action::setTxnId(uint32_t txnid) {
        this->_txnId = txnid;
    }

    uint32_t Action::getFuncId() {
        return this->_funcId;
    }

    void Action::setFuncId(uint32_t fid) {
        this->_funcId = fid;
    }

    uint64_t Action::getKey() {
        return this->_key;
    }

    void Action::setKey(uint64_t key) {
        this->_key = key;
    }

    char *Action::getFuncName() {
        return this->_funcName;
    }

    void Action::setFuncName(char *fname) {
        //fname should have the same length as _funcName, and equal to configinfo->charLength
        memcpy(this->_funcName, fname, sizeof(fname));
    }

    void Action::addIndegree(uint32_t cnt) {
        _indegree_lock->lock();
        _indegree += cnt;
        _indegree_lock->unlock();
    }

    uint32_t Action::getIndegree() {
        return _indegree;
    }

    void Action::setIndegree(uint32_t degree) {
        _indegree_lock->lock();
        _indegree = degree;
        _indegree_lock->unlock();
    }

    void Action::subIndegreeByOne() {
        _indegree_lock->lock();
        _indegree -= 1;
        _indegree_lock->unlock();
    }

    char **Action::getParametersChar() {
        return this->_parameters_char;
    }

    char *Action::getParametersChar(uint32_t indice) {
        if (indice >= this->_parameters_char_cnt) {
//            DEBUG_Q("Action::getParametersChar(): out of range!");
            return nullptr;
        }
        return _parameters_char[indice];
    }

    void Action::setParametersChar(char *para, uint32_t indice) {
        if (indice >= this->_parameters_char_cnt) {
//            DEBUG_Q("Action::getParametersChar(): out of range!");
            return;
        }
        memcpy(_parameters_char[indice], para, sizeof(para));
    }


    int *Action::getParametersInt() {
        return this->_parameters_int;
    }

    int Action::getParametersInt(uint32_t indice) {
        if (indice >= this->_parameters_int_cnt) {
//            DEBUG_Q("Action::getParametersInt(): out of range!");
            return RET_ERROR;
        }
        return _parameters_int[indice];
    }

    void Action::setParametersInt(int para, uint32_t indice) {
        if (indice >= this->_parameters_int_cnt) {
//            DEBUG_Q("Action::getParametersInt(): out of range!");
            return;
        }
        _parameters_int[indice] = para;
    }


    double *Action::getParametersDouble() {
        return this->_parameters_double;
    }

    double Action::getParametersDouble(uint32_t indice) {
        if (indice >= this->_parameters_double_cnt) {
//            DEBUG_Q("Action::getParametersDouble(): out of range!");
        }
        return _parameters_double[indice];
    }

    void Action::setParameterssDouble(double para, uint32_t indice) {
        if (indice >= this->_parameters_double_cnt) {
//            DEBUG_Q("Action::getParametersDouble(): out of range!");
        }
        _parameters_double[indice] = para;
    }

    void **Action::getParametersTuple() {
        return this->_parameters_tuple;
    }

    void *Action::getParametersTuple(uint32_t indice) {
        if (indice >= this->_parameters_tuple_cnt) {
//            DEBUG_Q("Action::getParametersTuple(): out of range!");
        }
        return _parameters_tuple[indice];
    }

    void Action::setParametersTuple(void *para, uint32_t indice) {
        if (indice >= this->_parameters_tuple_cnt) {
//            DEBUG_Q("Action::getParametersTuple(): out of range!");
        }
        _parameters_tuple[indice] = para;
    }


    uint32_t Action::getParameterCnt(uint32_t type) {
        switch (type) {
            case PCHAR:
                return _parameters_char_cnt;
            case PINT:
                return _parameters_int_cnt;
            case PDOUBLE:
                return _parameters_double_cnt;
            case PTUPLE:
                return _parameters_tuple_cnt;
            default:
                return RET_ERROR;
        }
    }

    void Action::setParameterCnt(uint32_t type, uint32_t cnt) {
        switch (type) {
            case PCHAR:
                _parameters_char_cnt = cnt;
                return;
            case PINT:
                _parameters_int_cnt = cnt;
                return;
            case PDOUBLE:
                _parameters_double_cnt = cnt;
                return;
            case PTUPLE:
                _parameters_tuple_cnt = cnt;
                return;
            default:
                return;
        }
    }

/**************** End of Action.cpp ************************/

    /**************** Start of ActionBuffer.cpp ************************/

    ActionBuffer::ActionBuffer(){
        this->_configinfo = nullptr;
        this->_envinfo = nullptr;
    }

    void ActionBuffer::init(ConfigInfo* configinfo, EnvInfo* envinfo){
        this->_configinfo = configinfo;
        this->_envinfo = envinfo;

        for (uint64_t j = 0; j < g_plan_thread_cnt; ++j) {
            action_free_list[j] = new boost::lockfree::queue<Action *>(FREE_LIST_INITIAL_SIZE);
        }

    }

    ConfigInfo* ActionBuffer::getConfigInfo(){
        return this->_configinfo;
    }

    EnvInfo* ActionBuffer::getEnvInfo(){
        return this->_envinfo;
    }

    void ActionBuffer::get(int cid, Action* &ret){
        get_cnt++;
        if (!action_free_list[cid]->pop(ret)){
            ret = (Action *) mem_allocator.alloc(sizeof(Action));
            ret->init(this->_configinfo, this->_envinfo);
            alloc_cnt++;
        }
        else{
            reuse_cnt++;
            ret->renew();
        }
        ret->cid = cid;
//        if (get_cnt % 10000 == 0){
//            DEBUG_Q("status: get_cnt=%ld, put_cnt=%ld, alloc_cnt=%ld, reuse_cnt=%ld\n",
//            get_cnt, put_cnt, alloc_cnt, reuse_cnt);
//        }
    }
    void ActionBuffer::put(int cid, Action* act){
        if (cid < 0){
            cid = act->cid;
//            DEBUG_Q("status: get_cnt=%ld, put_cnt=%ld, alloc_cnt=%ld, reuse_cnt=%ld\n")
        }
        while(!action_free_list[cid]->push(act)){}
        put_cnt++;
    }

    /**************** End of ActionBuffer.cpp ************************/

    /**************** Start of ActionDependencyGraph.cpp ************************/

    ActionDependencyGraph::ActionDependencyGraph(UInt32 id)
    {
        this->_configinfo = nullptr;
        this->_activeTupleList = nullptr;
        this->_actionlist_hashmap= nullptr;
        _id = id;
    }

    ActionDependencyGraph::ActionDependencyGraph(ConfigInfo* configinfo, UInt32 id)
    {
        this->_configinfo = configinfo;
        this->_activeTupleList = nullptr;
        this->_actionlist_hashmap= nullptr;
        _id = id;
    }
    void ActionDependencyGraph::init() {
        this->_activeTupleList = new ActiveTupleList(configinfo);
        this->_actionlist_hashmap = new TupleActionLinkedList*[configinfo->Conf_DATASIZE];
        for(uint32_t i=0; i<configinfo->Conf_DATASIZE; i++) {
            _actionlist_hashmap[i] = new TupleActionLinkedList(i, configinfo);
        }
    }

    void ActionDependencyGraph::clear() {
        this->_activeTupleList->clear();
        for(uint32_t i=0; i<this->_configinfo->Conf_DATASIZE; i++) {
            _actionlist_hashmap[i]->clear();
        }
    }

    ConfigInfo* ActionDependencyGraph::getConfigInfo()
    {
        return this->_configinfo;
    }

    void ActionDependencyGraph::setConfigInfo(ConfigInfo* configinfo)
    {
        this->_configinfo = configinfo;
    }


    ActiveTupleList* ActionDependencyGraph::getActiveTupleList()
    {
        return this->_activeTupleList;
    }

    void ActionDependencyGraph::setActiveTupleList(ActiveTupleList* activetuplelist)
    {
        this->_activeTupleList = activetuplelist;
    }

    TupleActionLinkedList* ActionDependencyGraph::getTupleActionLinkedListByKey(uint64_t key) {
        return _actionlist_hashmap[key];
    }

    bool ActionDependencyGraph::addActionToGraph(uint64_t key, Action* action)
    {
        /*
        TODO: getTupleActionLinkedListById(key) need to be updated!!!
        */
        TupleActionLinkedList* tupleactionlinkedlist = getTupleActionLinkedListByKey(key);
        bool ret;
        ret= tupleactionlinkedlist->pushbackAction(action);
        assert(ret);
        ret = this->_activeTupleList->markToActive(tupleactionlinkedlist);
        assert(ret);

        return ret;
    }

    TupleActionLinkedList** ActionDependencyGraph::getExecutableSetByPartId(uint32_t part_id)
    {
        return this->_activeTupleList->getExecutableSet(part_id);
    }

    uint32_t ActionDependencyGraph::getExecutableSetCntByPartId(uint32_t part_id)
    {
        return this->_activeTupleList->getExecutableSetCnt(part_id);
    }

    /**************** End of ActionDependencyGraph.cpp ************************/

    /*
 * Implemention of SyncWorker that is responsible for synchronization among
 * constructors and executors in each DGCCWorkSite
 * */
    SyncWorker::SyncWorker()
    {
        this->construction_mutex        = nullptr;
        this->construction_lock_mutex   = nullptr;
        this->execution_mutex           = nullptr;
        this->execution_lock_mutex      = nullptr;
        this->construction_cv           = nullptr;
        this->execution_cv              = nullptr;
        this->c_finish_cnt.store(0);
        this->e_finish_cnt.store(0);
        this->configinfo                = nullptr;
        this->execution_continue        = new bool;
        *(this->execution_continue)     = true;

        this->const_phase.store(true);
    }

    SyncWorker::SyncWorker(ConfigInfo* configinfo)
    {
        this->configinfo = configinfo;
        this->construction_mutex        = new std::mutex();
        this->construction_lock_mutex   = new std::mutex();
        this->execution_mutex           = new std::mutex();
        this->execution_lock_mutex      = new std::mutex();
        this->execution_slock = new spinlock();
        this->construction_slock = new spinlock();

        this->construction_cv           = new std::condition_variable();
        this->execution_cv              = new std::condition_variable();
        this->c_finish_cnt.store(0);
        this->e_finish_cnt.store(0);
        this->execution_continue        = new bool;
        *(this->execution_continue)     = true;

        this->const_phase.store(true);
        const_wakeup.store(true);
        exec_wakeup.store(false);
    }

/*
 * When a constructor finishes the construction, it will not enter
 * the execution phase immediately.
 * Instead, it will wait until all the constructors finish the construction.
 * Then all constructors enter the execution phase at the same time.
 * */
    void SyncWorker::constructor_wait(int cid)
    {
        std::unique_lock<std::mutex> lck(*construction_mutex);
        bool e = false;
        bool d = true;
        uint32_t e_cnt = 0;
        uint32_t d_cnt = 0;

        //critical section that maintains states in contructor
//        construction_lock_mutex->lock();
        construction_slock->lock();
        assert(const_phase.load() == true);

        c_finish_cnt.fetch_add(1);
        DEBUG_Q("CT_%d: incremented c_finish_cnt = %d\n", cid, c_finish_cnt.load());
        //if all constructors finished their works, notify_all executors to work
        if( c_finish_cnt.load() == configinfo->worker_thread_cnt) {
            DEBUG_Q("CT_%d: All Constts are done, c_finish_cnt = %d\n", cid, c_finish_cnt.load());
            e_cnt = configinfo->worker_thread_cnt;
            d_cnt = 0;
            if (!c_finish_cnt.compare_exchange_strong(e_cnt,d_cnt)){
                M_ASSERT_V(false, "CT_%d: not expected, found c_finish_cnt = %d\n", cid, c_finish_cnt.load());
            }

            e = true;
            d = false;
            if (!const_phase.compare_exchange_strong(e,d)){
                M_ASSERT_V(false, "CT_%d: not expected const_phase = %d\n", cid, const_phase.load());
            }

            e = false;
            d = true;
            if (!exec_wakeup.compare_exchange_strong(e,d)){
                M_ASSERT_V(false, "CT_%d: not expected exec_wakeup = %d\n", cid, exec_wakeup.load());
            }

            M_ASSERT_V(exec_wakeup.load(), "CT_%d: not expected value of exec_wakeup = %d\n", cid, exec_wakeup.load());

            DEBUG_Q("CT_%d: Notifying all executors!\n", cid);
            execution_cv->notify_all();
        }
        else if (c_finish_cnt == 1){
            // first constructor to finish
            e = true;
            d = false;
            if (!const_wakeup.compare_exchange_strong(e,d)){
                M_ASSERT_V(false, "CT_%d: not expected const_wakeup = %d\n", cid, const_wakeup.load());
            }
        }
//        construction_lock_mutex->unlock();
        construction_slock->unlock();
        //wait executor to wake it up
        DEBUG_Q("CT_%d: Constructor done my part, going to sleep, const_wakeup = %d\n",
                cid, const_wakeup.load());

//        M_ASSERT_V(const_wakeup.load() == false, "CT_%d: const_wakeup = %d is expected to be false\n",
//                   cid, const_wakeup.load());

        construction_cv->wait(lck);
        while(!const_wakeup.load()){
            DEBUG_Q("CT_%d: woke up in while loop, going back to sleep, const_wakeup = %d\n",
                    cid, const_wakeup.load());
            construction_cv->wait(lck);
        }
//        while(!sync_worker->const_wakeup.load()){
//            SAMPLED_DEBUG_Q("CT_%d: spinng as I am not allowed to wake up\n", cid);
//        } // spin here if we should not wake up

//        M_ASSERT_V(const_phase.load() && const_wakeup.load(),
//               "CT_%d: const_phase = %d\n",
//               cid,
//               const_phase.load()
//        );

        DEBUG_Q("CT_%d: Constructor woke up, const_phase = %d, const_wakeup =%d\n",
                cid, const_phase.load(), const_wakeup.load());
    }


    void SyncWorker::executor_wait(int eid, int dgraph_cnt, ActionDependencyGraph**  dgraphs)
    {
        std::unique_lock<std::mutex> lck(*execution_mutex);
        bool e = false;
        bool d = true;
        uint32_t e_cnt = 0;
        uint32_t d_cnt = 0;
        //critical section that maintains states in executor
//        execution_lock_mutex->lock();

        execution_slock->lock();
        assert(const_phase.load() == false);

        e_finish_cnt.fetch_add(1);
        DEBUG_Q("ET_%d: incremented e_finish_cnt = %d\n", eid, e_finish_cnt.load());
        //if all executors finish their work, notify all constructors to work
        if(e_finish_cnt.load() == configinfo->worker_thread_cnt) {
            DEBUG_Q("ET_%d: All ETs are done, e_finish_cnt=%d\n", eid, e_finish_cnt.load());
            e_cnt = configinfo->worker_thread_cnt;
            d_cnt = 0;
            if (!e_finish_cnt.compare_exchange_strong(e_cnt, d_cnt)){
                M_ASSERT_V(false, "ET_%d: not expected e_finish_cnt = %d\n", eid, e_finish_cnt.load());
            }

            for(int i=0; i<dgraph_cnt; i++) {
                // last ET to finish will increment and clear
                INC_STATS(eid, txn_cnt, g_batch_size); // should we increment by batch size here
                dgraphs[i]->clear();
            }
            e = false;
            d = true;
            if (!const_phase.compare_exchange_strong(e,d)){
                M_ASSERT_V(false, "ET_%d: not expected const_phase = %d\n", eid, const_phase.load());
            }
            M_ASSERT_V(const_phase.load(), "ET_%d: not expected value of const_phase = %d\n", eid, const_phase.load());

            e = false;
            d = true;
            if (!const_wakeup.compare_exchange_strong(e,d)){
                M_ASSERT_V(false, "ET_%d: not expected const_wakeup = %d\n", eid, const_wakeup.load());
            }
            M_ASSERT_V(const_wakeup.load(), "ET_%d: not expected value of const_wakeup = %d\n", eid, const_wakeup.load());

            DEBUG_Q("ET_%d: Notifying all constructors!\n", eid);
            construction_cv->notify_all();
        }
        else if (e_finish_cnt.load() == 1){
            // first one to finish from executors
            // set exec_wakeup to false to prepare for sleeping
            e = true;
            d = false;
            if (!exec_wakeup.compare_exchange_strong(e,d)){
                M_ASSERT_V(false, "ET_%d: not expected exec_wakeup = %d\n", eid, exec_wakeup.load());
            }
        }

//        execution_lock_mutex->unlock();
        execution_slock->unlock();


        //wait contructor to wait it up
        // TQ: no need to wait here
        DEBUG_Q("ET_%d: Executor done my part, going to sleep, exec_wakeup = %d\n", eid, exec_wakeup.load());
//        M_ASSERT_V(exec_wakeup.load() == false, "ET_%d: exec_wakeup = %d is expected to be false\n", eid, exec_wakeup.load())
//        execution_cv->wait(lck, []()-> bool {return sync_worker->exec_wakeup.load();});
        execution_cv->wait(lck);
        while(!exec_wakeup.load()){
            DEBUG_Q("ET_%d: woke up in while loop, going back to sleep, const_phase = %d\n", eid, exec_wakeup.load());
            execution_cv->wait(lck);
        }
//        while(!exec_wakeup.load()){
//            SAMPLED_DEBUG_Q("ET_%d: spinng as I am not allowed to wake up\n", eid);
//        } // spin here if we should not wake up

//        M_ASSERT_V(const_phase.load() == false && exec_wakeup.load(),
//                   "ET_%d: const_phase = %d, exec_wakeup = %d\n",
//                   eid,
//                   const_phase.load(),
//                   exec_wakeup.load()
//        );

        DEBUG_Q("ET_%d: Executor woke up, const_phase = %d\n", eid, const_phase.load());
    }


    void SyncWorker::executor_wait_begin(int eid)
    {
        std::unique_lock<std::mutex> lck(*execution_mutex);
        DEBUG_Q("ET_%d: Executor initially, going to sleep\n", eid);

        execution_cv->wait(lck);
        while(const_phase.load()){
            execution_cv->wait(lck);
        }
//        while(!sync_worker->exec_wakeup.load()){}
//        M_ASSERT_V(const_phase.load() == false && exec_wakeup.load(),
//                   "ET_%d: const_phase = %d, exec_wakeup = %d\n",
//                   eid,
//                   const_phase.load(),
//                   exec_wakeup.load()
//        );

        DEBUG_Q("ET_%d: Executor woke up from initial sleep\n", eid);
    }



//=============================================================================


/*
 * Implementation of Constructor
 * */
    Constructor::Constructor()
    {
        this->configinfo = nullptr;
        this->cid        = -1;
//        this->tmgr       = nullptr;
        this->batchsize  = 0;
        this->dgraph     = nullptr;
        this->syncworker = nullptr;
        this->action_allocator = nullptr;
        this->workload   = nullptr;
    }


    Constructor::Constructor(ConfigInfo* configinfo, int id, Workload* wload)
    {
        if(configinfo == nullptr) {
            M_ASSERT_V(false,"Constructor::Constructor() configinfo cannot be NULL");
        }
        this->configinfo    = configinfo;
        this->cid           = id;
        this->dgraph        = dgraphs[id];
        this->syncworker    = sync_worker;
        this->batchsize     = configinfo->Conf_DGCC_Batch_size;
        this->action_allocator = action_allocator;
        this->workload      = wload;
    }

    Constructor::~Constructor()
    {
        //TODO:
    }
    void Constructor::setup() {
//        this->configinfo    = configinfo;
//        this->cid           = id;
//        this->dgraph        = dgraphs[id];
//        this->syncworker    = sync_worker;
        this->batchsize     = g_batch_size;
//        this->action_allocator = action_allocator;
//        this->workload      = wload;
    }
    void Constructor::set_config_info(ConfigInfo* configinfo){
        this->configinfo = configinfo;
    }
    void Constructor::set_cid(int id) {
        this->cid = id;
    }

    void Constructor::setDependencyGraph(ActionDependencyGraph* graph)
    {
        this->dgraph = graph;
    }

    void Constructor::setSyncWorker(SyncWorker* syncworker)
    {
        this->syncworker = syncworker;
    }

    ActionBuffer* Constructor::getActionAllocator()
    {
        return this->action_allocator;
    }

    void Constructor::setActionAllocator(ActionBuffer* ab)
    {
        this->action_allocator = ab;
    }

    Workload* Constructor::get_workload()
    {
        return this->workload;
    }

    void  Constructor::set_workload(Workload* wload)
    {
        this->workload = wload;
    }

    void Constructor::bind_cpu()
    {
//        cpu_set_t cpuid;
//        CPU_ZERO(&cpuid);
        //we assume cpu cnt <= 24
//        CPU_SET(cid, &cpuid);
//        pthread_setaffinity_np(this->self(), sizeof(cpu_set_t), &cpuid);
    }

    void Constructor::run_one_queue()
    {
        //get the txn queue for this batch

//        BaseTxn**    workqueue = tmgr->get_txn_queue(cid);
//        int         q_size = tmgr->get_txn_cnt(cid);
//        int     begin  =   0;
//        int     end;
//        BaseTxn* txn;
//        action_allocator->resetActionBuffer();
//        while(begin < q_size){
//            action_allocator->resetActionBuffer();
//            end = begin + batchsize;
//            end = (end <= q_size) ? end : q_size;
//            for(int i=begin; i< end; i++) {
//                txn = workqueue[i];
//                workload->resolveTxn(txn, dgraph, action_allocator, cid);
//            }
//            begin = end;
//            syncworker->constructor_wait();
//        }
//        //when there are no work to do, need to tell executor
//        //one constructor do the notification
//        if(cid == 0) {
//            *(syncworker->execution_continue) = false;
//            syncworker->execution_cv->notify_all();
//        }
    }

//main work that handled by constructor
    RC Constructor::run()
    {
        DEBUG_Q("Setting up Constructor thread\n");
        tsetup();

        RC rc = RCOK;
        Message * msg;
        uint64_t idle_starttime = 0;
        uint64_t batch_cnt = 0;
        uint64_t batch_start_time = 0;
//        uint64_t prof_starttime = 0;
        uint64_t const_batch_size = BATCH_SIZE / g_plan_thread_cnt;

        bool force_batch_delivery = false;

        printf("Running LADS Constructor Thread %ld\n", _thd_id);

        while (!simulation->is_done()) {
            heartbeat();
            progress_stats();

            assert(syncworker->const_phase.load() == true);

            if (idle_starttime == 0 && simulation->is_warmup_done()){
                idle_starttime = get_sys_clock();
            }

//            prof_starttime = get_sys_clock();
            msg = work_queue.plan_dequeue(_thd_id, (uint64_t) cid);
//            INC_STATS(_thd_id, plan_queue_dequeue_time[_planner_id], get_sys_clock()-prof_starttime);

            if(!msg) {
                if(idle_starttime == 0){
                    idle_starttime = get_sys_clock();
                }
                // we have not recieved a transaction
                continue;
            }

            if(idle_starttime > 0) {
                INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                idle_starttime = 0;
            }

            batch_cnt++;
            if (batch_start_time == 0){
                batch_start_time = get_sys_clock();
            }

            M_ASSERT_V(BATCHING_MODE == SIZE_BASED, "only size-based batching is supported for LADS");
            force_batch_delivery = (batch_cnt == const_batch_size);

            if (force_batch_delivery){
                DEBUG_Q("ConstT_%d: Batch is ready\n", cid);
                // wait for executor to process the batch
//                    DEBUG_Q("ConstT_%d: going to wait\n", cid);
                // threads will not actually wait


//                DEBUG_Q("ConstT_%d: going to spin until all other ConsTs are done\n", cid);
//                while (!sync_worker->batch_ready.load()){
//                    if(idle_starttime == 0){
//                        idle_starttime = get_sys_clock();
//                    }
//                }


//                DEBUG_Q("ConstT_%d: going to spin until ETs are done\n", cid);
//                while (sync_worker->batch_ready.load()){
//                    if(idle_starttime == 0){
//                        idle_starttime = get_sys_clock();
//                    }
//                }

//                DEBUG_Q("ConstT_%d: All ETs are done starting next batch\n", cid);

                if(idle_starttime == 0){
                    idle_starttime = get_sys_clock();
                }

                syncworker->constructor_wait(cid);

                if(idle_starttime > 0) {
                    INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
                    idle_starttime = 0;
                }

                // starting a new batch
                batch_cnt = 0;
            }

            // process message
            M_ASSERT_V(WORKLOAD == YCSB, "Only YCSB workload is supported by LADS for now\n");
#if WORKLOAD == YCSB
            ((YCSBWorkload *)_wl)->resolve_txn_dependencies(msg, cid);
#else
            assert(false);
#endif
        }

//        if (sync_worker->const_phase.load()){
//            // we are done during construction phase, we need to stop ETs from spining/sleeping
//            sync_worker->batch_ready.store(true);
//        }

        if(cid == 0) {
            *(syncworker->execution_continue) = false;

            syncworker->exec_wakeup.store(true);


            DEBUG_Q("ConstT_%d: Notifying ETs\n", cid);
            syncworker->execution_cv->notify_all();

//            DEBUG_Q("ConstT_%d: going to wait now\n", cid);
//            syncworker->constructor_wait();
        }
        return rc;
    }



//=============================================================================================
/*
 * Implementation of Executor
 * */

    void Executor::setup() {
        assert(this->workload != 0);
        this->workload->get_txn_man(this->txn_man);
        txn_man->init((uint64_t) this->eid, this->workload);
    }

    void Executor::set_eid(int id){
        this->eid = id;
    }

    Executor::Executor()
    {
        this->configinfo    = nullptr;
        this->eid           = 0;
        this->batchsize     = 0;
        this->dgraphs       = nullptr;
        this->partid        = 0;
        this->syncworker    = nullptr;
        this->workload      = nullptr;
    }

    Executor::Executor(ConfigInfo* configinfo, int id, Workload* wload)
    {
        this->configinfo    = configinfo;
        this->batchsize     = BATCH_SIZE;
        this->dgraphs       = dgraphs; //need to be initialized mannually
        this->partid        = eid;
        this->syncworker    = sync_worker; //need to be initialized mannually
        this->workload      = wload;
    }

    Executor::~Executor()
    {
        //TODO
    }

    void Executor::setDependencyGraphQueue(ActionDependencyGraph** graphs)
    {
        this->dgraphs = graphs;
    }

    void Executor::setSyncWorker(SyncWorker* syncworker)
    {
        this->syncworker = syncworker;
    }


    Workload* Executor::get_workload()
    {
        return this->workload;
    }

    void Executor::set_workload(Workload* wload)
    {
        this->workload = wload;
    }

    void Executor::bind_cpu()
    {
//        cpu_set_t cpuid;
//        CPU_ZERO(&cpuid);
//        CPU_SET(eid, &cpuid);
//        pthread_setaffinity_np(this->self(), sizeof(cpu_set_t), &cpuid);
    }

    void Executor::run_one_queue()
    {
//        DEBUG_Q("ET_%d: Run one queue\n", eid);
//        bind_cpu();
        TupleActionLinkedList** wset = nullptr;
        uint32_t wsize = 0;
        int dgraph_cnt = g_plan_thread_cnt;
        //Now execute dependency graph in order
        ActionDependencyGraph* cur_graph = nullptr;

//        DEBUG_Q("ET_%d: going to wait\n", eid);
//        syncworker->executor_wait_begin();

//        DEBUG_Q("ET_%d: ready to execute\n", eid);
//        while( *(syncworker->execution_continue) ) {
            for(int i=0; i<dgraph_cnt; i++) {
                cur_graph = dgraphs[i];
                wset = cur_graph->getExecutableSetByPartId(eid);
                wsize = cur_graph->getExecutableSetCntByPartId(eid);

                DEBUG_Q("ET_%d: processing executuable set with partition %ld in dgraph %d\n", eid, (uint64_t) wset, i );
                do_work(wset, wsize);
            }

//            DEBUG_Q("ET_%d: going to wait -- for next batch\n", eid);
//            syncworker->executor_wait(eid, dgraph_cnt, dgraphs);
//        }//end while
    }

    bool Executor::do_work_on_tuple(TupleActionLinkedList* tset)
    {
        Action* cur_action UNUSED = nullptr;
        cur_action = tset->fetchAction();
        while(cur_action != nullptr) {
            if(cur_action->getIndegree() == 0) {
//                workload->executeAction(cur_action, db);
//                DEBUG_Q("ET_%d: executing action\n",eid);
                txn_man->execute_lads_action(cur_action, this->eid);
                action_allocator->put(-1, cur_action);
                cur_action = tset->fetchAction();
            }else{
                tset->pushfrontAction(cur_action);
                return false;
            }
        }
        return true;
    }


    void Executor::do_work(TupleActionLinkedList** wset, uint32_t wsize)
    {
        std::list<TupleActionLinkedList*> incomplete_list;
        TupleActionLinkedList* cur_workset;
        Action* cur_action UNUSED = nullptr;
        bool done = false;
        //first round
        //DEBUG_Q("wset = %d, wsize = %d", wset, wsize);
        for(uint32_t i=0; i<wsize; i++) {
            cur_workset = wset[i];
            done = do_work_on_tuple(cur_workset);
            if(!done) {
                incomplete_list.push_back(cur_workset);
            }
        }//end for

        while(!incomplete_list.empty()) {// continue la... XXOO forever!!!
            cur_workset = incomplete_list.front();
            incomplete_list.pop_front();
            done = do_work_on_tuple(cur_workset);
            if(!done) {
                incomplete_list.push_back(cur_workset);
            }
        }//end while
    }


    RC Executor::run()
    {
        DEBUG_Q("Setting up Executor thread\n");
        tsetup();
        RC rc = RCOK;
//        bind_cpu();
//        run_one_queue();
        printf("Running LADS Executor Thread %ld\n", _thd_id);
        sync_worker->executor_wait_begin(eid);

        while (!simulation->is_done()) {
            heartbeat();
            progress_stats();

            assert(syncworker->const_phase.load() == false);

//            if (sync_worker->batch_ready.load()){
                DEBUG_Q("ET_%d: processing batch\n", eid);
                run_one_queue();
                DEBUG_Q("ET_%d: DONE!! processing batch\n", eid);

                DEBUG_Q("ET_%d: going to wait -- for next batch\n", eid);
                syncworker->executor_wait(eid, g_plan_thread_cnt, dgraphs);
//            }
//            else{
//                M_ASSERT_V(false, "ET_%d: expected batch ready to be true, batch ready = %d\n", eid, sync_worker->batch_ready.load())
//            }
        }

//        if (!sync_worker->const_phase.load()){
//            // this means that simulation is done while in execution phase
//            // need to stop ConstT from spinning
////            sync_worker->batch_ready.store(false);
//        }

        if(eid == 0) {
            DEBUG_Q("ET_%d: Notifying CTs\n", eid);

            sync_worker->const_wakeup.store(true);

            syncworker->construction_cv->notify_all();

//            DEBUG_Q("ConstT_%d: going to wait now\n", cid);
//            syncworker->constructor_wait();
        }


        return rc;
    }

//==================================================================================

/*
 * Implementation of DGCCSite
 * */
    DGCCSite::DGCCSite()
    {
        this->configinfo    = nullptr;
        this->envinfo       = nullptr;
        this->siteid        = 0;
        this->workercnt     = 0;
        this->constructors  = nullptr;
        this->executors     = nullptr;
        this->syncworker    = nullptr;
//        this->coordinator   = nullptr;
//        this->tmgr          = nullptr;
        this->dgraphs       = nullptr;
//        this->db            = nullptr;
        this->workload      = nullptr;
    }

    DGCCSite::DGCCSite(ConfigInfo* configinfo, Workload* wload)
    {
        this->configinfo    = configinfo;
        this->envinfo       = nullptr;
        this->siteid        = 0; //TODO
        this->workercnt     =configinfo->worker_thread_cnt;
        this->workload      = wload;
        this->constructors  = new Constructor*[workercnt];
        this->executors     = new Executor*[workercnt];
        this->syncworker    = new SyncWorker(configinfo);
//        this->coordinator   = new Coordinator();
//        this->tmgr          = mgr;
//        this->db            = new Database(); //TODO

        this->dgraphs       = new ActionDependencyGraph*[workercnt];
        for(int i=0; i<workercnt; i++) {
            dgraphs[i] = new ActionDependencyGraph(configinfo, (UInt32) i);
        }

        //init the data structure in constructor and executor
        for(int i=0; i<workercnt; i++) {
            constructors[i] = new Constructor(configinfo, i, workload);
//            constructors[i]->setTxnManager(tmgr);
            constructors[i]->setDependencyGraph(dgraphs[i]);
            constructors[i]->setSyncWorker(syncworker);

            executors[i]    = new Executor(configinfo, i, workload);
//            executors[i]->setDatabase(db);
            executors[i]->setDependencyGraphQueue(dgraphs);
            executors[i]->setSyncWorker(syncworker);
        }
    }

    DGCCSite::~DGCCSite()
    {
        //TODO
    }

//main function and the entry of this class
    // TODO(tq): refactor this to main.cpp
    void DGCCSite::startwork()
    {
//#ifdef DGCC_DEBUG
//        DEBUG_Q("Site %d starts to work!", siteid);
//#endif
//        struct timespec ss, ee;
//        clock_gettime(CLOCK_MONOTONIC, &ss);
//
//        for(int i=0; i<workercnt; i++) {
//            executors[i]->start();
//        }
//#ifdef DGCC_DEBUG
//        DEBUG_Q("Executors start to work!");
//#endif
//
//        for(int i=0; i<workercnt; i++) {
//            constructors[i]->start();
//        }
//#ifdef DGCC_DEBUG
//        DEBUG_Q("Constructors start to work!");
//#endif
//
//        for(int i=0; i<workercnt; i++) {
//            constructors[i]->join();
//        }
//#ifdef DGCC_DEBUG
//        DEBUG_Q("Constructors finish work!");
//#endif
//
//        for(int i=0; i<workercnt; i++) {
//            executors[i]->join();
//        }
//#ifdef DGCC_DEBUG
//        DEBUG_Q("Executors finish work!");
//#endif
//
//        clock_gettime(CLOCK_MONOTONIC, &ee);
//        double timediff = (ee.tv_sec - ss.tv_sec) + (ee.tv_nsec - ss.tv_nsec) / 1000000000.0;
//
//        DEBUG_Q("DGCCSite total run time: %f second", timediff);
//
//        long totaltxn = (long)configinfo->Txn_Queue_Size * (long)configinfo->worker_thread_cnt;
//        DEBUG_Q("Average Throughput: %f txn/second", (double)totaltxn/timediff);
    }
}