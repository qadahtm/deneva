//
// Created by Thamir Qadah on 8/30/17.
//

#include "global.h"
#include "mem_alloc.h"
#include "smanager.h"

void RIDMgr::init() {

    rids = (uint64_t *) mem_allocator.align_alloc(sizeof(uint64_t)*g_thread_cnt);
    rid_max = (uint64_t *) mem_allocator.align_alloc(sizeof(uint64_t)*g_thread_cnt);
    memset(rids,0,sizeof(uint64_t)*g_thread_cnt);
    memset(rid_max,0,sizeof(uint64_t)*g_thread_cnt);
    batch_max_rid.store(0);
}

uint64_t RIDMgr::next_rid(uint64_t thd_id){
    //TODO(tq): refactor this
    if (batch_max_rid.load() == 0){
        // first thread to call next_rid
        reserve_rid_range(thd_id);
        return rids[thd_id];
    }
    else{
        if (rid_max[thd_id] == 0){
            // first time for this thread to call next_rid
            reserve_rid_range(thd_id);
            return rids[thd_id];
        }
        else if (rids[thd_id] == (rid_max[thd_id]-1)){
            // reached end of reserved RIDs for this thread, request another reservation
            reserve_rid_range(thd_id);
            return rids[thd_id];
        }
        else{
            // use a rid from reserved set
            ++rids[thd_id];
            return rids[thd_id];
        }
    }
}

void RIDMgr::reserve_rid_range(uint64_t thd_id) {
    uint64_t start_rid = batch_max_rid.fetch_add(INSERT_RID_BATCH_SIZE);
    rids[thd_id] = start_rid;
    rid_max[thd_id] = start_rid + INSERT_RID_BATCH_SIZE;
}