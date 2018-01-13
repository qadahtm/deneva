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

#if WORKLOAD == YCSB
    uint64_t range_size = UINT64_MAX/g_part_cnt;
    uint64_t range_part_cnt = g_thread_cnt; // for non-partitioned stores
    rid_ranges = (atomic<uint64_t> **) mem_allocator.alloc(sizeof(atomic<uint64_t> *)*g_thread_cnt);
    for (uint64_t i =0; i < range_part_cnt; i++){
        rid_ranges[i] = (atomic<uint64_t> *) mem_allocator.align_alloc(sizeof(atomic<uint64_t>));
        rid_ranges[i]->store(i*range_size);
    }
#elif WORKLOAD == TPCC
//    uint64_t part_cnt = NUM_WH; // for partitioned stores
    uint64_t range_part_cnt = g_thread_cnt; // for non-partitioned stores
    rid_ranges = (atomic<uint64_t> **) mem_allocator.alloc(sizeof(atomic<uint64_t> *)*range_part_cnt);
    uint64_t range_size = UINT64_MAX/range_part_cnt;
    for (uint64_t i =0; i < range_part_cnt; i++){
        rid_ranges[i] = (atomic<uint64_t> *) mem_allocator.align_alloc(sizeof(atomic<uint64_t>));
        rid_ranges[i]->store(i*range_size);
    }
#else
    M_ASSERT(false, "Only YCSB and TPCC are currently supported in RID Manager\n");
#endif
}

// Sequential RID assignemnt

uint64_t RIDMgr::next_rid(uint64_t thd_id){
    return next_rid_fixed(thd_id);
}

//uint64_t RIDMgr::next_rid(uint64_t thd_id){
//    //TODO(tq): refactor this
//    if (batch_max_rid.load() == 0){
//        // first thread to call next_rid
//        reserve_rid_range(thd_id);
//        return rids[thd_id];
//    }
//    else{
//        if (rid_max[thd_id] == 0){
//            // first time for this thread to call next_rid
//            reserve_rid_range(thd_id);
//            return rids[thd_id];
//        }
//        else if (rids[thd_id] == (rid_max[thd_id]-1)){
//            // reached end of reserved RIDs for this thread, request another reservation
//            reserve_rid_range(thd_id);
//            return rids[thd_id];
//        }
//        else{
//            // use a rid from reserved set
//            ++rids[thd_id];
//            return rids[thd_id];
//        }
//    }
//}

// Assuming TPCC, partitioned RID assignments based on warehouse ID.
// based on the result of schism and H-Store for TPCC where each warehouse is a partition.
// We assume integer RID values
// TODO(tq): generalize this.
uint64_t RIDMgr::next_rid_fixed(uint64_t part_id){
    // a quick fixs if the passed part_id > than g_part_id
#if PART_CNT == 1
    uint64_t tpart_id = part_id; // for non-partitioned stores
#else
    uint64_t tpart_id = part_id % g_part_cnt; // for partitioned stores
#endif
    uint64_t arid = 0;
    uint64_t brid = 0;
    do{
        brid = rid_ranges[tpart_id]->load(memory_order_acq_rel);
        arid = brid +1;
    } while(!rid_ranges[tpart_id]->compare_exchange_strong(brid,arid,memory_order_acq_rel));
//    return rid_ranges[tpart_id]->fetch_add(1);
    return arid;
}

void RIDMgr::reserve_rid_range(uint64_t thd_id) {
    uint64_t start_rid = batch_max_rid.fetch_add(INSERT_RID_BATCH_SIZE);
    rids[thd_id] = start_rid;
    rid_max[thd_id] = start_rid + INSERT_RID_BATCH_SIZE;
}