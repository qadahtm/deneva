//
// Created by Thamir Qadah on 8/30/17.
//

#ifndef DENEVA_SMANAGER_H
#define DENEVA_SMANAGER_H

#include "global.h"

class RIDMgr {
public:
    uint64_t next_rid(uint64_t thd_id);
    void init();
    void reserve_rid_range(uint64_t thd_id);
    // for TPCC
    uint64_t next_rid_fixed(uint64_t part_id);

    std::atomic<uint64_t> batch_max_rid;
    std::atomic<uint64_t> ** rid_ranges;

private:


//#if CC_ALG == QUECC
//    uint64_t rids[PLAN_THREAD_CNT];
//    uint64_t rid_max[PLAN_THREAD_CNT];
//#else
    uint64_t * rids;
    uint64_t * rid_max;
//#endif
};

#endif //DENEVA_SMANAGER_H
