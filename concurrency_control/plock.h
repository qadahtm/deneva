#ifndef _PLOCK_H_
#define _PLOCK_H_

#include "global.h"
#include "helper.h"
#include "array.h"
class TxnManager;

// Parition manager for HSTORE
class PartMan {
public:
    void init();
    RC lock(TxnManager * txn);
    void unlock(TxnManager * txn);
private:
    pthread_mutex_t latch;
    pthread_spinlock_t slatch;
    TxnManager * owner;
    TxnManager ** waiters;
    UInt32 waiter_cnt;
};

// Partition Level Locking
class Plock {
public:
    void init();
    // lock all partitions in parts
    RC lock(TxnManager * txn, Array<uint64_t> * parts, uint64_t part_cnt);
    void unlock(TxnManager * txn, Array<uint64_t> * parts, uint64_t part_cnt);
private:
    PartMan ** part_mans;
};

#endif
