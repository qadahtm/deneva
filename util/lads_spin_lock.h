//
// Created by Thamir Qadah on 7/26/17.
// Copied from LADS source code
//

#ifndef DENEVA_LADS_SPIN_LOCK_H
#define DENEVA_LADS_SPIN_LOCK_H


/*
this is a simple spinlock implementations.
author: Yao Chang
contact: yaochang2009@gmail.com
*/

#include <sched.h>

namespace gdgcc{

struct Spinlock {
    Spinlock(bool yield = false)
        : value(0), yield(yield)
    {
    }

    void init()
    {
        value = 0;
        yield = false;
    }

    inline void lock()
    {
        acquire();
    }

    inline void unlock()
    {
        release();
    }

    bool locked() const
    {
        return value;
    }

    int try_acquire()
    {
        if (__sync_bool_compare_and_swap(&value, 0, 1))
            return 0;
        return -1;
    }

    bool try_lock()
    {
        return try_acquire() == 0;
    }

    int acquire()
    {
        for (int tries = 0; true;  ++tries) {
            if (!__sync_lock_test_and_set(&value, 1))
                return 0;
            if (tries == 100 && yield) {
                tries = 0;
                sched_yield();
            }
        }
    }

    int release()
    {
        __sync_lock_release(&value);
        return 0;
    }

    volatile int value;
    bool yield;
};


} // namespace gdgcc

#endif //DENEVA_LADS_SPIN_LOCK_H
