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

#ifndef _ARR_H_
#define _ARR_H_

#include "global.h"
#include "helper.h"
#include "mem_alloc.h"

template <class T> class Array {
public:
  Array() : items(NULL), capacity(0), count(0) {
  }
  void init(uint64_t size, int64_t et_id, int64_t pt_id) {
      _et_id = et_id;
      _pt_id = pt_id;
      init(size);
  }
    inline int64_t et_id(){
        return _et_id;
    }

    inline int64_t pt_id(){
        return _pt_id;
    }

    inline void set_et_id(int64_t et_id){
        _et_id = et_id;
    }

    inline void set_pt_id(int64_t pt_id){
        _pt_id = pt_id;
    }

  void init(uint64_t size) {
    /*
    if(!items) {
      DEBUG("Array init: %ld * %ld = %ld\n",sizeof(T),size,sizeof(T)*size);
      items = (T*) mem_allocator.alloc(sizeof(T)*size);
    }
    */
    //DEBUG("Array init: %ld * %ld = %ld\n",sizeof(T),size,sizeof(T)*size);
    DEBUG_M("Array::init %ld*%ld\n",sizeof(T),size);
    items = (T*) mem_allocator.alloc(sizeof(T)*size);
    capacity = size;
    assert(items);
    assert(capacity == size);
    count = 0;
#if EXPANDABLE_EQS
      expandable = false;
#endif
  }
#if EXPANDABLE_EQS
    void init_expandable(uint64_t size, uint64_t cap_factor) {
        max_capacity = size*cap_factor;
        cap_inc = size;
        items = (T*) mem_allocator.alloc(sizeof(T)*max_capacity);
        capacity = size;
        assert(items);
        count = 0;
        expandable = true;
    }

    bool expand(){
        if (expandable){
            M_ASSERT_V((capacity+cap_inc) <= max_capacity, "Cannot expand, reached max_capacitty = %ld\n",max_capacity);
            capacity += cap_inc;
            return true;
        }
        else{
//            M_ASSERT_V(expandable, "Cannot expand this array, capacity = %ld\n");
            return false;
        }
    }
#endif
//    void init_numa(uint64_t size, uint64_t thd_id) {
//        DEBUG_M("Array::init %ld*%ld\n",sizeof(T),size);
//        items = (T*) mem_allocator.align_alloc_onnode(sizeof(T)*size, thd_id);
//        capacity = size;
//        assert(items);
//        assert(capacity == size);
//        count = 0;
//    }

  void clear() {
    count = 0;
  }

  void copy(Array a) {
    init(a.size());
    for(uint64_t i = 0; i < a.size(); i++) {
      add(a[i]);
    }
    assert(size() == a.size());
  }

  void append(Array a) {
    M_ASSERT_V(count + a.size() <= capacity, "count=%ld,a.size()=%ld,capacity=%ld\n",count,a.size(),capacity);
    for(uint64_t i = 0; i < a.size(); i++) {
      add(a[i]);
    }
  }


  void release() {
    //DEBUG("Array release: %ld * %ld = %ld\n",sizeof(T),capacity,sizeof(T)*capacity);
    DEBUG_M("Array::release %ld*%ld\n",sizeof(T),capacity);
      if (items != NULL && capacity > 0){
          mem_allocator.free(items,sizeof(T)*capacity);
          items = NULL;
          count = 0;
          capacity = 0;
      }
  }

  void add_unique(T item){
    for(uint64_t i = 0; i < count; i++) {
      if(items[i] == item)
        return;
    }
    add(item);
  }
// Atomic add
    void atomic_add(T item){
//    assert(count < capacity);
        M_ASSERT_V(count < capacity, "count < capacity failed, count = %ld, capacity = %ld\n", count, capacity);
        uint64_t ecount;
        do{
            ecount = count;
        }while(!ATOM_CAS(count,ecount, ecount+1));
        items[ecount] = item;
    }
  void add(T item){
//    assert(count < capacity);
    M_ASSERT_V(count < capacity, "count < capacity failed, count = %ld, capacity = %ld\n", count, capacity);
    items[count] = item;
    ++count;
  }

  void add() {
    assert(count < capacity);
    //items[count] = (T*)mem_allocator.alloc(sizeof(T));
    ++count;
  }

  T get(uint64_t idx) {
      M_ASSERT_V(idx < count, "idx < count failed, count = %ld, idx = %ld\n", count, idx);
    return items[idx];
  }
    T last() {
        M_ASSERT_V(0 < count, "iarray is empty, count = %ld\n", count);
        return items[count-1];
    }

    T* get_ptr(uint64_t idx) {
//        assert(idx < count);
        M_ASSERT_V(idx < count, "idx < count failed, count = %ld, idx = %ld\n", count, idx);
        return &(items[idx]);
    }

  void set(uint64_t idx, T item) {
    assert(idx < count);
    items[idx] = item;
  }

  bool contains(T item) {
      for (uint64_t i = 0; i < count; i++) {
          if (items[i] == item) {
              return true;
          }
      }
      return false;
  }

  uint64_t getPosition(T item) {
      for (uint64_t i = 0; i < count; i++) {
          if (items[i] == item) {
              return i;
          }
      }
      return count;
  }

  void swap(uint64_t i, uint64_t j) {
    T tmp = items[i];
    items[i] = items[j];
    items[j] = tmp;
  }
  inline T operator[](const uint64_t idx) {assert(idx < count); return items[idx];}
  uint64_t get_count() {return count;}
    //TQ: added const modifier to allow using boost::heap container
  uint64_t size() const {return count;}
  bool is_full() { return count == capacity;}
  bool is_empty() { return count == 0;}
    bool isInitilized() {return capacity > 0;}

    T * items;
    uint64_t capacity;
    uint64_t count;
#if EXPANDABLE_EQS
    uint64_t cap_inc;
    uint64_t max_capacity;
    bool expandable;
#endif
private:
//  uint64_t head;
    int64_t _et_id =-1;
    int64_t _pt_id =-1;
};


#endif
