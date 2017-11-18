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

#ifndef _MEM_ALLOC_H_
#define _MEM_ALLOC_H_

#include "global.h"
#include <numa.h>

class mem_alloc {
public:
    void * alloc(uint64_t size);
    void * align_alloc(uint64_t size);
    void * realloc(void * ptr, uint64_t size);
    void free(void * block, uint64_t size);

//    void * alloc_local(uint64_t size){
//        void * memret = numa_alloc_local(size);
//        M_ASSERT_V(memret, "could not allocate memory on local node\n")
//        return memret;
//    };
//    void * align_alloc_local(uint64_t size){
//        uint64_t aligned_size = size + CL_SIZE - (size % CL_SIZE);
//        void * memret = numa_alloc_local(aligned_size);
//        M_ASSERT_V(memret, "could not allocate memory on local node\n")
//        return memret;
//    };
//
//    void * alloc_onnode(uint64_t size, uint64_t thd_id) {
//        int node = thd_id/(CORE_CNT/NUMA_NODE_CNT);
//        void * memret = numa_alloc_onnode(size, node);
//        if (!memret){
//            uint64_t  free_mem;
//            uint64_t total_mem = numa_node_size64(node, &free_mem);
//            M_ASSERT_V(memret, "could not allocate memory for thread %ld, on node %d, node_total_mem %ld, node_free_mem = %ld\n",
//                       thd_id,total_mem ,free_mem);
//        }
//        return memret;
//    }
//
//    void * align_alloc_onnode(uint64_t size, uint64_t thd_id){
//        int node = thd_id/(CORE_CNT/NUMA_NODE_CNT);
//        uint64_t aligned_size = size + CL_SIZE - (size % CL_SIZE);
//        void * memret = numa_alloc_onnode(aligned_size, node);
//        if (!memret){
//            uint64_t  free_mem;
//            uint64_t total_mem = numa_node_size64(node, &free_mem);
//            M_ASSERT_V(memret, "could not allocate memory for thread %ld, on node %d, node_total_mem %ld, node_free_mem = %ld\n",
//                       thd_id,total_mem ,free_mem);
//        }
//        return memret;
//    }
//    void free_numa(void * block, uint64_t size){
//        numa_free(block, size);
//    }
};

#endif
