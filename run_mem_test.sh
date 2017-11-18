#! /bin/sh

ccmd="g++ -o mem_test test/memory_test.cpp -lpthread -lnuma"
eval $ccmd

cmd="./mem_test > ./mem_test_results/$1_t0.txt"
eval $cmd

cmd="./mem_test > ./mem_test_results/$1_t1.txt"
eval $cmd
