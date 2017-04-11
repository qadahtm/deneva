#! /bin/bash
pid=`pgrep $1`
echo "Pid=$pid"
watch -n $2 ps H -o pid,tid,comm,user,psr,pcpu,stat -p $pid;
