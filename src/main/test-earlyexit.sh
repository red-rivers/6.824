#!/bin/bash

rm mr-*

go build -race -buildmode=plugin ../mrapps/early_exit.go

go run -race mrcoordinator.go pg-*.txt &

sleep 1

go run -race mrworker.go early_exit.so &
#go run -race mrworker.go early_exit.so &

jobs &> /dev/null

wait -n

echo first job done

sort mr-out* | grep .> mr-wc-all-initial

wait

echo wait all

sort mr-out* | grep .> mr-wc-all-final

if cmp mr-wc-all-initial mr-wc-all-final
then 
   echo '---' early exit test : PASS
else 
   cat mr-wc-all-initial
   cat mr-wc-all-final
fi
