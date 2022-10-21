#!/bin/bash


cd /home/6.824/go/src/6.824/src/main

rm mr-*

go build -race -buildmode=plugin ../mrapps/jobcount.go

go run -race mrcoordinator.go pg-*.txt &

sleep 1

go run -race mrworker.go ./jobcount.so &
go run -race mrworker.go ./jobcount.so
go run -race mrworker.go ./jobcount.so &
go run -race mrworker.go ./jobcount.so 

echo all exit

NT=`cat mr-out* | awk '{print $2}'`
if [ "$NT" -eq "8" ]
then 
   echo '---' job count test:PASS
else 
   echo '---' job count test: FAIL
   echo '---' "($NT != 8)"
fi
