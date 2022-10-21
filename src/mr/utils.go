package mr

import (
	"fmt"
)

func tmpMapOutputFile(wokerId int64, taskId int64, reduceIdx int) string {
	return fmt.Sprintf("mr-tmp-%d-%d-%d",wokerId, taskId, reduceIdx)
}

func finalMapOutputFile(taskId int64, reduceIdx int) string {
	return fmt.Sprintf("mr-intermediate-%d-%d",taskId,reduceIdx)
}

func tmpReduceOutputFile(workerId int64, reduceIdx int) string {
	return fmt.Sprintf("mr-tmp-%d-%d",workerId, reduceIdx)
}

func finalReduceOutputFile(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d",reduceIdx)
}