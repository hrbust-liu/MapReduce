package mapreduce

import (
	"fmt"
	"strconv"
)

const debugEnabled = false	// 是否开启某些打印信息

func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type jobPhase string

const (
	mapPhase jobPhase = "mapPhase"
	reducePhase	= "reducePhase"
)

type KeyValue struct {
	Key string
	Value string
}
// 中间值 第mapTask的Map任务交给第reduceTask的Reduce任务的文件
func reduceName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

func mergeName(jobName string, reduceTask int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}
