package mapreduce

import (
	"io/ioutil"
	"log"
	"os"
	"encoding/json"
	"hash/fnv"
)

func doMap(
	jobName string,	// job名
	mapTask int,	// 第i个task
	inFile string,	// 输入文件名
	nReduce int,	// reduce个数
	mapF func(filename string, contents string) []KeyValue,
) {
	data, err := ioutil.ReadFile(inFile)

	if err != nil {
		log.Fatal("doMap Open: ", err)
	}
	slice := mapF(inFile, string(data))

	var reduceKv [][]KeyValue	// 为每个reduce创建空列表
	for i := 0; i < nReduce; i++ {
		temp := make([]KeyValue, 0)
		reduceKv = append(reduceKv, temp)
	}

	for _, s := range slice {	// 数据hash放入对应reduce列表
		index := ihash(s.Key) % nReduce
		reduceKv[index] = append(reduceKv[index], s)
	}

	for i := 0; i < nReduce; i++ {
		file, err := os.Create(reduceName(jobName, mapTask, i))
		if err != nil {
			log.Fatal("common_map.doMap: fail to create the file. The error is \n", err)
		}
		enc := json.NewEncoder(file)
		for _, kv := range(reduceKv[i]) {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("common_map.doMap: fail to encode. The error is \n", err)
			}
		}
		file.Close()
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
