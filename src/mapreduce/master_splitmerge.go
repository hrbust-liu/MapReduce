package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)
// 合并每个reduce产生的文件
func (mr *Master) merge() {
	kvs := make(map[string]string)
	for i := 0; i < mr.nReduce; i++ {
		p := mergeName(mr.jobName, i)	// 每个reduce产生的文件
		fmt.Printf("Merge")
		file, _ := os.Open(p)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = kv.Value
		}
		file.Close()
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys) // 获取所有key并排序

	file, _ := os.Create("mrtmp." + mr.jobName)

	w := bufio.NewWriter(file)
	for _, k := range keys {
		fmt.Fprintf(w, "%s : %s\n", k, kvs[k])
	}
	w.Flush()
	file.Close()
}
// 删除文件
func removeFile(n string) {
	err := os.Remove(n)
	if err != nil {
		log.Fatal("CleanupFiles")
	}
}
// 删除所有map和reduce产生的文件,以及最终合并文件
func (mr *Master) CleanupFiles() {
	for i := range mr.files {
		for j := 0; j < mr.nReduce; j++ {
			removeFile(reduceName(mr.jobName, i, j))
		}
	}
	for i := 0; i < mr.nReduce; i++ {
		removeFile(mergeName(mr.jobName, i))
	}
	removeFile("mrtmp." + mr.jobName)
}

