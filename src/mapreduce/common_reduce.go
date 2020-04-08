package mapreduce

import (
	"sort"
	"os"
	"log"
	"encoding/json"
)

func doReduce(
	jobName string,	// job名字
	reduceTask int, // 第i个reduce任务
	outFile string,	// 输出文件名
	nMap int,		// 第j个map
	reduceF func(key string, values []string) string,
) {
	keyValues := make(map[string][]string, 0)
	for i := 0; i < nMap; i++ {	// 获取每个map所产生作为reduce的输入文件
		fileName := reduceName(jobName, i, reduceTask)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal("doReduce: 文件打开失败 ", fileName, " error: ", err)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {	// 将所有相同key的value连接起来
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}

			_, ok := keyValues[kv.Key]
			if !ok {
				keyValues[kv.Key] = make([]string, 0)
			}
			keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
		}
	}

	var keys []string
	for k, _ := range keyValues {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	mergeFileName := mergeName(jobName, reduceTask)		// 理论上和outFile一样
	mergeFile, err := os.Create(mergeFileName)
	if err != nil {
		log.Fatal("doReduce: 创建merge文件失败\n")
	}
	defer mergeFile.Close()

	enc := json.NewEncoder(mergeFile)
	for _, k := range keys {
		res := reduceF(k, keyValues[k])
		err := enc.Encode(&KeyValue{k, res})
		if err != nil {
			log.Fatal("doReduce: encode error\n")
		}
	}
}
