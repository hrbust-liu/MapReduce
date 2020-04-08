package main

import (
	"strings"
//	"log"
	"strconv"
	"unicode"
	"fmt"
	"mapreduce"
	"os"
)

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
func mapF(filename string, contents string) []mapreduce.KeyValue {
	// Your code here (Part II).
	f := func(x rune) bool{
 		return !unicode.IsLetter(x)
 	}
 	words := strings.FieldsFunc(contents, f)
 	var res []mapreduce.KeyValue
 	for _, word := range words{
 		res = append(res, mapreduce.KeyValue{word,filename})
 	}
 	return res
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
func reduceF(key string, values []string) string {

	fileName := make(map[string]bool)

	for _,value := range values{
		fileName[value]=true;
	}
	num := 0
	var words []string
	//words := make([]string)
	for key := range fileName{
		num+=1
		words = append(words, key)
	}
	sort.Strings(words)
	var res string
	for i, files := range words{
		if i>=1 {
			res +=","
		}
		res += files
	}
	return strconv.Itoa(num)+" "+res

//	sum := 0
//	for _,value := range values {
//		i, err := strconv.Atoi(value)
//		if err != nil{
//			log.Fatal("wc.reduceF: fail to convert. The error is ", err)
//		}
//		sum += i
//	}
//	return strconv.Itoa(sum);
	// Your code here (Part II).
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("wcseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100, nil)
	}
}
