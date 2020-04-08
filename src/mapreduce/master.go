package mapreduce

import (
	"fmt"
	"net"
	"sync"
)

type Master struct {
	sync.Mutex

	address	string			// master名字
	doneChannel	chan bool	// 等待结束通道
	newCond	*sync.Cond
	workers []string	// 所有worker

	jobName string		// job名字
	files	[]string	// 输入文件
	nReduce	int			// 需要reduce分区的个数

	shutdown	chan struct{}	// 用来关闭的通道
	l			net.Listener
	stats		[]int
}
// worker来master报道(注册)
func (mr *Master) Register(args *RegisterArgs, _ *struct{}) error {
	mr.Lock()
	defer mr.Unlock()
	mr.workers = append(mr.workers, args.Worker)

	mr.newCond.Broadcast()	// 下发所有通知给等待锁的

	return nil
}
// 创建master
func newMaster(master string) (mr *Master) {
	mr = new(Master)
	mr.address = master
	mr.shutdown = make(chan struct{})
	mr.newCond = sync.NewCond(mr)
	mr.doneChannel = make(chan bool)
	return
}
// 用来启动run : mapPhase函数为每个file调用一个map, reducePhase函数为每个reduce调用一个reduce, 回调函数是返回status
func Sequential(jobName string, files []string, nreduce int,
	mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string,
) (mr *Master) {
	mr = newMaster("master")
	go mr.run(jobName, files, nreduce, func(phase jobPhase) {
		switch phase {
		case mapPhase:
			for i, f := range mr.files {
				doMap(mr.jobName, i, f, mr.nReduce, mapF)
			}
		case reducePhase:
			for i := 0; i < mr.nReduce; i++ {
				doReduce(mr.jobName, i, mergeName(mr.jobName, i), len(mr.files), reduceF)
			}
		}
	}, func() {
		mr.stats = []int{len(files) + nreduce}
	})
	return
}

func (mr *Master) forwardRegistrations(ch chan string) {
	i := 0
	for {
		mr.Lock()
		if len(mr.workers) > i {
			w := mr.workers[i]
			go func() { ch <- w }()
			i = i + 1
		} else {
			mr.newCond.Wait()
		}
		mr.Unlock()
	}
}

func Distributed(jobName string, files []string, nreduce int, master string) (mr *Master) {
	mr = newMaster(master)
	mr.startRPCServer()
	go mr.run(jobName, files, nreduce,
		func(phase jobPhase) {
			ch := make(chan string)
			go mr.forwardRegistrations(ch)
			schedule(mr.jobName, mr.files, mr.nReduce, phase, ch)
		},
		func() {
			mr.stats = mr.killWorkers()
			mr.stopRPCServer()
		})
	return
}
// 开始允许map和reduce，完成以后执行回调函数
func (mr *Master) run(jobName string, files []string, nreduce int,
	schedule func(phase jobPhase),
	finish func(),
) {
	mr.jobName = jobName
	mr.files = files
	mr.nReduce = nreduce

	fmt.Printf("%s: Starting Map/Reduce task %s\n", mr.address, mr.jobName)

	schedule(mapPhase)
	schedule(reducePhase)
	finish()
	mr.merge()

	fmt.Printf("%s: Map/Reduce task completed\n", mr.address)

	mr.doneChannel <- true
}
// 等待run结束
func (mr *Master) Wait() {
	<-mr.doneChannel
}
// kill所有worker
func (mr *Master) killWorkers() []int {
	mr.Lock()
	defer mr.Unlock()
	ntasks := make([]int, 0, len(mr.workers))
	for _, w:= range mr.workers {
		var reply ShutdownReply
		ok := call(w, "Worker.Shutdown", new(struct{}), &reply)
		if ok == false {
			fmt.Printf("Master RPC shutdown")
		} else {
			ntasks = append(ntasks, reply.Ntasks)
		}
	}
	return ntasks
}
