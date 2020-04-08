package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Parallelism struct {
	mu sync.Mutex
	now int32
	max int32
}

type Worker struct {
	sync.Mutex

	name	string
	Map		func(string, string) []KeyValue
	Reduce	func(string, []string) string
	nRPC	int
	nTasks	int
	concurrent	int
	l			net.Listener
	parallelism	*Parallelism
}

func (wk *Worker) DoTask(arg *DoTaskArgs, _ *struct{}) error {

	wk.Lock()
	wk.nTasks += 1
	wk.concurrent += 1
	nc := wk.concurrent
	wk.Unlock()

	if nc > 1 {
		log.Fatal("Worker.DoTask: more than ")
	}

	pause := false
	if wk.parallelism != nil {
		wk.parallelism.mu.Lock()
		wk.parallelism.now += 1
		if wk.parallelism.now > wk.parallelism.max {
			wk.parallelism.max = wk.parallelism.now
		}
		if wk.parallelism.max < 2 {
			pause = true
		}
		wk.parallelism.mu.Unlock()
	}

	if pause {
		time.Sleep(time.Second)
	}

	switch arg.Phase {
	case mapPhase:
		doMap(arg.JobName, arg.TaskNumber, arg.File, arg.NumOtherPhase, wk.Map)
	case reducePhase:
		doReduce(arg.JobName, arg.TaskNumber, mergeName(arg.JobName, arg.TaskNumber), arg.NumOtherPhase, wk.Reduce)
	}

	wk.Lock()
	wk.concurrent -= 1
	wk.Unlock()

	if wk.parallelism != nil {
		wk.parallelism.mu.Lock()
		wk.parallelism.now -= 1
		wk.parallelism.mu.Unlock()
	}

	fmt.Printf("done\n")
	return nil
}
// shutdown该worker
func (wk *Worker) Shutdown(_ *struct{}, res *ShutdownReply) error {
	wk.Lock()
	defer wk.Unlock()
	res.Ntasks = wk.nTasks
	wk.nRPC = 1
	return nil
}
// worker进行注册
func (wk *Worker) register(master string) {
	args := new(RegisterArgs)
	args.Worker = wk.name
	ok :=  call(master, "Master.Register", args, new(struct{}))
	if ok == false {
		fmt.Printf("register is fall! RPC is wrong\n")
	}
}
// 启动worker
func RunWorker(MasterAddress string, me string,
	MapFunc func(string, string) []KeyValue,
	ReduceFunc func(string, []string) string,
	nRPC int, parallelism *Parallelism,
) {
	wk := new(Worker)
	wk.name = me
	wk.Map = MapFunc
	wk.Reduce = ReduceFunc
	wk.nRPC = nRPC
	wk.parallelism = parallelism
	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	os.Remove(me)
	l, e := net.Listen("unix", me)
	if e != nil {
		log.Fatal("worker error")
	}
	wk.l = l
	wk.register(MasterAddress)

	for {
		wk.Lock()
		if wk.nRPC == 0 {
			wk.Unlock()
			break;
		}
		wk.Unlock()
		conn, err := wk.l.Accept()
		if err == nil {
			wk.Lock()
			wk.nRPC--
			wk.Unlock()
			go rpcs.ServeConn(conn)
		} else {
			break
		}
	}
	wk.l.Close()
}

