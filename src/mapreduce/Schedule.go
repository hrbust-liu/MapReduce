package mapreduce

import "fmt"
import "sync"

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule : %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	var wg sync.WaitGroup
	wg.Add(ntasks)

	taskChan := make(chan int, ntasks)
	for i := 0; i < ntasks; i++ {
		taskChan <- i
	}

	go func() {
		for {
			ch := <-registerChan
			go func(address string) {
				for {
					index := <-taskChan
					result := call(address, "Worker.DoTask", &DoTaskArgs{jobName, mapFiles[index], phase, index, n_other}, new(struct{}))
					if result {
						wg.Done()
						fmt.Printf("Task %v has done\n", index)
					} else {
						taskChan <- index
					}
				}
			}(ch)
		}
	}()
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
