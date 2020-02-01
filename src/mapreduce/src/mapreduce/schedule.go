package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	var executedTasks []int
	var nFinishedTasks int
	for i := 0; i < ntasks; i++ {
		executedTasks = append(executedTasks, i)
	}
	var mutex1 = &sync.Mutex{}
	var mutex2 = &sync.Mutex{}
	var mutex3 = &sync.Mutex{}
	var stop bool = false
	for {
		mutex3.Lock()
		if nFinishedTasks >= ntasks {
			mutex3.Unlock()
			break
		}
		mutex3.Unlock()
		wk := <-registerChan
		if wk == "stop" {
			break
		}
		go func(wk string) {
			for {
				mutex1.Lock()
				if len(executedTasks) == 0 {
					mutex1.Unlock()
					mutex3.Lock()
					tmp := nFinishedTasks
					mutex3.Unlock()
					if tmp >= ntasks {
						break
					} else {
						continue
					}
				}
				currentTasks := executedTasks[0]
				executedTasks = executedTasks[1:]
				mutex1.Unlock()
				c := make(chan bool)
				go func() {
					args := DoTaskArgs{jobName, mapFiles[currentTasks], phase, currentTasks, n_other}
					ok := call(wk, "Worker.DoTask", args, nil)
					if ok {
						mutex3.Lock()
						nFinishedTasks++
						tmp := nFinishedTasks
						mutex3.Unlock()
						if tmp >= ntasks {
							c <- true
						} else {
							c <- false
						}
					} else {
						mutex1.Lock()
						executedTasks = append(executedTasks, currentTasks)
						mutex1.Unlock()
						c <- false
					}
				}()
				v := <-c
				if v {
					break
				}
			}
			mutex2.Lock()
			if !stop {
				stop = true
				registerChan <- "stop"
			}
			mutex2.Unlock()
		}(wk)
	}
	fmt.Printf("Schedule: %v done\n", phase)
}

