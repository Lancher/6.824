package mapreduce

import (
	"fmt"
	"strconv"
	"time"
	"sync"
	"log"
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

	// 1) Any variables share in the Goroutine need the `mutex.Lock()`
	// 2) How to use the mutex? `https://tour.golang.org/concurrency/9`
	// 3) `Sync.wait` is also the option
	
	log.Println("------------------------------------------------")
	log.Println(jobName, mapFiles, nReduce, phase)
	log.Println("------------------------------------------------")

	switch phase {

	// *****map*****
	case mapPhase:
		// variables
		ntasks = len(mapFiles)
		n_other = nReduce
		taskState := SafeState{v: make(map[string]string)}
		taskStateCheckTimeoutSec := 1
		taskStateCheckTimeoutChan := make(chan bool, 1)
		workerChan := make(chan string, 1000)

		// initialize
		for t := 0; t < ntasks; t++ {
			taskState.Set(strconv.Itoa(t), "idle")
		}

		// goroutine start
		go func() {
			time.Sleep(time.Duration(taskStateCheckTimeoutSec) * time.Second)
			taskStateCheckTimeoutChan <- true
		}()
		go func() {
			for {
				select {
				case worker := <-registerChan:
					workerChan <- worker
				}
			}
		}()

		// ioloop start
		mapLoop:
		for {
			select {
			case worker := <- workerChan:
				for t := 0; t < ntasks; t++ {
					task := strconv.Itoa(t)
					if taskState.Get(task) == "idle" {
						taskState.Set(task, "waiting")
						args := DoTaskArgs{JobName: jobName, File: mapFiles[t],
						Phase: mapPhase, TaskNumber: t, NumOtherPhase: n_other}

						// rpc:
						// 1) the worker may have executed it but the reply was lost
						// 2) the worker may still be executing but the master's RPC timed out
						go func() {
							taskState.Set(task, "running")
							suc := call(worker, "Worker.DoTask", args, nil)
							if suc {
								taskState.Set(task, "done")
								registerChan <- worker
							} else {
								taskState.Set(task, "idle")
								time.Sleep(10 * time.Second)
								registerChan <- worker
							}
						}()
						break
					}
				}
			case <- taskStateCheckTimeoutChan:
				counter := 0
				for t := 0; t < ntasks; t++ {
					task := strconv.Itoa(t)
					if taskState.Get(task) == "done" {
						counter ++
					}
				}
				if counter == ntasks {
					break mapLoop
				}
				go func() {
					time.Sleep(time.Duration(taskStateCheckTimeoutSec) * time.Second)
					taskStateCheckTimeoutChan <- true
				}()
			}
		}

	// *****reduce*****
	case reducePhase:
		// variables
		ntasks = nReduce
		n_other = len(mapFiles)
		taskState := SafeState{v: make(map[string]string)}
		taskStateCheckTimeoutSec := 1
		taskStateCheckTimeoutChan := make(chan bool, 1)
		workerChan := make(chan string, 1000)

		// initialize
		for t := 0; t < ntasks; t++ {
			taskState.Set(strconv.Itoa(t), "idle")
		}

		// goroutine start
		go func() {
			time.Sleep(time.Duration(taskStateCheckTimeoutSec) * time.Second)
			taskStateCheckTimeoutChan <- true
		}()
		go func() {
			for {
				select {
				case worker := <-registerChan:
					workerChan <- worker
				}
			}
		}()

		// ioloop start
		reduceLoop:
		for {
			select {
			case worker := <- workerChan:
				for t := 0; t < ntasks; t++ {
					task := strconv.Itoa(t)
					if taskState.Get(task) == "idle" {
						taskState.Set(task, "waiting")
						args := DoTaskArgs{JobName: jobName, File: "",
						Phase: reducePhase, TaskNumber: t, NumOtherPhase: n_other}

						// rpc:
						// 1) the worker may have executed it but the reply was lost
						// 2) the worker may still be executing but the master's RPC timed out
						go func() {
							taskState.Set(task, "running")
							suc := call(worker, "Worker.DoTask", args, nil)
							if suc {
								taskState.Set(task, "done")
								registerChan <- worker
							} else {
								taskState.Set(task, "idle")
								time.Sleep(10 * time.Second)
								registerChan <- worker
							}
						}()
						break
					}
				}
			case <- taskStateCheckTimeoutChan:
				counter := 0
				for t := 0; t < ntasks; t++ {
					task := strconv.Itoa(t)
					if taskState.Get(task) == "done" {
						counter ++
					}
				}
				if counter == ntasks {
					break reduceLoop
				}
				go func() {
					time.Sleep(time.Duration(taskStateCheckTimeoutSec) * time.Second)
					taskStateCheckTimeoutChan <- true
				}()
			}
		}
	}
}

// the best way to use mutex!!!!
type SafeState struct {
	v   map[string]string
	mux sync.Mutex
}

func (s *SafeState) Set(key string, val string) {
	s.mux.Lock()
	s.v[key] = val
	s.mux.Unlock()
}

func (s *SafeState) Get(key string) string {
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.v[key]
}
