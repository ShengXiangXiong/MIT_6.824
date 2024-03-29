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

	//schedule() must wait for a worker to finish before it can give it another task
	//otherwise endless loop case if certain task not excuted by worker successfully

	//Using Channel to simulate semaphores
	sem := make(chan int, ntasks)

	var wg sync.WaitGroup
	taskArgsChan := make(chan interface{}, ntasks)

	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		//add taskArgsChan
		taskArgsChan <- DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}
		//intital semaphore
		sem <- 1
	}

	for len(sem) > 0 {
		workerAddr := <-registerChan
		taskArgs := <-taskArgsChan
		if taskArgs == nil {
			fmt.Printf("finish\n")
			break
		}
		go func() {
			ok := call(workerAddr, "Worker.DoTask", taskArgs, nil)
			if ok {
				<-sem
				wg.Done()
				fmt.Printf("success work %v\n", taskArgs)
				//must write conditional interpretation here
				if len(sem) == 0 {
					taskArgsChan <- nil
				}
				registerChan <- workerAddr
			} else {
				//the master should re-assign the task given to the failed worker to another worker
				taskArgsChan <- taskArgs
			}
			//fail when write conditional interpretation, can't understand why
			// if len(sem) == 0 {
			// 	taskArgsChan <- nil
			// 	fmt.Printf("--------------------------success work %v\n", taskArgs)
			// }
		}()
	}
	//wait until all task completed for this phase
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
	return
}
