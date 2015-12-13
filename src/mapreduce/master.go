package mapreduce

import "container/list"
import (
	"fmt"
	"log"
	"sync"
//	"time"
	"time"
)


type WorkerInfo struct {
	address string
	state   WorkState
	lock sync.Mutex
	// You can add definitions here.
}

func (wi *WorkerInfo) UpdateState(workSate WorkState) {
	wi.lock.Lock()
	defer  wi.lock.Unlock()
	wi.state = workSate
}
func (wi *WorkerInfo) GetState() WorkState{
	wi.lock.Lock()
	defer  wi.lock.Unlock()
	return wi.state
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	log.Println("start Master and wait for client finishing register")
	workerExec := func(worker string, fileName string, operation JobType, jobNumberChan chan int, numOtherPhase int, wg *sync.WaitGroup) {
		for ch := range jobNumberChan {
			args := &DoJobArgs{File:fileName, Operation:operation, JobNumber:ch, NumOtherPhase:numOtherPhase}
			var reply DoJobReply
			mr.Workers[worker].UpdateState(Using)
			ok := call(mr.Workers[worker].address, "Worker.DoJob", args, &reply)
			if ok == false {
				fmt.Printf("DoWork: RPC %s dojob error\n", mr.Workers[worker])
				log.Println("job failed, and re-assign work to other workers, failed worker: " + worker)
				mr.Workers[worker].UpdateState(InValid)
				jobNumberChan <- ch
				return
			}
			mr.Workers[worker].UpdateState(Valid)
			wg.Done()
		}
	}

	jobHandle := func(jobType JobType, jobNumber int, jobChan chan int, numOtherPhase int) {
		log.Println(string(jobType) + " started ")
		var wg sync.WaitGroup
		wg.Add(jobNumber)
		for i := 0; i < jobNumber; i++ {
			jobChan <- i
		}
		for len(jobChan) > 0 {
			for _, m := range mr.Workers {
				if m.GetState() == Valid {
					go workerExec(m.address, mr.file, jobType, jobChan, numOtherPhase, &wg)
				}
			}
			time.Sleep(1 * time.Millisecond)
		}

		defer close(jobChan)
		wg.Wait()
		log.Println(string(jobType) + " finished ")

	}

	go func() {
		//	REGISTER:
		for {
			select {
			case worker := <-mr.registerChannel:
				mr.Workers[worker] = &WorkerInfo{address:worker, state:Valid}
			//			case <- time.After(1*time.Second):
			//				break REGISTER
			default:
				time.Sleep(1 * time.Second)
				log.Println("waiting for new workers to join")
			}
		}
	}()
	log.Println("start to mapreduce")
	for len(mr.Workers) <= 0 {
		log.Println("wait for valid workers")
		time.Sleep(1 * time.Second)
	}

	mapTaskChan := make(chan int, mr.nMap)
	reduceTaskChan := make(chan int, mr.nReduce)

	jobHandle(Map, mr.nMap, mapTaskChan, mr.nReduce)

	jobHandle(Reduce, mr.nReduce, reduceTaskChan, mr.nMap)

	return mr.KillWorkers()
}
