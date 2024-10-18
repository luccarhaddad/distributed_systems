package mapreduce

import (
	"log"
	"sync"
)

// Schedules map operations on remote workers. This will run until InputFilePathChan
// is closed. If there is no worker available, it'll block.
func (master *Master) schedule(task *Task, proc string, filePathChan chan string) int {

	var (
		wg         sync.WaitGroup
		counter    int
		maxRetries int = 3
	)

	log.Printf("Scheduling %v operations\n", proc)

	// Initial scheduling of operations
	for filePath := range filePathChan {
		operation := &Operation{proc, counter, filePath}
		counter++
		wg.Add(1)

		go func() {
			defer wg.Done()
			master.scheduleOperation(operation)
		}()
	}
	wg.Wait()

	// Retry failed operations
	for retry := 0; retry < maxRetries; retry++ {
		master.failedOperationsMutex.Lock()
		failedOps := make([]*Operation, 0, len(master.failedOperations))
		for _, op := range master.failedOperations {
			failedOps = append(failedOps, op)
		}
		master.failedOperations = make(map[int]*Operation) // Clear the map
		master.failedOperationsMutex.Unlock()

		if len(failedOps) == 0 {
			break
		}

		log.Printf("Retrying %d failed operations (attempt %d/%d)", len(failedOps), retry+1, maxRetries)

		for _, operation := range failedOps {
			wg.Add(1)
			go func(op *Operation) {
				defer wg.Done()
				master.scheduleOperation(op)
			}(operation)
		}
		wg.Wait()
	}

	log.Printf("%vx %v operations completed\n", counter, proc)
	return counter
}

// scheduleOperation attempts to run a single operation on an available RemoteWorker.
// If the operation fails, it's added back to the failedOperations map for potential retry.
func (master *Master) scheduleOperation(operation *Operation) {
	worker := <-master.idleWorkerChan
	err := master.runOperation(worker, operation)

	if err != nil {
		log.Printf("Operation %v '%v' Failed. Error: %v\n", operation.proc, operation.id, err)
		master.failedWorkerChan <- worker
		master.failedOperationsMutex.Lock()
		master.failedOperations[operation.id] = operation
		master.failedOperationsMutex.Unlock()
	} else {
		master.idleWorkerChan <- worker
		master.successfulOperationsMutex.Lock()
		master.successfulOperations[operation.id] = operation
		master.successfulOperationsMutex.Unlock()
	}
}

// runOperation executes a single operation on a RemoteWorker and returns any error that occurs.
// It does not handle retries or worker management; this is left to the calling function.
func (master *Master) runOperation(remoteWorker *RemoteWorker, operation *Operation) error {
	log.Printf("Running %v (ID: '%v' File: '%v' Worker: '%v')\n", operation.proc, operation.id, operation.filePath, remoteWorker.id)

	args := &RunArgs{operation.id, operation.filePath}
	return remoteWorker.callRemoteWorker(operation.proc, args, new(struct{}))
}
