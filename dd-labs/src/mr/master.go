package mr

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MAP_TASK    int = 1
	REDUCE_TASK int = 2
	IDLE_TASK   int = 3
	PLEASE_TERM int = 4

	NUM_SECONDS_TO_WAIT_FOR_WORKER time.Duration = 10
	SLEEP_DURATION time.Duration = 2
	TASKS_CHAN_SIZE int = 10000
)

type Task struct {
	ID           int
	TaskType     int
	SpecificTask interface{}
}

type MapTask struct {
	MapTaskID      int
	InputFilenames []string
	NumReducers    int
}

type ReduceTask struct {
	ReduceTaskID   int
	InputFilenames []string
	OutputFilename string
}

type IdleTask struct {
	SleepDuration time.Duration
}

type Master struct {
	nextID           int
	files            []string
	isDone           bool
	numReducers      int
	nextReduceTaskID int
	numReducersLeft  int
	numMappers       int
	nextMapTaskID    int
	numMappersLeft   int
	tasks            chan Task
	isTaskDone       map[int]bool

	mutex     sync.Mutex
	waitGroup sync.WaitGroup
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.tasks) == 0 { // no tasks
		reply.Task = m.getTaskOfIdleTask()
	} else {
		task := <-m.tasks
		reply.Task = task
		go m.workerController(task)
	}

	return nil
}

func (m *Master) getTaskOfIdleTask() Task {
	return Task{
		ID:       -1,
		TaskType: IDLE_TASK,
		SpecificTask: IdleTask{
			SleepDuration: SLEEP_DURATION,
		},
	}
}

func (m *Master) workerController(task Task) {
	time.Sleep(NUM_SECONDS_TO_WAIT_FOR_WORKER * time.Second)
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.isTaskDone[task.ID] == false {
		m.tasks <- task // rescedule task
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.isDone
}

func (m *Master) PostTaskDone(args *PostTaskDoneArgs, reply *PostTaskDoneReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if isDone, ok := m.isTaskDone[args.Task.ID]; ok && !isDone {
		switch args.Task.TaskType {

		case MAP_TASK:
			m.numMappersLeft--
			if m.numMappersLeft == 0 {
				m.initReduceTasks()
			}
		case REDUCE_TASK:
			m.numReducersLeft--
			if m.numReducersLeft == 0 {
				m.isDone = true
			}
		}
		m.isTaskDone[args.Task.ID] = true
	} else {
		fmt.Printf("-- PostTaskDone: no ID detected: %d\n", args.Task.ID)
	}

	return nil
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	registerStructsForGOB()

	m.nextID = 0
	m.numMappers = 0
	m.nextMapTaskID = 1
	m.numMappersLeft = 0
	m.numReducers = nReduce
	m.nextReduceTaskID = 1
	m.numReducersLeft = nReduce
	m.isDone = false
	m.files = files
	m.tasks = make(chan Task, TASKS_CHAN_SIZE)
	m.isTaskDone = make(map[int]bool)

	m.server()

	m.initMapTasks()

	return &m
}

func registerStructsForGOB() {
	gob.Register(Task{})
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
	gob.Register(IdleTask{})

	gob.Register(GetTaskArgs{})
	gob.Register(GetTaskReply{})
	gob.Register(PostTaskDoneArgs{})
	gob.Register(PostTaskDoneReply{})
}

// returns next Task.ID started from 0
func (m *Master) getNextID() int {
	nextID := m.nextID
	m.nextID++
	return nextID
}

// returns next MapTask.ID started from 1
func (m *Master) getNextMapTaskID() int {
	nextMapTaskID := m.nextMapTaskID
	m.numMappers++
	m.nextMapTaskID++
	m.numMappersLeft++
	return nextMapTaskID
}

// returns next ReduceTask.ID started from 1
func (m *Master) getNextReduceTaskID() int {
	nextReduceTaskID := m.nextReduceTaskID
	m.nextReduceTaskID++
	return nextReduceTaskID
}

func (m *Master) initMapTasks() {

	for _, filename := range m.files {
		if _, err := os.Stat(filename); err == nil {
			task := Task{
				ID:       m.getNextID(),
				TaskType: MAP_TASK,
				SpecificTask: MapTask{
					MapTaskID:      m.getNextMapTaskID(),
					InputFilenames: []string{filename},
					NumReducers:    m.numReducers,
				},
			}
			m.tasks <- task
			m.isTaskDone[task.ID] = false
		} else {
			fmt.Printf("-- initMapTasks: error during opening %s\n", filename)
		}
	}
}

func (m *Master) initReduceTasks() {
	for reducerID := 1; reducerID <= m.numReducers; reducerID++ {
		reduceTask := ReduceTask{
			ReduceTaskID:   m.getNextReduceTaskID(),
			InputFilenames: []string{},
			OutputFilename: fmt.Sprintf("mr-out-%d", reducerID),
		}

		for mapperID := 1; mapperID <= m.numMappers; mapperID++ {
			inputFilename := fmt.Sprintf("mr-%d-%d", mapperID, reducerID)
			reduceTask.InputFilenames = append(reduceTask.InputFilenames, inputFilename)
		}

		task := Task{
			ID:           m.getNextID(),
			TaskType:     REDUCE_TASK,
			SpecificTask: reduceTask,
		}
		m.tasks <- task
		m.isTaskDone[task.ID] = false
	}
}
