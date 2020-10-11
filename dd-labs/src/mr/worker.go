package mr

import "encoding/gob"
import "encoding/json"
import "fmt"
import "hash/fnv"
import "io/ioutil"
import "log"
import "net/rpc"
import "os"
import "time"
import "sort"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
			reducef func(string, []string) string) {

	// fmt.Println("-- Worker")

	gob.Register(Task{})
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
	gob.Register(IdleTask{})

	// gob.Register(GetTaskArgs{})
	// gob.Register(GetTaskReply{})
	// gob.Register(PostTaskDoneArgs{})
	// gob.Register(PostTaskDoneReply{})

	for {
		task, status := callGetTask()

		// fmt.Println("task got")

		if !status {
			// fmt.Println("error during geting task")
			log.Fatal("Error during calling GetTask from master")
		}
// 
		// fmt.Println("task got")

		switch task.TaskType {
			case MAP_TASK:
				doMapTask(task, mapf)
			case REDUCE_TASK:
				doReduceTask(task, reducef)
			case IDLE_TASK:
				doIdleTask(task.SpecificTask.(IdleTask))
			case PLEASE_TERM:
				break
			default:
				fmt.Println("-- Worker: Unknown task type")
		}
	}
}

func callGetTask() (Task, bool) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	// fmt.Println("-- now will call Master.GetTask")
	status := call("Master.GetTask", &args, &reply)

	return reply.Task, status
}

func doMapTask(t Task, mapf func(string, string) []KeyValue) bool {
	task := t.SpecificTask.(MapTask)
	for _, filename := range task.InputFilenames {
		status := doMapTaskOverOneFile(task, mapf, filename)
		if !status {
			return false
		}
	}

	postTaskDone(t)

	return true
}

func postTaskDone(task Task) bool {
	args := PostTaskDoneArgs{}
	args.Task = task
	reply := PostTaskDoneReply{}

	status := call("Master.PostTaskDone", &args, &reply)

	return status

}

func doMapTaskOverOneFile(task MapTask, mapf func(string, string) []KeyValue, filename string) bool {

	// copied part from "mrsequential.go"
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return false
	}
	file.Close()
	fmt.Printf("-- doMapTaskOverOneFile: MapTaskID: %d\n\n", task.MapTaskID)
	fmt.Printf("-- -- -- filename: %s\n", filename)
	kva := mapf(filename, string(content))
	for k, v := range kva {
		fmt.Printf("-- -- -- k: %v v: %s\n", k, v)
	}
	
	return writeKVsIntoFiles(kva, task.NumReducers, task.MapTaskID)
}

func writeKVsIntoFiles(kva []KeyValue, numReducers int, mapTaskID int) bool {
	// fmt.Println("-- writeKVsIntoFiles")
	filesMap := make(map[int]*os.File)
	encodersMap := make(map[int]*json.Encoder)

	var encoder *json.Encoder

	for _, kv := range kva {
		reducerID := ihash(kv.Key) % numReducers + 1
		if oldEncoder, ok := encodersMap[reducerID]; ok {
			encoder = oldEncoder
		} else { // encoder and file have not been initialized yet
			file, err := ioutil.TempFile(".", fmt.Sprintf("mr-%d-%d", mapTaskID, reducerID)) // TODO change name
			if err != nil {
				fmt.Println("-- writeKVsIntoFiles: Error during creating TempFile ->", err)
				return false
			}

			encoder = json.NewEncoder(file)
			filesMap[reducerID] = file
			encodersMap[reducerID] = encoder
		}

		err := encoder.Encode(&kv)
		if(err != nil) {
			fmt.Println("-- writeKVsIntoFiles: Error during encoding ->", err)
			return false
		}
	}

	for reducerID, file := range filesMap {
		file.Close()
		if err := os.Rename(file.Name(), fmt.Sprintf("mr-%d-%d", mapTaskID, reducerID)); err != nil {
			fmt.Println("-- writeKVsIntoFiles: Error during os.Rename -> ", err)
			// return false
		}
	}
	return true
}

func doReduceTask(t Task, reducef func(string, []string) string) bool {
	task := t.SpecificTask.(ReduceTask)
	
	kva := []KeyValue{}

	for _, inputFilename := range task.InputFilenames {
		inputFile, err := os.Open(inputFilename)
		if err != nil { 
			log.Fatalf("Error during doing reduce task", err)
		}
		
		decoder := json.NewDecoder(inputFile)
		var kv KeyValue
		for decoder.Decode(&kv) == nil { // no error
			kva = append(kva, kv)
		}
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//

	sort.Sort(ByKey(kva))

	ofile, err := ioutil.TempFile(".", task.OutputFilename) // TODO err change
	if err != nil {
		fmt.Println("-- doReduceTask: Error during creating TempFile ->", err)
		return false
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
	if err := os.Rename(ofile.Name(), task.OutputFilename); err == nil {
		// removeInputFiles(task.InputFilenames)
		postTaskDone(t)
	}

	return err == nil
}

func removeInputFiles(filenames []string){
	for _, filename := range filenames {
		os.Remove(filename)
	}
}

func doIdleTask(task IdleTask) bool {
	time.Sleep(task.SleepDuration * time.Second)
	return true
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
