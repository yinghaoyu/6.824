package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"
import "sort"
import "time"
import "encoding/json"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	// 工作进程需要处理的任务
	var newTask TaskReply
	var finishedTask TaskArgs = TaskArgs{DoneType: TaskTypeNone}
	for {
		// 把上次的任务结果通过rpc给coordinator，同时获得新任务
		newTask = GetTask(&finishedTask)
		switch newTask.Type {
		case TaskTypeMap:
			// map任务只处理一个文件
			f := newTask.Files[0]
			file, err := os.Open(f)
			if err != nil {
				log.Fatalf("cannot open %v", f)
			}
			defer file.Close()
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", f)
			}
			// 执行map任务
			intermediate := mapf(f, string(content))

			// 将中间文件进行hash分组
			// Key : 根据key得到的hash值
			// Value : KeyValue切片
			byReduceFiles := make(map[int][]KeyValue)
			for _, kv := range intermediate {
				idx := ihash(kv.Key) % newTask.NReduce
				byReduceFiles[idx] = append(byReduceFiles[idx], kv)
			}

			// 把中间文件输出到json文件
			// 需要将此次map生成的文件记录告知给coordinator
			// files是一个长度为newTask.NReduce的string类型切片
			files := make([]string, newTask.NReduce)
			for reduceId, kvs := range byReduceFiles {
				filename := fmt.Sprintf("mr-%d-%d", newTask.Id, reduceId)
				ofile, _ := os.Create(filename)
				defer ofile.Close()
				enc := json.NewEncoder(ofile)
				for _, kv := range kvs {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatal(err)
					}
				}
				files[reduceId] = filename
			}
			// 记录本次worker进程的任务结果
			finishedTask = TaskArgs{DoneType: TaskTypeMap, Id: newTask.Id, Files: files}
		case TaskTypeReduce:
			intermediate := []KeyValue{}
			// 把json文件的键值对解析出来
			for _, filename := range newTask.Files {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				defer file.Close()
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			// 以下做法跟mrsequential.go相同
			// 根据键值对排序
			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", newTask.Id)
			ofile, _ := os.Create(oname)
			defer ofile.Close()

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			// 记录本次worker进程的任务结果
			finishedTask = TaskArgs{DoneType: TaskTypeReduce, Id: newTask.Id, Files: []string{oname}}
		case TaskTypeSleep:
			time.Sleep(500 * time.Millisecond)
			finishedTask = TaskArgs{DoneType: TaskTypeNone}
		case TaskTypeExit:
			// log.Println("Finish my job, ready to exit")
			return
		default:
			panic(fmt.Sprintf("unknown type: %v", newTask.Type))
		}
		/**
		  switch finishedTask.DoneType {
		  case TaskTypeMap:
		    log.Printf("Finish task map#%d\n", finishedTask.Id)
		  case TaskTypeReduce:
		    log.Printf("Finish task reduce#%d\n", finishedTask.Id)
		  }
		  **/
	}
}

func GetTask(finishedtask *TaskArgs) TaskReply {
	reply := TaskReply{}
	ok := call("Coordinator.GetTask", finishedtask, &reply)
	if !ok {
		fmt.Print("call failed!\n")
		os.Exit(0)
	}
	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
