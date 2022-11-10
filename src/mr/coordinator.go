package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

type MapTask struct {
	id      int       // 任务id
	file    string    // 输入文件
	startAt time.Time // 开始时间
	done    bool      // 结束标记
}

type ReduceTask struct {
	id      int       // 任务id
	files   []string  // 输入文件(M files)
	startAt time.Time // 开始时间
	done    bool      // 结束标记
}

type Coordinator struct {
	mutex        sync.Mutex   // 资源互斥锁
	mapTasks     []MapTask    // 所有map任务
	mapRemain    int          // 剩余map任务数量
	reduceTasks  []ReduceTask // 所有reduce任务
	reduceRemain int          // 剩余reduce任务数量
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// args是worker进程任务完成的结果
	switch args.DoneType {
	case TaskTypeMap:
		if !c.mapTasks[args.Id].done {
			// 根据args更新任务
			c.mapTasks[args.Id].done = true
			// 把所有map任务生成的中间文件根据hash值分配给reduce任务
			for reduceId, file := range args.Files {
				if len(file) > 0 {
					c.reduceTasks[reduceId].files = append(c.reduceTasks[reduceId].files, file)
				}
			}
			c.mapRemain--
		}
	case TaskTypeReduce:
		if !c.reduceTasks[args.Id].done {
			c.reduceTasks[args.Id].done = true
			c.reduceRemain--
		}
	}

	/**
	log.Printf("Remaining map: %d, reduce: %d\n", c.mapRemain, c.reduceRemain)
	defer log.Printf("Distribute task %v\n", reply)
	*/

	now := time.Now()
	timeoutAgo := now.Add(-10 * time.Second)
	if c.mapRemain > 0 {
		// 存在map任务未完成
		for idx := range c.mapTasks {
			t := &c.mapTasks[idx]
			// 返回超时未完成的map任务给worker进程
			if !t.done && t.startAt.Before(timeoutAgo) {
				reply.Type = TaskTypeMap
				reply.Id = t.id
				reply.Files = []string{t.file}
				reply.NReduce = len(c.reduceTasks)

				t.startAt = now

				return nil
			}
		}
		// 没有map任务，让worker进程sleep
		reply.Type = TaskTypeSleep
	} else if c.reduceRemain > 0 {
		// 返回未完成的reduce任务给worker进程
		for idx := range c.reduceTasks {
			t := &c.reduceTasks[idx]
			if !t.done && t.startAt.Before(timeoutAgo) {
				reply.Type = TaskTypeReduce
				reply.Id = t.id
				reply.Files = t.files

				t.startAt = now

				return nil
			}
		}
		// 没有reduce任务，让worker进程sleep
		reply.Type = TaskTypeSleep
	} else {
		// 所有任务完成了，让worker进程exit
		reply.Type = TaskTypeExit
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	// Unlock after function finished
	defer c.mutex.Unlock()
	return c.mapRemain == 0 && c.reduceRemain == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:     make([]MapTask, len(files)),
		reduceTasks:  make([]ReduceTask, nReduce),
		mapRemain:    len(files),
		reduceRemain: nReduce,
	}

	/**
	log.Printf(
		"Coordinator has %d map tasks and %d reduce tasks to distribute\n",
		c.mapRemain,
		c.reduceRemain,
	)
	*/

	// init map tasks
	for i, f := range files {
		c.mapTasks[i] = MapTask{id: i, file: f, done: false}
	}
	// init reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{id: i, done: false}
	}

	c.server()
	return &c
}
