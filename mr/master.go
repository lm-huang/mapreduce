package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MasterTaskStatus int

const (
	Idle MasterTaskStatus = iota
	Inprogress
	Complete
)

type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

type Master struct {
	TaskQueue     chan *Task
	TaskMeta      map[int]*MasterTask
	MasterPhase   State
	NReduce       int //整个mapreduce作业中的reducer的数量
	InputFiles    []string
	Intermediates [][]string
}

type MasterTask struct {
	TaskStatus    MasterTaskStatus
	StartTime     time.Time
	TaskReference *Task
}

type Task struct {
	Input         string
	TaskState     State
	NReducer      int //单个Map任务中的reducer的数量
	TaskNumber    int
	Intermediates []string //都是文件名
	Output        string   //reduce以后的文件名
}

var mu sync.Mutex

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		TaskQueue:     make(chan *Task, max(nReduce, len(files))), //using a channel as a makeshift rather than container/list (type conversion is needed)
		TaskMeta:      make(map[int]*MasterTask),
		MasterPhase:   Map,
		NReduce:       nReduce, //input
		InputFiles:    files,   // input
		Intermediates: make([][]string, nReduce),
	}

	//step 1
	m.createMapTask()
	//step 2
	m.server()

	go m.catchTimeOut()

	return &m
}

func (m *Master) createMapTask() {
	for idx, filename := range m.InputFiles { //遍历inputFiles
		taskMeta := Task{ //创建Task
			Input:      filename,
			TaskState:  Map,
			NReducer:   m.NReduce,
			TaskNumber: idx,
		}
		m.TaskQueue <- &taskMeta       //将Task发送到队列
		m.TaskMeta[idx] = &MasterTask{ //记录Task元数据
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}

func (m *Master) AssignTask(arg *ExampleArgs, reply *Task) error {
	//看自己queue里还有没有Task
	mu.Lock()
	defer mu.Unlock()
	if len(m.TaskQueue) > 0 {
		//有空就发出去
		*reply = *<-m.TaskQueue //对管道取值再进行解指针
		//记录task的启动时间
		m.TaskMeta[reply.TaskNumber].TaskStatus = Inprogress
		m.TaskMeta[reply.TaskNumber].StartTime = time.Now()
	} else if m.MasterPhase == Exit {
		*reply = Task{TaskState: Exit}
	} else {
		//没有就让worker等待
		*reply = Task{TaskState: Wait}
	}
	return nil
}

func (m *Master) server() {
	rpc.Register(m) //可以调用m的方法
	rpc.HandleHTTP()
	sockname := masterSock()
	os.Remove(sockname)
	//Unix 域套接字是一种进程间通信（IPC）机制，它在同一台机器上的不同进程之间提供高效的通信方式。
	//与网络套接字不同，Unix 域套接字不涉及网络传输，只在本地系统内部使用。
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) //在新的goroutine中异步运行http服务
}

func (m *Master) TaskCompleted(task *Task, reply *ExampleReply) error {
	mu.Lock()
	if task.TaskState != m.MasterPhase || m.TaskMeta[task.TaskNumber].TaskStatus == Complete {
		//因为worker在同一个文件磁盘上，重复的结果需要丢弃
		return nil
	}
	m.TaskMeta[task.TaskNumber].TaskStatus = Complete
	mu.Unlock()
	defer m.processTaskResult(task)
	return nil
}

func (m *Master) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Map:
		for reduceTaskId, filePath := range task.Intermediates { //[文件名1, 文件名2 ] 下标也是reducer编号，根据key的值哈希得到
			m.Intermediates[reduceTaskId] = append(m.Intermediates[reduceTaskId], filePath)
		}
		if m.allTaskDone() {
			m.createReduceTask()
			m.MasterPhase = Reduce
		}
	case Reduce:
		if m.allTaskDone() {
			m.MasterPhase = Exit
		}
	}
}

func (m *Master) createReduceTask() {
	m.TaskMeta = make(map[int]*MasterTask)
	for idx, files := range m.Intermediates {
		taskMeta := Task{
			TaskState:     Reduce,
			NReducer:      m.NReduce,
			TaskNumber:    idx,
			Intermediates: files,
		}
		m.TaskQueue <- &taskMeta
		m.TaskMeta[idx] = &MasterTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}

func (m *Master) allTaskDone() bool {
	for _, task := range m.TaskMeta {
		if task.TaskStatus != Complete {
			return false
		}
	}
	return true
}

func (m *Master) catchTimeOut() {

}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
