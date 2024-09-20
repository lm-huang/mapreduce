package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		//从master获取任务
		task := getTask()

		switch task.TaskState {
		case Map:
			mapper(&task, mapf)
		case Reduce:
			reducer(&task, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}
	}
}

func getTask() Task {
	args := ExampleArgs{}
	reply := Task{}
	call("Master.AssignTask", &args, &reply)
	return reply
}

func mapper(task *Task, mapf func(string, string) []KeyValue) {
	content, err := ioutil.ReadFile(task.Input)
	if err != nil {
		log.Fatal("Failed to read file: "+task.Input, err)
	}
	//wo.go中，返回的是kva := []mr.KeyValue作为intermediates，里面都是{key,1}的结构
	intermediates := mapf(task.Input, string(content))

	buffer := make([][]KeyValue, task.NReducer)
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.NReducer   //保证相同key的总是去相同的reducer
		buffer[slot] = append(buffer[slot], intermediate) //buffer[i][j]中，i代表reducer编号
	}
	mapOutPut := make([]string, 0)
	for i := 0; i < task.NReducer; i++ {
		mapOutPut = append(mapOutPut, writeToLocalFile(task.TaskNumber, i, &buffer[i])) //从0开始添加对应reducer编号的kv文件名
	}
	task.Intermediates = mapOutPut //将文件路径字符串保存在task.Intermediates中，格式task编号-reducer编号
	TaskCompleted(task)            //
}

func writeToLocalFile(x, y int, kvs *[]KeyValue) string {
	dir, _ := os.Getwd()                              //获取当前文件目录
	tempFire, err := ioutil.TempFile(dir, "mr-tmp-*") //在dir中创建临时文件，*为随机名，返回文件指针
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	enc := json.NewEncoder(tempFire) //json编译器
	for _, kv := range *kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	} //所有相同reducer编号的intermediates的kv都写到了文件中
	tempFire.Close()
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tempFire.Name(), outputName) //重命名，原子操作，其他进程读取时要么是旧版本，要么是新写入，不会是中间状态的不完整文件
	return filepath.Join(dir, outputName)  //返回文件路径字符串
}

func reducer(task *Task, reducef func(string, []string) string) {
	intermediate := *readFromLocalFile(task.Intermediates)
	sort.Sort(ByKey(intermediate))

	dir, _ := os.Getwd() //当前工作路径
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[j].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskNumber)
	os.Rename(tempFile.Name(), oname)
	task.Output = oname
	TaskCompleted(task)
}

func readFromLocalFile(files []string) *[]KeyValue {
	kva := []KeyValue{}
	for _, path := range files {
		file, err := os.Open(path)
		if err != nil {
			log.Fatal("Failed to read file"+path, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue //解码器逐行解码，传入kv的指针，其修改为解码后的结果
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return &kva
}

// 任务完成后通知master，发送task信息
func TaskCompleted(task *Task) {
	reply := ExampleReply{}
	call("Master.TaskCompleted", task, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// interface{}空接口类型，表示任何数据
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
