package utils

import (
	"log"
)

type WorkerPool struct {
	Counter     map[string]int
	ControlChan map[string]map[int](chan int)
}

// 回调函数
type WorkerFunc func(f interface{}, workerId int, control <-chan int)

// 实例化WorkerPool
func NewWorkerPool() WorkerPool {
	return WorkerPool{
		make(map[string]int),
		make(map[string]map[int](chan int)),
	}
}

// Worker封装
// 注意避免在goroutine中读写map
func (this *WorkerPool) workerWrapper(name string, workerId int, f WorkerFunc, args interface{}, control <-chan int) {
	defer func() {
		err := recover()
		if err != nil {
			log.Printf("worker panic. name:%v, err:%v", name, err)
		}
	}()

	log.Printf("worker start. name:%v, id:%v", name, workerId)
	f(args, workerId, control)
	log.Printf("worker finish. name:%v, id:%v", name, workerId)
}

// 初始化一个worker
func (this *WorkerPool) workerInit(name string, workerId int) {
	if _, ok := this.ControlChan[name]; !ok {
		this.ControlChan[name] = make(map[int](chan int))
	}

	if _, ok := this.ControlChan[name][workerId]; !ok {
		this.ControlChan[name][workerId] = make(chan int, 1)
	}
}

// 增加worker
func (this *WorkerPool) incr(name string, target int, f WorkerFunc, args interface{}) {
	count, _ := this.Counter[name]

	for i := count; i < target; i++ {
		this.workerInit(name, i)
		control := this.ControlChan[name][i]

		go this.workerWrapper(name, i, f, args, control)
	}

	this.Counter[name] = target
}

// 减少worker
func (this *WorkerPool) decr(name string, target int) {
	if target < 0 {
		target = 0
	}

	count, _ := this.Counter[name]

	for i := count; i > target; i-- {
		log.Printf("name:%v, i:%v", name, i-1)

		this.ControlChan[name][i-1] <- 1
	}

	this.Counter[name] = target
}

// 增加或减少worker
func (this *WorkerPool) Start(name string, count int, f WorkerFunc, args interface{}) {
	countNow, _ := this.Counter[name]

	if countNow < count {
		this.incr(name, count, f, args)
	}

	if countNow > count {
		this.decr(name, count)
	}
}

// 停止Worker
func (this *WorkerPool) Stop(name string) {
	this.decr(name, 0)

	delete(this.Counter, name)
	delete(this.ControlChan, name)
}
