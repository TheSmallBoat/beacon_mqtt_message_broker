package pool

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"awesomeProject/beacon/mqtt-broker-sn/tool"
	//"github.com/segmentio/fasthash/fnv1a"
)

const (
	MinuteKOfStreamTaskTopType int8 = 1 << iota
	HourKOfStreamTaskTopType
	DayKOfStreamTaskTopType
)

const staticChannelSize = 32  //The static channel size for single task queue
const counterChannelSize = 32 //The channel size for real-time task queue counter

const kOfStreamTaskTopMinute = 16 //the top k tasks of the stream for minute
const kOfStreamTaskTopHour = 32   //the top k tasks of the stream for hour
const kOfStreamTaskTopDay = 64    //the top k tasks of the stream for day
const kOfStreamTaskTopAll = 128   //the top k tasks of the stream for all

type StreamOfTopK struct {
	mu                      sync.Mutex
	minuteStreamOfTopKTasks *tool.Stream
	hourStreamOfTopKTasks   *tool.Stream
	dayStreamOfTopKTasks    *tool.Stream
	allStreamOfTopKTasks    *tool.Stream
}

type TaskQueueMetricsSingleWorker struct {
	sumOfSubmitTasks     uint64 //Total number of tasks submitted for a single worker
	sumOfExecuteTasks    uint64 //Total number of tasks executed for a single worker
	failedOfExecuteTasks uint64 //Total failed number of tasks executed for a single worker
	numOfTasksInQueue    int32  //Number of tasks in the queue for a single worker,less than the channel size
}

type TaskQueueMetricsAllWorker struct {
	totalSubmitTasks        uint64 //Total number of tasks submitted for all worker
	totalExecuteTasks       uint64 //Total number of tasks executed for all worker
	totalFailedExecuteTasks uint64 //Total failed number of tasks executed for all worker
}

//Statistics for all worker from the statistics for each real-time task queue counter
type TaskQueueStatsForWorkerStatusStats struct {
	statSize                        *tool.Stats
	statMin                         *tool.Stats
	statMax                         *tool.Stats
	statMean                        *tool.Stats
	statPopulationVariance          *tool.Stats
	statSampleVariance              *tool.Stats
	statPopulationStandardDeviation *tool.Stats
	statSampleStandardDeviation     *tool.Stats
	statPopulationSkew              *tool.Stats
	statSampleSkew                  *tool.Stats
	statPopulationKurtosis          *tool.Stats
	statSampleKurtosis              *tool.Stats
}

type FixedWorkPool struct {
	maxWorkers                             uint16
	taskQueue                              []chan func()
	taskQueueMetrics                       []*TaskQueueMetricsSingleWorker
	taskQueueCounter                       []chan int32  //The channel for single real-time task queue worker counter
	taskQueueWorkerStatusStatsList         []*tool.Stats //Statistics for every real-time task queue worker counter
	taskQueueStatsForWorkerStatusStats     *TaskQueueStatsForWorkerStatusStats
	taskQueueStatsForWorkerStatusStatsJson chan string
	streamOfTopKTasks                      *StreamOfTopK
}

func NewFixedWorkPool(maxWorkers uint16) *FixedWorkPool {
	// There must be at least one worker.
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	taskQueueStatsForStats := &TaskQueueStatsForWorkerStatusStats{
		&tool.Stats{},
		&tool.Stats{},
		&tool.Stats{},
		&tool.Stats{},
		&tool.Stats{},
		&tool.Stats{},
		&tool.Stats{},
		&tool.Stats{},
		&tool.Stats{},
		&tool.Stats{},
		&tool.Stats{},
		&tool.Stats{},
	}
	theStreamOfTopK := &StreamOfTopK{
		sync.Mutex{},
		tool.New(kOfStreamTaskTopMinute),
		tool.New(kOfStreamTaskTopHour),
		tool.New(kOfStreamTaskTopDay),
		tool.New(kOfStreamTaskTopAll),
	}

	// taskQueue is unbuffered since items are always removed immediately.
	pool := &FixedWorkPool{
		maxWorkers:                             maxWorkers,
		taskQueue:                              make([]chan func(), maxWorkers),
		taskQueueMetrics:                       make([]*TaskQueueMetricsSingleWorker, maxWorkers),
		taskQueueCounter:                       make([]chan int32, maxWorkers),
		taskQueueWorkerStatusStatsList:         make([]*tool.Stats, maxWorkers),
		taskQueueStatsForWorkerStatusStats:     taskQueueStatsForStats,
		taskQueueStatsForWorkerStatusStatsJson: make(chan string, 1),
		streamOfTopKTasks:                      theStreamOfTopK,
	}
	// Start the task dispatcher.
	pool.dispatch()

	// Get the task queue stats info of worker status.
	go pool.getTaskQueueStatsInfoOfWorkerStatusWithInterval()
	pool.getStreamTopKWithInterval()

	return pool
}

func (this *FixedWorkPool) dispatch() {
	for i := uint16(0); i < this.maxWorkers; i++ {
		this.taskQueue[i] = make(chan func(), staticChannelSize)
		this.taskQueueMetrics[i] = &TaskQueueMetricsSingleWorker{sumOfSubmitTasks: 0, sumOfExecuteTasks: 0, failedOfExecuteTasks: 0, numOfTasksInQueue: 0}
		this.taskQueueCounter[i] = make(chan int32, counterChannelSize)
		this.taskQueueWorkerStatusStatsList[i] = &tool.Stats{}

		go executeTask(this.taskQueue[i], this.taskQueueCounter[i], this.taskQueueMetrics[i])
		go executeTaskQueueCounterStat(this.taskQueueCounter[i], this.taskQueueWorkerStatusStatsList[i])
	}
}

/*
func (this *FixedWorkPool) assignments(uid string) uint16 {
	return uint16(fnv1a.HashString32(uid) % uint32(this.maxWorkers))
}*/

func (this *FixedWorkPool) getIdWithRandom() uint16 {
	return uint16(rand.Uint32() % uint32(this.maxWorkers))
}

func (this *FixedWorkPool) SubmitTask(uid string, task func()) {
	if task != nil {
		idx := this.getIdWithRandom()
		this.taskQueue[idx] <- task
		submitTasksCounterOperation(this.taskQueueMetrics[idx])
		this.taskQueueCounter[idx] <- getNumOfTasksInQueue(this.taskQueueMetrics[idx])
		this.streamOfTopKTasks.mu.Lock()
		this.streamOfTopKTasks.minuteStreamOfTopKTasks.Insert(uid)
		this.streamOfTopKTasks.mu.Unlock()
	}
}

func (this *FixedWorkPool) ReportTaskQueueMetricsByWorkerWithString() string {
	var s string
	workers := uint16(len(this.taskQueueMetrics))
	for i := uint16(0); i < workers; i++ {
		s += fmt.Sprintf(" [W%d: S%d/E%d/F%d/Q%d] ", i, getSumOfSubmitTasks(this.taskQueueMetrics[i]), getSumOfExecuteTasks(this.taskQueueMetrics[i]), getFailedOfExecuteTasks(this.taskQueueMetrics[i]), getNumOfTasksInQueue(this.taskQueueMetrics[i]))
	}
	return s
}

func (this *FixedWorkPool) ReportTaskQueueMetricsByWorkerWithJson() string {
	var s = "{"
	workers := uint16(len(this.taskQueueMetrics))
	for i := uint16(0); i < workers; i++ {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf("\"Worker_%d\":{\"Submit_Tasks\":%d,\"Execute_Tasks\":%d,\"Failed_Execute_Tasks\":%d,\"In_Queue_Tasks\":%d}", i, getSumOfSubmitTasks(this.taskQueueMetrics[i]), getSumOfExecuteTasks(this.taskQueueMetrics[i]), getFailedOfExecuteTasks(this.taskQueueMetrics[i]), getNumOfTasksInQueue(this.taskQueueMetrics[i]))
	}
	s += "}"
	return s
}

func (this *FixedWorkPool) ReportTaskQueueMetricsWithString() string {
	tqm := this.ReportTaskQueueMetricsAllWorker()
	return fmt.Sprintf("Total Submitted Tasks [%d],Total Executed Tasks [%d],Total Failed Executed Tasks [%d].", tqm.totalSubmitTasks, tqm.totalExecuteTasks, tqm.totalFailedExecuteTasks)
}

func (this *FixedWorkPool) ReportTaskQueueMetricsWithJson() string {
	tqm := this.ReportTaskQueueMetricsAllWorker()
	return fmt.Sprintf("{\"Total Submitted Tasks\":%d,\"Total Executed Tasks\":%d,\"Total Failed Executed Tasks\":%d}", tqm.totalSubmitTasks, tqm.totalExecuteTasks, tqm.totalFailedExecuteTasks)
}

func (this *FixedWorkPool) ReportTaskQueueMetricsAllWorker() *TaskQueueMetricsAllWorker {
	totalSubmitTasks := uint64(0)
	totalExecuteTasks := uint64(0)
	totalFailedExecuteTasks := uint64(0)

	workers := uint16(len(this.taskQueueMetrics))
	for i := uint16(0); i < workers; i++ {
		totalSubmitTasks += getSumOfSubmitTasks(this.taskQueueMetrics[i])
		totalExecuteTasks += getSumOfExecuteTasks(this.taskQueueMetrics[i])
		totalFailedExecuteTasks += getFailedOfExecuteTasks(this.taskQueueMetrics[i])
	}

	return &TaskQueueMetricsAllWorker{
		totalSubmitTasks:        totalSubmitTasks,
		totalExecuteTasks:       totalExecuteTasks,
		totalFailedExecuteTasks: totalFailedExecuteTasks,
	}
}

func executeTask(taskChannel chan func(), taskQueueCounter chan int32, metrics *TaskQueueMetricsSingleWorker) {
	go func() {
		var task func()
		var ok bool
		for {
			task, ok = <-taskChannel
			if !ok {
				executeTasksFailedCounterOperation(metrics)
				taskQueueCounter <- getNumOfTasksInQueue(metrics)
				break
			}
			if task != nil {
				// Execute the task.
				task()
			}
			executeTasksCounterOperation(metrics)
			taskQueueCounter <- getNumOfTasksInQueue(metrics)
		}
	}()
}

func getNumOfTasksInQueue(metrics *TaskQueueMetricsSingleWorker) int32 {
	return int32(atomic.LoadInt32((*int32)(&metrics.numOfTasksInQueue)))
}

func getSumOfSubmitTasks(metrics *TaskQueueMetricsSingleWorker) uint64 {
	return uint64(atomic.LoadUint64((*uint64)(&metrics.sumOfSubmitTasks)))
}
func getSumOfExecuteTasks(metrics *TaskQueueMetricsSingleWorker) uint64 {
	return uint64(atomic.LoadUint64((*uint64)(&metrics.sumOfExecuteTasks)))
}
func getFailedOfExecuteTasks(metrics *TaskQueueMetricsSingleWorker) uint64 {
	return uint64(atomic.LoadUint64((*uint64)(&metrics.failedOfExecuteTasks)))
}

func submitTasksCounterOperation(metrics *TaskQueueMetricsSingleWorker) {
	atomic.AddUint64(&metrics.sumOfSubmitTasks, 1)
	atomic.AddInt32(&metrics.numOfTasksInQueue, 1)
}

func executeTasksCounterOperation(metrics *TaskQueueMetricsSingleWorker) {
	atomic.AddUint64(&metrics.sumOfExecuteTasks, 1)
	atomic.AddInt32(&metrics.numOfTasksInQueue, -1)
}

func executeTasksFailedCounterOperation(metrics *TaskQueueMetricsSingleWorker) {
	atomic.AddUint64(&metrics.failedOfExecuteTasks, 1)
	atomic.AddInt32(&metrics.numOfTasksInQueue, -1)
}

func executeTaskQueueCounterStat(taskQueueCounter chan int32, taskQueueWorkerStatusStats *tool.Stats) {
	go func() {
		for {
			nTasksInQueue, ok := <-taskQueueCounter
			if !ok {
				break
			}
			taskQueueWorkerStatusStats.Update(float64(nTasksInQueue))
		}
	}()
}

func (this *FixedWorkPool) getTaskQueueWorkerStatusStatInfo(label string) {
	for _, d := range this.taskQueueWorkerStatusStatsList {
		this.taskQueueStatsForWorkerStatusStats.statSize.Update(float64(d.Size()))
		this.taskQueueStatsForWorkerStatusStats.statMin.Update(d.Min())
		this.taskQueueStatsForWorkerStatusStats.statMax.Update(d.Max())
		this.taskQueueStatsForWorkerStatusStats.statMean.Update(d.Mean())
		this.taskQueueStatsForWorkerStatusStats.statPopulationVariance.Update(d.PopulationVariance())
		this.taskQueueStatsForWorkerStatusStats.statSampleVariance.Update(d.SampleVariance())
		this.taskQueueStatsForWorkerStatusStats.statPopulationStandardDeviation.Update(d.PopulationStandardDeviation())
		this.taskQueueStatsForWorkerStatusStats.statSampleStandardDeviation.Update(d.SampleStandardDeviation())
		this.taskQueueStatsForWorkerStatusStats.statPopulationSkew.Update(d.PopulationSkew())
		this.taskQueueStatsForWorkerStatusStats.statSampleSkew.Update(d.SampleSkew())
		this.taskQueueStatsForWorkerStatusStats.statPopulationKurtosis.Update(d.PopulationKurtosis())
		this.taskQueueStatsForWorkerStatusStats.statSampleKurtosis.Update(d.SampleKurtosis())
		d.Reset()
	}
	this.taskQueueStatsForWorkerStatusStatsJson <- this.getTaskQueueStatsForWorkerStatusStatsJson(label)
}

func (this *FixedWorkPool) GetTaskQueueStatsForWorkerStatusStatsJson() *chan string {
	return &this.taskQueueStatsForWorkerStatusStatsJson
}

func (this *FixedWorkPool) ReportTaskQueueWorkerStatusStatInfoWithJson() string {
	info := "{"
	for idx, d := range this.taskQueueWorkerStatusStatsList {
		if idx > 0 {
			info += ","
		}
		info += d.ReportStatsInfoWithJson(fmt.Sprintf("Worker_%d", idx))
	}
	info += "}"
	return info
}

func (this *FixedWorkPool) getTaskQueueStatsForWorkerStatusStatsJson(label string) string {
	info := fmt.Sprintf("{\"%s\":{", label)
	info += this.taskQueueStatsForWorkerStatusStats.statSize.ReportStatsInfoWithJson("Stats Size")
	info += "," + this.taskQueueStatsForWorkerStatusStats.statMin.ReportStatsInfoWithJson("Stats Min")
	info += "," + this.taskQueueStatsForWorkerStatusStats.statMean.ReportStatsInfoWithJson("Stats Mean")
	info += "," + this.taskQueueStatsForWorkerStatusStats.statMax.ReportStatsInfoWithJson("Stats Max")
	info += "," + this.taskQueueStatsForWorkerStatusStats.statPopulationVariance.ReportStatsInfoWithJson("Stats PopulationVariance")
	info += "," + this.taskQueueStatsForWorkerStatusStats.statSampleVariance.ReportStatsInfoWithJson("Stats SampleVariance")
	info += "," + this.taskQueueStatsForWorkerStatusStats.statPopulationStandardDeviation.ReportStatsInfoWithJson("Stats PopulationStandardDeviation")
	info += "," + this.taskQueueStatsForWorkerStatusStats.statSampleStandardDeviation.ReportStatsInfoWithJson("Stats SampleStandardDeviation")
	info += "," + this.taskQueueStatsForWorkerStatusStats.statPopulationSkew.ReportStatsInfoWithJson("Stats PopulationSkew")
	info += "," + this.taskQueueStatsForWorkerStatusStats.statSampleSkew.ReportStatsInfoWithJson("Stats SampleSkew")
	info += "," + this.taskQueueStatsForWorkerStatusStats.statPopulationKurtosis.ReportStatsInfoWithJson("Stats PopulationKurtosis")
	info += "," + this.taskQueueStatsForWorkerStatusStats.statSampleKurtosis.ReportStatsInfoWithJson("Stats SampleKurtosis")
	info += "}}"
	return info
}

func (this *FixedWorkPool) ReportStreamTopK() (string, string, string, string) {
	//Last Minute,Last Hour,Last Day,all
	m := this.streamOfTopKTasks.minuteStreamOfTopKTasks.ReportTopKInfoByJson()
	h := this.streamOfTopKTasks.hourStreamOfTopKTasks.ReportTopKInfoByJson()
	d := this.streamOfTopKTasks.dayStreamOfTopKTasks.ReportTopKInfoByJson()
	a := this.streamOfTopKTasks.allStreamOfTopKTasks.ReportTopKInfoByJson()
	return m, h, d, a
}

func (this *FixedWorkPool) getTaskQueueStatsInfoOfWorkerStatusWithInterval() {
	timeSticker := time.NewTicker(time.Second * 300)
	for {
		select {
		case <-timeSticker.C:
			this.getTaskQueueWorkerStatusStatInfo("300s Interval")
		}
	}
}

func (this *FixedWorkPool) processSteamOfTopKTasksWithType(pType int8) {
	switch pType {
	case MinuteKOfStreamTaskTopType:
		this.streamOfTopKTasks.mu.Lock()
		sm := this.streamOfTopKTasks.minuteStreamOfTopKTasks.Query()
		this.streamOfTopKTasks.hourStreamOfTopKTasks.SetNewElementBeforeMerge(sm)
		this.streamOfTopKTasks.hourStreamOfTopKTasks.Merge(sm)
		this.streamOfTopKTasks.minuteStreamOfTopKTasks.Reset()
		this.streamOfTopKTasks.mu.Unlock()
	case HourKOfStreamTaskTopType:
		this.streamOfTopKTasks.mu.Lock()
		sm := this.streamOfTopKTasks.hourStreamOfTopKTasks.Query()
		this.streamOfTopKTasks.dayStreamOfTopKTasks.SetNewElementBeforeMerge(sm)
		this.streamOfTopKTasks.dayStreamOfTopKTasks.Merge(sm)
		this.streamOfTopKTasks.hourStreamOfTopKTasks.Reset()
		this.streamOfTopKTasks.mu.Unlock()
	case DayKOfStreamTaskTopType:
		this.streamOfTopKTasks.mu.Lock()
		sm := this.streamOfTopKTasks.dayStreamOfTopKTasks.Query()
		this.streamOfTopKTasks.allStreamOfTopKTasks.SetNewElementBeforeMerge(sm)
		this.streamOfTopKTasks.allStreamOfTopKTasks.Merge(sm)
		this.streamOfTopKTasks.dayStreamOfTopKTasks.Reset()
		this.streamOfTopKTasks.mu.Unlock()
	default:
		return
	}
}

func (this *FixedWorkPool) getStreamTopKWithInterval() {
	go func() {
		minuteTimeSticker := time.NewTicker(time.Minute)
		for {
			select {
			case <-minuteTimeSticker.C:
				this.processSteamOfTopKTasksWithType(MinuteKOfStreamTaskTopType)
			}
		}
	}()
	go func() {
		hourTimeSticker := time.NewTicker(time.Hour)
		for {
			select {
			case <-hourTimeSticker.C:
				this.processSteamOfTopKTasksWithType(HourKOfStreamTaskTopType)
			}
		}
	}()
	go func() {
		dayTimeSticker := time.NewTicker(time.Hour * 24)
		for {
			select {
			case <-dayTimeSticker.C:
				this.processSteamOfTopKTasksWithType(DayKOfStreamTaskTopType)
			}
		}
	}()
}
