package pool

//Three type of the fixed work pools : Regular/Special/Supreme
const (
	RegularType int8 = 1 << iota
	SpecialType
	SupremeType
)

type MultiFixedWorkPools struct {
	RegularFixedWorkPool *FixedWorkPool
	SpecialFixedWorkPool *FixedWorkPool
	SupremeFixedWorkPool *FixedWorkPool
}

func (this *MultiFixedWorkPools) MaxRegularWorkers() uint16 {
	return this.RegularFixedWorkPool.maxWorkers
}

func (this *MultiFixedWorkPools) MaxSpecialWorkers() uint16 {
	return this.SpecialFixedWorkPool.maxWorkers
}

func (this *MultiFixedWorkPools) MaxSupremeWorkers() uint16 {
	return this.SupremeFixedWorkPool.maxWorkers
}

func NewMultiFixedWorkPools(maxRegularWorkers uint16, maxSpecialWorkers uint16, maxSupremeWorkers uint16) *MultiFixedWorkPools {
	pools := &MultiFixedWorkPools{
		RegularFixedWorkPool: NewFixedWorkPool(maxRegularWorkers),
		SpecialFixedWorkPool: NewFixedWorkPool(maxSpecialWorkers),
		SupremeFixedWorkPool: NewFixedWorkPool(maxSupremeWorkers),
	}
	return pools
}

func (this *MultiFixedWorkPools) SubmitRegularTask(uid string, task func()) {
	this.RegularFixedWorkPool.SubmitTask(uid, task)
}

func (this *MultiFixedWorkPools) SubmitSpecialTask(uid string, task func()) {
	this.SpecialFixedWorkPool.SubmitTask(uid, task)
}

func (this *MultiFixedWorkPools) SubmitSupremeTask(uid string, task func()) {
	this.SupremeFixedWorkPool.SubmitTask(uid, task)
}

func (this *MultiFixedWorkPools) MaxWorkers(pType int8) uint16 {
	switch pType {
	case RegularType:
		return this.RegularFixedWorkPool.maxWorkers
	case SpecialType:
		return this.SpecialFixedWorkPool.maxWorkers
	case SupremeType:
		return this.SupremeFixedWorkPool.maxWorkers
	default:
		return 0
	}
}

func (this *MultiFixedWorkPools) ReportTaskQueueMetricsWithType(pType int8) *TaskQueueMetricsAllWorker {
	switch pType {
	case RegularType:
		return this.RegularFixedWorkPool.ReportTaskQueueMetricsAllWorker()
	case SpecialType:
		return this.SpecialFixedWorkPool.ReportTaskQueueMetricsAllWorker()
	case SupremeType:
		return this.SupremeFixedWorkPool.ReportTaskQueueMetricsAllWorker()
	default:
		return nil
	}
}

func (this *MultiFixedWorkPools) SubmitTaskWithType(pType int8, uid string, task func()) {
	switch pType {
	case RegularType:
		this.RegularFixedWorkPool.SubmitTask(uid, task)
	case SpecialType:
		this.SpecialFixedWorkPool.SubmitTask(uid, task)
	case SupremeType:
		this.SupremeFixedWorkPool.SubmitTask(uid, task)
	default:
		this.RegularFixedWorkPool.SubmitTask(uid, task)
	}
}
