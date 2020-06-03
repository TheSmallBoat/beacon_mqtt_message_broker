package pool

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	step              = 8
	maxRegularWorkers = uint16(10)
	maxSpecialWorkers = uint16(20)
	maxSupremeWorkers = uint16(10)
)

var sum int32
var wg sync.WaitGroup

func myFunc() {
	n := int32(step)
	atomic.AddInt32(&sum, n)
	time.Sleep(1 * time.Microsecond)
	wg.Done()
}

func myFuncSleepMillisecond() {
	n := int32(1)
	atomic.AddInt32(&sum, n)
	time.Sleep(1 * time.Millisecond)
	wg.Done()
}

var mfwps = NewMultiFixedWorkPools(maxRegularWorkers, maxSpecialWorkers, maxSupremeWorkers)

func TestMultiFixedWorkerPools(t *testing.T) {
	if mfwps.MaxRegularWorkers() != maxRegularWorkers {
		t.Fatalf("Max Regular workers error")
	}
	if mfwps.MaxSpecialWorkers() != maxSpecialWorkers {
		t.Fatalf("Max Special workers error")
	}
	if mfwps.MaxSupremeWorkers() != maxSupremeWorkers {
		t.Fatalf("Max Supreme workers error")
	}
	if mfwps.MaxWorkers(RegularType) != maxRegularWorkers {
		t.Fatalf("Max Regular[type] workers error")
	}
	if mfwps.MaxWorkers(SpecialType) != maxSpecialWorkers {
		t.Fatalf("Max Special[type] workers error")
	}
	if mfwps.MaxWorkers(SupremeType) != maxSupremeWorkers {
		t.Fatalf("Max Supreme[type] workers error")
	}

	if mfwps.RegularFixedWorkPool.maxWorkers != uint16(len(mfwps.RegularFixedWorkPool.taskQueueMetrics)) {
		t.Fatalf(" Regular pool workers number not match the size of taskQueueMetrics")
	}

	if mfwps.SpecialFixedWorkPool.maxWorkers != uint16(len(mfwps.SpecialFixedWorkPool.taskQueueMetrics)) {
		t.Fatalf(" Special pool workers number not match the size of taskQueueMetrics")
	}

	if mfwps.SupremeFixedWorkPool.maxWorkers != uint16(len(mfwps.SupremeFixedWorkPool.taskQueueMetrics)) {
		t.Fatalf(" Supreme pool workers number not match the size of taskQueueMetrics")
	}

	t.Logf("Regular workers:[%d]=>Type[%d], Special workers:[%d]=>Type[%d], Supreme workers:[%d]=>Type[%d]. \n", maxRegularWorkers, RegularType, maxSpecialWorkers, SpecialType, maxSupremeWorkers, SupremeType)

	for i := 0; i < 1024; i++ {
		wg.Add(1)
		time.Sleep(1 * time.Nanosecond)
		mfwps.SubmitRegularTask(fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/10), myFunc)
		wg.Add(1)
		time.Sleep(1 * time.Nanosecond)
		mfwps.SubmitSpecialTask(fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/10), myFunc)
		wg.Add(1)
		time.Sleep(1 * time.Nanosecond)
		mfwps.SubmitSupremeTask(fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/10), myFunc)
	}
	wg.Wait()
	t.Logf("sum=[%d],step=[%d]. \n", sum, step)
	t.Logf("finish section one tasks. \n")

	for j := 0; j < 1024; j++ {
		wg.Add(1)
		time.Sleep(2 * time.Nanosecond)
		mfwps.SubmitTaskWithType(RegularType, fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/10), myFunc)
		wg.Add(1)
		time.Sleep(2 * time.Nanosecond)
		mfwps.SubmitTaskWithType(SpecialType, fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/10), myFunc)
		wg.Add(1)
		time.Sleep(2 * time.Nanosecond)
		mfwps.SubmitTaskWithType(SupremeType, fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/10), myFunc)
	}
	wg.Wait()
	t.Logf("sum=[%d],step=[%d]. \n", sum, step)
	t.Logf("finish section two tasks. \n")

	t.Logf("Regular => %s", mfwps.RegularFixedWorkPool.ReportTaskQueueMetricsWithString())
	t.Logf("Special => %s", mfwps.SpecialFixedWorkPool.ReportTaskQueueMetricsWithString())
	t.Logf("Supreme => %s", mfwps.SupremeFixedWorkPool.ReportTaskQueueMetricsWithString())

}

func BenchmarkMultiFixedWorkPools(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		mfwps.SubmitRegularTask(fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/10), myFuncSleepMillisecond)
	}
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		mfwps.SubmitSpecialTask(fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/10), myFuncSleepMillisecond)
	}
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		mfwps.SubmitSupremeTask(fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/10), myFuncSleepMillisecond)
	}
	wg.Wait()
}

func BenchmarkMultiFixedWorkPoolsWithType(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		mfwps.SubmitTaskWithType(RegularType, fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/10), myFuncSleepMillisecond)
	}
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		mfwps.SubmitTaskWithType(SpecialType, fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/10), myFuncSleepMillisecond)
	}
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		mfwps.SubmitTaskWithType(SupremeType, fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/10), myFuncSleepMillisecond)
	}
	wg.Wait()
}

func BenchmarkMultiFixedWorkPoolsForRegularType(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		mfwps.SubmitRegularTask(fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/10), myFunc)
		wg.Add(1)
		mfwps.SubmitTaskWithType(RegularType, fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/10), myFunc)
	}
	wg.Wait()
}
func BenchmarkMultiFixedWorkPoolsForSpecialType(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		mfwps.SubmitSpecialTask(fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/10), myFunc)
		wg.Add(1)
		mfwps.SubmitTaskWithType(SpecialType, fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/10), myFunc)
	}
	wg.Wait()
}
func BenchmarkMultiFixedWorkPoolsForSupremeType(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		mfwps.SubmitSupremeTask(fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/10), myFunc)
		wg.Add(1)
		mfwps.SubmitTaskWithType(SupremeType, fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/10), myFunc)
	}
	wg.Wait()
}

func TestMultiFixedWorkerPoolsWithMetrics(t *testing.T) {
	var mfwpsp = NewMultiFixedWorkPools(3, 5, 7)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		mfwpsp.SubmitRegularTask(fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/100000), myFuncSleepMillisecond)
		wg.Add(1)
		mfwpsp.SubmitSpecialTask(fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/100000), myFuncSleepMillisecond)
		wg.Add(1)
		mfwpsp.SubmitSupremeTask(fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/100000), myFuncSleepMillisecond)
	}
	wg.Wait()

	for i := 0; i < 3; i++ {
		wg.Add(1)
		mfwpsp.SubmitRegularTask(fmt.Sprintf("uuid_%d", i), myFuncSleepMillisecond)
		t.Logf("Regular [%d] => %s", i, mfwpsp.RegularFixedWorkPool.ReportTaskQueueMetricsByWorkerWithString())
	}
	wg.Wait()

	for i := 0; i < 3; i++ {
		wg.Add(1)
		mfwpsp.SubmitSpecialTask(fmt.Sprintf("uuid_%d", i), myFuncSleepMillisecond)
		t.Logf("Special [%d] => %s", i, mfwpsp.SpecialFixedWorkPool.ReportTaskQueueMetricsByWorkerWithString())
	}
	wg.Wait()

	for i := 0; i < 3; i++ {
		wg.Add(1)
		mfwpsp.SubmitSupremeTask(fmt.Sprintf("uuid_%d", i), myFuncSleepMillisecond)
		t.Logf("Supreme [%d] => %s", i, mfwpsp.SupremeFixedWorkPool.ReportTaskQueueMetricsByWorkerWithString())
	}
	wg.Wait()

	t.Logf("Regular => %s", mfwpsp.RegularFixedWorkPool.ReportTaskQueueMetricsByWorkerWithString())
	t.Logf("Special => %s", mfwpsp.SpecialFixedWorkPool.ReportTaskQueueMetricsByWorkerWithString())
	t.Logf("Supreme => %s", mfwpsp.SupremeFixedWorkPool.ReportTaskQueueMetricsByWorkerWithString())

	t.Logf("Regular => %s", mfwpsp.RegularFixedWorkPool.ReportTaskQueueMetricsWithString())
	t.Logf("Special => %s", mfwpsp.SpecialFixedWorkPool.ReportTaskQueueMetricsWithString())
	t.Logf("Supreme => %s", mfwpsp.SupremeFixedWorkPool.ReportTaskQueueMetricsWithString())

	t.Logf("Regular => %s", mfwpsp.RegularFixedWorkPool.ReportStreamTopK())
	t.Logf("Special => %s", mfwpsp.SpecialFixedWorkPool.ReportStreamTopK())
	t.Logf("Supreme => %s", mfwpsp.SupremeFixedWorkPool.ReportStreamTopK())
}
