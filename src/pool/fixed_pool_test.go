package pool

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestFixedWorkerPoolsWithMetrics(t *testing.T) {
	fwp := NewFixedWorkPool(5)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		fwp.SubmitTask(fmt.Sprintf("uuid_%d", time.Now().Second()), myFuncSleepMillisecond)
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		fwp.SubmitTask(fmt.Sprintf("uuid_%d", i), myFuncSleepMillisecond)
		t.Logf("[%d] => %s", i, fwp.ReportTaskQueueMetricsByWorkerWithString())
	}
	wg.Wait()

	t.Logf("%s", fwp.ReportTaskQueueMetricsByWorkerWithString())
	t.Logf(" %s", fwp.ReportTaskQueueMetricsWithString())

	for i := 0; i < 100; i++ {
		wg.Add(1)
		fwp.SubmitTask(fmt.Sprintf("uuid_%d", i%3), myFuncSleepMillisecond)
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		fwp.SubmitTask(fmt.Sprintf("uuid_%d", i), myFuncSleepMillisecond)
		if i%4 == 0 {
			wg.Wait()
		}
		t.Logf("[%d] => %s", i, fwp.ReportTaskQueueMetricsByWorkerWithString())
		t.Logf("[%d] => %s", i, fwp.ReportTaskQueueMetricsByWorkerWithJson())
	}
	wg.Wait()

	t.Logf("%s", fwp.ReportTaskQueueMetricsByWorkerWithString())
	t.Logf("%s", fwp.ReportTaskQueueMetricsByWorkerWithJson())
	t.Logf(" %s", fwp.ReportTaskQueueMetricsWithString())
	t.Logf(" %s", fwp.ReportTaskQueueMetricsWithJson())

	t.Log(fwp.ReportStreamTopK())
}

func TestFixedWorkerPoolsGetTaskQueueWorkerStatusStatInfo(t *testing.T) {
	fwp := NewFixedWorkPool(20)
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		fwp.SubmitTask(fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/100000), myFuncSleepMillisecond)
		wg.Add(1)
		fwp.SubmitTask(fmt.Sprintf("uuid_%d", int8(i%5)), myFuncSleepMillisecond)
	}
	wg.Wait()

	for idx, d := range fwp.taskQueueWorkerStatusStatsList {
		t.Log(d.ReportStatsInfo(fmt.Sprintf("Stats for Worker %d", idx)))
	}
	t.Log(fwp.ReportTaskQueueWorkerStatusStatInfoWithJson())

	fwp.getTaskQueueWorkerStatusStatInfo("600s Interval")
	t.Log(<-fwp.taskQueueStatsForWorkerStatusStatsJson)
	/*
		t.Log(fwp.taskQueueStatsForWorkerStatusStats.statSize.ReportStatsInfo("Stats For Size"))
		t.Log(fwp.taskQueueStatsForWorkerStatusStats.statMin.ReportStatsInfo("Stats For Min"))
		t.Log(fwp.taskQueueStatsForWorkerStatusStats.statMean.ReportStatsInfo("Stats For Mean"))
		t.Log(fwp.taskQueueStatsForWorkerStatusStats.statMax.ReportStatsInfo("Stats For Max"))

		t.Log(fwp.taskQueueStatsForWorkerStatusStats.statPopulationVariance.ReportStatsInfo("Stats For PopulationVariance"))
		t.Log(fwp.taskQueueStatsForWorkerStatusStats.statSampleVariance.ReportStatsInfo("Stats For SampleVariance"))
		t.Log(fwp.taskQueueStatsForWorkerStatusStats.statPopulationStandardDeviation.ReportStatsInfo("Stats For PopulationStandardDeviation"))
		t.Log(fwp.taskQueueStatsForWorkerStatusStats.statSampleStandardDeviation.ReportStatsInfo("Stats For SampleStandardDeviation"))

		t.Log(fwp.taskQueueStatsForWorkerStatusStats.statPopulationSkew.ReportStatsInfo("Stats For PopulationSkew"))
		t.Log(fwp.taskQueueStatsForWorkerStatusStats.statSampleSkew.ReportStatsInfo("Stats For SampleSkew"))
		t.Log(fwp.taskQueueStatsForWorkerStatusStats.statPopulationKurtosis.ReportStatsInfo("Stats For PopulationKurtosis"))
		t.Log(fwp.taskQueueStatsForWorkerStatusStats.statSampleKurtosis.ReportStatsInfo("Stats For SampleKurtosis"))
	*/
	t.Log(fwp.ReportTaskQueueMetricsByWorkerWithString())
	t.Log(fwp.ReportTaskQueueMetricsWithString())

	t.Log(fwp.ReportStreamTopK())
}

func TestFixedWorkerPoolsGetTopKTaskSubmitWithUid(t *testing.T) {
	fwp := NewFixedWorkPool(10)
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		fwp.SubmitTask(fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/100000), myFuncSleepMillisecond)
	}
	wg.Wait()
	t.Log(fwp.ReportStreamTopK())

	fwp.processSteamOfTopKTasksWithType(MinuteKOfStreamTaskTopType)
	t.Log(fwp.ReportStreamTopK())
	//fwp.processSteamOfTopKTasksWithType(HourKOfStreamTaskTopType)
	//t.Log(fwp.getStreamTopK())
	//fwp.processSteamOfTopKTasksWithType(DayKOfStreamTaskTopType)
	//t.Log(fwp.getStreamTopK())

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		fwp.SubmitTask(fmt.Sprintf("uuid_%d", int8(rand.ExpFloat64())), myFuncSleepMillisecond)
	}
	wg.Wait()
	t.Log(fwp.ReportStreamTopK())

	fwp.processSteamOfTopKTasksWithType(MinuteKOfStreamTaskTopType)
	t.Log(fwp.ReportStreamTopK())
	fwp.processSteamOfTopKTasksWithType(HourKOfStreamTaskTopType)
	t.Log(fwp.ReportStreamTopK())
	//fwp.processSteamOfTopKTasksWithType(DayKOfStreamTaskTopType)
	//t.Log(fwp.getStreamTopK())

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		fwp.SubmitTask(fmt.Sprintf("uuid_%d", int8(rand.ExpFloat64())), myFuncSleepMillisecond)
	}
	wg.Wait()
	t.Log(fwp.ReportStreamTopK())

	fwp.processSteamOfTopKTasksWithType(MinuteKOfStreamTaskTopType)
	t.Log(fwp.ReportStreamTopK())
	fwp.processSteamOfTopKTasksWithType(HourKOfStreamTaskTopType)
	t.Log(fwp.ReportStreamTopK())
	fwp.processSteamOfTopKTasksWithType(DayKOfStreamTaskTopType)
	t.Log(fwp.ReportStreamTopK())

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		fwp.SubmitTask(fmt.Sprintf("uuid_%d", time.Now().Nanosecond()/100000), myFuncSleepMillisecond)
	}
	wg.Wait()
	fwp.processSteamOfTopKTasksWithType(MinuteKOfStreamTaskTopType)
	fwp.processSteamOfTopKTasksWithType(HourKOfStreamTaskTopType)
	fwp.processSteamOfTopKTasksWithType(DayKOfStreamTaskTopType)
	t.Log(fwp.ReportStreamTopK())

}
