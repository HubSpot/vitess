/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tabletenv

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// approxEqualNanos reports whether got is within 5% of want. Both are durations
// expressed as float64 nanoseconds, matching what recordStats observes.
func approxEqualNanos(got, want float64) bool {
	if want == 0 {
		return got == 0
	}
	diff := got - want
	if diff < 0 {
		diff = -diff
	}
	return diff/want < 0.05
}

// TestTimingStatsRecordStats feeds LogStats with known durations and asserts the
// summarized median and 99th percentile reflect them. TotalTime is derived from
// StartTime/EndTime; the other two are read straight off the LogStats fields.
func TestTimingStatsRecordStats(t *testing.T) {
	start := time.Now()
	ls := &LogStats{
		StartTime:            start,
		EndTime:              start.Add(10 * time.Millisecond),
		MysqlResponseTime:    4 * time.Millisecond,
		WaitingForConnection: 1 * time.Millisecond,
	}
	for i := 0; i < 1000; i++ {
		TimingStatistics.recordStats(ls)
	}

	m := TimingStatistics.GetMeasurements()
	cases := []struct {
		name        string
		measurement TimingMeasurement
		want        float64
	}{
		{"TotalQueryTime", m.TotalQueryTime, float64((10 * time.Millisecond).Nanoseconds())},
		{"MysqlQueryTime", m.MysqlQueryTime, float64((4 * time.Millisecond).Nanoseconds())},
		{"ConnectionAcquisitionTime", m.ConnectionAcquisitionTime, float64((1 * time.Millisecond).Nanoseconds())},
	}
	for _, c := range cases {
		if !approxEqualNanos(c.measurement.Median, c.want) {
			t.Errorf("%s.Median = %v, want ~%v", c.name, c.measurement.Median, c.want)
		}
		if !approxEqualNanos(c.measurement.NinetyNinth, c.want) {
			t.Errorf("%s.NinetyNinth = %v, want ~%v", c.name, c.measurement.NinetyNinth, c.want)
		}
	}
}

// TestTimingStatsMeasurementsJson verifies the JSON published to /debug/vars as
// "AggregateQueryTimings"; collectd parses this to emit the percentile metrics.
func TestTimingStatsMeasurementsJson(t *testing.T) {
	start := time.Now()
	TimingStatistics.recordStats(&LogStats{
		StartTime:         start,
		EndTime:           start.Add(5 * time.Millisecond),
		MysqlResponseTime: 2 * time.Millisecond,
	})

	raw := TimingStatistics.GetMeasurementsJson()
	var measurements TimingMeasurements
	if err := json.Unmarshal([]byte(raw), &measurements); err != nil {
		t.Fatalf("GetMeasurementsJson did not return valid JSON: %v\n%s", err, raw)
	}
	for _, key := range []string{"TotalQueryTime", "MysqlQueryTime", "ConnectionAcquisitionTime", "Median", "NinetyNinth"} {
		if !strings.Contains(raw, key) {
			t.Errorf("GetMeasurementsJson missing key %q in:\n%s", key, raw)
		}
	}
}
