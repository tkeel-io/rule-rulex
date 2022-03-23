/*
 * Copyright (C) 2019 Yunify, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this work except in compliance with the License.
 * You may obtain a copy of the License in the LICENSE file, or at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package metrics

import (
	"fmt"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	gometrics "github.com/rcrowley/go-metrics"
	"go.uber.org/zap/zapcore"
	"time"
)

func Register(name string, i interface{}) error {
	return gometrics.Register(name, i)
}

// NewCounter constructs a new StandardCounter.
func NewCounter() Counter {
	return gometrics.NewCounter()
}

// NewCounter constructs a new StandardCounter.
func NewTimer() Timer {
	return gometrics.NewTimer()
}

// NewCounter constructs a new StandardCounter.
func NewGauge() Gauge {
	return gometrics.NewGauge()
}

// NewCounter constructs a new StandardCounter.
func NewGaugeFloat64() GaugeFloat64 {
	return gometrics.NewGaugeFloat64()
}

// NewCounter constructs a new StandardCounter.
func NewMeter() Meter {
	return gometrics.NewMeter()
}

type PrintFun func(msg string, fields ...zapcore.Field)

func Log(freq time.Duration, fn PrintFun) {
	LogScaled(gometrics.DefaultRegistry, freq, time.Nanosecond*1000, fn)
}

// Output each metric in the given registry periodically using the given
// logger. Print timings in `scale` units (eg time.Millisecond) rather than nanos.
func LogScaled(r Registry, freq time.Duration, scale time.Duration, fn PrintFun) {
	du := float64(scale)
	duSuffix := scale.String()[1:]

	for _ = range time.Tick(freq) {
		r.Each(func(name string, i interface{}) {
			switch metric := i.(type) {
			case Counter:
				fn(fmt.Sprintf("counter %s(%9d)\n", name, metric.Count()))
			case Gauge:
				fn(fmt.Sprintf("gauge %s\n", name),
					logf.Any("value", metric.Value()))
			case GaugeFloat64:
				fn(fmt.Sprintf("gauge %s\n", name),
					logf.Any("value", metric.Value()))
			case Healthcheck:
				metric.Check()
				fn(fmt.Sprintf("healthcheck %s\n", name),
					logf.Any("error", metric.Error()))
			case Histogram:
				h := metric.Snapshot()
				ps := h.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
				fn(fmt.Sprintf("histogram %s\n", name),
					logf.Any("count", fmt.Sprintf("count=%9d\n,min=%6.2f%s,max=%6.2f%s,mean=%6.2f%s",
						h.Count(),
						float64(h.Min())/du, duSuffix,
						float64(h.Max())/du, duSuffix,
						float64(h.Mean())/du, duSuffix)),
					logf.Any("count", fmt.Sprintf("median=%6.2f%s,75=%6.2f%s,95=%6.2f%s,99=%6.2f%s,99=%99.9f%s",
						ps[0]/du, duSuffix,
						ps[1]/du, duSuffix,
						ps[2]/du, duSuffix,
						ps[3]/du, duSuffix,
						ps[4]/du, duSuffix)),
				)
			case Meter:
				m := metric.Snapshot()
				fn(fmt.Sprintf("meter %s\n", name),
					logf.Any("count", m.Count()),
					logf.Any("rate_1-min", m.Rate1()),
					logf.Any("rate_5", m.Rate5()),
					logf.Any("rate_15", m.Rate15()),
					logf.Any("mean_rate", m.RateMean()))
			case Timer:
				t := metric.Snapshot()
				ps := t.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
				//2019/06/28 16:28:16 mean: 321601 ns, median: 301893 ns, max: 20336300 ns, min: 198623 ns, p99.9: 2450477 ns
				//2019/06/28 16:28:16 mean: 0 ms, median: 0 ms, max: 20 ms, min: 0 ms, p99: 2 ms
				fn(fmt.Sprintf("timer %s\n", name),
					logf.Any("count", t.Count()),
					logf.Any("rate_1-min", t.Rate1()),
					logf.Any("rate_5", t.Rate5()),
					logf.Any("rate_15", t.Rate15()),
					logf.Any("mean_rate", t.RateMean()),
					logf.Any("min", fmt.Sprintf("%8.2f%s", float64(t.Min())/du, duSuffix)),
					logf.Any("max", fmt.Sprintf("%8.2f%s", float64(t.Max())/du, duSuffix)),
					logf.Any("mean", fmt.Sprintf("%8.2f%s", float64(t.Mean())/du, duSuffix)),
					logf.Any("median", fmt.Sprintf("%8.2f%s", ps[0]/du, duSuffix)),
					logf.Any("75", fmt.Sprintf("%8.2f%s", ps[1]/du, duSuffix)),
					logf.Any("95", fmt.Sprintf("%8.2f%s", ps[2]/du, duSuffix)),
					logf.Any("99", fmt.Sprintf("%8.2f%s", ps[3]/du, duSuffix)),
					logf.Any("99.9", fmt.Sprintf("%8.2f%s", ps[4]/du, duSuffix)), )
			}
		})
	}
}
