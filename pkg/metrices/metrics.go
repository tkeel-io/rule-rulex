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
	"context"
	"encoding/json"
	"fmt"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/rcrowley/go-metrics"
	"sync"
	"time"

	xstream "github.com/tkeel-io/rule-util/stream"
)

type VecOpts struct {
	report func(k string, v interface{})
	step   int
}

type VecOptFn func(*VecOpts)

func ReportStep(stepSec int) VecOptFn {
	return func(opts *VecOpts) {
		opts.step = stepSec
	}
}

func ReportFunc(fn func(k string, v interface{})) VecOptFn {
	return func(opts *VecOpts) {
		opts.report = fn
	}
}

type CounterVec struct {
	rlock      sync.RWMutex
	metricVec  sync.Map
	reportFunc func(k string, v interface{})
	stepSec    int
}

func NewCounterVec(optFuns ...VecOptFn) *CounterVec {
	opts := &VecOpts{
		report: func(k string, v interface{}) {
			fmt.Println("Report", k, v)
		},
		step: 10,
	}
	for _, fn := range optFuns {
		fn(opts)
	}
	return &CounterVec{
		reportFunc: opts.report,
		stepSec:    opts.step,
	}
}

func (v *CounterVec) Run(ctx context.Context) {
	v.report(ctx, v.reportFunc, v.stepSec)
}

func (v *CounterVec) GetMetricWith(key interface{}) (metrics.Counter) {
	if val, ok := v.metricVec.Load(key); ok {
		return val.(metrics.Counter)
	}
	v.rlock.Lock()
	metric := metrics.NewCounter()
	v.metricVec.Store(key, metric)
	v.rlock.Unlock()
	return metric
}

func (v *CounterVec) report(ctx context.Context, fn func(k string, v interface{}), stepSec int) {
	for _ = range time.Tick(time.Duration(stepSec)*time.Second) {
		v.metricVec.Range(func(key, val interface{}) bool {
			counter := val.(metrics.Counter)
			k, cnt := key.(string), counter.Count()
			if cnt <= 0 {
				return true
			}
			counter.Clear()
			log.Debug("Report",
				logf.String("metrics", k),
				logf.Int64("cnt", cnt),
				logf.Int64("counter.Count", counter.Count()))
			fn(k, cnt)
			return true
		})
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}
func NewMessage(data interface{}) (xstream.Message, error) {
	byt, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	event := xstream.NewMessage()
	event.SetMethod("Publish")
	event.SetTopic("/sys/metrics/mdmp-message")
	event.SetData(byt)
	return event, nil
}
