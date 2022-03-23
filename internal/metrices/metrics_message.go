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
	"fmt"
	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/pkg/metrices"
	xstream "github.com/tkeel-io/rule-util/stream"
	"strings"
	"sync"
	"time"
)

var (
	version      = "mdmpv1"
	metricUser   = "system"
	metricDevice = "mdmp-metric"
	countFactor  *metrics.CounterVec
	once         = sync.Once{}
	metricSink   xstream.Sink
	err          error
)

func InitMetrics(sink xstream.Sink) {
	if sink == nil {
		log.Fatal("metric sink nil error")
	}
	metricSink = sink

	once.Do(initMetrics)
}

func initMetrics() {
	if countFactor != nil {
		return
	}
	stepSec := 20
	ctx := context.Background()
	countFactor = metrics.NewCounterVec(
		metrics.ReportStep(stepSec),
		metrics.ReportFunc(func(k string, v interface{}) {
			if v == nil {
				return
			}
			arr := strings.Split(k, "|")
			if len(arr) != 4 && arr[0] != version {
				log.Error("data marshal error",
					logf.String("key", k),
					logf.Error(err))
			}
			data := metrics.IoTMetricValue{}
			data.UserID = arr[1]
			data.DeviceID = arr[2]
			data.Action = arr[3]
			data.Type = "GAUGE"
			data.Step = int64(stepSec)
			data.Source = "message"
			data.Value = v
			data.Timestamp = time.Now().UnixNano() / 1000 / 1000

			//log.Info("metrics", logf.Any("data", data))
			event, err := metrics.NewMessage(data)
			event.SetDomain(metricUser)
			event.SetEntity(metricDevice)
			event.SetTopic(fmt.Sprintf("/sys/metrics/mdmp-message/%s/%s/%s", data.UserID, data.DeviceID, data.Action))
			if err != nil {
				log.Error("data marshal error",
					logf.Error(err))
			}

			log.Debug("send metric", logf.Any("event", event))

			err = metricSink.Send(ctx, event)
			if err != nil {
				log.Error("send metric error",
					logf.Error(err))
			}
		}),
	)
	go func() {
		countFactor.Run(ctx)
	}()
	log.Info("metrics start",
		logf.Any("ReportStep(s)", stepSec))
	return
}

func MetricName(userID, key, MetricType string) string {
	return fmt.Sprintf("%s|%s|%s|%s", version, userID, key, MetricType)
}

func isInternalMetric(metric string) bool {
	return (-1 != strings.Index(metric, fmt.Sprintf("|%s|%s|", metricUser, metricDevice)))
}

func Inc(metric string) {
	if metric != "" && !isInternalMetric(metric) {
		countFactor.GetMetricWith(metric).Inc(1)
	}
}

func Dec(metric string) {
	if metric != "" && !isInternalMetric(metric) {
		countFactor.GetMetricWith(metric).Dec(1)
	}
}
