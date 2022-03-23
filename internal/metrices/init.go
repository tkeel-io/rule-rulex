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
	"github.com/tkeel-io/rule-rulex/internal/utils"
	gometrics "github.com/rcrowley/go-metrics"
	"time"
)

var (
	RuleMetrics = gometrics.NewMeter()
	MsgMetrics = gometrics.NewMeter()
)

func init() {
	Register("[rule metrices]", RuleMetrics)
	Register("[msg metrices]", MsgMetrics)
	go Log(10*time.Second, utils.Log.Bg().Info)
}
