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
	gometrics "github.com/rcrowley/go-metrics"
)

type (
	Registry     gometrics.Registry
	Counter      gometrics.Counter
	Gauge        gometrics.Gauge
	GaugeFloat64 gometrics.GaugeFloat64
	Healthcheck  gometrics.Healthcheck
	Histogram    gometrics.Histogram
	Meter        gometrics.Meter
	Timer        gometrics.Timer
	Logger       gometrics.Logger
)

/*
1	消息上行	pub	当前用户ID	设备ID
2	消息下行	sub	当前用户ID	设备ID
3	通过API下行	api	当前用户ID	设备ID
4	规则引擎	rule	当前用户ID	规则ID
5	规则引擎-处理成功	rule_ok	当前用户ID	规则ID
6	规则引擎-处理失败	rule_fail	当前用户ID	规则ID
7	规则引擎-动作处理	rule_action	当前用户ID	规则ID
8	规则引擎-动作处理-成功	rule_action_ok	当前用户ID	规则ID
9	规则引擎-动作处理-失败	rule_action_fail	当前用户ID	规则ID
*/
const (
	MtcMessageUp      = "pub"
	MtcMessageDown    = "sub"
	MtcMessageAPIDown = "api"
	MtcRouteMessage   = "route"
	MtcRule           = "rule"
	MtcRuleInvoke     = "rule_invoke"
	MtcRuleOK         = "rule_ok"
	MtcRuleFAIL       = "rule_fail"
	MtcRuleAction     = "rule_action"
	MtcRuleActionOK   = "rule_action_ok"
	MtcRuleActionFAIL = "rule_action_fail"
)
