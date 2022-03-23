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

package rulex

type Status string

const (
	RuleStart            = "RuleStart"
	RuleStartError       = "RuleStartError"
	RuleActionStart      = "RuleActionStart"
	RuleActionStartError = "RuleActionStartError"
	RuleStarted          = "RuleStarted"
	RuleStoped           = "RuleStoped"
	RuleActionError      = "RuleActionError"
	RuleActionFail       = "RuleActionFail"
)

type RulexStatus struct {
	UserId      string `json:"user_id"`
	RulexId     string `json:"rulex_id"`
	Status      Status `json:"status"`
	RuleId      string `json:"rule_id"`
	ActionId    string `json:"action_id"`
	Desc        string `json:"desc"`
	Error       string `json:"error"`
	Timestamp   int64  `json:"time_stamp"`
	RefreshTime int64  `json:"refresh_time"`
}
