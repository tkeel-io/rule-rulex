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

package rule

import (
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/opentracing/opentracing-go"

	_ "github.com/tkeel-io/rule-rulex/pkg/sink/republish"
	_ "github.com/tkeel-io/rule-rulex/pkg/sink/stream"
)

//Option
type Option struct {
	Tracer          opentracing.Tracer
	Logger          log.Factory
	ResourceManager types.ResourceManager
}

type Factor struct {
	cli *cli
}

type cli struct {
	tracer          opentracing.Tracer
	logger          log.Factory
	resourceManager types.ResourceManager
}

type ErrorMessage struct {
	RuleName              string           `json:"ruleName,omitempty"`
	Topic                 string           `json:"topic,omitempty"`
	DeviceId              string           `json:"deviceId,omitempty"`
	MessageId             string           `json:"messageId,omitempty"`
	Base64OriginalPayload []byte           `json:"base64OriginalPayload,omitempty"`
	Failures              []FailureMessage `json:"failures,omitempty"`
}

type FailureMessage struct {
	ActionId     string `json:"actionId,omitempty"`
	ActionType   string `json:"actionType,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}
