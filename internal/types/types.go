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

package types

import (
	pb "github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/tracing"
	"github.com/tkeel-io/rule-util/stream"
	"reflect"
)

type TaskID int32
type MsgSlice = []stream.PublishMessage

type STATUS int

var (
	IDLE     = STATUS(0)
	STARTING = STATUS(1)
	STARTED  = STATUS(2)
	ERROR    = STATUS(-1)
)

type PubSubClient = pb.PubSubClient
type JobManagerClient = pb.JobManagerClient
type RuleActionClient = pb.RuleActionClient
type ResourcesManagerClient = pb.ResourcesManagerClient

type GrpcClient interface {
	Manager() pb.JobManagerClient
	PubSub() pb.PubSubClient
	Rule() pb.RuleActionClient
	Resource() pb.ResourcesManagerClient
}

type ServiceConfig struct {
	Client GrpcClient
	Logger log.Factory
	Tracer tracing.Tracer
}

type Type = reflect.Type

var TypeOf = reflect.TypeOf

type Message = pb.Message
type PublishMessage = pb.PublishMessage
var NewMessage= pb.NewMessage