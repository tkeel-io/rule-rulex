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
	"context"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"github.com/tkeel-io/rule-rulex/internal/utils/topic"
	"github.com/tkeel-io/rule-util/stream"

	metapb "github.com/tkeel-io/rule-util/metadata/v1"
)

type EventType int32

const (
	RuleEvent = EventType(iota)
	PubSubEvent
	RouteEvent
)

type EventActionType int32

const (
	PUT = EventActionType(iota)
	DELETE
)

type ResourceEvent interface {
	EventType()
}

type SourceEvent struct {
	Stream string
}

func (*SourceEvent) EventType() {}

type SinkEvent struct {
	Stream string
}

func (*SinkEvent) EventType() {}

type TaskEvent struct {
	Type   EventType
	UserId string
	Topic  string
	Task   Task
}

func (*TaskEvent) EventType() {}

// Object
type BaseObject interface {
	ID() string
	UserID() string
	String() string
	RefreshTime() int64
}

// Task
type Task interface {
	BaseObject
	Invoke(ctx context.Context, evalCtx ruleql.Context, msg stream.PublishMessage) error
	Close(ctx context.Context) error
}

type TaskGroup = []Task

// Slot User slot with private tire topic tree
type Slot interface {
	AddTask(ctx context.Context, event *TaskEvent) bool
	DelTask(ctx context.Context, event *TaskEvent) bool
	Invoke(ctx context.Context, evalCtx ruleql.Context, messages stream.PublishMessage) error
	Tree() *topic.Tree
}

// Direct Pubsub Entity Key
var DirectEntityKey struct{}

type ResourceManager interface {
	Sink(dsn string) (sink stream.Sink, err error)
	Sources() map[string]stream.Source
	RePublishSink() stream.Sink
	PubsubSink() stream.Sink
	SetPubsubSink(stream.Sink)
	Slot(userID string) Slot
	HandleEvent(ctx context.Context, evt *metapb.ResourceObject) error
}
type ResourceEventValue struct {
	Type metapb.ResourceObject_EventType
	Body interface{}
}
type ResourceCallbackFunc func(ctx context.Context, evt *metapb.ResourceObject) error

type CallbackFunc func(ctx context.Context, evt interface{}) error
type SyncManager interface {
	Run()
	SetReceiver(ctx context.Context, fn ResourceCallbackFunc)
}
