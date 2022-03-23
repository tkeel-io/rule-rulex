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

package api

import (
	"context"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"time"
)

// StateFreeCheckFunc if false, State clean
type StateFreeCheckFunc func(ctx context.Context, state interface{}) bool

//
type StateFunc func(ctx context.Context, state interface{}) error




type AggregateCallValuer interface {
	ruleql.Context
	StateFunc(ctx context.Context, state interface{}) error
}

// Batch Operation types
// StreamTrigger interface provides logic to trigger when batch is done.
type StreamTrigger interface {
	Done(ctx context.Context, item interface{}, index int64) bool
}

// BatchTriggerFunc a function type adapter that implements StreamTrigger
type BatchTriggerFunc func(context.Context, interface{}, int64) bool

// Done implements BatchOperation.Done
func (f BatchTriggerFunc) Done(ctx context.Context, item interface{}, index int64) bool {
	return f(ctx, item, index)
}

// StreamTriggerFunc a function type adapter that implements StreamTrigger
type StreamTriggerFunc func(context.Context, interface{}, time.Duration) bool

// Done implements BatchOperation.Done
func (f StreamTriggerFunc) Done(ctx context.Context, item interface{}, time time.Duration) bool {
	return f(ctx, item, time)
}
