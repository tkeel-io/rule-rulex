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
	"context"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"github.com/tkeel-io/rule-util/stream"
)

type RuleQL interface {
	ID() string
	Filter(ctx context.Context, evalCtx ruleql.Context, message stream.PublishMessage) bool
	Invoke(ctx context.Context, evalCtx ruleql.Context, message stream.PublishMessage) error
}

type AggregateOperator interface {
	Invoke(evalCtx ruleql.Context, message stream.PublishMessage) error
}

type State interface {

}

type StateOperator interface {
	Invoke(state State, evalCtx ruleql.Context, message stream.PublishMessage) error
}
