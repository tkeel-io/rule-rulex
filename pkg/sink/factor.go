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

package sink

import (
	"context"
	"fmt"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-util/stream/utils"
)

var actionFactors map[string]types.ActionBuilder

type pipeContext struct {
	context context.Context
}

func (ctx *pipeContext) Ack() {

}
func (ctx *pipeContext) Nack(err error) {
	utils.Log.Bg().Error("receive error",
		logf.Error(err))
}

func (ctx *pipeContext) Context() context.Context {
	return ctx.context
}

func NewContext(ctx context.Context) types.ActionContent {
	return &pipeContext{
		context: ctx,
	}
}

func NewAction(entityType, entityID string) types.Action {
	factor := actionFactors[entityType]
	if factor != nil {
		return factor(entityType, entityID)
	}
	log.GlobalLogger().Bg().Error("factor not fount",
		logf.String("entityType", entityType))
	return nil
}

func Registered(name string, ac types.ActionBuilder) {
	if _, ok := actionFactors[name]; ok {
		panic(fmt.Sprintf("action[%v] register twice", name))
	}
	actionFactors[name] = ac
}

func SinkNames() []string {
	names := []string{}
	for name, _ := range actionFactors {
		names = append(names, name)
	}
	return names
}

func init() {
	actionFactors = make(map[string]types.ActionBuilder)
}
