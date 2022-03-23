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

package postgresql

import (
	"encoding/json"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"github.com/buger/jsonparser"
	"regexp"
)

var (
	//pattern  = regexp.MustCompile(`\[(?P<key>\w+)\]`)
	pattern        = regexp.MustCompile(`{{(?P<key>[^}]+)}}`)
	NewJSONContext = ruleql.NewJSONContext
	NewMapContext  = ruleql.NewMapContext
)

type EvalContext struct {
	ctx     ruleql.MutilContext
	baseCtx []ruleql.Context
}

func (this *EvalContext) Value(key string) ruleql.Node {
	return this.ctx.Value(key)
}

func (this *EvalContext) Call(expr *ruleql.CallExpr, args []ruleql.Node) ruleql.Node {
	return this.ctx.Call(expr, args)
}

//NewContext new context from json
func NewContext(key string, rawJsonStr string, baseCtx ...ruleql.Context) *EvalContext {
	ctx := append(baseCtx,
		NewMapContext(
			nil,
			map[string]ruleql.ContextFunc{
				"index": func(args ...ruleql.Node) ruleql.Node {
					return ruleql.StringNode(key)
				},
			}),
		NewJSONContext(rawJsonStr),
	)
	return &EvalContext{
		ctx:     ctx,
		baseCtx: baseCtx,
	}
}

func (this *EvalContext) Range(expr string) []*EvalContext {
	ret := this.ctx.Value(expr)
	switch ret := ret.(type) {
	case ruleql.JSONNode:
		ctxs := []*EvalContext{}
		_ = jsonparser.ObjectEach([]byte(ret), func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
			ctxs = append(ctxs, NewContext(string(key), string(value),
				this.baseCtx...,
			))
			return nil
		}, )
		return ctxs
	}
	return nil
}

//
func (this *EvalContext) Range2(expr string) []*EvalContext {
	ret := this.ctx.Value(expr)
	switch ret := ret.(type) {
	case ruleql.JSONNode:
		subNodes := make(map[string]interface{})
		err := json.Unmarshal([]byte(ret), &subNodes)
		if err != nil {
			log.Error("subNodes unmarshal",
				logf.Any("ret", ret),
				logf.Error(err))
			return nil
		}
		ctxs := []*EvalContext{}
		for snName, _ := range subNodes {
			subNode, ok := this.ctx.Value(expr + "." + snName).(ruleql.JSONNode)
			if !ok {
				continue
			}
			ctxs = append(ctxs, NewContext(snName, string(subNode),
				this.baseCtx...,
			))
		}
		return ctxs
	}
	return nil
}
