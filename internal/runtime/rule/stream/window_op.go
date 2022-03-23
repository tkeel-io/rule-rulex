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

package stateful

import (
	"fmt"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"github.com/tkeel-io/rule-rulex/internal/runtime/rule/stream/functions"
)

type AggregateFunc func(evalCtx functions.Context, state *WindowState) error

type AggregateCallExpr struct {
	*ruleql.CallExpr
	jsonPath string
}

type WindowsOperator struct {
	expr     ruleql.Expr
	window   *ruleql.WindowExpr
	aggExprs map[string]*AggregateCallExpr
	fn       func()
}

func NewWindowsOperator(expr ruleql.Expr, window *ruleql.WindowExpr,
	funcs map[string]*ruleql.CallExpr) *WindowsOperator {
	winFuncs := make(map[string]*AggregateCallExpr, 0)
	for key, fn := range funcs {
		key = fn.FuncName()
		if _, ok := functions.AggregateFuncs[key]; ok {
			raw := fn.String()
			if len(raw) <= 3 {
				fmt.Println("error NewWindowsOperator")
			}
			winFuncs[raw] = &AggregateCallExpr{
				fn,
				raw[len(fn.FuncName())+1 : len(raw)-1],
			}
		}
	}
	windows := &WindowsOperator{
		expr:     expr,
		window:   window,
		aggExprs: winFuncs,
	}
	return windows
}

//func (op *WindowsOperator) Exce(ctx Context, state *WindowState, message stream.Message) error {
//	//fmt.Println("+2. ProcessElement+", time.Now(), state, ctx)
//	//fmt.Println("+2.1 Process Op+", time.Now())
//	state.state = message
//	state.cnt++
//	for k, ac := range op.aggExprs {
//		if err := evalAggregateCallExpr(ctx, state, ac); err != nil {
//			fmt.Println("Exce Error", k, ac)
//		}
//	}
//	return nil
//}
