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

//func init() {
//	ruleql.EvalCallExpr = func(ctx Context, expr *ruleql.CallExpr) Node {
//		n := len(expr.Args())
//		if n == 0 {
//			return ctx.Call(expr.key, []Node{})
//		}
//		values := make([]Node, 0, n)
//		for _, expr := range expr.args {
//			values = append(values, eval(ctx, expr))
//		}
//		ret := ctx.Call(expr.key, values)
//		if ret.Type() != Undefined {
//			return ret
//		}
//	}
//
//}
