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
	"bytes"
	"fmt"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/tkeel-io/rule-util/stream"
	"regexp"
	"strings"
	"time"
)

type Context = ruleql.Context
type Node = ruleql.Node

var (
	UNDEFINED_RESULT = ruleql.UNDEFINED_RESULT
	exprPattern      = regexp.MustCompile(`{{(?P<key>[^}]+)}}`)
)

type ContextAggCallableFunc func(hash string, args ...Node) Node

type messageContext struct {
	message      stream.Message
	context      ruleql.Context
	functions    map[string]ruleql.ContextCallableFunc
	aggFunctions map[string]ContextAggCallableFunc
}

//NewJSONContext new context from json
func NewMessageContext(message stream.PublishMessage) Context {
	return &messageContext{
		message: message,
		context: ruleql.NewJSONContext(string(message.Data())),
		functions: map[string]ruleql.ContextCallableFunc{
			"userid": func(args ...ruleql.Node) ruleql.Node {
				return ruleql.StringNode(message.Domain())
			},
			"deviceid": func(args ...ruleql.Node) ruleql.Node {
				return ruleql.StringNode(message.Entity())
			},
			"deviceId": func(args ...ruleql.Node) ruleql.Node {
				return ruleql.StringNode(message.Entity())
			},
			"topic": func(args ...ruleql.Node) ruleql.Node {
				if len(args) == 0 {
					return ruleql.StringNode(message.Topic())
				}
				if len(args) == 1 {
					offset, ok1 := args[0].(ruleql.IntNode)
					if ok1 {
						offset := int(offset)
						arr := strings.Split(message.Topic(), "/")
						if offset >= 0 && offset < len(arr) {
							return ruleql.StringNode(arr[offset])
						} else {
							return ruleql.StringNode("")
						}

					}
				}
				return ruleql.StringNode("")
			},
			"str": func(args ...ruleql.Node) ruleql.Node {
				if len(args) == 1 {
					n, ok := args[0].(ruleql.JSONNode)
					if ok {
						snode := strings.ReplaceAll(string(n), "\"", "\\\"")
						snode = strings.ReplaceAll(string(snode), "\n", " ")
						return ruleql.StringNode(snode)
					}
				}
				return ruleql.UNDEFINED_RESULT
			},
			"timestamp": func(args ...ruleql.Node) ruleql.Node {
				return ruleql.IntNode(time.Now().UnixNano() / 1000 / 1000)
			},
			"timeFormat": func(args ...ruleql.Node) ruleql.Node {
				if len(args) == 1 {
					ts, ok1 := args[0].(ruleql.IntNode)
					if ok1 {
						t := time.Unix(int64(ts), 0)
						s := t.Format("2006-01-02T15:04:05Z07:00")
						return ruleql.StringNode(s)
					}
				}
				return ruleql.UNDEFINED_RESULT
			},
		},
		aggFunctions: map[string]ContextAggCallableFunc{
			"count": func(hash string, args ...ruleql.Node) ruleql.Node {
				byt := message.Attr(hash)
				//fmt.Println(string(byt))
				return ruleql.StringNode(string(byt)).To(ruleql.Int)
			},
			"avg": func(hash string, args ...ruleql.Node) ruleql.Node {
				byt := message.Attr(hash)
				//fmt.Println(string(byt))
				return ruleql.StringNode(string(byt)).To(ruleql.Float)
			},
			"sum": func(hash string, args ...ruleql.Node) ruleql.Node {
				byt := message.Attr(hash)
				//fmt.Println(string(byt))
				return ruleql.StringNode(string(byt)).To(ruleql.Float)
			},
			"max": func(hash string, args ...ruleql.Node) ruleql.Node {
				byt := message.Attr(hash)
				//fmt.Println(string(byt))
				return ruleql.StringNode(string(byt)).To(ruleql.Float)
			},
			"min": func(hash string, args ...ruleql.Node) ruleql.Node {
				byt := message.Attr(hash)
				//fmt.Println(string(byt))
				return ruleql.StringNode(string(byt)).To(ruleql.Float)
			},
		},
	}
}

//Value get value from context
func (this *messageContext) Value(expr string) Node {
	return this.context.Value(expr)
}

//Call call function from context
func (this *messageContext) Call(expr *ruleql.CallExpr, args []Node) Node {
	if ret, ok := this.aggFunctions[expr.FuncName()]; ok {
		return ret(expr.String(), args...)
	}
	if ret, ok := this.functions[expr.FuncName()]; ok {
		return ret(args...)
	}
	return UNDEFINED_RESULT
}

//Value get value from context
func byteExecute(ctx ruleql.Context, expr []byte) []byte {
	exp, err := ruleql.ParseExpr(string(expr))
	if err != nil {
		utils.Log.Bg().Error("Parse expr error",
			logf.ByteString("expr", expr),
			logf.Error(err))
	}
	ret := ruleql.EvalRuleQL(ctx, exp)
	return []byte(fmt.Sprintf("%v", toValue(ret)))
}

//Value get value from context
func Execute(c ruleql.Context, expr string) []byte {
	var buf bytes.Buffer
	content := []byte(expr)
	allIndexes := exprPattern.FindAllSubmatchIndex(content, -1)
	idx := 0
	for _, loc := range allIndexes {
		ret := byteExecute(c, content[loc[2]:loc[3]])
		//fmt.Println("-----")
		//fmt.Println(string(content[idx:loc[2]-2]),ret)
		buf.Write(content[idx : loc[2]-2])
		buf.Write(ret)
		idx = loc[3] + 2
	}
	buf.Write(content[idx:])
	return buf.Bytes()
}

func toValue(ret Node) interface{} {
	switch ret := ret.(type) {
	case ruleql.BoolNode:
		return bool(ret)
	case ruleql.StringNode:
		return string(ret)
	case ruleql.FloatNode:
		return float64(ret)
	case ruleql.IntNode:
		return int64(ret)
	case ruleql.JSONNode:
		return string(ret)
	}
	return ""
}
