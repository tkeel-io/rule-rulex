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

package utils

import (
	"bytes"
	"fmt"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-util/stream/utils"
	"math/big"
	"math/rand"
	"regexp"
	"strings"
	"time"
)

type Context = ruleql.Context
type Node = ruleql.Node

var (
	UNDEFINED_RESULT = ruleql.UNDEFINED_RESULT
	exprPattern      = regexp.MustCompile(`{{(?P<key>[^}]+)}}`)
	rawMsg           = `{
	"params": {
		"Cpu": {
			"value": 1111123.4,
			"time": 1534975332000,
			"value_type": "string"
		},
		"Mem": {
			"value": 1111123.4,
			"time": 1534975332000,
			"value_type": "percent"
		}
	},
	"version":"1.0",
	"id": "shanghai"
	}`
	msgCtx     = NewJSONContext(rawMsg)
	NewMessage = func(idx int) types.PublishMessage {
		message := types.NewMessage()
		topic := "/sys/xxxxx/deviceId/thing/event/property/post"
		message.SetTopic(topic)
        var i int64 = 1579420383073
        var j int64 = int64(idx*1000)
		msgCtx.SetValue("params.Cpu.time", big.NewInt(i+j))
		msgCtx.SetValue("params.Cpu.value", rand.Float32())
		msgCtx.SetValue("params.Mem.time", big.NewInt(i+j))
		msgCtx.SetValue("params.Mem.value", rand.Float32())
		message.SetData([]byte(msgCtx.String()))
		return types.PublishMessage(message)
	}
)

type metaContext struct {
	message   types.PublishMessage
	context   ruleql.Context
	functions map[string]ruleql.ContextFunc
}

//NewJSONContext new context from json
func NewMessageContext(message types.PublishMessage) Context {
	return &metaContext{
		message: message,
		context: ruleql.NewJSONContext(string(message.Data())),
		functions: map[string]ruleql.ContextFunc{
			"int": func(args ...ruleql.Node) ruleql.Node {
				if len(args) == 0 {
					return ruleql.IntNode(0)
				}
				return args[0].To(ruleql.Int)
			},
			"float": func(args ...ruleql.Node) ruleql.Node {
				if len(args) == 0 {
					return ruleql.FloatNode(0)
				}
				return args[0].To(ruleql.Float)
			},
			"userid": func(args ...ruleql.Node) ruleql.Node {
				return ruleql.StringNode(message.Domain())
			},
			"deviceid": func(args ...ruleql.Node) ruleql.Node {
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
	}
}

//Value get value from context
func (this *metaContext) Value(expr string) Node {
	return this.context.Value(expr)
}

//Call call function from context
func (this *metaContext) Call(expr *ruleql.CallExpr, args []Node) Node {
	if ret, ok := this.functions[expr.FuncName()]; ok {
		return ret(args...)
	}
	return UNDEFINED_RESULT
}

func Execute(ctx ruleql.Context, expr string) interface{} {
	var buf bytes.Buffer
	content := []byte(expr)
	allIndexes := exprPattern.FindAllSubmatchIndex(content, -1)
	if len(allIndexes) == 1 && expr[:2] == "{{" && expr[len(expr)-2:len(expr)] == "}}" {
		return executeExpr(ctx, expr[2:len(expr)-2])
	} else {
		idx := 0
		for _, loc := range allIndexes {
			ret := executeByteExpr(ctx, content[loc[2]:loc[3]])
			//fmt.Println("-----")
			//fmt.Println(string(content[idx:loc[2]-2]),ret)
			buf.Write(content[idx : loc[2]-2])
			buf.Write(ret)
			idx = loc[3] + 2
		}
		buf.Write(content[idx:])
		return string(buf.Bytes())
	}
}

//Value get value from context
func executeByteExpr(ctx ruleql.Context, expr []byte) []byte {
	exprStr := string(expr)
	if exprStr == "$index" {
		return []byte(fmt.Sprintf("%v", toValue(ctx.Value(exprStr))))
	}
	return []byte(fmt.Sprintf("%v", executeExpr(ctx, exprStr)))
}

func executeExpr(ctx ruleql.Context, expr string) interface{} {
	if expr == "$index" {
		return fmt.Sprintf("%v", toValue(ctx.Value(expr)))
	}
	exp, err := ruleql.ParseExpr(expr)
	if err != nil {
		utils.Log.Bg().Error("Parse expr error",
			logf.String("expr", expr),
			logf.Error(err))
	}
	ret := ruleql.EvalRuleQL(ctx, exp)
	return toValue(ret)
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
	return nil
}
