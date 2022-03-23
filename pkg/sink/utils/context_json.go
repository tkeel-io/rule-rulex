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
	"github.com/tkeel-io/rule-util/ruleql/pkg/json/gjson"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"regexp"
	"strings"
)

var (
	pattern  = regexp.MustCompile(`\[(?P<key>\w+)\]`)
	template = `.$1`
)

type JsonContext struct {
	raw string
}

//NewJSONContext new context from json
func NewJSONContext(jsonRaw string) *JsonContext {
	return &JsonContext{
		raw: jsonRaw,
	}
}

//Value get value from context
func (c *JsonContext) Value(path string) interface{} {
	ret := gjson.Get(c.raw, thePath(path))
	switch ret.Type {
	case gjson.True:
		return true
	case gjson.False:
		return false
	case gjson.String:
		return ret.Str
	case gjson.Number:
		if strings.Index(ret.Raw, ".") != -1 {
			return ret.Num
		}
		return int64(ret.Num)
	case gjson.JSON:
		return ret.Raw
	case gjson.Null:
		return nil
	}
	return nil
}

//Value get value from context
func (c *JsonContext) SetValue(xpath string, vaule interface{}) {
	var node ruleql.Node
	switch vaule := vaule.(type) {
	case int32:
		node = ruleql.IntNode(vaule)
	case int64:
		node = ruleql.IntNode(vaule)
	case float32:
		node = ruleql.FloatNode(vaule)
	case float64:
		node = ruleql.FloatNode(vaule)
	case bool:
		node = ruleql.BoolNode(vaule)
	}
	c.raw, _ = ruleql.JSONNode(c.raw).Update(xpath, node)
	return
}

//Value get value from context
func (c *JsonContext) String() string {
	return c.raw
}

func thePath(path string) string {
	return pattern.ReplaceAllString(path, template)
}
