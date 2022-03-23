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
	"fmt"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/smartystreets/gunit"
	"regexp"
	"testing"
)

func TestContext(t *testing.T) {
	//gunit.RunSequential(new(Context), t)
	gunit.Run(new(TContext), t)
}

type TContext struct {
	*gunit.Fixture
}

func (this *TContext) Setup() {

}

func (this *TContext) TestContext() {
	msg := NewMessage(1).SetTopic("/abc/def/12345/67890")
	msg = msg.SetDomain("domain001")
	msg = msg.SetEntity("entity001")
	msgCtx := NewMessageContext(msg.(types.PublishMessage))

	this.AssertEqual(Execute(msgCtx, "{{userid()}}"), "domain001")
	this.AssertEqual(Execute(msgCtx, "{{deviceid()}}"), "entity001")
	this.AssertEqual(Execute(msgCtx, "{{deviceidERR()}}"), "")
	this.AssertEqual(Execute(msgCtx, "{{topic()}}"), "/abc/def/12345/67890")
	this.AssertEqual(Execute(msgCtx, "{{topic(0)}}"), "")
	this.AssertEqual(Execute(msgCtx, "{{topic(1)}}"), "abc")
	this.AssertEqual(Execute(msgCtx, "{{topic(2)}}"), "def")
	this.AssertEqual(Execute(msgCtx, "{{topic(3)}}"), "12345")
	this.AssertEqual(Execute(msgCtx, "{{topic(4)}}"), "67890")
	this.AssertEqual(Execute(msgCtx, "{{topic(100)}}"), "")
}

func (this *TContext) TestRegexContext() {
	pattern = regexp.MustCompile(`\[(?P<key>\w+)\]`)
	msg := NewMessage(1).SetTopic("/abc/def/12345/67890")
	msg = msg.SetDomain("domain001")
	msg = msg.SetEntity("entity001")
	msgCtx := NewMessageContext(msg.(types.PublishMessage))

	this.AssertEqual(Execute(msgCtx, "{{int()}}"), int64(0))
	this.AssertEqual(Execute(msgCtx, "{{int(2)}}"), int64(2))
	this.AssertEqual(Execute(msgCtx, `{{int('2')}}`), int64(2))
	this.AssertEqual(Execute(msgCtx, "{{userid()}}"), "domain001")
	this.AssertEqual(Execute(msgCtx, "{{deviceid()}}"), "entity001")
	this.AssertEqual(Execute(msgCtx, "{{topic()}}"), "/abc/def/12345/67890")
	this.AssertEqual(Execute(msgCtx, "{{topic(0)}}"), "")
	this.AssertEqual(Execute(msgCtx, "{{topic(1)}}"), "abc")
	this.AssertEqual(Execute(msgCtx, "{{topic(2)}}"), "def")
	this.AssertEqual(Execute(msgCtx, "{{topic(3)}}"), "12345")
	this.AssertEqual(Execute(msgCtx, "{{topic(4)}}"), "67890")
	this.AssertEqual(Execute(msgCtx, "{{topic(100)}}"), "")
}

func Println(nodes ...interface{}) {
	for _, n := range nodes {
		switch n := n.(type) {
		case ruleql.StringNode:
			fmt.Printf("%v\n", string(n))
		case ruleql.IntNode:
			fmt.Printf("%v\n", int(n))
		case ruleql.JSONNode:
			fmt.Printf("%v\n", string(n))
		case ruleql.BoolNode:
			fmt.Printf("%v\n", bool(n))
		}
		fmt.Printf("%v\n", n)
	}
}
