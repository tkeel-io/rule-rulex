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

package functions

import (
	"encoding/json"
	v1 "github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/stream"
	"github.com/smartystreets/gunit"
	"testing"
)

var (
	PubMessage = MessageFromString(`{"matedata":{"x-stream-domain":"mdmp-test","x-stream-entity":"iotd-mock001","x-stream-method":"Publish","x-stream-qos":"0","x-stream-topic":"/mqtt-mock/benchmark/0","x-stream-version":"1.0"},"time":{},"raw_data":"MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMw=="}`)
)

func MessageFromString(s string) v1.Message {
	m := v1.NewMessage()
	if err := json.Unmarshal([]byte(s), m); err != nil {
		panic(err)
	} else {
		return m
	}
}

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
	msg := PubMessage.SetTopic("/abc/def/12345/67890")
	msg = msg.SetDomain("domain001")
	msg = msg.SetEntity("entity001")
	msgCtx := NewMessageContext(msg.(stream.PublishMessage))

	this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{userid()}}")), "domain001")
	this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{deviceid()}}")), "entity001")
	this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{deviceId()}}")), "entity001")
	this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{deviceidERR()}}")), "")
	this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{topic()}}")), "/abc/def/12345/67890")
	this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{topic(0)}}")), "")
	this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{topic(1)}}")), "abc")
	this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{topic(2)}}")), "def")
	this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{topic(3)}}")), "12345")
	this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{topic(4)}}")), "67890")
	this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{topic(100)}}")), "")
	this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{upper('abcd')}}")), "ABCD")
	this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{topic(4)}}:{{upper('abcd')}}")), "67890:ABCD")
	this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{topic(4)}}-{{to_base64('abcd')}}")), "67890-YWJjZA==")
	this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{sin(0)}}")), "0.000000")
	this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{sin(3.1415926/2)}}")), "1.000000")
	this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{sin(3.1415926)}}")), "0.000000")
	this.Assert(string(ExecuteTemplate(msgCtx, "{{rand1(0)}}"))!="")
	this.Assert(string(ExecuteTemplate(msgCtx, "{{rand1(0)}}"))!="")
	this.Assert(string(ExecuteTemplate(msgCtx, "{{endWith('aaabbbccc', 'ccc')}}"))!="")
	//this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{substring('012345', 2)}}")),"2345")
	//this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{substring('012345', 2.745)}}")),"2345")
	//this.AssertEqual(string(ExecuteTemplate(msgCtx, "{{substring(123, 2)}}")),"3")


}

