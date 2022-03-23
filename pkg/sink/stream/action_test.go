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

package stream

import (
	"context"
	"fmt"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/pkg/sink"
	"github.com/tkeel-io/rule-rulex/pkg/sink/utils"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/smartystreets/gunit"
	"testing"
	"time"
)

var matedata = map[string]string{
	"option": `{
		"sink": "kafka://101.200.198.4:9094/hainayun-qingcloud-test/qingcloud"  	
	}`,
}

func TestKafka(t *testing.T) {
	//gunit.RunSequential(new(KafkaSink), t)
	gunit.Run(new(KafkaSink), t)
}

type KafkaSink struct {
	*gunit.Fixture
	ctx    types.ActionContent
	cancel context.CancelFunc
	action *streamSink
	msgCtx *utils.JsonContext
	idx    int
}

func (this *KafkaSink) Setup() {
	sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	this.ctx = sink.NewContext(sctx)
	this.cancel = cancel
	this.action = newSink("cloudsat", "cloudsat-001")
	if err := this.action.Setup(this.ctx, matedata); err != nil {
		log.Error("err:", logf.Error(err))
		log.Fatal("exit")
	}
}

func (this *KafkaSink) TestKafkaSink() {
	ts := time.Now()
	for i := 0; i < 1; i++ {
		if err := this.action.Invoke(this.ctx, NewMessage(i).SetData([]byte("Test"))); err != nil {
			log.Fatal("err:", logf.Error(err))
		}
	}
	this.action.Close()
	fmt.Println("~TestKafkaSink~", time.Now().Sub(ts))
}
