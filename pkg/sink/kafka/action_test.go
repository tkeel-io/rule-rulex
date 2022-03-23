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

package kafka

import (
	"context"
	"fmt"
	v1 "github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/pkg/sink"
	"github.com/tkeel-io/rule-rulex/pkg/sink/utils"
	"github.com/tkeel-io/rule-util/stream"
	"github.com/tkeel-io/rule-util/stream/checkpoint"
	"github.com/smartystreets/gunit"
	"testing"
	"time"
)

var matedata = map[string]string{
	"option": `{
		"sink": "kafka://139.198.186.46:9080/test/qingcloud"  	
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
	msgCtx *utils.JsonContext
	idx    int
	action *kafkaSink
}

func (this *KafkaSink) Setup() {
	sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	this.ctx = sink.NewContext(sctx)
	this.cancel = cancel
	this.action = newKafkaSink("kafka", "cloudsat-001")
	if err := this.action.Setup(this.ctx, matedata); err != nil {
		log.Error("err:", logf.Error(err))
		log.Fatal("exit")
	}
}

func (this *KafkaSink) TestKafkaSink() {
	ts := time.Now()
	for i := 0; i < 5; i++ {
		if err := this.action.Invoke(this.ctx, NewMessage(i).SetData([]byte("Test:"+time.Now().String()))); err != nil {
			log.Fatal("err:", logf.Error(err))
		}
	}
	this.action.Close()
	fmt.Println("~TestKafkaSink~", time.Now().Sub(ts))
}

func TestKafkaSink111(t *testing.T) {

	sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	ctx := sink.NewContext(sctx)
	act := newKafkaSink("kafka", "cloudsat-001")
	if err := act.Setup(ctx, matedata); err != nil {
		log.Error("err:", logf.Error(err))
		log.Fatal("exit")
	}

	t.Log("setup successful.")

	ts := time.Now()
	for i := 0; i < 5; i++ {
		t.Log("invoke data.")
		if err := act.Invoke(ctx, NewMessage(i).SetData([]byte("Test:"+time.Now().String()))); err != nil {
			log.Fatal("err:", logf.Error(err))
		}
	}

	act.Close()
	fmt.Println("~TestKafkaSink~", time.Now().Sub(ts))
}

func (this *KafkaSink) TestKafkaSource() {
	checkpoint.InitCoordinator(context.Background(), 2*time.Second)
	source, _ := stream.OpenSource("kafka://139.198.186.46:9080/test/qingcloud")
	source.StartReceiver(context.Background(), func(ctx context.Context, message interface{}) error {
		switch message := message.(type) {
		case v1.Message:
			fmt.Println("Message", message)
		case []byte:
			fmt.Println("byte", string(message), time.Now())
		default:
			fmt.Println("default", message)
		}
		return nil
	})
	select {}
}
