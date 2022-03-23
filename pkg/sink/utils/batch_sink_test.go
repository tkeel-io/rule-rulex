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
	"context"
	"fmt"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"sync"
	"testing"
	"time"
)

var INTERVAL = time.Nanosecond * 1

func TestBatchSinkUnit(t *testing.T) {
	gunit.RunSequential(new(BatchSinkUnit), t)
	//gunit.Run(new(BatchSinkUnit), t)
}

type BatchSinkUnit struct {
	*gunit.Fixture
	opt   *SinkBatchOptions
	COUNT int
}

func (this *BatchSinkUnit) Setup() {
	log.InitLogger("BatchSinkUnit")
	this.So(nil, should.BeNil)
	//this.COUNT = 2000000
	this.COUNT = 200000
	this.opt = &SinkBatchOptions{
		5,
		2000,
		time.Second,
	}
}

func (this *BatchSinkUnit) TestBasic() {
	ctx := context.Background()
	termNum := 0
	msgCnt := 0
	wg := &sync.WaitGroup{}
	bs, err := NewBatchSink("sendTest", this.opt, func(msgs []types.Message) (err error) {
		termNum++
		for _, msg := range msgs {
			_ = msg
			msgCnt ++
		}
		return nil
	})
	this.So(err, should.BeNil)
	this.So(bs, should.NotBeNil)

	wg.Add(this.COUNT)
	go func() {
		for i := 0; i < this.COUNT; i++ {
			msg := types.NewMessage().SetPacketIdentifier(fmt.Sprintf("%d", i))
			err := bs.Send(ctx, msg)
			this.So(err, should.BeNil)
			wg.Done()
			//fmt.Println(
			//	fmt.Sprintf("Push[%d]:%s", termNum, msg.PacketIdentifier()))
			//time.Sleep(INTERVAL)
		}
	}()
	wg.Wait()
	bs.Close()
	this.So(msgCnt, should.Equal, this.COUNT)
	this.So(bs.pendingQueue.Size(), should.Equal, 0)
}

func (this *BatchSinkUnit) TestSlowProcess() {
	ctx := context.Background()
	start := time.Now()
	termNum := 0
	msgCnt := 0
	wg := &sync.WaitGroup{}
	bs, err := NewBatchSink("sendTest", this.opt, func(msgs []types.Message) (err error) {
		termNum++
		for _, msg := range msgs {
			_ = msg
			msgCnt ++
		}
		//time.Sleep(time.Duration(rand.Intn(1000000000)) * INTERVAL)
		time.Sleep(time.Second)
		return nil
	})
	log.Error("Size")
	this.So(err, should.BeNil)
	this.So(bs, should.NotBeNil)

	wg.Add(this.COUNT)
	go func() {
		for i := 0; i < this.COUNT; i++ {
			msg := types.NewMessage().SetPacketIdentifier(fmt.Sprintf("%d", i))
			err := bs.Send(ctx, msg)
			this.So(err, should.BeNil)
			wg.Done()
			//fmt.Println(
			//	fmt.Sprintf("Push[%d]:%s", termNum, msg.PacketIdentifier()))
			//time.Sleep(INTERVAL)
		}
	}()
	go func() {
		for {
			log.Error(fmt.Sprintf("Size[%v]:%d:%d", time.Now().Sub(start), bs.sendCnt, bs.pendingQueue.Size()))
			time.Sleep(time.Second * 100)
		}
	}()
	wg.Wait()
	bs.Close()
	this.So(msgCnt, should.Equal, this.COUNT)
	this.So(bs.pendingQueue.Size(), should.Equal, 0)
}
