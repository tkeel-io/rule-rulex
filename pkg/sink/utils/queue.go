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
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/uber-go/atomic"
	"sync"
	"time"
)

const (
	StatusIdle = iota
	StatusInProgress
)

func init() {
	fmt.Println("init MessageBatchQueue 1")
}

type Callback func(msgs []types.Message) (err error)

type CallbackMessage struct {
	Message types.Message
	Callback
}

var idx = 0

type MessageBatchQueue struct {
	id             string
	ctx            context.Context
	queue          chan types.Message
	messages       []types.Message
	callback       Callback
	timer          *time.Timer
	interval       time.Duration
	status         int
	batchSize      int
	pendingRecords *atomic.Int32
	mutex          *sync.RWMutex
	processWG      *sync.WaitGroup
	block          bool
	log            log.Factory
	cancle         context.CancelFunc
	pendingSleep   time.Duration
	flushWG        *sync.WaitGroup
}

func NewQueue(ctx context.Context, id string, queueSize, batchSize int, interval time.Duration, callback Callback) *MessageBatchQueue {
	queueSize = 0
	//log.Warn("Message Batch Queue Size", logf.Any("queueSize", queueSize))
	ctx, cancle := context.WithCancel(ctx)
	mb := &MessageBatchQueue{
		id:     id,
		ctx:    ctx,
		cancle: cancle,
		//queue:     make(chan types.Message, queueSize),
		messages:       make([]types.Message, 0, batchSize),
		callback:       callback,
		interval:       interval,
		timer:          time.NewTimer(interval),
		status:         StatusIdle,
		batchSize:      batchSize,
		mutex:          new(sync.RWMutex),
		processWG:      new(sync.WaitGroup),
		flushWG:        new(sync.WaitGroup),
		pendingSleep:   100 * time.Millisecond,
		pendingRecords: atomic.NewInt32(0),
		log:            log.GlobalLogger().With(logf.String("queue id", id)),
	}
	go mb.process()
	return mb
}

func (this *MessageBatchQueue) process() {

	this.processWG.Add(1)
	defer this.processWG.Done()

	for {
		select {
		case <-this.timer.C:
			log.For(this.ctx).Debug("time refresh")
			this.doCallback()
		case <-this.ctx.Done():
			return
		}
	}
}

func (this *MessageBatchQueue) Flush() {
	ts := time.Now()
	log.For(this.ctx).Debug("MessageBatchQueue.Flush", logf.Any("now", ts))
	this.flushWG.Add(1)
	defer this.flushWG.Done()
	this.reset()
	this.doCallback()
	for ; this.pendingRecords.Load() > 0; {
		time.Sleep(this.pendingSleep)
	}
	if (this.pendingRecords.Load() != 0) {
		log.Error("Pending record error", logf.Any("pendingRecords", this.pendingRecords))
	}

	log.For(this.ctx).Debug("MessageBatchQueue.Flush", logf.Any("dt", time.Now().Sub(ts)))
}

func (this *MessageBatchQueue) Close() {
	this.cancle()
	this.Flush()
	this.processWG.Wait()
}

func (this *MessageBatchQueue) Push(msg types.Message) {

	this.debug2("MessageBatchQueue.Push", msg)

	this.flushWG.Wait()
	this.pendingRecords.Add(1)
	//fmt.Printf("%p,%p,%v\n", this.messages, msg, msg.PacketIdentifier())  // 2
	this.mutex.Lock()
	this.messages = append(this.messages, msg)
	this.mutex.Unlock()
	if len(this.messages) >= this.batchSize {
		this.doCallback()
	}
}

func (this *MessageBatchQueue) debug2(typ string, msg types.Message) {
	jsonCtx := NewJSONContext(string(msg.Data()))
	log.For(this.ctx).Debug(typ,
		logf.Any("id", jsonCtx.Value("id")))
}

func (this *MessageBatchQueue) doCallback() {
	this.status = StatusIdle

	this.mutex.Lock()
	slice := this.messages
	this.messages = nil
	this.mutex.Unlock()
	if slice != nil && len(slice) > 0 {
		for _, msg := range slice {
			this.debug2(fmt.Sprintf("MessageBatchQueue.doCallback[%d]", idx), msg)
		}
		if err := this.callback(slice); err != nil {
			log.Error("MessageBatchQueue.callback",logf.Error(err))
		}
		this.pendingRecords.Sub(int32(len(slice)))
	}
}

func (this *MessageBatchQueue) doCallback1() {
	this.status = StatusIdle

	var slice []types.Message
	if this.messages != nil {
		slice = this.messages
		this.messages = nil
	}
	if slice != nil {
		this.callback(slice)
	}
}

func (this *MessageBatchQueue) reset() {
	switch this.status {
	case StatusIdle:
		this.status = StatusInProgress
		this.timer.Reset(this.interval)
	case StatusInProgress:
	default:
	}
}
