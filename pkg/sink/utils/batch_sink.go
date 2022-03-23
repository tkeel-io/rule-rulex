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

/**
SinkBatch
1. 线程池，限定最大并发数
2. 累计 message, 定时提交
3. 可以 Flush，手动触发提交
*/

package utils

import (
	"context"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"sync"
	"time"
)

type sinkBatchState int

const (
	sinkBatchInit sinkBatchState = iota
	sinkBatchReady
	sinkBatchClosing
	sinkBatchClosed
)

type processState int

const (
	processIdle processState = iota
	processInProgress
)

type SinkBatchOptions struct {
	// BatchingMaxMessages set the maximum number of messages permitted in a batch. (default: 1000)
	MaxBatching int

	// MaxPendingMessages set the max size of the queue.
	MaxPendingMessages uint

	// BatchingMaxPublishDelay set the time period within which the messages sent will be batched (default: 10ms)
	BatchingMaxPublishDelay time.Duration
}

type BatchSink interface {
	Send(ctx context.Context, msg types.Message) error
	Flush(ctx context.Context) error
	Close()
}

type sendRequest struct {
	ctx context.Context
	msg types.Message
}

type closeRequest struct {
	waitGroup *sync.WaitGroup
}

type flushRequest struct {
	waitGroup *sync.WaitGroup
	err       error
}

type pendingItem struct {
	sync.Mutex
	sequenceID uint64
	batchData  []types.Message
	callback   []CallbackFn
	status     processState
	err        error
}

func (item *pendingItem) GetSequenceId() uint64 {
	return item.sequenceID
}

func (pi *pendingItem) Callback() {
	// lock the pending item
	pi.Lock()
	defer pi.Unlock()
	for _, fn := range pi.callback {
		fn(pi.sequenceID, pi.err)
	}
}

func (pi *pendingItem) Release() {
	pi.batchData = nil
}

type CallbackFn func(sequenceID uint64, e error)

type ProcessFn func(msgs []types.Message) (err error)

type batchSink struct {
	batchBuilder     *BatchBuilder
	batchFlushTicker *time.Ticker

	// Channel where app is posting messages to be published
	eventsChan chan interface{}

	//pendingQueue
	pendingQueue BlockingQueue

	processFn ProcessFn
	state     sinkBatchState
	log       log.Factory
	options   *SinkBatchOptions
	sinkName  string
	lock      sync.Mutex

	sendCnt int64
}

const defaultBatchingMaxPublishDelay = 10 * time.Millisecond

func NewBatchSink(name string, options *SinkBatchOptions, doSinkFn ProcessFn) (*batchSink, error) {
	var batchingMaxPublishDelay time.Duration
	if options.BatchingMaxPublishDelay != 0 {
		batchingMaxPublishDelay = options.BatchingMaxPublishDelay
	} else {
		batchingMaxPublishDelay = defaultBatchingMaxPublishDelay
	}

	var maxPendingProcesses int
	if options.MaxPendingMessages == 0 {
		maxPendingProcesses = 5
	} else {
		maxPendingProcesses = options.MaxBatching
	}

	p := &batchSink{
		batchBuilder:     NewBatchBuilder(options.MaxPendingMessages),
		batchFlushTicker: time.NewTicker(batchingMaxPublishDelay),
		eventsChan:       make(chan interface{}, 1),
		pendingQueue:     NewBlockingQueue(maxPendingProcesses),
		processFn:        doSinkFn,
		state:            sinkBatchInit,
		options:          options,
		sinkName:         name,
	}

	p.log = log.With(logf.String("batchSinkName", name))
	
	p.log.Bg().Info("Created producer")
	p.state = sinkBatchReady

	go p.runEventsLoop()

	return p, nil
}

func (p *batchSink) runEventsLoop() {
	for {
		select {
		case i := <-p.eventsChan:
			switch v := i.(type) {
			case *sendRequest:
				p.internalSend(v)
			case *flushRequest:
				p.internalFlush(v)
			case *closeRequest:
				p.internalClose(v)
				return
			}

		case _ = <-p.batchFlushTicker.C:
			p.internalFlushCurrentBatch()
		}
	}
}

func (p *batchSink) internalSend(request *sendRequest) {
	p.log.Bg().Debug("Received send request", logf.Any("request", *request))

	msg := request.msg

	isFull := p.batchBuilder.Add(msg)
	if isFull == true {
		// The current batch is full.. flush it
		p.internalFlushCurrentBatch()
	}
	p.sendCnt ++
}

func (p *batchSink) internalFlushCurrentBatch() {
	batchData, sequenceID := p.batchBuilder.Flush()
	if batchData == nil {
		return
	}

	item := pendingItem{
		batchData:  batchData,
		sequenceID: sequenceID,
		callback:   []CallbackFn{},
		status:     processInProgress,
	}
	p.pendingQueue.Put(&item)

	go func(item *pendingItem) {
		p.callbackReceipt(item, p.processFn(item.batchData))
	}(&item)
}

func (p *batchSink) internalFlush(fr *flushRequest) {
	p.internalFlushCurrentBatch()

	pi, ok := p.pendingQueue.PeekLast().(*pendingItem)
	if !ok {
		fr.waitGroup.Done()
		return
	}

	// lock the pending request while adding requests
	// since the ReceivedSendReceipt func iterates over this list
	pi.Lock()
	pi.callback = append(pi.callback, func(sequenceID uint64, e error) {
		fr.err = e
		fr.waitGroup.Done()
	})
	pi.Unlock()
}

func (p *batchSink) internalClose(req *closeRequest) {
	defer req.waitGroup.Done()
	if p.state != sinkBatchReady {
		return
	}

	p.state = sinkBatchClosing
	p.log.Bg().Info("Closing producer")

	p.state = sinkBatchClosed
	p.batchFlushTicker.Stop()

	wg := sync.WaitGroup{}
	wg.Add(1)
	fr := &flushRequest{&wg, nil}
	p.internalFlush(fr)
	wg.Wait()
}

func (p *batchSink) callbackReceipt(item *pendingItem, err error) {
	p.log.Bg().Debug("Response receipt",
		logf.Any("sequenceID", item.sequenceID))
	item.status = processIdle
	item.err = err
	p.sendCnt = p.sendCnt - int64(len(item.batchData))

	for {
		pi, ok := p.pendingQueue.Peek().(*pendingItem)

		if !ok {
			break
		}
		if pi.status == processInProgress {
			//p.log.Bg().Debug("Response receipt unexpected",
			//	logf.Any("pendingSequenceId", pi.sequenceID),
			//	logf.Any("responseSequenceId", item.sequenceID))
			break
		}

		// We can remove the item which is done
		p.pendingQueue.Poll()

		// Trigger the callback and release item
		pi.Callback()
		pi.Release()
	}
}

func (p *batchSink) Send(ctx context.Context, msg types.Message) error {
	var err error
	sr := &sendRequest{
		ctx: ctx,
		msg: msg,
	}
	p.internalSend(sr)

	return err
}

func (p *batchSink) Flush(ctx context.Context) error {
	wg := sync.WaitGroup{}
	wg.Add(1)

	fr := &flushRequest{&wg, nil}
	p.eventsChan <- fr

	wg.Wait()
	return fr.err
}

func (p *batchSink) Close() {
	if p.state != sinkBatchReady {
		// SinkBench is closing
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	cp := &closeRequest{&wg}
	p.eventsChan <- cp

	wg.Wait()
}
