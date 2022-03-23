package utils

import (
	"context"
	"fmt"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"sync"
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	//gunit.Run(new(Queue), t)
	gunit.RunSequential(new(Queue), t)
}

var COUNT = 2000000
//var INTERVAL = time.Millisecond * 10
//var INTERVAL = time.Nanosecond * 10

type Queue struct {
	*gunit.Fixture
	ctx context.Context
}

func (this *Queue) Setup() {
	var messages []types.Message
	messages = nil
	messages = append(messages, types.NewMessage())

	initLog()
	this.ctx = context.Background()
}

func initLog() {
	//logger, level := log.NewDevelopment(
	logger, _ := log.NewDevelopment(
		logf.Fields(logf.String("service", "pipe2device")),
		logf.AddStacktrace(log.FatalLevel),
	)
	//level.SetLevel(log.DebugLevel)
	log.SetGlobalLogger(logger)
}

func (this *Queue) TestBasic() {
	termNum := 0
	msgCnt := 0
	wg := &sync.WaitGroup{}
	queue := NewQueue(this.ctx, "test", 1000, 10, INTERVAL, func(msgs []types.Message) (err error) {
		termNum++
		for _, msg := range msgs {
			_ = msg
			msgCnt ++
			//fmt.Println(
			//	fmt.Sprintf("CallBack[%d]:%s", termNum, msg.PacketIdentifier()))
		}
		return nil
	})

	wg.Add(COUNT)
	go func() {
		for i := 0; i < COUNT; i++ {
			msg := types.NewMessage().SetPacketIdentifier(fmt.Sprintf("%d", i))
			queue.Push(msg)
			wg.Done()
			//fmt.Println(
			//	fmt.Sprintf("Push[%d]:%s", termNum, msg.PacketIdentifier()))
			time.Sleep(INTERVAL)
		}
	}()
	wg.Wait()
	queue.Close()
	this.So(msgCnt, should.Equal, COUNT)
}

func (this *Queue) TestSlowProcess() {
	termNum := 0
	msgCnt := 0
	wg := &sync.WaitGroup{}
	queue := NewQueue(this.ctx, "test", 1000, 3, 4*INTERVAL, func(msgs []types.Message) (err error) {
		termNum++
		for _, msg := range msgs {
			_ = msg
			msgCnt++
			//fmt.Println(
			//	fmt.Sprintf("CallBack[%d]:%s", termNum, msg.PacketIdentifier()))
		}
		time.Sleep(INTERVAL * 1)
		return nil
	})

	wg.Add(COUNT)
	go func() {
		for i := 0; i < COUNT; i++ {
			msg := types.NewMessage().SetPacketIdentifier(fmt.Sprintf("%d", i))
			queue.Push(msg)
			wg.Done()
			//fmt.Println(
			//	fmt.Sprintf("Push[%d]:%s", termNum, msg.PacketIdentifier()))
		}
	}()
	wg.Wait()
	queue.Flush()
	this.So(msgCnt, should.Equal, COUNT)
	queue.Close()
	this.So(msgCnt, should.Equal, COUNT)
}

func (this *Queue) TestFlushSlowProcess() {
	termNum := 0
	msgCnt := 0
	wg := &sync.WaitGroup{}
	queue := NewQueue(this.ctx, "test", 1000, 3, 4*INTERVAL, func(msgs []types.Message) (err error) {
		termNum++
		for _, msg := range msgs {
			_ = msg
			msgCnt++
			//fmt.Println(
			//	fmt.Sprintf("CallBack[%d]:%s", termNum, msg.PacketIdentifier()))
		}
		time.Sleep(INTERVAL * 1)
		return nil
	})

	wg.Add(COUNT)
	go func() {
		for i := 0; i < COUNT; i++ {
			msg := types.NewMessage().SetPacketIdentifier(fmt.Sprintf("%d", i))
			queue.Push(msg)
			wg.Done()
			time.Sleep(INTERVAL * 10)
		}
	}()
	go func(){
		time.Sleep(3 * time.Second)
		queue.Flush()
	}()
	wg.Wait()
	queue.Flush()
	this.So(msgCnt, should.Equal, COUNT)
	queue.Close()
	this.So(msgCnt, should.Equal, COUNT)
}
