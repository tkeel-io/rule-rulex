package stateful

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/pkg/errors"
	"github.com/tkeel-io/rule-util/pkg/pprof"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"github.com/tkeel-io/rule-rulex/internal/runtime/rule/stream/functions"
	"github.com/tkeel-io/rule-util/stream"
	"testing"
	"time"
)

var (
	PubMessage   = MessageFromString(`{"matedata":{"x-stream-domain":"mdmp-test","x-stream-entity":"iotd-mock001","x-stream-method":"Publish","x-stream-qos":"0","x-stream-topic":"/mqtt-mock/benchmark/0","x-stream-version":"1.0"},"time":{},"raw_data":"MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMw=="}`)
	SubMessage   = MessageFromString(`{"matedata":{"x-stream-domain":"mdmp-test","x-stream-entity":"iotd-mock001","x-stream-method":"Subscribe","x-stream-version":"1.0"},"time":{},"raw_data":"eyJ0b3BpY19maWx0ZXJzIjpbeyJ0b3BpY19uYW1lIjoiLyMiLCJxb3MiOjB9XX0="}`)
	UnSubMessage = MessageFromString(`{"matedata":{"x-stream-domain":"mdmp-test","x-stream-entity":"iotd-mock001","x-stream-method":"Unsubscribe","x-stream-version":"1.0"},"time":{},"raw_data":"eyJ0b3BpY19maWx0ZXJzIjpbeyJ0b3BpY19uYW1lIjoiLyMiLCJxb3MiOjB9XX0="}`)
	jsonRaw1     = `{"id":"f2d5b10e-d946-4022-81fa-bbf0ac0cf4c0","version":"v1.0.0","type":"thing.property.post","metadata":{"entityId":"iotd-a98b6b52-6caa-456c-80cc-bb7613c4a787","modelId":"iott-v9zy5nvTz0","sourceId":["iotd-b3493702-3f41-4e1f-97ee-a8dadcaed17e"],"epochTime":1597302
189000},"params":{"cpuPercent":{"value":71,"time":1597302189000},"downRate":{"value":0.00008081887973013532,"time":1597302189000},"edgeVersion":{"value":"0.8","time":1597302189000},"hardware":{"value":"1C 1G 20G","time":1597302189000},"memUsage":{"value":23.187116781714142
,"time":1597302189000},"netIn":{"value":13262,"time":1597302189000},"netOut":{"value":2385,"time":1597302189000},"system":{"value":"centos 7.3.1611 3.10.0-514.10.2.el7.x86_64(linux)","time":1597302189000},"upRate":{"value":0.2998482750046526,"time":1597302189000}}}`

	jsonRaw2 = `{"id":"f2d5b10e-d946-4022-81fa-bbf0ac0cf4c0","version":"v1.0.0","type":"thing.property.post","metadata":{"entityId":"iotd-a98b6b52-6caa-456c-80cc-bb7613c4a787","modelId":"iott-v9zy5nvTz0","sourceId":["iotd-b3493702-3f41-4e1f-97ee-a8dadcaed17e"],"epochTime":1597302
189000},"params":{"diskUsage":{"value":10.068365853482936,"time":1597302189000},"downRate":{"value":0.00008081887973013532,"time":1597302189000},"edgeVersion":{"value":"0.8","time":1597302189000},"hardware":{"value":"1C 1G 20G","time":1597302189000},"memUsage":{"value":23
.187116781714142,"time":1597302189000},"netIn":{"value":13262,"time":1597302189000},"netOut":{"value":2385,"time":1597302189000},"system":{"value":"centos 7.3.1611 3.10.0-514.10.2.el7.x86_64(linux)","time":1597302189000},"upRate":{"value":0.2998482750046526,"time":1597302
189000}}}`

	jsonRaw = `{
  "id": "58674812-7beb-42c0-b98b-9c218aa2cb10",
  "version": "v1.0",
  "metadata": {
    "entity_id": "iotd-3dc45402-2318-4c26-a97a-1202b8b9b0a3",
    "model_id": "iott-dUukhoUgPr",
    "sourceId": [
      "iotd-3dc45402-2318-4c26-a97a-1202b8b9b0a3"
    ],
    "epochTime": 1596421859000
  },
  "type": "thing.property.post",
  "params": {
    "cpuPercent": {
      "value": 99.0,
      "time": 1596421859000,
      "status": "alarm"
    },
    "diskUsage": {
      "value": 82.6208122911928,
      "time": 1596421859000,
      "status": "ok"
    },
    "memUsage": {
      "value": 15.63534423157975,
      "time": 1596421859000,
      "status": "ok"
    },
    "netIn": {
      "value": 13060,
      "time": 1596421859000,
      "status": "ok"
    },
    "netOut": {
      "value": 28282,
      "time": 1596421859000,
      "status": "ok"
    },
    "upRate": {
      "value": 0.2986699336292639,
      "time": 1596421859000,
      "status": "ok"
    }
  }
}`

	jsonOutRaw = `{
  "id": "58674812-7beb-42c0-b98b-9c218aa2cb10",
  "version": "v1.0",
  "metadata": {
    "entity_id": "iotd-3dc45402-2318-4c26-a97a-1202b8b9b0a3",
    "model_id": "iott-dUukhoUgPr",
    "sourceId": [
      "iotd-3dc45402-2318-4c26-a97a-1202b8b9b0a3"
    ],
    "epochTime": 1596421859000
  },
  "type": "thing.property.post",
  "params": {
    "cpuPercent": {
      "avg": 99.99999999969785,
      "time": 1596421859000,
      "status": "alarm"
    },
    "diskUsage": {
      "avg: 82.6208122911928,
      "time": 1596421859000,
      "status": "ok"
    },
    "memUsage": {
      "value": 15.63534423157975,
      "time": 1596421859000,
      "status": "ok"
    },
    "netIn": {
      "value": 13060,
      "time": 1596421859000,
      "status": "ok"
    },
    "netOut": {
      "value": 28282,
      "time": 1596421859000,
      "status": "ok"
    },
    "upRate": {
      "value": 0.2986699336292639,
      "time": 1596421859000,
      "status": "ok"
    }
  }
}
`
	sql_simply = `select 1 as type from /sys/+/+/thing/property/platform/post `

	sql = `select sum(1) as count, 1 as type, 1 as target_type, '6 cpu\344\275\277\347\224\250\347\216\207 > 70% \347\241\254\347\233\230\344\275\277\347\224\250\347\216\207 > 60% ' as description, 
           'mid' as level, 
           deviceid() as target, 
           userid() as user_id, 
           '' as notice_list, 
           'index_11' as policy_id,  
           sum(params.cpuPercent.value) as params.cpuPercent.sum,
           avg(params.cpuPercent.value) as params.cpuPercent.avg, 
           params.cpuPercent.time,(avg(params.cpuPercent.value) > 70.000000) as params.cpuPercent.status, 
           avg(params.diskUsage.value) as params.diskUsage.avg, 
           params.diskUsage.time,(avg(params.diskUsage.value) > 60.000000) as params.diskUsage.status 
           from /sys/+/+/thing/property/platform/post 
           where avg(params.cpuPercent.value) > 70.000000 or 
           avg(params.diskUsage.value) > 60.000000 
           GROUP BY metadata.entityId, TUMBLINGWINDOW(ss, 1)`

	sql3 = `select
           avg(params.cpuPercent.value) as params.cpuPercent.avg,
           avg(params.memUsage.value) as params.memUsage.avg,
           sum(params.cpuPercent.value) as params.cpuPercent.sum,
           max(params.cpuPercent.value) as params.cpuPercent.max,
           min(params.cpuPercent.value) as params.cpuPercent.min,
           (avg(params.cpuPercent.value) > 58) as params.cpuPercent.status0,
           (avg(params.cpuPercent.value) > 48) as params.cpuPercent.status1,
           (avg(params.cpuPercent.value) > 38) as params.cpuPercent.status2
           from sys/+/+/thing/event/+/post
           where
               avg(params.cpuPercent.value) > 1
        
           GROUP BY metadata.entity_id, TUMBLINGWINDOW(ss, 2)`

	sql1 = `select
           sum(params.memUsage.value) as params.memUsage.sum,
           avg(params.cpuPercent.value) as params.cpuPercent.avg,
           sum(params.cpuPercent.value) as params.cpuPercent.sum,
           max(params.cpuPercent.value) as params.cpuPercent.max,
           min(params.cpuPercent.value) as params.cpuPercent.min,
           (avg(params.cpuPercent.value) > 60) as params.cpuPercent.status60,
           (avg(params.cpuPercent.value) > 50) as params.cpuPercent.status50,
           (avg(params.cpuPercent.value) > 40) as params.cpuPercent.status40
           from sys/+/+/thing/event/+/post
           where
           avg(params.cpuPercent.value) > 45 and
           sum(params.cpuPercent.value) > 80
           GROUP BY metadata.entity_id, TUMBLINGWINDOW(ss, 2)`

	sql2 = `select
			temperature as avg,
			mem as memavg,
			((temperature - 32) * 5 / 9.0) + '°C' as temperature.c
			from sys/+/+/thing/event/+/post
			where
				color = 'red' and
				temperature > 49`
)

func MessageFromString(s string) stream.Message {
	m := stream.NewMessage()
	if err := json.Unmarshal([]byte(s), m); err != nil {
		return nil
	} else {
		return m
	}
}

func TestBatchOp_New(t *testing.T) {

	defer pprof.WriteHeapProfile("Test_2")
	pprof.StartCPUProfile("Test_2")
	defer pprof.StopCPUProfile()
	pprof.StartPrefTrace("Test_2")
	defer pprof.StopPrefTrace()

	ctx := context.Background()
	s, _ := New(ctx, &v1.RuleQL{Body: []byte(sql)}, func(ctx context.Context, state interface{}) error {
		if state == nil {
			fmt.Println("Error", nil)
			return nil
		}
		msg, ok := state.(v1.PublishMessage)
		if !ok {
			return errors.New("Trigger Callback Type Error")
		}
		evalCtx := functions.NewMessageContext(msg.Copy().(stream.PublishMessage))
		expr, _ := ruleql.Parse(sql)
		fmt.Println("Output1", ruleql.EvalFilter(evalCtx, expr))
		fmt.Println("Output2", ruleql.EvalRuleQL(evalCtx, expr))
		return nil
	})

	fmt.Println("############")
	idx := 0
	go func() {
		for {
			select {
			case <-time.Tick(time.Millisecond * 10):
				idx++
				//ret := jsonRaw
				//v := ruleql.JSONNode(ret)
				//val := ruleql.IntNode(rand.Intn(100))
				//ret, err := v.Update("params.cpuPercent.value", val)
				//v = ruleql.JSONNode(ret)
				//ret, err = v.Update("params.cpuPercent.time", 100000+ruleql.IntNode(idx))
				//msg := PubMessage.Copy().SetData([]byte(ret))
				//err = s.Exce(ctx, msg)
				//fmt.Println("Invoke", err, val)
				if idx%2 == 0 {
					msg := PubMessage.Copy().SetData([]byte(jsonRaw1))
					vCtx := functions.NewMessageContext(msg.(stream.PublishMessage))
					err := s.Exce(ctx, vCtx, msg)
					if err != nil {
						fmt.Println("Invoke0", err)
					}
				} else {
					msg := PubMessage.Copy().SetData([]byte(jsonRaw2))
					vCtx := functions.NewMessageContext(msg.(stream.PublishMessage))
					err := s.Exce(ctx, vCtx, msg)
					if err != nil {
						fmt.Println("Invoke1", err)
					}
				}
			}
			//time.Sleep(1 * time.Millisecond)
			if idx%30 == 0 {
				fmt.Println("###time.Sleep")
				time.Sleep(2 * time.Second)
				fmt.Println("###time.Sleep done")
			}
		}
	}()
	time.Sleep(200 * time.Second)
	fmt.Println(idx)
}

func TestBatchOp_New2(t *testing.T) {
	ctx := context.Background()
	//evalCtx := ruleql.NewJSONContext(jsonRaw)
	s, _ := New(ctx, &v1.RuleQL{Body: []byte(sql)}, func(ctx context.Context, state interface{}) error {
		if state == nil {
			fmt.Println("TTTTT", nil)
			return nil
		}
		msg, ok := state.(v1.PublishMessage)
		if !ok {
			return errors.New("Trigger Callback Type Error")
		}
		evalCtx := functions.NewMessageContext(msg.Copy().(stream.PublishMessage))
		expr, _ := ruleql.Parse(sql)
		fmt.Println("Output1", ruleql.EvalFilter(evalCtx, expr))
		fmt.Println("Output2", ruleql.EvalRuleQL(evalCtx, expr))
		return nil
	})
	go func(ctx context.Context, s *StreamOperator) {
		idx := 0
		for {
			idx++
			if idx%2 == 0 {
				msg := PubMessage.Copy().SetData([]byte(jsonRaw1))
				vCtx := functions.NewMessageContext(msg.(stream.PublishMessage))
				err := s.Exce(ctx, vCtx, msg)
				fmt.Println("Invoke0", err)
			} else {
				msg := PubMessage.Copy().SetData([]byte(jsonRaw2))
				vCtx := functions.NewMessageContext(msg.(stream.PublishMessage))
				err := s.Exce(ctx, vCtx, msg)
				fmt.Println("Invoke1", err)
			}
			//if idx % 200 {
			//	break
			//}
		}
	}(ctx, s)
	go func(ctx context.Context, s *StreamOperator) {
		idx := 0
		for {
			idx++
			if idx%2 == 0 {
				msg := PubMessage.Copy().SetData([]byte(jsonRaw1))
				vCtx := functions.NewMessageContext(msg.(stream.PublishMessage))
				err := s.Exce(ctx, vCtx, msg)
				fmt.Println("Invoke0", err)
			} else {
				msg := PubMessage.Copy().SetData([]byte(jsonRaw2))
				vCtx := functions.NewMessageContext(msg.(stream.PublishMessage))
				err := s.Exce(ctx, vCtx, msg)
				fmt.Println("Invoke1", err)
			}
		}
	}(ctx, s)
	time.Sleep(2000 * time.Second)
}

func TestBatchOp_Benchmark(t *testing.T) {
	defer pprof.WriteHeapProfile("Test_2")
	pprof.StartCPUProfile("Test_2")
	defer pprof.StopCPUProfile()
	pprof.StartPrefTrace("Test_2")
	defer pprof.StopPrefTrace()

	ctx := context.Background()
	//evalCtx := ruleql.NewJSONContext(jsonRaw)
	expr, _ := ruleql.Parse(sql)
	s, _ := New(ctx, &v1.RuleQL{Body: []byte(sql)}, func(ctx context.Context, state interface{}) error {
		if state == nil {
			fmt.Println("TTTTT", nil)
			return nil
		}
		msg, ok := state.(v1.PublishMessage)
		if !ok {
			return errors.New("Trigger Callback Type Error")
		}
		evalCtx := functions.NewMessageContext(msg.Copy().(stream.PublishMessage))
		_ = ruleql.EvalFilter(evalCtx, expr)
		_ = ruleql.EvalRuleQL(evalCtx, expr)
		//fmt.Println("Output1", ruleql.EvalFilter(evalCtx, expr))
		//fmt.Println("Output2", ruleql.EvalRuleQL(evalCtx, expr))
		return nil
	})
	idx := 0
	go func() {
		for {
			idx++
			if idx%2 == 0 {
				msg := PubMessage.Copy().SetData([]byte(jsonRaw1))
				vCtx := functions.NewMessageContext(msg.(stream.PublishMessage))
				if err := s.Exce(ctx, vCtx, msg); err != nil {
					fmt.Println("Invoke0", err)
				}
			} else {
				msg := PubMessage.Copy().SetData([]byte(jsonRaw2))
				vCtx := functions.NewMessageContext(msg.(stream.PublishMessage))
				if err := s.Exce(ctx, vCtx, msg); err != nil {
					fmt.Println("Invoke1", err)
				}
			}
			//if idx % 200 {
			//	break
			//}
		}
	}()
	go func() {
		for {
			idx++
			if idx%2 == 0 {
				msg := PubMessage.Copy().SetData([]byte(jsonRaw1))
				vCtx := functions.NewMessageContext(msg.(stream.PublishMessage))
				if err := s.Exce(ctx, vCtx, msg); err != nil {
					fmt.Println("Invoke0", err)
				}
			} else {
				msg := PubMessage.Copy().SetData([]byte(jsonRaw2))
				vCtx := functions.NewMessageContext(msg.(stream.PublishMessage))
				if err := s.Exce(ctx, vCtx, msg); err != nil {
					fmt.Println("Invoke1", err)
				}
			}
			//if idx % 200 {
			//	break
			//}
		}
	}()
	time.Sleep(10 * time.Second)
	fmt.Println(idx)
}

type A struct {
	int
}

func Test_k(t *testing.T) {
	var (
		a = &A{1}
		b = &A{2}
		c = a
	)
	fmt.Println(a)
	fmt.Println(b)
	fmt.Println(c)

	fmt.Println(a == c)
	fmt.Println(b == c)
}
