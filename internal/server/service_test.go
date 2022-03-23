package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/tracing"
	"github.com/tkeel-io/rule-rulex/internal/conf"
	"github.com/tkeel-io/rule-rulex/internal/metrices"
	"github.com/tkeel-io/rule-rulex/internal/transport/grpc"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-util/stream"
	"github.com/uber/jaeger-lib/metrics/expvar"
	"testing"

	xstream "github.com/tkeel-io/rule-util/stream"
)

type Message = stream.Message

var NewMessage = stream.NewMessage
var PubMessage = MessageFromString(`{"matedata":{"x-stream-domain":"mdmp-test","x-stream-entity":"iotd-mock001","x-stream-method":"Publish","x-stream-qos":"0","x-stream-topic":"/mqtt-mock/benchmark/0","x-stream-version":"1.0"},"time":{},"raw_data":"MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMw=="}`)

var JsonRaw = []byte(`{
  "id": "58674812-7beb-42c0-b98b-9c218aa2cb10",
  "version": "v1.0",
  "metadata": {
    "entityId": "iotd-3dc45402-2318-4c26-a97a-1202b8b9b0a3",
    "modelId": "iott-dUukhoUgPr",
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
}`)

func MessageFromString(s string) Message {
	m := NewMessage()
	if err := json.Unmarshal([]byte(s), m); err != nil {
		return nil
	} else {
		return m
	}
}

func TestNew(t *testing.T) {
	cfg := conf.LoadTomlFile("./router-example.toml")
	ctx := context.Background()
	metrics.InitMetrics()
	metricsFactory := expvar.NewFactory(10) // 10 buckets for histograms
	logger, _ := log.InitLogger("IOTHub", "router")
	tracer := tracing.Init("router", metricsFactory, logger)
	s := New(ctx, cfg.Slot.SlotNum, &types.ServiceConfig{
		Client: grpc.NewClient(cfg),
		Tracer: tracer,
		Logger: logger,
	})
	s.Start(ctx)
	fmt.Println("Start")

	loopbackStream, err := xstream.OpenSink(types.LoopbackStream)
	if err != nil {
		panic(err)
	}
	fmt.Println("Loop")
	for {
		msg := PubMessage.Copy().SetData(JsonRaw).SetTopic("/sys/iott-opc-system/iotd-9897ba20-8452-4efb-8dc1-541fc952fd96/thing/event/property/post")
		loopbackStream.Send(ctx, msg)
	}

	fmt.Println("End")
}
