package rulex

import (
	"context"
	"encoding/json"
	"fmt"
	metapb "github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/stream/types"
	"testing"
)

type Message = types.Message

var NewMessage = types.NewMessage
var (
	PubMessage   = MessageFromString(`{"matedata":{"x-stream-domain":"mdmp-test","x-stream-entity":"iotd-mock001","x-stream-method":"Publish","x-stream-qos":"0","x-stream-topic":"/mqtt-mock/benchmark/0","x-stream-version":"1.0"},"time":{},"raw_data":"MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMw=="}`)
	SubMessage   = MessageFromString(`{"matedata":{"x-stream-domain":"mdmp-test","x-stream-entity":"iotd-mock001","x-stream-method":"Subscribe","x-stream-version":"1.0"},"time":{},"raw_data":"eyJ0b3BpY19maWx0ZXJzIjpbeyJ0b3BpY19uYW1lIjoiLyMiLCJxb3MiOjB9XX0="}`)
	UnSubMessage = MessageFromString(`{"matedata":{"x-stream-domain":"mdmp-test","x-stream-entity":"iotd-mock001","x-stream-method":"Unsubscribe","x-stream-version":"1.0"},"time":{},"raw_data":"eyJ0b3BpY19maWx0ZXJzIjpbeyJ0b3BpY19uYW1lIjoiLyMiLCJxb3MiOjB9XX0="}`)

	JsonRaw = `{
}`
)

func init() {
	PubMessage = PubMessage.SetData([]byte(JsonRaw))
	PubMessage.SetDomain("aaaa")
	PubMessage.SetEntity("aaaa")
	PubMessage.SetEntitySource("aaaa")
}

func MessageFromString(s string) Message {
	m := NewMessage()
	if err := json.Unmarshal([]byte(s), m); err != nil {
		return nil
	} else {
		return m
	}
}
func MessageFrom(s []byte) Message {
	m := NewMessage()
	if err := json.Unmarshal(s, m); err != nil {
		return nil
	} else {
		return m
	}
}

func TestEndpoint_ExecRule(t *testing.T) {
	ctx := context.Background()
	e := &Endpoint{}
	request := &metapb.RuleExecRequest{
		UserId:   "1111",
		PacketId: 11111,
		Rule: &metapb.RuleQL{
			Id:   "1111",
			Body: []byte(`select
  params.netOut,
  params.schedulerStatus,
  abs(-3) as f_abs,
  cosh(30) as f_cosh,
  exp(2) as f_exp,
  concat('hello', 'world') as f_concat,
  floor(4.3) as f_float,
  log(2, 4) as f_log,
  substring('012345', -2, 2) as f_sub,
  substring('012345', 2) as f_sub1,
  substring('012345', 2.1) as f_sub11,
  substring('012345', 5) as f_sub2,
  substring('012345', -10) as f_sub3,
  lower(deviceId()) as f_lower,
  mod(3, 2) as f_mod,
  newuuid() as uuid,
  deviceId() as deviceId,
  updateTime() as updateTime
from
  /sys/iott-S0qqDzgFaq/iotd-6094b5cd-e3c8-429b-a771-734e6cebc56e/thing/property/+/post
`),
		},
		Message: PubMessage.(*metapb.ProtoMessage),
	}
	got, err := e.ExecRule(ctx, request)
	fmt.Println(err)
	fmt.Println("=====================")
	fmt.Println(got.Message)
	fmt.Println(got.ErrMsg)
	fmt.Println("=====================")
	/*

	*/
}
