package influxdb

import (
	"testing"

	"github.com/tkeel-io/rule-rulex/internal/types"
)

func TestInflux_Invoke(t *testing.T) {

	msg := types.NewMessage()
	msg.SetData([]byte(`{"id":"iotd-39eb858b-2842-4b5f-86cd-0e1fefbfdb8f","properties":{"telemetry":{"a1":{"ts":1657785821522,"value":1236},"a2":{"ts":1657785821522,"value":360},"a3":{"ts":1657785821522,"value":true},"abc":{"ts":1657785821522,"value":123}}}}`))
	t.Run("test influxdb", func(t *testing.T) {
	})

}

func TestInflux_parseOption(t *testing.T) {
	metadata := make(map[string]string)
	metadata["option"] = `{"tags":{"k1":"v1"}, "urls":[":tr2zy29F1Tusl8bkBwOR@10.96.180.167:8086/tkeel?core"]}`
	i := Influx{}
	i.metadata = metadata
	i.parseOption()
}
