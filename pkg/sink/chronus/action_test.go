package chronus

import (
	"context"
	"fmt"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/pkg/sink"
	xutils "github.com/tkeel-io/rule-rulex/pkg/sink/utils"
	"github.com/smartystreets/gunit"
	"testing"
	"time"
)

/*
-- auto-generated definition
create table equipment_alarm
(
  date         Date,
  timestamp    DateTime,
  device_id    String,
  tag_id       String,
  tag_value    String,
  data_quality String
)
  engine = MergeTree() PARTITION BY toYYYYMM(date) ORDER BY (device_id, tag_id, timestamp) SETTINGS index_granularity = 8192;
*/
var deviceId = "XXXX"
var rawMsg = `{
  "event_id": "message-OPC.1",   	"event_type": "info", 
  "event_name": "ABC",   			"event_level": "low",
  "create_time": 1576737423000,   	"message": "124",
  "value": {
    "key1": "val1", 			    "key2": val2
  },
  "netOut": {
    "value": 111
  },
  "source": ["iotd-aaa"],
  "noticeList": "string"
}`

//var matedata = map[string]string{
//	"option": `{
//		"urls": ["tcp://139.198.18.173:9089?username=default&password=qingcloud2019&database=iot_manage_dev"],
//		"dbName":"iot_manage_dev",
//		"table": "table_test"
//	}`,
//	"fields": `{
//		"user_id": "{{event_name}}",
//		"device_id": "{{event_level}}",
//		"event_id": "{{event_id}}",
//		"source_id": "{{event_type}}",
//		"identifier": "{{tag_id}}",
//		"type": "{{source}}",
//		"thing_id": "{{value}}",
//		"metadata": "{{event_name}}",
//		"time": "{{create_time}}",
//		"action_time": "{{create_time}}",
//		"action_date": "{{create_time}}"
//	}`,
//	"fieldTypes": `{
//		"user_id": "String",
//		"device_id": "String",
//		"event_id": "String",
//		"source_id": "String",
//		"identifier": "String",
//		"type": "String",
//		"thing_id": "String",
//		"metadata": "String",
//		"time": "UInt64",
//		"action_time": "DateTime",
//		"action_date": "Date"
//	}`,
//}
var matedata = map[string]string{
	"version": `v1`,
	"option": `{
    "dbName": "default",
    "fields": {
      "deviceid": {
        "type": "String",
        "value": "{{deviceid}}"
      },
      "netOut": {
        "type": "Int16",
        "value": "{{netOut.value}}"
      }
    },
    "table": "test_ccg3",
    "urls": [
      "http://default:mdmp2019@139.198.186.46:18123"
    ]
  }`,
}

func TestAction(t *testing.T) {
	log.InitLogger("mdmp", "rulex")
	//gunit.RunSequential(new(Action), t)
	gunit.Run(new(Action), t)
}

type Action struct {
	*gunit.Fixture
	ac     *chronus
	ctx    types.ActionContent
	cancel context.CancelFunc
	msg    *xutils.JsonContext
	idx    int
}

func (this *Action) Setup() {
	sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	this.ctx = sink.NewContext(sctx)
	this.cancel = cancel
	this.ac = newClickhouse("chronus", "iotd-1234")
	if err := this.ac.Setup(this.ctx, matedata); err != nil {
		log.Error("err:", logf.Error(err))
		log.Fatal("exit")
	}
	this.msg = xutils.NewJSONContext(rawMsg)
}

func (this *Action) genMessage(idx int) types.Message {
	message := types.NewMessage()
	//this.msg.SetValue("params.time", int64(1579420383073+this.idx*1000))
	//this.msg.SetValue("params.value.val", rand.Float32())

	message.SetTopic(fmt.Sprintf("/sys/xxxxx/%s/thing/event/property/post", deviceId))
	message.SetData([]byte(this.msg.String()))
	return message
}

func (this *Action) Test() {
	fmt.Println("+", this.ac.Close())
	if err := this.ac.Setup(this.ctx, matedata); err != nil {
		log.Error("err:", logf.Error(err))
		log.Fatal("exit")
	}
	fmt.Println("+", this.ac.Close())
	if err := this.ac.Setup(this.ctx, matedata); err != nil {
		log.Error("err:", logf.Error(err))
		log.Fatal("exit")
	}
	fmt.Println("+", this.ac.Close())
	if err := this.ac.Setup(this.ctx, matedata); err != nil {
		log.Error("err:", logf.Error(err))
		log.Fatal("exit")
	}
	fmt.Println("+", this.ac.Close())
	if err := this.ac.Setup(this.ctx, matedata); err != nil {
		log.Error("err:", logf.Error(err))
		log.Fatal("exit")
	}
	fmt.Println("+", this.ac.Close())
	if err := this.ac.Setup(this.ctx, matedata); err != nil {
		log.Error("err:", logf.Error(err))
		log.Fatal("exit")
	}
	ts := time.Now()
	i := 0
	for {
		i++
		if err := this.ac.Invoke(this.ctx, this.genMessage(i)); err != nil {
			log.Fatal("err:", logf.Error(err))
		}
		time.Sleep(time.Second)
	}
	//fmt.Println("~~~", time.Now().Sub(ts))
	//time.Sleep(5 * time.Second)
	this.ac.Close()
	fmt.Println("~~~", time.Now().Sub(ts))
}


func Test_fillExecNode(t *testing.T) {
	type args struct {
		name string
		typ  string
		val  interface{}
		data *execNode
	}
	var tests = []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"t1", args{"aaa", "Int8", int64(1), &execNode{}}, false},
		{"t1", args{"aaa", "UInt8", int64(1), &execNode{}}, false},
		{"t1", args{"aaa", "Int16", int64(1), &execNode{}}, false},
		{"t1", args{"aaa", "UInt16", int64(1), &execNode{}}, false},
		{"t1", args{"aaa", "Int32", int64(1), &execNode{}}, false},
		{"t1", args{"aaa", "UInt32", int64(1), &execNode{}}, false},
		{"t1", args{"aaa", "Int64", int64(1), &execNode{}}, false},
		{"t1", args{"aaa", "UInt64", int64(1), &execNode{}}, false},
		{"t1", args{"aaa", "Float32", int64(1), &execNode{}}, false},
		{"t1", args{"aaa", "Float64", int64(1), &execNode{}}, false},
		{"t1", args{"aaa", "Int8", float64(1), &execNode{}}, false},
		{"t1", args{"aaa", "UInt8", float64(1), &execNode{}}, false},
		{"t1", args{"aaa", "Int16", float64(1), &execNode{}}, false},
		{"t1", args{"aaa", "UInt16", float64(1), &execNode{}}, false},
		{"t1", args{"aaa", "Int32", float64(1), &execNode{}}, false},
		{"t1", args{"aaa", "UInt32", float64(1), &execNode{}}, false},
		{"t1", args{"aaa", "Int64", float64(1), &execNode{}}, false},
		{"t1", args{"aaa", "UInt64", float64(1), &execNode{}}, false},
		{"t1", args{"aaa", "Float32", float64(1), &execNode{}}, false},
		{"t1", args{"aaa", "Float64", float64(1), &execNode{}}, false},
		{"t1", args{"aaa", "Float64", "", &execNode{}}, false},
		{"t1", args{"aaa", "Float64", "", &execNode{}}, false},
		{"t1", args{"aaa", "Float64", nil, &execNode{}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := fillExecNode(tt.args.val, tt.args.typ, tt.args.data, tt.args.name); (err != nil) != tt.wantErr {
				t.Errorf("fillExecNode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
