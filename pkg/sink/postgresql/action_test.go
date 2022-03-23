package postgresql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/pkg/sink"
	xutils "github.com/tkeel-io/rule-rulex/pkg/sink/utils"
	"github.com/smartystreets/gunit"
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
  "noticeList": "string",
  "deviceId":"xxxx"
}`

var matedata = map[string]string{ //root:123456@tcp(127.0.0.1:3306)/iot_manage?charset=utf8
	"version": `v1`,
	"option": `{
    "dbName": "iot_Manage",
    "fields": {
      "deviceid": {
        "type": "String",
        "value": "{{deviceId}}"
      },
      "netOut": {
        "type": "Int16",
        "value": "{{netOut.value}}"
      }
    },
    "table": "test",
    "urls": [
      "user=postgres dbname=iot_Manage password=123456 host=139.198.36.134 sslmode=disable"
    ]
  }`,
}

//postgresql://postgres:123456@139.198.36.134/iot_Manage?sslmode=require

func TestAction(t *testing.T) {
	//gunit.RunSequential(new(Action), t)
	log.InitLogger("mdmp", "rulex")
	gunit.Run(new(Action), t)

}

type Action struct {
	*gunit.Fixture
	ac     *postgresql
	ctx    types.ActionContent
	cancel context.CancelFunc
	msg    *xutils.JsonContext
	idx    int
}

func (this *Action) Setup() {

	sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	this.ctx = sink.NewContext(sctx)
	this.cancel = cancel
	this.ac = newPostgresql("postgresql", "iotd-1234")
	if err := this.ac.Setup(this.ctx, matedata); err != nil { //初始化postgresql
		log.Error("err:", logf.Error(err))
		log.Fatal("exit")
	}
	this.msg = xutils.NewJSONContext(rawMsg)

}

func (this *Action) genMessage(idx int) types.Message { //提供待插入的msg
	message := types.NewMessage()
	//var i int64 = 1579420383073
	//var j int64 = int64(this.idx*1000)
	//this.msg.SetValue("params.time", big.NewInt(i+j))
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

func TestGensql(t *testing.T) {
	p := &postgresql{
		option: &Option{
			Table: "Temp1",
		},
	}
	r := &execNode{
		fields: []string{"deviceId", "updateTime", "value"},
	}
	sql := p.genSql(r)
	fmt.Println("sql", sql)
}
