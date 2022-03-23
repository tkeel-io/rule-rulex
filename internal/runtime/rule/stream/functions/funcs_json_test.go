package functions

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/tkeel-io/rule-util/stream"
	"reflect"
	"testing"
	"time"
)

func Example() {
	msg := "Hello, 世界"
	encoded := base64.StdEncoding.EncodeToString([]byte(msg))
	fmt.Println(encoded)
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		fmt.Println("decode error:", err)
		return
	}
	fmt.Println(string(decoded))
	// Output:
	// SGVsbG8sIOS4lueVjA==
	// Hello, 世界
}

var payload = `{
  "id": "29320317-8e02-4363-bd38-cc907cd8bc39",
  "version": "v1.0.0",
  "type": "thing.property.post",
  "metadata": {
    "entityId": "iotd-2f94b970-fca0-4065-8f10-4128cf8f1566",
    "modelId": "iott-S0qqDzgFaq",
    "sourceId": [
      "iotd-2f94b970-fca0-4065-8f10-4128cf8f1566"
    ],
    "epochTime": 1608725652000
  },
  "params": {
    "Location": {
      "value": "Floor",
      "time": 1608725652000
    },
    "Temperature": {
      "value": 10,
      "time": 1608725652000
    },
    "Humidity": {
      "value": 45,
      "time": 1608725652000
    }
  }
}`

func TestNewJsonCallableFunc(t *testing.T) {
	byt, _ := json.Marshal(payload)
	msg := PubMessage.SetTopic("/abc/def/12345/67890")
	msg = msg.SetDomain("domain001").
		SetEntity("entity001").
		SetData(byt)
	msgCtx := NewMessageContext(msg.(stream.PublishMessage))
	now := time.Now()
	tests := []struct {
		name string
		expr string
		want string
	}{
		//{"11", "deviceid()", "entity001"},
		//{"12", "deviceId()", "entity001"},
		//{"13", "updateTime()", fmt.Sprintf("%v", now.Unix())},
		{"11", "timestamp()", fmt.Sprintf("%v", now.Unix())},
		//{"11", "abs(1)", "1"},
		//{"11", "abs(-1)", "1"},
		{"11", "asin(1)", "1.570796"},
		{"11", "asin(0)", "0.000000"},
		{"11", "asin(-1)", "-1.570796"},
		{"11", "sin(90)", "0.893997"},
		{"11", "sin(3.1415926/2)", "1.000000"},
		{"11", "concat('aaabbbccc', 'ccc', 'ddd')", "aaabbbcccccc"},
		{"11", "cos(0)", "1.000000"},
		{"11", "cos(0.5)", "0.877583"},
		{"11", "acos(0.877583)", "0.499999"},
		{"11", "cosh(0.877583)", "1.410433"},
		{"11", "deviceName()", "entity001"},
		{"11", "endswith('aaabbbccc', 'aaa')", "false"},
		{"11", "endswith('aaabbbccc', 'ccc')", "true"},
		{"11", "exp(1)", "2.718282"},
		{"11", "floor(100.1)", "100"},
		{"11", "log(100)", "2.000000"},
		{"11", "lower('aaABbBCcc')", "aaabbbccc"},
		{"11", "mod(101,10)", "1"},
		{"11", "newuuid()", "cd3d99f5-3f19-413d-9061-cc27d9bf70a8"},
		{"11", "startswith('aaabbbccc', 'aaa')", "true"},
		{"11", "startswith('aaabbbccc', 'ccc')", "false"},
		{"11", "to_base64('aaabbbccc')", "YWFhYmJiY2Nj"},
		{"11", "replace('aaabbbccc', 'ccc', 'ddd')", "aaabbbddd"},
		{"11", "replace('aaabbbccc', 'ccc', 'ddd')", "aaabbbddd"},
		{"11", "replace('aaabbbccc', 'ccc', 'ddd')", "aaabbbddd"},
		{"11", "replace('aaabbbccc', 'ccc', 'ddd')", "aaabbbddd"},
		{"11", "replace('aaabbbccc', 'ccc', 'ddd')", "aaabbbddd"},
		{"11", "replace('aaabbbccc', 'ccc', 'ddd')", "aaabbbddd"},
		{"11", "replace('aaabbbccc', 'ccc', 'ddd')", "aaabbbddd"},

	}
	//| timestamp(format)                       | 返回当前系统时间，格式化后返回当前地域的时间。format为可选。如果为空，则返回当前系统时间戳毫秒值，例如，使用`timestamp()`，返回的时间戳为`1543373798943`；如果指定format，则根据指定格式，返回当前系统时间，如`timestamp('yyyy-MM-dd\'T\'HH:mm:ss\'Z\'')`，返回的时间戳为`2018-11-28T10:56:38Z`。 |

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := byteExecute(msgCtx, []byte(tt.expr)); !reflect.DeepEqual(string(got), tt.want) {
				t.Errorf("(%v) = %v, want %v", string(tt.expr), string(got), tt.want)
			}
		})
	}
}
