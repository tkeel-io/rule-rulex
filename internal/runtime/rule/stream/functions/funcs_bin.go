package functions

import (
	"encoding/base64"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"github.com/tkeel-io/rule-util/stream"
	"strings"
)

func NewBinCallableFunc(message stream.PublishMessage) map[string]ruleql.ContextCallableFunc {
	return map[string]ruleql.ContextCallableFunc{
		//|payload\(textEncoding\)|返回设备发布消息的payload转义字符串。
		//
		//字符编码默认UTF-8，即`payload()`默认等价于`payload(‘utf-8’)`。 |
		//|to\_base64\(\*\)|当原始Payload数据为二进制数据时，可使用该函数，将所有二进制数据转换成base64String。|
		"to_base64": func(args ...ruleql.Node) ruleql.Node {
			if len(args) < 1 {
				return UNDEFINED_RESULT
			}
			encoded := base64.StdEncoding.EncodeToString([]byte(args[0].String()))
			return ruleql.StringNode(encoded)
		},
		//|messageId\(\)|返回物联网平台生成的消息ID。|
		"messageId": func(args ...ruleql.Node) ruleql.Node {
			return ruleql.StringNode(message.PacketIdentifier())
		},
		//|substring\(target, start, end\)|返回从start（包括）到end（不包括）的字符串。 参数说明：
		//
		//-   target：要操作的字符串。必填。
		//-   start：起始下标。必填。
		//-   end：结束下标。非必填。
		//
		//**说明：**
		//
		//-   目前，仅支持String和Int类型数据。Int类型数据会被转换成String类型后再处理。
		//-   常量字符串，请使用单引号。双引号中的数据，将视为Int类型数据进行解析。
		//-   如果传入的参数错误，例如参数类型不支持，会导致SQL解析失败，而放弃执行规则。
		//
		//字符串截取示例：
		//
		//-   substring\('012345', 0\) = "012345"
		//-   substring\('012345', 2\) = "2345"
		//-   substring\('012345', 2.745\) = "2345"
		//-   substring\(123, 2\) = "3"
		//-   substring\('012345', -1\) = "012345"
		//-   substring\(true, 1.2\) error
		//-   substring\('012345', 1, 3\) = "12"
		//-   substring\('012345', -50, 50\) = "012345"
		//-   substring\('012345', 3, 1\) = "" |
		//|to\_hex\(\*\)|当原始Payload数据为二进制数据时，可使用该函数，将二进制数据转换成十六进制字符串。|
		//"to_hex": func(args ...ruleql.Node) ruleql.Node {
		//	return ruleql.StringNode(encoded)
		//},
		"userid": func(args ...ruleql.Node) ruleql.Node {
			return ruleql.StringNode(message.Domain())
		},
		"str": func(args ...ruleql.Node) ruleql.Node {
			if len(args) == 1 {
				n, ok := args[0].(ruleql.JSONNode)
				if ok {
					snode := strings.ReplaceAll(string(n), "\"", "\\\"")
					snode = strings.ReplaceAll(string(snode), "\n", " ")
					return ruleql.StringNode(snode)
				}
			}
			return ruleql.UNDEFINED_RESULT
		},

		//|to\_hex\(\*\)|当原始Payload数据为二进制数据时，可使用该函数，将二进制数据转换成十六进制字符串。|
		//"to_hex": func(args ...ruleql.Node) ruleql.Node {
		//	return ruleql.StringNode(encoded)
		//},
	}
}
