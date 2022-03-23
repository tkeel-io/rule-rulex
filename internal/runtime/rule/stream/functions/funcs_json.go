package functions

import (
	"encoding/base64"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-util/stream"
	"github.com/google/uuid"
	"strings"
	"time"
)

func NewJsonCallableFunc(message stream.PublishMessage) map[string]ruleql.ContextCallableFunc {
	return map[string]ruleql.ContextCallableFunc{
		/***************/
		"ruleId": func(args ...ruleql.Node) ruleql.Node {
			return ruleql.StringNode(message.Attr(types.MATE_RULE_ID))
		},
		"ruleBody": func(args ...ruleql.Node) ruleql.Node {
			return ruleql.StringNode(message.Attr(types.MATE_RULE_BODY))
		},
		//"userid": func(args ...ruleql.Node) ruleql.Node {
		//	return ruleql.StringNode(message.Domain())
		//},
		"deviceid": func(args ...ruleql.Node) ruleql.Node {
			return ruleql.StringNode(message.Entity())
		},
		"deviceId": func(args ...ruleql.Node) ruleql.Node {
			return ruleql.StringNode(message.Entity())
		},
		"updateTime": func(args ...ruleql.Node) ruleql.Node {
			now := time.Now()
			return ruleql.IntNode(now.Unix())
		},
		//"topic": func(args ...ruleql.Node) ruleql.Node {
		//	if len(args) == 0 {
		//		return ruleql.StringNode(message.Topic())
		//	}
		//	if len(args) == 1 {
		//		offset, ok1 := args[0].(ruleql.IntNode)
		//		if ok1 {
		//			offset := int(offset)
		//			arr := strings.Split(message.Topic(), "/")
		//			if offset >= 0 && offset < len(arr) {
		//				return ruleql.StringNode(arr[offset])
		//			} else {
		//				return ruleql.StringNode("")
		//			}
		//
		//		}
		//	}
		//	return ruleql.StringNode("")
		//},
		//"str": func(args ...ruleql.Node) ruleql.Node {
		//	if len(args) == 1 {
		//		n, ok := args[0].(ruleql.JSONNode)
		//		if ok {
		//			snode := strings.ReplaceAll(string(n), "\"", "\\\"")
		//			snode = strings.ReplaceAll(string(snode), "\n", " ")
		//			return ruleql.StringNode(snode)
		//		}
		//	}
		//	return ruleql.UNDEFINED_RESULT
		//},
		"timeFormat": func(args ...ruleql.Node) ruleql.Node {
			if len(args) == 2 {
				ts, ok1 := args[0].(ruleql.IntNode)
				format, ok2 := args[1].(ruleql.StringNode)
				if ok1 && ok2 {
					t := time.Unix(int64(ts), 0)
					s := t.Format(format.String())
					return ruleql.StringNode(s)
				}
			}
			return ruleql.UNDEFINED_RESULT
		},
		"timestamp": func(args ...ruleql.Node) ruleql.Node {
			if len(args) == 0 {
				now := time.Now()
				return ruleql.IntNode(now.UnixNano()/1000/1000)
			}
			if len(args) == 1 {
				format, ok1 := args[0].(ruleql.StringNode)
				if ok1 {
					now := time.Now()
					s := now.Format(format.String())
					return ruleql.StringNode(s)
				}
			}
			return ruleql.UNDEFINED_RESULT
		},
		/***************/
		//|abs\(number\)|返回绝对值。|
		"abs": warp("abs"),
		//|asin\(number\)|返回number值的反正弦。|
		"asin": warp("asin"),
		//|asin\(number\)|返回number值的反正弦。|
		"acos": warp("acos"),
		//|concat\(string1, string2\)|用于连接字符串。返回连接后的字符串。 示例：`concat(field,'a')`。 |
		"concat": func(args ...ruleql.Node) ruleql.Node {
			if len(args) < 1 {
				return UNDEFINED_RESULT
			}
			if len(args) < 2 {
				return args[0].To(ruleql.String)
			}
			return ruleql.StringNode(args[0].String() + args[1].String())
		},
		//|cos\(number\)|返回number值的余弦。|
		"cos": warp("cos"),
		//|cosh\(number\)|返回number值的双曲余弦（hyperbolic cosine）。|
		"cosh": warp("cosh"),
		//|crypto\(field,String\)|对field的值进行加密。
		//
		//第二个参数String为算法字符串。可选：MD2、MD5、SHA1、SHA-256、SHA-384、SHA-512。 |
		//|deviceName\(\)|返回当前设备名称。使用SQL调试时，因为没有真实设备，返回值为空。|
		"deviceName": func(args ...ruleql.Node) ruleql.Node {
			return ruleql.StringNode(message.Entity())
		},
		//|endswith\(input, suffix\)|判断input的值是否以suffix结尾。|
		"endswith": func(args ...ruleql.Node) ruleql.Node {
			if len(args) < 2 {
				return UNDEFINED_RESULT
			}
			return ruleql.BoolNode(strings.HasSuffix(args[0].String(), args[1].String()))
		},
		"startswith": func(args ...ruleql.Node) ruleql.Node {
			if len(args) < 2 {
				return UNDEFINED_RESULT
			}
			return ruleql.BoolNode(strings.HasPrefix(args[0].String(), args[1].String()))
		},
		//|exp\(number\)|返回以自然常数e为底的指定次幂。|
		"exp": warp("exp"),
		//|floor\(number\)|返回一个最接近它的整数，它的值小于或等于这个浮点数。|
		"floor": warp("floor"),
		//|log\(n, m\)|返回自然对数。
		"log": warp("log"),
		//
		//如果不传m值，则返回`log(n)`。 |
		//|lower\(string\)|返回小写字符串。|
		"lower": func(args ...ruleql.Node) ruleql.Node {
			if len(args) < 1 {
				return UNDEFINED_RESULT
			}
			return ruleql.StringNode(strings.ToLower(args[0].String()))
		},
		//|mod\(n, m\)|n%m余数。|
		"mod": warp("mod"),
		//|nanvl\(value, default\)|返回属性值。
		//
		//若属性值为null，则返回default。 |
		//|newuuid\(\)|返回一个随机UUID字符串。|
		"newuuid": func(args ...ruleql.Node) ruleql.Node {
			return ruleql.StringNode(uuid.New().String())
		},
		//|payload\(textEncoding\)|返回设备发布消息的payload转义字符串。
		//
		//字符编码默认UTF-8，即`payload()`默认等价于`payload(‘utf-8’)`。 |
		//|power\(n,m\)|返回n的m次幂。|
		"power": warp("power"),
		//|rand\(\)|返回\[0~1\)之间的一个随机数。|
		"rand": warp("rand"),
		//|replace\(source, substring, replacement\)|对某个目标列值进行替换，即用replacement替换source中的substring。
		//
		//示例：`replace(field,’a’,’1’)`。 |
		"replace": func(args ...ruleql.Node) ruleql.Node {
			if len(args) < 3 {
				return UNDEFINED_RESULT
			}
			return ruleql.StringNode(strings.ReplaceAll(args[0].String(), args[1].String(), args[2].String()))
		},
		//|sin\(n\)|返回n值的正弦。|
		"sin": warp("sin"),
		//|sinh\(n\)|返回n值的双曲正弦（hyperbolic sine）。|
		"sinh": warp("sinh"),
		//|tan\(n\)|返回n值的正切。|
		"tan": warp("tan"),
		//|tanh\(n\)|返回n值的双曲正切（hyperbolic tangent）。|
		"tanh": warp("tanh"),
		//|thingPropertyFlatMap\(property\)|获取物模型属性对应数值，并去掉item层级，value有多个时，使用“\_”拼接。当物模型属性多于50个时，云产品流转无法流转全量物模型属性，使用该函数可以拉平物模型属性结构，实现全量物模型属性流转。 property为需要获取的属性，可以传入多个property，为空则表示提取所有属性。
		//
		//示例：使用`thingPropertyFlatMap('Power', 'Position')`，将在消息体中添加`"Power": "On", "Position_latitude": 39.9, "Position_longitude": 116.38` |
		//|timestamp\(format\)|返回当前系统时间，格式化后返回当前地域的时间。
		//
		//|topic\(number\)|返回Topic分段信息。
		//
		//例如，Topic：/alDbcLe\*\*\*\*/TestDevice/user/set。使用函数`topic()`，则返回整个Topic；使用`topic(1)`，则返回Topic的第一级类目`alDbcLe****`；使用`topic(2)`，则返回第二级类目`TestDevice`，以此类推。 |
		"topic": func(args ...ruleql.Node) ruleql.Node {
			if len(args) == 0 {
				return ruleql.StringNode(message.Topic())
			}
			if len(args) == 1 {
				offset, ok1 := args[0].(ruleql.IntNode)
				if ok1 {
					offset := int(offset)
					arr := strings.Split(message.Topic(), "/")
					if offset >= 0 && offset < len(arr) {
						return ruleql.StringNode(arr[offset])
					} else {
						return ruleql.StringNode("")
					}
				}
			}
			return ruleql.StringNode("")
		},
		//|upper\(string\)|将字符串中的小写字母转为大写字母。 示例：函数`upper(alibaba)`的返回结果是`ALIBABA` |
		"upper": func(args ...ruleql.Node) ruleql.Node {
			if len(args) < 1 {
				return UNDEFINED_RESULT
			}
			return ruleql.StringNode(strings.ToUpper(args[0].String()))
		},
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
		"substring": func(args ...ruleql.Node) ruleql.Node {
			var (
				str        string
				start, end int64
				err        error
			)

			switch len(args) {
			case 2:
				str = args[0].String()
				start, err = toInt64(args[1].To(ruleql.Int))
				if err != nil {
					return UNDEFINED_RESULT
				}
				end = int64(len(str))
			case 3:
				str = args[0].String()
				start, err = toInt64(args[1].To(ruleql.Int))
				if err != nil {
					return UNDEFINED_RESULT
				}
				end, err = toInt64(args[2].To(ruleql.Int))
				if err != nil {
					return UNDEFINED_RESULT
				}
			default:
				return UNDEFINED_RESULT
			}

			if len(str) < int(end) {
				return UNDEFINED_RESULT
			}
			if start < 0 {
				start = 0
			}
			return ruleql.StringNode(str[start:end])
		},
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
	}
}
