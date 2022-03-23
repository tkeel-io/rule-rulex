---
keyword: [物联网, 物联网平台, IoT, 规则引擎, 流转数据, 发送数据, 数据流转规则, SQL, 筛选数据, 函数]
---

# 函数列表

规则引擎提供多种函数，您可以在编写SQL时使用这些函数，实现多样化数据处理。

## 数据流转支持的函数


## Math函数
|函数名|函数说明|
|:--|:---|
|abs\(number\)|返回绝对值。|
|asin\(number\)|返回number值的反正弦。|
|concat\(string1, string2\)|用于连接字符串。返回连接后的字符串。 示例：`concat(field,'a')`。 |
|cos\(number\)|返回number值的余弦。|
|cosh\(number\)|返回number值的双曲余弦（hyperbolic cosine）。|
|crypto\(field,String\)|对field的值进行加密。

第二个参数String为算法字符串。可选：MD2、MD5、SHA1、SHA-256、SHA-384、SHA-512。 |
|deviceName\(\)|返回当前设备名称。使用SQL调试时，因为没有真实设备，返回值为空。|
|endswith\(input, suffix\)|判断input的值是否以suffix结尾。|
|exp\(number\)|返回以自然常数e为底的指定次幂。|
|floor\(number\)|返回一个最接近它的整数，它的值小于或等于这个浮点数。|
|log\(n, m\)|返回自然对数。

如果不传m值，则返回`log(n)`。 |
|lower\(string\)|返回小写字符串。|
|mod\(n, m\)|n%m余数。|
|nanvl\(value, default\)|返回属性值。

若属性值为null，则返回default。 |
|newuuid\(\)|返回一个随机UUID字符串。|
|payload\(textEncoding\)|返回设备发布消息的payload转义字符串。

字符编码默认UTF-8，即`payload()`默认等价于`payload(‘utf-8’)`。 |
|power\(n,m\)|返回n的m次幂。|
|rand\(\)|返回\[0~1\)之间的一个随机数。|
|replace\(source, substring, replacement\)|对某个目标列值进行替换，即用replacement替换source中的substring。

示例：`replace(field,’a’,’1’)`。 |
|sin\(n\)|返回n值的正弦。|
|sinh\(n\)|返回n值的双曲正弦（hyperbolic sine）。|
|tan\(n\)|返回n值的正切。|
|tanh\(n\)|返回n值的双曲正切（hyperbolic tangent）。|
|thingPropertyFlatMap\(property\)|获取物模型属性对应数值，并去掉item层级，value有多个时，使用“\_”拼接。当物模型属性多于50个时，云产品流转无法流转全量物模型属性，使用该函数可以拉平物模型属性结构，实现全量物模型属性流转。 property为需要获取的属性，可以传入多个property，为空则表示提取所有属性。

示例：使用`thingPropertyFlatMap('Power', 'Position')`，将在消息体中添加`"Power": "On", "Position_latitude": 39.9, "Position_longitude": 116.38` |
|timestamp\(format\)|返回当前系统时间，格式化后返回当前地域的时间。

format为可选。如果为空，则返回当前系统时间戳毫秒值，例如，使用`timestamp()`，返回的时间戳为`1543373798943`；如果指定format，则根据指定格式，返回当前系统时间，如`timestamp('yyyy-MM-dd\'T\'HH:mm:ss\'Z\'')`，返回的时间戳为`2018-11-28T10:56:38Z`。 |
|timestamp\_utc\(format\)|返回当前系统时间，格式化后返回UTC时间。 format为可选。如果format为空，则返回当前系统时间戳毫秒值，例如，使用`timestamp_utc()`，返回的时间戳为`1543373798943`；使用`timestamp_utc('yyyy-MM-dd\'T\'HH:mm:ss\'Z\'')`，返回的时间戳为`2018-11-28T02:56:38Z`。 |
|topic\(number\)|返回Topic分段信息。

例如，Topic：/alDbcLe\*\*\*\*/TestDevice/user/set。使用函数`topic()`，则返回整个Topic；使用`topic(1)`，则返回Topic的第一级类目`alDbcLe****`；使用`topic(2)`，则返回第二级类目`TestDevice`，以此类推。 |
|upper\(string\)|将字符串中的小写字母转为大写字母。 示例：函数`upper(alibaba)`的返回结果是`ALIBABA` |
|to\_base64\(\*\)|当原始Payload数据为二进制数据时，可使用该函数，将所有二进制数据转换成base64String。|
|messageId\(\)|返回物联网平台生成的消息ID。|
|substring\(target, start, end\)|返回从start（包括）到end（不包括）的字符串。 参数说明：

-   target：要操作的字符串。必填。
-   start：起始下标。必填。
-   end：结束下标。非必填。

**说明：**

-   目前，仅支持String和Int类型数据。Int类型数据会被转换成String类型后再处理。
-   常量字符串，请使用单引号。双引号中的数据，将视为Int类型数据进行解析。
-   如果传入的参数错误，例如参数类型不支持，会导致SQL解析失败，而放弃执行规则。

字符串截取示例：

-   substring\('012345', 0\) = "012345"
-   substring\('012345', 2\) = "2345"
-   substring\('012345', 2.745\) = "2345"
-   substring\(123, 2\) = "3"
-   substring\('012345', -1\) = "012345"
-   substring\(true, 1.2\) error
-   substring\('012345', 1, 3\) = "12"
-   substring\('012345', -50, 50\) = "012345"
-   substring\('012345', 3, 1\) = "" |
|to\_hex\(\*\)|当原始Payload数据为二进制数据时，可使用该函数，将二进制数据转换成十六进制字符串。|

## 使用示例

您可以在数据流转SQL语句的SELECT字段和WHERE条件字段中，使用函数获取数据或者对数据做处理。

例如，某温度感应器产品有一个温度属性（Temperature），设备上报的温度属性数据经物模型转化后的数据格式如下：

```
{
    "deviceType": "Custom",
    "iotId": "H5KURkKdibnZvSls****000100",
    "productKey": "a1HHrkm****",
    "gmtCreate": 1564569974224,
    "deviceName": "TestDevice1",
    "items": {
        "Temperature": {
            "value": 23.5,
            "time": 1564569974229
        }
    }
}
```

该温度感应器产品下有多个设备，但只有当设备TestDevice1、TestDevice2或TestDevice3上报的温度大于38时，需将温度属性数据流转到函数计算中进行计算处理。设置筛选设备上报数据的规则SQL如下图。

![编写SQL](https://static-aliyun-doc.oss-cn-hangzhou.aliyuncs.com/assets/img/zh-CN/7570470061/p168910.png)

SQL语句：

```
SELECT deviceName() as deviceName,items.Temperature.value as Temperature FROM "/sys/a1HHrkm****/+/thing/event/property/post" WHERE items.Temperature.value>38 and deviceName() in ('TestDevice1', 'TestDevice2', 'TestDevice3')
```

以上示例中，使用了deviceName\(\)函数：

-   在SELECT字段使用该函数，表示从数据中筛选出设备名称。
-   在条件字段使用该函数，并指定为`in ('TestDevice1', 'TestDevice2', 'TestDevice3')`，表示仅当设备名称的值为TestDevice1、TestDevice2或TestDevice3中的其中一个时，才会进行数据流转。

规则SQL中，SELECT字段和WHERE条件字段的具体编写方法，和规则引擎支持的条件表达式列表，请参见[SQL表达式](/cn.zh-CN/消息通信/云产品流转/SQL表达式.md)。
