# MDMP router


## Development

To regenerate the proto files, ensure you have installed the generate dependencies:

```bash
$ make install
go install \
        ./vendor/github.com/gogo/protobuf/protoc-gen-gogo
go mod vendor      
```

It also requires you to have the Google Protobuf compiler `protoc` installed.
Please follow instructions for your platform on the
[official protoc repo](https://github.com/google/protobuf#protocol-compiler-installation).

Regenerate the files by running `make generate`:

```bash 
$ make generate
```
#### 1. quick start 
##### 1.1 install && run
```
# linux build
$ make build

# docker images build && push
$ make docker-auto
```
#### 2 rule-sql
```
# input
{
    "id": "iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e",
    "owner": "abVM4Nh9",
    "properties":
    {
        "rawData":
        {
            "id": "iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e",
            "mark": "upstream",
            "path": "iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e/v1/devices/me/telemetry",
            "ts": 1657959026659,
            "type": "telemetry",
            "values":
            {
                "counter01": 12
            }
        }
    },
    "subscribe_id": "iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e_16_mdmp-topic"
}

# rule-sql
select id, 
       owner, 
       subscribe_id, 
       properties.rawData.values.counter01 as counter02 
from rulex/rule-16
where owner = 'abVM4Nh9' and 
      properties.rawData.ts >= 1657959026659

# output
{
    "id": "iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e",
    "owner": "abVM4Nh9",
    "subscribe_id": "iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e_16_mdmp-topic",
    "counter02": 12
}
```

#### 3 sql-function (built-in)
1. updateTime
```
# rule-sql
select id, updateTime() as update_time from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","update_time":1658369532}
```
2. startswith
```
# rule-sql
select id, startswith(id, 'iotd') as is_startswith from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","is_startswith":true}
```
3. newuuid
```
# rule-sql
select id, newuuid() as uuid from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","uuid":"328868e7-c60e-4a5c-b4ee-c9d707fac4d5"}
```
4. rand
```
# rule-sql
select id, rand(0, 1) as rand_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","rand_val":0.604660}
```
5. tan
```
# rule-sql
select id, tan(10) as tan_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","tan_val":0.648361}
```
6. upper
```
# rule-sql
select id, upper(id) as upper_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","upper_val":"IOTD-6497D1E2-C2C6-4AA5-A72F-D8617EE8AD9E"}
```
7. asin
```
# rule-sql
select id, asin(0.5) as asin_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","asin_val":0.523599}
```
8. concat
```
# rule-sql
select id, concat(id, '-test') as concat_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","concat_val":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e-test"}
```
9. sin
```
# rule-sql
select id, sin(0.5) as sin_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","sin_val":0.479426}
```
10. tanh
```
# rule-sql
select id, tanh(0.5) as tanh_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","tanh_val":0.462117}
```
11. deviceid
```
# rule-sql
select id, deviceid() as device_id from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","device_id":"iotd-mock001"}
```
12. timeFormat
```
# rule-sql
select id, timeFormat(properties.rawData.ts/1000, '2006-01-02') as time from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","time":"2022-07-16"}
```
13. floor
```
# rule-sql
select id, floor(0.56) as floor_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","floor_val":0}
```
14. lower
```
# rule-sql
select id, lower('LOWER') as lower_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","lower_val":"lower"}
```
15. sinh
```
# rule-sql
select id, sinh(0.5) as sinh_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","sinh_val":0.521095}
```
16. topic
```
# rule-sql
select id, topic() as topic, topic(1) as topic2, topic(2) as topic3, topic(3) as topic4 from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","topic":"/mqtt-mock/benchmark/0","topic2":"mqtt-mock","topic3":"benchmark","topic4":"0"}
```
17. messageId
```
# rule-sql
select id, messageId() as message_id from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","message_id":"id-mock"}
```
18. userid
```
# rule-sql
select id, userid() as userid from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","userid":"mdmp-test"}
```
19. exp
```
# rule-sql
select id, exp(2) as exp_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","exp_val":7.389056}
```
20. power
```
# rule-sql
select id, power(2, 3) as power_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","power_val":8.000000}
```
21. ruleBody
```
# rule-sql
select id, ruleBody() as rule_body from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","rule_body":"rule-body-mock"}
```
22. timestamp
```
# rule-sql
select id, timestamp() as timestamp, timestamp('2006-01-02') as time_format from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","timestamp":1658373354768,"time_format":"2022-07-21"}
```
23. abs
```
# rule-sql
select id, abs(-1) as abs_val  from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","abs_val":1}
```
24. acos
```
# rule-sql
select id, acos(0.5) as acos_val from rulex/rule-16`
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","acos_val":1.047198}
```
25. cosh
```
# rule-sql
select id, cosh(0.5) as cosh_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","cosh_val":1.127626}
```
26. deviceName
```
# rule-sql
select id, deviceName() as device_name from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","device_name":"iotd-mock001"}
```
27. mod
```
# rule-sql
select id, mod(10, 3) as mod_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","mod_val":1}
```
28. substring
```
# rule-sql
select id, substring('123', 1) as substring_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","substring_val":"23"}
```
29. ruleId
```
# rule-sql
select id, ruleId() as rule_id from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","rule_id":"rule-id-mock"}
```
30. endswith
```
# rule-sql
select id, endswith(id, 'd8617ee8ad9e') as is_endswith from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","is_endswith":true}
```
31. replace
```
# rule-sql
select id, replace(id, 'iotd', 'iotd-mock') as replace_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","replace_val":"iotd-mock-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e"}
```
32. str
```
# rule-sql
select id, str(properties) as str_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","str_val":"{\"rawData\":{\"id\":\"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e\",\"mark\":\"upstream\",\"path\":\"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e/v1/devices/me/telemetry\",\"ts\":1657959026659,\"type\":\"telemetry\",\"values\":{\"counter01\":12}}}"}
```
33. deviceID
```
# rule-sql
select id, deviceId() as device_id from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","device_id":"iotd-mock001"}
```
34. cos
```
# rule-sql
select id, cos(0.5) as cos_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","cos_val":0.877583}
```
35. log
```
# rule-sql
select id, log(100) as log_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","log_val":2.000000}
```
36. to_base64
```
# rule-sql
select id, to_base64('{"counter": 10}') as log_val from rulex/rule-16
```
```
# output
{"id":"iotd-6497d1e2-c2c6-4aa5-a72f-d8617ee8ad9e","log_val":"eyJjb3VudGVyIjogMTB9"}
```
