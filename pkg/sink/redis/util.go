package redis

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// ToInt coerce a value to an int

// ToInteger coerce a value to an integer
func ToHash(val interface{}) (map[string]interface{}, error) {
	var mapResult map[string]interface{}
	re :=val.(string)
	 err :=json.Unmarshal([]byte(re),&mapResult)
	if err !=nil{
		var temp map[string]interface{}
		return temp, fmt.Errorf("unable to coerce %#v to hash", val)
	}else {
		return mapResult,nil
	}
}


func ToString(val interface{}) (string, error) {

	switch t := val.(type) {
	case string:
		return t, nil
	case int:
		return strconv.Itoa(t), nil
	case int64:
		return strconv.FormatInt(t, 10), nil
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 64), nil
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), nil
	case json.Number:
		return t.String(), nil
	case bool:
		return strconv.FormatBool(t), nil
	case nil:
		return "", nil
	case []byte:
		return string(t), nil
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return "", fmt.Errorf("unable to coerce %#v to string", t)
		}
		return string(b), nil
	}
}
func BuildValue(key string, value interface{}) (interface{}, error) {
	switch key {

	case THING_PROPERTY_TYPE_Hash:
		return ToHash(value)
	case THING_PROPERTY_TYPE_String:
		return ToString(value)

	default:
		return ToString(value)
		//return nil, errors.New(fmt.Sprintf("[buildValue] DeviceMdmp default data error:%+v", value))
	}
}
