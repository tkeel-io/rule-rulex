package chronus

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// ToInt coerce a value to an int
func ToInt(val interface{}) (int, error) {
	switch t := val.(type) {
	case int:
		return t, nil
	case int64:
		return int(t), nil
	case float64:
		return int(t), nil
	case json.Number:
		i, err := t.Int64()
		return int(i), err
	case string:
		return strconv.Atoi(t)
	case bool:
		if t {
			return 1, nil
		}
		return 0, nil
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("unable to coerce %#v to int", val)
	}
}

// ToInteger coerce a value to an integer
func ToInt32(val interface{}) (int32, error) {
	switch t := val.(type) {
	case int:
		return int32(t), nil
	case int32:
		return t, nil
	case int64:
		return int32(t), nil
	case float32:
		return int32(t), nil
	case float64:
		return int32(t), nil
	case json.Number:
		i, err := t.Int64()
		return int32(i), err
	case string:
		i, err := strconv.Atoi(t)
		return int32(i), err
	case bool:
		if t {
			return 1, nil
		}
		return 0, nil
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("unable to coerce %#v to int32", val)
	}
}

// ToInteger coerce a value to an integer
func ToInt64(val interface{}) (int64, error) {
	switch t := val.(type) {
	case int:
		return int64(t), nil
	case int32:
		return int64(t), nil
	case int64:
		return t, nil
	case float32:
		return int64(t), nil
	case float64:
		return int64(t), nil
	case json.Number:
		return t.Int64()
	case string:
		return strconv.ParseInt(t, 10, 64)
	case bool:
		if t {
			return 1, nil
		}
		return 0, nil
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("unable to coerce %#v to integer", val)
	}
}

// ToFloat64 coerce a value to a double/float64
func ToFloat32(val interface{}) (float32, error) {
	switch t := val.(type) {
	case int:
		return float32(t), nil
	case int32:
		return float32(t), nil
	case int64:
		return float32(t), nil
	case float32:
		return t, nil
	case float64:
		return float32(t), nil
	case json.Number:
		f, err := t.Float64()
		return float32(f), err
	case string:
		f, err := strconv.ParseFloat(t, 32)
		return float32(f), err
	case bool:
		if t {
			return 1.0, nil
		}
		return 0.0, nil
	case nil:
		return 0.0, nil
	default:
		return 0.0, fmt.Errorf("unable to coerce %#v to float32", val)
	}
}

// ToFloat64 coerce a value to a double/float64
func ToFloat64(val interface{}) (float64, error) {
	switch t := val.(type) {
	case int:
		return float64(t), nil
	case int32:
		return float64(t), nil
	case int64:
		return float64(t), nil
	case float32:
		return float64(t), nil
	case float64:
		return t, nil
	case json.Number:
		return t.Float64()
	case string:
		return strconv.ParseFloat(t, 64)
	case bool:
		if t {
			return 1.0, nil
		}
		return 0.0, nil
	case nil:
		return 0.0, nil
	default:
		return 0.0, fmt.Errorf("unable to coerce %#v to float64", val)
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
	case THING_PROPERTY_TYPE_INT32:
		return ToInt32(value)
	case THING_PROPERTY_TYPE_FLOAT32:
		return ToFloat32(value)
	case THING_PROPERTY_TYPE_FLOAT64:
		return ToFloat64(value)
	case THING_PROPERTY_TYPE_STRING:
		return ToString(value)
	case THING_PROPERTY_TYPE_STRUCT:
		fallthrough
	case THING_PROPERTY_TYPE_ARRAY:
		fallthrough
	case THING_PROPERTY_TYPE_BOOL:
		fallthrough
	case THING_PROPERTY_TYPE_ENUM:
		fallthrough
	case THING_PROPERTY_TYPE_DATE:
		fallthrough
	default:
		return ToFloat32(value)
		//return nil, errors.New(fmt.Sprintf("[buildValue] DeviceMdmp default data error:%+v", value))
	}
}
