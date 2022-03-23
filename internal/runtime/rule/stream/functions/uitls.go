package functions

import (
	"fmt"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"math"
	"math/rand"
)

var (
	ErrUnsupportType   = fmt.Errorf("only supported float64 & int64 type")
	ErrExpectInt       = fmt.Errorf("Expect int type for operands")
	ErrExpectBothInt   = fmt.Errorf("Expect int type for both operands")
	ErrUnknownFunction = fmt.Errorf("Unknown math function name")
)

func warp(name string) func(args ...ruleql.Node) ruleql.Node {
	return func(args ...ruleql.Node) ruleql.Node {
		ret, err := callMath(name, args...)
		if err != nil {
			return ruleql.UNDEFINED_RESULT
		}
		switch ret := ret.(type) {
		case int64:
			return ruleql.IntNode(ret)
		case float64:
			return ruleql.FloatNode(ret)
		}
		return ruleql.UNDEFINED_RESULT
	}
}

func callMath(name string, args ...ruleql.Node) (interface{}, error) {
	if len(args) == 0 {
		return 0, fmt.Errorf("args error")
	}
	switch name {
	case "abs":
		if v, e := toInt64(args[0]); e == nil {
			t := float64(v)
			var ret = int(math.Abs(t))
			return ret, nil
		} else if v, e := toFloat64(args[0]); e == nil {
			return math.Abs(v), nil
		} else {
			return 0, ErrUnsupportType
		}
	case "acos":
		if v, e := toFloat64(args[0]); e == nil {
			return math.Acos(v), nil
		} else {
			return 0, e
		}
	case "asin":
		if v, e := toFloat64(args[0]); e == nil {
			return math.Asin(v), nil
		} else {
			return 0, e
		}
	case "atan":
		if v, e := toFloat64(args[0]); e == nil {
			return math.Atan(v), nil
		} else {
			return 0, e
		}
	case "atan2":
		if v1, e := toFloat64(args[0]); e == nil {
			if v2, e1 := toFloat64(args[1]); e1 == nil {
				return math.Atan2(v1, v2), nil
			} else {
				return 0, e1
			}
		} else {
			return 0, e
		}
	case "bitand":
		if v1, e := toInt64(args[0]); e == nil {
			if v2, e1 := toInt64(args[1]); e1 == nil {
				return v1 & v2, nil
			} else {
				return 0, e1
			}
		} else {
			return 0, e
		}
	case "bitor":
		if v1, e := toInt64(args[0]); e == nil {
			if v2, e1 := toInt64(args[1]); e1 == nil {
				return v1 | v2, nil
			} else {
				return 0, e1
			}
		} else {
			return 0, e
		}
	case "bitxor":
		if v1, e := toInt64(args[0]); e == nil {
			if v2, e1 := toInt64(args[1]); e1 == nil {
				return v1 ^ v2, nil
			} else {

				return 0, e1
			}
		} else {
			return 0, e
		}
	case "bitnot":
		if v, e := toInt64(args[0]); e == nil {
			return ^v, nil
		} else {
			return 0, ErrExpectInt
		}
	case "ceil":
		if v, e := toFloat64(args[0]); e == nil {
			return math.Ceil(v), nil
		} else {
			return 0, e
		}
	case "cos":
		if v, e := toFloat64(args[0]); e == nil {
			return math.Cos(v), nil
		} else {
			return 0, e
		}
	case "cosh":
		if v, e := toFloat64(args[0]); e == nil {
			return math.Cosh(v), nil
		} else {
			return 0, e
		}
	case "exp":
		if v, e := toFloat64(args[0]); e == nil {
			return math.Exp(v), nil
		} else {
			return 0, e
		}
	case "floor":
		if v, e := toFloat64(args[0]); e == nil {
			return int64(math.Floor(v)), nil
		} else {
			return 0, e
		}
	case "ln":
		if v, e := toFloat64(args[0]); e == nil {
			return math.Log2(v), nil
		} else {
			return 0, e
		}
	case "log":
		if v, e := toFloat64(args[0]); e == nil {
			return math.Log10(v), nil
		} else {
			return 0, e
		}
	case "mod":
		if v, e := toFloat64(args[0]); e == nil {
			if v1, e1 := toFloat64(args[1]); e == nil {
				return int64(math.Mod(v, v1)), nil
			} else {
				return 0, e1
			}
		} else {
			return 0, e
		}
	case "power":
		if v1, e := toFloat64(args[0]); e == nil {
			if v2, e1 := toFloat64(args[1]); e1 == nil {
				return math.Pow(v1, v2), nil
			} else {
				return 0, e1
			}
		} else {
			return 0, e
		}
	case "rand":
		return rand.Float64(), nil
	case "round":
		if v, e := toFloat64(args[0]); e == nil {
			return math.Round(v), nil
		} else {
			return 0, e
		}
	case "sign":
		if v, e := toFloat64(args[0]); e == nil {
			if v > 0 {
				return 1, nil
			} else if v < 0 {
				return -1, nil
			} else {
				return 0, nil
			}
		} else {
			return 0, e
		}
	case "sin":
		if v, e := toFloat64(args[0]); e == nil {
			return math.Sin(v), nil
		} else {
			return 0, e
		}
	case "sinh":
		if v, e := toFloat64(args[0]); e == nil {
			return math.Sinh(v), nil
		} else {
			return 0, e
		}
	case "sqrt":
		if v, e := toFloat64(args[0]); e == nil {
			return math.Sqrt(v), nil
		} else {
			return 0, e
		}

	case "tan":
		if v, e := toFloat64(args[0]); e == nil {
			return math.Tan(v), nil
		} else {
			return 0, e
		}

	case "tanh":
		if v, e := toFloat64(args[0]); e == nil {
			return math.Tanh(v), nil
		} else {
			return 0, e
		}
	}
	return 0, ErrUnknownFunction
}

func toInt64(arg ruleql.Node) (int64, error) {
	v := arg.To(ruleql.Number)
	switch v := v.(type) {
	case ruleql.FloatNode:
		return int64(v), nil
	case ruleql.IntNode:
		return int64(v), nil
	}
	return 0, ErrUnsupportType
}

func toFloat64(arg ruleql.Node) (float64, error) {
	v := arg.To(ruleql.Number)
	switch v := v.(type) {
	case ruleql.FloatNode:
		return float64(v), nil
	case ruleql.IntNode:
		return float64(v), nil
	}
	return 0, ErrUnsupportType
}
