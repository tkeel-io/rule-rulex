package stateful

import (
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"math"
)

type AggregateData interface {
	ruleql.Context
	Result() ruleql.Node
	Clean() error
}

type AggregateAcc func() AggregateData

var aggregateFuncs = map[string]AggregateAcc{
	"avg":   NewAvgAggregateFunc,
	"sum":   NewSumAggregateFunc,
	"max":   NewMaxAggregateFunc,
	"min":   NewMinAggregateFunc,
}

type CountAggregateData struct {
	cnt int64
}

func NewCountAggregateFunc() AggregateData {
	return &CountAggregateData{}
}

func (fn *CountAggregateData) Clean() error {
	fn.cnt = 0
	return nil
}

func (fn *CountAggregateData) Result() ruleql.Node {
	return ruleql.IntNode(fn.cnt)
}

func (fn *CountAggregateData) Value(key string) ruleql.Node {
	return ruleql.UNDEFINED_RESULT
}

func (fn *CountAggregateData) Call(expr *ruleql.CallExpr, args []ruleql.Node) ruleql.Node {
	if len(args) == 1 {
		switch args[0].Type() {
		case ruleql.Int:
			fn.cnt = fn.cnt + 1
			return fn.Result()
		case ruleql.Float:
			fn.cnt = fn.cnt + 1
			return fn.Result()
		default:
			if env := utils.Log.Check(log.DebugLevel, "error type"); env != nil {
				env.Write(
					logf.String("FuncName", expr.FuncName()),
					logf.Any("Type", args[0].Type()),
					logf.Any("args", args))
			}
			return ruleql.UNDEFINED_RESULT
		}
	}
	if env := utils.Log.Check(log.DebugLevel, "error args"); env != nil {
		env.Write(
			logf.String("FuncName", expr.FuncName()),
			logf.Any("args", args))
	}
	return ruleql.UNDEFINED_RESULT
}

type AvgAggregateData struct {
	sum float64
	cnt float64
}

func NewAvgAggregateFunc() AggregateData {
	return &AvgAggregateData{}
}

func (fn *AvgAggregateData) Clean() error {
	fn.sum = 0
	fn.cnt = 0
	return nil
}

func (fn *AvgAggregateData) Result() ruleql.Node {
	if fn.cnt == 0 {
		return ruleql.UNDEFINED_RESULT
	}
	return ruleql.FloatNode(fn.sum / fn.cnt)
}

func (fn *AvgAggregateData) Value(key string) ruleql.Node {
	return ruleql.UNDEFINED_RESULT
}

func (fn *AvgAggregateData) Call(expr *ruleql.CallExpr, args []ruleql.Node) ruleql.Node {
	if len(args) == 1 {
		switch args[0].Type() {
		case ruleql.Int:
			fn.cnt = fn.cnt + 1
			fn.sum = fn.sum + float64(args[0].(ruleql.IntNode))
			return fn.Result()
		case ruleql.Float:
			fn.cnt = fn.cnt + 1
			fn.sum = fn.sum + float64(args[0].(ruleql.FloatNode))
			return fn.Result()
		default:
			if env := utils.Log.Check(log.DebugLevel, "error type"); env != nil {
				env.Write(
					logf.String("FuncName", expr.FuncName()),
					logf.Any("Type", args[0].Type()),
					logf.Any("args", args))
			}
			return ruleql.UNDEFINED_RESULT
		}
	}
	if env := utils.Log.Check(log.DebugLevel, "error args"); env != nil {
		env.Write(
			logf.String("FuncName", expr.FuncName()),
			logf.Any("args", args))
	}
	return ruleql.UNDEFINED_RESULT
}

type SumAggregateData struct {
	sum float64
	cnt float64
}

func NewSumAggregateFunc() AggregateData {
	return &SumAggregateData{}
}

func (fn *SumAggregateData) Clean() error {
	fn.sum = 0
	fn.cnt = 0
	return nil
}

func (fn *SumAggregateData) Result() ruleql.Node {
	if fn.cnt == 0 {
		return ruleql.UNDEFINED_RESULT
	}
	return ruleql.FloatNode(fn.sum)
}

func (fn *SumAggregateData) Value(key string) ruleql.Node {
	return ruleql.UNDEFINED_RESULT
}

func (fn *SumAggregateData) Call(expr *ruleql.CallExpr, args []ruleql.Node) ruleql.Node {
	if len(args) == 1 {
		switch args[0].Type() {
		case ruleql.Int:
			fn.cnt = fn.cnt + 1
			fn.sum = fn.sum + float64(args[0].(ruleql.IntNode))
			return fn.Result()
		case ruleql.Float:
			fn.cnt = fn.cnt + 1
			fn.sum = fn.sum + float64(args[0].(ruleql.FloatNode))
			return fn.Result()
		default:
			if env := utils.Log.Check(log.DebugLevel, "error type"); env != nil {
				env.Write(
					logf.String("FuncName", expr.FuncName()),
					logf.Any("Type", args[0].Type()),
					logf.Any("args", args))
			}
			return ruleql.UNDEFINED_RESULT
		}
	}
	if env := utils.Log.Check(log.DebugLevel, "error args"); env != nil {
		env.Write(
			logf.String("FuncName", expr.FuncName()),
			logf.Any("args", args))
	}
	return ruleql.UNDEFINED_RESULT
}

type MaxAggregateData struct {
	init bool
	val  float64
}

func NewMaxAggregateFunc() AggregateData {
	return &MaxAggregateData{
		val: -1 * math.MaxFloat64,
	}
}

func (fn *MaxAggregateData) Clean() error {
	fn.init = false
	fn.val = -1 * math.MaxFloat64
	return nil
}

func (fn *MaxAggregateData) Result() ruleql.Node {
	if !fn.init {
		return ruleql.UNDEFINED_RESULT
	}
	return ruleql.FloatNode(fn.val)
}

func (fn *MaxAggregateData) Value(key string) ruleql.Node {
	return ruleql.UNDEFINED_RESULT
}

func (fn *MaxAggregateData) Call(expr *ruleql.CallExpr, args []ruleql.Node) ruleql.Node {
	if len(args) == 1 {
		fn.init = true
		switch args[0].Type() {
		case ruleql.Int:
			ret := float64(args[0].(ruleql.IntNode))
			if fn.val < ret {
				fn.val = ret
			}
			return fn.Result()
		case ruleql.Float:
			ret := float64(args[0].(ruleql.FloatNode))
			if fn.val < ret {
				fn.val = ret
			}
			return fn.Result()
		default:
			if env := utils.Log.Check(log.DebugLevel, "error type"); env != nil {
				env.Write(
					logf.String("FuncName", expr.FuncName()),
					logf.Any("Type", args[0].Type()),
					logf.Any("args", args))
			}
			return ruleql.UNDEFINED_RESULT
		}
	}
	if env := utils.Log.Check(log.DebugLevel, "error args"); env != nil {
		env.Write(
			logf.String("FuncName", expr.FuncName()),
			logf.Any("args", args))
	}
	return ruleql.UNDEFINED_RESULT
}

type MinAggregateData struct {
	init bool
	val  float64
}

func NewMinAggregateFunc() AggregateData {
	return &MinAggregateData{
		val: math.MaxFloat64,
	}
}

func (fn *MinAggregateData) Clean() error {
	fn.init = false
	fn.val = math.MaxFloat64
	return nil
}

func (fn *MinAggregateData) Result() ruleql.Node {
	if !fn.init {
		return ruleql.UNDEFINED_RESULT
	}
	return ruleql.FloatNode(fn.val)
}

func (fn *MinAggregateData) Value(key string) ruleql.Node {
	return ruleql.UNDEFINED_RESULT
}

func (fn *MinAggregateData) Call(expr *ruleql.CallExpr, args []ruleql.Node) ruleql.Node {
	if len(args) == 1 {
		fn.init = true
		switch args[0].Type() {
		case ruleql.Int:
			ret := float64(args[0].(ruleql.IntNode))
			if fn.val > ret {
				fn.val = ret
			}
			return fn.Result()
		case ruleql.Float:
			ret := float64(args[0].(ruleql.FloatNode))
			if fn.val > ret {
				fn.val = ret
			}
			return fn.Result()
		default:
			if env := utils.Log.Check(log.DebugLevel, "error type"); env != nil {
				env.Write(
					logf.String("FuncName", expr.FuncName()),
					logf.Any("Type", args[0].Type()),
					logf.Any("args", args))
			}
			return ruleql.UNDEFINED_RESULT
		}
	}
	if env := utils.Log.Check(log.DebugLevel, "error args"); env != nil {
		env.Write(
			logf.String("FuncName", expr.FuncName()),
			logf.Any("args", args))
	}
	return ruleql.UNDEFINED_RESULT
}
