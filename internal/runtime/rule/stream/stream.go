package stateful

import (
	"context"
	"fmt"
	"github.com/tkeel-io/rule-util/metadata/v1"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/runtime/rule/stream/api"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"github.com/tkeel-io/rule-util/stream"
	"sync"
	"time"
)

// StreamOperator is an executor that batches incoming streamed items based
// on provided criteria.  The batched items are streamed on the
// ouptut channel for downstream processing.
var timer utils.Timer
var once sync.Once

type StreamOperator struct {
	ctx       context.Context
	id        string
	rule      *v1.RuleQL
	stateFunc api.StateFunc
	timer     utils.Timer
	funcs     map[string]*ruleql.CallExpr

	window   *ruleql.WindowExpr
	windowOp *WindowsOperator

	stateManager *stateManager

	trigger api.StreamTrigger
	expr    ruleql.Expr
}

// New returns a new StreamOperator operator
func New(ctx context.Context, rule *v1.RuleQL, stateFunc api.StateFunc) (*StreamOperator, error) {
	// init timer
	once.Do(func() {
		timer = utils.NewTimer(1024)
	})

	if rule.Body == nil {
		return nil, fmt.Errorf("rule Body nil")
	}

	exp, err := ruleql.Parse(string(rule.Body))
	if err != nil {
		utils.Log.Bg().Error("Parse expr error",
			logf.ByteString("expr", rule.Body),
			logf.Error(err))
	}
	_, ok := ruleql.GetTopic(exp)
	if !ok {
		return nil, fmt.Errorf("json unmarshal error")
	}

	op := &StreamOperator{
		ctx:       ctx,
		id:        "",
		rule:      rule,
		expr:      exp,
		stateFunc: stateFunc,
		//stateLock:    sync.RWMutex{},
		//stateManager: sync.Map{},
		stateManager: NewStateManager(),
		timer:        timer,
	}

	op.funcs = make(map[string]*ruleql.CallExpr)
	for _, expr := range ruleql.ParseFunc(exp) {
		op.funcs[expr.String()] = expr
	}

	windows := ruleql.GetWindow(exp)
	op.window = windows
	op.windowOp = NewWindowsOperator(exp, windows, op.funcs)

	return op, nil
}

func (this *StreamOperator) ID() string {
	return this.id
}

//Update message
func (this *StreamOperator) Exce(ctx context.Context, evalCtx ruleql.Context, msg stream.Message) error {
	//fmt.Println("+Invoke+", time.Now(), this.expr)
	if ruleql.HasDimensions(this.expr) {
		return this.ExceStateful(ctx, evalCtx, msg)
	} else {
		return this.ExceStateless(ctx, evalCtx, msg)
	}
}

func (this *StreamOperator) ExceStateless(ctx context.Context, evalCtx ruleql.Context, msg stream.Message) error {
	return this.stateFunc(ctx, msg)
}

func (this *StreamOperator) ExceStateful(ctx context.Context, evalCtx ruleql.Context, msg stream.Message) error {
	//1. byKey(rule-key)
	k := ruleql.EvalDimensions(evalCtx, this.expr)
	if k == "" {
		return fmt.Errorf("stream key empty")
	}
	key := fmt.Sprintf("stream:op:%v", k)

	//1.1 Prepare WindowsState(Message, State)
	state, loaded := this.prepareWindowState(key, msg)

	//1.2 Exce WindowsState(Message, State)
	ret := state.Exce(evalCtx, this.windowOp)

	//1.3 Start New State(Message, State)
	if !loaded {
		fmt.Println("+InitTrigger#New+", state.id, time.Now())
		state.initTrigger()
	}

	return ret
}

func (this *StreamOperator) prepareWindowState(id string, message stream.Message) (state *WindowState, loaded bool) {
	key := fmt.Sprintf("state-%s-%s", this.id, id)
	state, loaded = this.stateManager.LoadOrStore(key, func() *WindowState {
		return newWindowState(this.ctx,
			key,
			this.removeFunc,
			this.window,
			this.timer,
			this.stateFunc,
		)
	})
	if state.GetState() == nil {
		state.SetState(message)
	}
	return
}

//Filter message
func (this *StreamOperator) Filter(ctx context.Context, evalCtx ruleql.Context, message stream.PublishMessage) bool {
	return ruleql.EvalFilter(evalCtx, this.expr)
}

func (this *StreamOperator) Invoke(ctx context.Context, evalCtx ruleql.Context, msg stream.PublishMessage) error {
	return Exce(evalCtx, this.expr, msg)
}

func (this *StreamOperator) removeFunc(state *WindowState) {
	this.stateManager.Del(state)
}

func Exce(evalCtx ruleql.Context, expr ruleql.Expr, msg stream.PublishMessage) error {
	ret := ruleql.EvalRuleQL(evalCtx, expr)
	switch ret := ret.(type) {
	case ruleql.JSONNode:
		msg.SetData([]byte(string(ret)))
	default:
		return fmt.Errorf("invalid type(%v)", ret)
	}
	return nil
}
