package rule

import (
	"context"
	"errors"
	"time"

	metapb "github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/metadata/v1error"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/tkeel-io/rule-rulex/pkg/sink"
	stypes "github.com/tkeel-io/rule-util/stream/types"
	"github.com/uber-go/atomic"
)

type ActionTask struct {
	ActionData      *metapb.Action
	Action          types.Action
	EntityID        string
	isError         *atomic.Bool
	error           error
	onErrorCallback func(*ActionTask, error)
}

func (this *ActionTask) Invoke(ctx types.ActionContent, message metapb.Message) error {
	// if this.isError.Load() {
	// 	return this.error
	// }
	return this.Action.Invoke(ctx, message)
}

func NewAction(ctx context.Context, entityID string, event *metapb.Action) (*ActionTask, error) {
	ac := sink.NewAction(event.Type, entityID)
	if ac == nil {
		err := errors.New("nil pipe")
		utils.Log.For(ctx).Error("Load rule action",
			logf.StatusCode(v1error.RuleActionOpenFail),
			logf.EntityID(entityID),
			logf.Any("EntityType", event.Type),
			logf.Any("Event", event),
			logf.Error(err))
		return nil, err
	} else {
		utils.Log.For(ctx).Info("Load rule action",
			logf.StatusCode(v1error.RuleActionOpen),
			logf.EntityID(entityID),
			logf.Any("EntityType", event.Type),
			logf.Any("Event", event))
	}

	return &ActionTask{
		EntityID:   entityID,
		ActionData: event,
		Action:     ac,
		isError:    atomic.NewBool(false),
	}, nil
}

func (this *ActionTask) SetErrorCallback(fn func(*ActionTask, error)) {
	this.onErrorCallback = fn
	return
}

func (this *ActionTask) Setup(ctx context.Context) error {
	ctx, _ = context.WithTimeout(ctx, 2*time.Second)
	metadata := this.ActionData.Metadata
	entityID := this.EntityID
	err := this.Action.Setup(this.Context(ctx), metadata)
	if err != nil {
		utils.Log.For(ctx).Error("Setup rule action",
			logf.StatusCode(v1error.RuleActionOpenFail),
			logf.EntityID(entityID),
			logf.Any("meta", metadata),
			logf.Error(err))
		this.onError(err)
	} else {
		utils.Log.For(ctx).Info("Setup rule action",
			logf.StatusCode(v1error.RuleActionOpen),
			logf.EntityID(entityID),
			logf.Any("meta", metadata))
	}

	//if ac, ok := this.Action.(types.FlushAction); ok {
	//	if env := utils.Log.Check(log.DebugLevel, fmt.Sprintf("Add Sink")); env != nil {
	//		env.Write(logf.Any("entityID", entityID))
	//	}
	//	checkpoint.AddSink(warpCheckpointedFunction(ac))
	//}

	return nil
}

func (this *ActionTask) onError(err error) {
	this.isError = atomic.NewBool(true)
	this.error = err
	utils.Log.Bg().Error("ActionTask onError",
		logf.StatusCode(v1error.RuleActionOpenFail),
		logf.Any("Task", this.ActionData),
		logf.Error(err))
	this.onErrorCallback(this, err)
}

type checkpointedAction struct {
	action types.FlushAction
}

func (this *checkpointedAction) SnapshotState(ctx stypes.FunctionSnapshotContext) error {
	return this.action.Flush(sink.NewContext(context.Background()))
}

func (this *checkpointedAction) InitializeState(context stypes.FunctionInitializationContext) error {
	return nil
}

func warpCheckpointedFunction(action types.FlushAction) stypes.CheckpointedFunction {
	return &checkpointedAction{action}
}

type actionContent struct {
	action *ActionTask
	ctx    context.Context
}

func (a *actionContent) Ack() {
}

func (a *actionContent) Nack(err error) {
	if err != nil {
		a.action.onError(err)
	}
}

func (a *actionContent) Context() context.Context {
	return a.ctx
}

func (this *ActionTask) Context(ctx context.Context) types.ActionContent {
	return &actionContent{
		ctx:    ctx,
		action: this,
	}
}

func (this *ActionTask) Close() error {
	return this.Action.Close()
}
