///*
// * Copyright (C) 2019 Yunify, Inc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this work except in compliance with the License.
// * You may obtain a copy of the License in the LICENSE file, or at:
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
package rule

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	metrics "github.com/tkeel-io/rule-rulex/internal/metrices"
	xmetrics "github.com/tkeel-io/rule-rulex/internal/metrices/prometheus"
	"github.com/tkeel-io/rule-rulex/internal/report"
	stateful "github.com/tkeel-io/rule-rulex/internal/runtime/rule/stream"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	rulex "github.com/tkeel-io/rule-rulex/pkg/define"
	"github.com/tkeel-io/rule-rulex/pkg/sink"
	metapb "github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/pkg/errors"
	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"github.com/tkeel-io/rule-util/stream"
	"go.uber.org/atomic"
)

type Task struct {
	id     string
	userId string
	value  *metapb.RuleQL
	cli    *cli
	ruleql *stateful.StreamOperator
	inited *atomic.Bool

	actions    map[string]*ActionTask
	errActions map[string]*ActionTask
	ruleId     string
}

func newStreamRuleTask(ctx context.Context, value *metapb.RuleQL, factor *Factor) (*Task, error) {
	rs := &Task{
		id:     value.Id,
		value:  value,
		cli:    factor.cli,
		userId: value.UserId,
		ruleId: value.Id,
		inited: atomic.NewBool(false),
	}
	return rs, nil
}

func (this *Task) init(ctx context.Context) (err error) {
	// parse SQL
	if rql, err := stateful.New(ctx, this.value, this.invoke); err == nil {
		this.ruleql = rql
	} else {
		report.TaskError(ctx, this, rulex.RuleStartError, err)
		return err
	}
	return nil
}

func (this *Task) ID() string {
	return fmt.Sprintf("%s", this.id)
}

func (this *Task) UserID() string {
	return fmt.Sprintf("%s", this.userId)
}

func (this *Task) RefreshTime() int64 {
	return this.value.RefreshTime
}

func (this *Task) Close(ctx context.Context) error {
	this.closeRule()
	return this.closeAction(ctx)
}

func (this *Task) String() string {
	byt, err := json.Marshal(this.value)
	if err != nil {
		utils.Log.Bg().Error("Marshal metapb.RuleQL error", logf.Any("value", this.value))
	}
	return string(byt)
}

func (this *Task) Invoke(ctx context.Context, vCtx ruleql.Context, msg stream.PublishMessage) error {
	metrics.RuleMetrics.Mark(1)
	metrics.Inc(metrics.MetricName(this.userId, this.ruleId, metrics.MtcRule))
	if !this.inited.Load() {
		//if err := this.initAction(ctx); err != nil {
		//	return err
		//}
		this.inited.Store(true)
	}
	msg.SetAttr("rule", this.value.Body)
	err := this.exec(ctx, vCtx, msg)
	return err
}

// call on trigger
func (this *Task) exec(ctx context.Context, vCtx ruleql.Context, msg stream.PublishMessage) error {
	if env := utils.Log.Check(log.DebugLevel, "exec"); env != nil {
		env.Write(logf.String("filter", string(this.value.Body)))
	}
	err := this.ruleql.Exce(ctx, vCtx, msg)
	return err
}

// call on trigger
func (this *Task) invoke(ctx context.Context, state interface{}) error {
	if state == nil {
		fmt.Println("invoke state", nil)
		return nil
	}
	metrics.Inc(metrics.MetricName(this.userId, this.ruleId, metrics.MtcRuleInvoke))
	m := state.(stream.PublishMessage)
	msg, ok := m.Copy().(stream.PublishMessage)
	msg.SetAttr(types.MATE_RULE_ID, []byte(this.value.Id))
	msg.SetAttr(types.MATE_RULE_BODY, this.value.Body)

	if !ok {
		return errors.New("Trigger Callback Type Error")
	}
	/*
		evalCtx := functions.NewMessageContext(msg.(stream.PublishMessage))
		// 1. filter
			skipFlag := this.ruleql.Filter(ctx, evalCtx, msg) == false
			if skipFlag {
				if env := utils.Log.Check(log.DebugLevel, fmt.Sprintf("1.filter message")); env != nil {
					env.Write(logf.Message(msg),
						logf.Bool("skip", true),
						logf.String("topic", msg.Topic()),
						logf.ByteString("filter", this.value.Body))
				}
				return nil
			}

			if env := utils.Log.Check(log.DebugLevel, fmt.Sprintf("1.filter message")); env != nil {
				env.Write(logf.Message(msg),
					logf.Bool("skip", false),
					logf.String("topic", msg.Topic()),
					logf.ByteString("filter", this.value.Body))
			}

			// 2. rule
			if err := this.ruleql.Invoke(ctx, evalCtx, msg); err != nil {
				utils.Log.For(ctx).Error(
					"2.filter message",
					logf.Message(msg),
					logf.Error(err),
					logf.String("topic", msg.Topic()),
				)
				return err
			} else {
				if env := utils.Log.Check(log.DebugLevel, fmt.Sprintf("2.filter message")); env != nil {
					env.Write(logf.Message(msg),
						logf.String("topic", msg.Topic()))
				}
			}
	*/
	// 3. action
	okCount, errCount := 0, 0
	errReportCount, errReportFailCount := 0, 0

	// 3.2 actions
	failures := make([]FailureMessage, 0)
	sctx := sink.NewContext(ctx)
	for _, ac := range this.actions {
		err := ac.Invoke(sctx, msg)
		if err != nil {
			utils.Log.For(ctx).Error(
				"3.2 action invoke error",
				logf.Message(msg),
				logf.Any("RuleID", this.ruleId),
				logf.Any("RuleQL", this.String()),
				logf.Any("Action", ac.ActionData),
				logf.Error(err),
			)

			report.ActionError(ctx, this, ac.ActionData, rulex.RuleActionError, err)
			failures = append(failures, FailureMessage{
				ActionId:     ac.ActionData.Id,
				ActionType:   ac.ActionData.Type,
				ErrorMessage: err.Error(),
			})

			//	xmetrics.MsgSent(xmetrics.StatusFailure, 0)
			xmetrics.GetIns().RuleExecute(this.userId, xmetrics.StatusFailure)
			//	metrics.Inc(metrics.MetricName(this.userId, this.ruleId, metrics.MtcRuleAction))
			//	metrics.Inc(metrics.MetricName(this.userId, this.ruleId, metrics.MtcRuleActionFAIL))
			errCount++
		} else {
			if env := utils.Log.Check(log.DebugLevel, fmt.Sprintf("3.2 action invoke")); env != nil {
				env.Write(logf.Message(msg),
					logf.String("topic", msg.Topic()))
			}

			//	xmetrics.MsgSent(xmetrics.StatusSuccess, len(msg.Data()))
			xmetrics.GetIns().RuleExecute(this.userId, xmetrics.StatusSuccess)
			//	metrics.Inc(metrics.MetricName(this.userId, this.ruleId, metrics.MtcRuleAction))
			//	metrics.Inc(metrics.MetricName(this.userId, this.ruleId, metrics.MtcRuleActionOK))
			okCount++
		}
	}

	if len(failures) > 0 {
		errMsg := msg.Copy().(metapb.PublishMessage)
		errorMessage := ErrorMessage{
			Topic:                 m.Topic(),
			RuleName:              this.value.Id,
			MessageId:             msg.PacketIdentifier(),
			Failures:              failures,
			Base64OriginalPayload: errMsg.Data(),
		}
		// TODO
		bty, err := json.Marshal(map[string]interface{}{"data": errorMessage})
		if err != nil {
			utils.Log.For(ctx).Error(
				"4.1 Marshal failures msg error",
				logf.Message(errMsg),
				logf.Error(err),
				logf.String("topic", errMsg.Topic()),
			)
			return err
		}
		errMsg.SetData(bty)
		for _, ac := range this.errActions {
			err = ac.Invoke(sctx, errMsg)
			if err != nil {
				utils.Log.For(ctx).Error(
					"4.2 action invoke error message fail",
					logf.Error(err),
				)
				report.ActionError(ctx, this, ac.ActionData, rulex.RuleActionFail, err)
				errReportFailCount++
				continue
			} else {
				if env := utils.Log.Check(log.DebugLevel, fmt.Sprintf("4.2.1 action invoke error message")); env != nil {
					env.Write(logf.Message(errMsg),
						logf.String("topic", errMsg.Topic()))
				}
				errReportCount++
			}
		}
	}

	if env := utils.Log.Check(log.DebugLevel, fmt.Sprintf("5 action done")); env != nil {
		env.Write(logf.Message(msg),
			logf.Int("ok_count", okCount),
			logf.Int("err_count", errCount),
			logf.Int("err_report_count", errReportCount),
			logf.Int("err_report_fail_count", errReportFailCount))
	}

	return nil
}

func (this *Task) initAction(ctx context.Context) (err error) {
	actions := make(map[string]*ActionTask)
	errActions := make(map[string]*ActionTask)
	for idx, value := range this.value.Actions {
		report.ActionStatus(ctx, this, value, rulex.RuleActionStart)
		ac, err := NewAction(ctx, value.Type, value) // actionBuilder
		if err != nil {
			utils.Log.For(ctx).Error("open stream error",
				logf.Error(err))
			report.ActionError(ctx, this, value, rulex.RuleActionError, err)
			return nil
		}
		ac.SetErrorCallback(func(at *ActionTask, err error) {
			report.ActionError(ctx, this, at.ActionData, rulex.RuleActionError, err)
		})
		if err := ac.Setup(ctx); err != nil {
			utils.Log.For(ctx).Error("init action error",
				logf.Error(err))
			report.ActionError(ctx, this, value, rulex.RuleActionError, err)
			continue
		}
		if value.ErrorFlag == true {
			errActions[strconv.Itoa(idx)] = ac
		} else {
			actions[strconv.Itoa(idx)] = ac
		}
	}
	this.actions = actions
	this.errActions = errActions

	return nil
}

func (this *Task) startAction(ctx context.Context) (err error) {
	return nil
}

func (this *Task) closeAction(ctx context.Context) (err error) {
	for _, ac := range this.actions {
		if err := ac.Action.Close(); err != nil {
			utils.Log.For(ctx).Error("Close action error",
				logf.Any("action", ac),
				logf.Error(err))
		}
	}
	for _, ac := range this.errActions {
		if err := ac.Action.Close(); err != nil {
			utils.Log.For(ctx).Error("Close action error",
				logf.Any("action", ac),
				logf.Error(err))
		}
	}
	return nil
}

func (this *Task) Check() error {
	if this.ruleql == nil {
		return errors.New("make rule failed, ruleql is nil")
	}
	if len(this.actions) == 0 {
		utils.Log.Bg().Warn("make rule failed, action is empty")
	}
	if len(this.errActions) > 1 {
		utils.Log.Bg().Warn("rule's  error actions is more than 1")
	}
	return nil
}

func (this *Task) closeRule() {
	//@TODO
}
