/*
 * Copyright (C) 2019 Yunify, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this work except in compliance with the License.
 * You may obtain a copy of the License in the LICENSE file, or at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rulex

import (
	"context"
	"errors"
	"fmt"
	metapb "github.com/tkeel-io/rule-util/metadata/v1"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	stateful "github.com/tkeel-io/rule-rulex/internal/runtime/rule/stream"
	"github.com/tkeel-io/rule-rulex/internal/runtime/rule/stream/functions"
	"github.com/tkeel-io/rule-rulex/internal/runtime/task/rule"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/tkeel-io/rule-rulex/pkg/sink"
	"github.com/tkeel-io/rule-util/stream"
	"github.com/go-kit/kit/endpoint"
)

var _ metapb.RulexNodeActionServer = (*Endpoint)(nil)

//Endpoint subject endpoint
type Endpoint struct {
	endpoint.Endpoint
	context context.Context
}

//NewEndpoint creat endpoint from config
func NewEndpoint(ctx context.Context) *Endpoint {
	return &Endpoint{
		context: ctx,
	}
}

func (e *Endpoint) ExecAction(ctx context.Context, request *metapb.ActionExecRequest) (*metapb.ActionExecResponse, error) {
	utils.Log.Bg().Info("ExecAction",
		logf.Any("request", request))
	if request == nil || request.Action == nil {
		return nil, errors.New("request nil")
	}
	ac, err := rule.NewAction(ctx, "Exce", request.Action)
	if err != nil {
		return nil, err
	}
	actx := sink.NewContext(ctx)
	err = ac.Action.Setup(actx, request.Action.Metadata)
	if err != nil {
		return &metapb.ActionExecResponse{
			UserId:   request.UserId,
			PacketId: request.PacketId,
			ErrMsg:   err.Error(),
		}, nil
	}
	ac.Action.Close()
	return &metapb.ActionExecResponse{
		UserId:   request.UserId,
		PacketId: request.PacketId,
	}, nil
}

func (e *Endpoint) ExecRule(ctx context.Context, request *metapb.RuleExecRequest) (*metapb.RuleExecResponse, error) {
	utils.Log.Bg().Info("ExecRule",
		logf.Any("request", request))
	if request == nil {
		return nil, errors.New("request nil")
	}
	if request.Rule == nil {
		return nil, errors.New("request rule nil")
	}
	if request.Rule.Body == nil {
		return nil, errors.New("request rule body nil")
	}
	if request == nil || request.Message == nil {
		return nil, errors.New("request message nil")
	}
	msg := request.Message
	rule := request.Rule

	evalCtx := functions.NewMessageContext(msg.Copy().(stream.PublishMessage))

	if rule.Body == nil {
		return nil, fmt.Errorf("rule Body nil")
	}
	exp, err := ruleql.Parse(string(rule.Body))
	if err != nil {
		utils.Log.Bg().Error("Parse expr error",
			logf.ByteString("expr", rule.Body),
			logf.Error(err))
		return &metapb.RuleExecResponse{
			UserId:   request.UserId,
			PacketId: request.PacketId,
			ErrMsg:   err.Error(),
			Rule:     rule,
			Message:  msg,
		}, nil
	}

	_, ok := ruleql.GetTopic(exp)
	if !ok {
		return nil, fmt.Errorf("json unmarshal error")
	}

	ok = ruleql.EvalFilter(evalCtx, exp)
	if ok {
		err = stateful.Exce(evalCtx, exp, msg)
		if err != nil {
			return nil, fmt.Errorf("exce rule error")
		}
	}else{
		msg.SetData([]byte(""))
	}

	utils.Log.Bg().Info("ExecRule.EvalFilter",
		logf.Any("request", request))

	return &metapb.RuleExecResponse{
		UserId:   request.UserId,
		PacketId: request.PacketId,
		Rule:     rule,
		Message:  msg,
	}, nil
}
