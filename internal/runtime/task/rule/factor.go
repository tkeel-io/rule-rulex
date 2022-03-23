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

package rule

import (
	"context"
	"errors"
	"github.com/tkeel-io/rule-util/metadata/v1error"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/report"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	rulex "github.com/tkeel-io/rule-rulex/pkg/define"
	"sync"

	metapb "github.com/tkeel-io/rule-util/metadata/v1"
)

var (
	tasks sync.Map
	//map[string]*Task
)

func NewFactor(opt *Option) *Factor {
	mg := &Factor{
		cli: &cli{
			tracer:          opt.Tracer,
			logger:          opt.Logger,
			resourceManager: opt.ResourceManager,
		},
	}
	mg.check()
	return mg
}

func (fc *Factor) check() {
	if fc.cli == nil {
		panic("cli is nil")
	}
}

func (fc *Factor) Tasks(ctx context.Context, typ metapb.ResourceObject_EventType, rule *metapb.RuleQL) (*types.TaskEvent, error) {

	if env := utils.Log.Check(log.DebugLevel, "watchRuleChange"); env != nil {
		env.Write(
			logf.String("type", typ.String()),
			logf.String("rule", rule.String()))
	}
	switch typ {
	case metapb.ResourceObject_ADDED:
		utils.Log.For(ctx).Info("Load rule",
			logf.StatusCode(v1error.RuleOpen),
			logf.Any("EntityType", typ),
			logf.Any("Rule", rule))
		userId, topic, rrule, err := fc.makePutRule(ctx, rule)
		if err != nil {
			return nil, err
		}
		report.TaskStatus(ctx, rrule, rulex.RuleStarted)
		return &types.TaskEvent{
			types.RuleEvent,
			userId,
			topic,
			rrule,
		}, nil
	case metapb.ResourceObject_DELETED:
		utils.Log.For(ctx).Info("Del rule",
			logf.StatusCode(v1error.RuleClose),
			logf.Any("EntityType", typ),
			logf.Any("Rule", rule))
		userId, topic, rrule, err := fc.makeDelRule(ctx, rule)
		if err != nil {
			return nil, err
		}
		report.TaskStatus(ctx, rrule, rulex.RuleStoped)
		return &types.TaskEvent{
			types.RuleEvent,
			userId,
			topic,
			rrule,
		}, nil
	}
	return nil, ErrEventTypeUndefined
}

func (fc *Factor) makePutRule(ctx context.Context, value *metapb.RuleQL) (userId string, topic string, rrule *Task, err error) {
	u := value.UserId
	t := value.TopicFilter
	if u == "" {
		err = errors.New("userId empty")
		utils.Log.Bg().Error(
			"add:fail creat rule",
			logf.Any("rule", value),
			logf.Error(err),
		)
		return u, t, nil, err
	}
	if t == "" {
		err = errors.New("topic empty")
		utils.Log.Bg().Error(
			"add:fail creat rule",
			logf.Any("rule", value),
			logf.Error(err),
		)
		return u, t, nil, err
	}
	if value.Id == "" {
		err = errors.New("id empty")
		utils.Log.Bg().Error(
			"add:fail creat rule",
			logf.Any("rule", value),
			logf.Error(err),
		)
		return u, t, nil, err
	}

	r, err := newStreamRuleTask(ctx, value, fc)
	if err != nil {
		utils.Log.Bg().Error(
			"add:fail creat rule",
			logf.Any("rule", value),
			logf.Error(err),
		)
		return u, t, nil, err
	}

	//new rule
	if err := r.init(ctx); err != nil {
		utils.Log.Bg().Error(
			"init rule failed",
			logf.Any("value", value),
			logf.Error(err),
		)
		return u, t, nil, err
	}
	if err := r.initAction(ctx); err != nil {
		utils.Log.Bg().Error(
			"init rule action failed",
			logf.Any("value", value),
			logf.Error(err),
		)
		return u, t, nil, err
	}

	if err := r.Check(); err != nil {
		utils.Log.Bg().Error(
			"check rule failed",
			logf.Any("value", value),
			logf.Error(err),
		)
		return u, t, nil, err
	}
	return u, t, r, nil
}

func (fc *Factor) makeDelRule(ctx context.Context, value *metapb.RuleQL) (userId string, topic string, rrule *Task, err error) {
	u := value.UserId
	t := value.TopicFilter
	if u == "" {
		err = errors.New("userId empty")
		utils.Log.Bg().Error(
			"del:fail creat rule",
			logf.Any("rule", value),
			logf.Error(err),
		)
		return u, t, nil, err
	}
	if value.Id == "" {
		err = errors.New("id empty")
		utils.Log.Bg().Error(
			"del:fail creat rule",
			logf.Any("rule", value),
			logf.Error(err),
		)
		return u, t, nil, err
	}

	r, err := newStreamRuleTask(ctx, value, fc)
	if err != nil {
		utils.Log.Bg().Error(
			"del:fail creat rule",
			logf.Any("rule", value),
			logf.Error(err),
		)
	}

	return u, t, r, nil
}
