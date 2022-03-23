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

package resource

import (
	"context"
	"fmt"
	"sync"

	"github.com/tkeel-io/rule-util/metadata/v1error"
	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-util/pkg/tracing"
	metrics "github.com/tkeel-io/rule-rulex/internal/metrices"
	xmetrics "github.com/tkeel-io/rule-rulex/internal/metrices/prometheus"
	"github.com/tkeel-io/rule-rulex/internal/report"
	"github.com/tkeel-io/rule-rulex/internal/runtime"
	"github.com/tkeel-io/rule-rulex/internal/runtime/task/rule"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/tkeel-io/rule-rulex/pkg/sink/republish"
	"github.com/tkeel-io/rule-util/stream"

	metapb "github.com/tkeel-io/rule-util/metadata/v1"
)

var _ types.ResourceManager = (*resourceManager)(nil)

type resourceManager struct {
	slots    map[string]types.Slot
	treeLock sync.RWMutex

	sinks    map[string]stream.Sink
	sinkLock sync.RWMutex

	sources    map[string]stream.Source
	sourceLock sync.RWMutex

	logger          log.Factory
	tracer          tracing.Tracer
	pubSubQueueSink stream.Sink
	topicQueueSink  stream.Sink

	//pubSubFactor *pubsub.Factor
	ruleFactor *rule.Factor
	//routeFactor  *route.Factor
}

func NewManager(ctx context.Context, c *types.ServiceConfig) (*resourceManager, error) {
	rm := &resourceManager{
		slots:   make(map[string]types.Slot),
		sinks:   make(map[string]stream.Sink),
		sources: make(map[string]stream.Source),
		logger:  c.Logger,
		tracer:  c.Tracer,
	}

	rm.ruleFactor = rule.NewFactor(
		&rule.Option{
			Tracer:          c.Tracer,
			Logger:          c.Logger,
			ResourceManager: rm,
		})
	//rm.pubSubFactor = pubsub.NewFactor(
	//	&pubsub.Option{
	//		Client:          c.Client.PubSub(),
	//		Tracer:          c.Tracer,
	//		Logger:          c.Logger,
	//		ResourceManager: rm,
	//	})
	//rm.routeFactor = route.NewFactor(
	//	&route.Option{
	//		Tracer:          c.Tracer,
	//		Logger:          c.Logger,
	//		ResourceManager: rm,
	//	})
	return rm, nil
}

func (sm *resourceManager) Sinks() map[string]stream.Sink {
	return sm.sinks
}

func (sm *resourceManager) Sources() map[string]stream.Source {

	return sm.sources
}

func (sm *resourceManager) Sink(dsn string) (sink stream.Sink, err error) {
	sm.sinkLock.Lock()
	defer sm.sinkLock.Unlock()
	if sink, ok := sm.sinks[dsn]; ok {
		return sink, nil
	}
	if sink, err := stream.OpenSink(dsn); err != nil {
		return nil, err
	} else {
		sm.sinks[dsn] = sink
		return sink, err
	}
}

func (sm *resourceManager) Source(dsn string) (source stream.Source, err error) {
	sm.sourceLock.Lock()
	defer sm.sourceLock.Unlock()
	if source, ok := sm.sources[dsn]; ok {
		return source, nil
	}
	if source, err := stream.OpenSource(dsn); err != nil {
		return nil, err
	} else {
		sm.sources[dsn] = source
		return source, err
	}
}

func (sm *resourceManager) PubsubSink() stream.Sink {
	if sm.pubSubQueueSink == nil {
		log.Fatal("pubsub stream is nil")
	}
	return sm.pubSubQueueSink
}
func (sm *resourceManager) SetPubsubSink(sink stream.Sink) {
	if sm.pubSubQueueSink != nil {
		log.Warn("pubsub stream is reset",
			logf.Any("pubsubActionSink", sm.pubSubQueueSink.String()),
		)
	}
	sm.pubSubQueueSink = sink
	return
}

func (sm *resourceManager) RePublishSink() stream.Sink {
	if sm.topicQueueSink == nil {
		log.Fatal("pubsub stream is nil")
	}
	return sm.topicQueueSink
}

func (sm *resourceManager) SetRePublishSink(sink stream.Sink) {
	if sm.topicQueueSink != nil {
		log.Warn("republish stream is reset",
			logf.Any("republishActionSink", sm.topicQueueSink.String()),
		)
	}
	sm.topicQueueSink = sink
	republish.SetRepublishStream(sink)
	return
}

func (sm *resourceManager) SetMetricsSink(sink stream.Sink) {
	metrics.InitMetrics(sink)
	return
}

func (sm *resourceManager) SetReportSink(sink stream.Sink) {
	report.InitReportSink(sink)
	return
}

func (sm *resourceManager) Slot(userID string) (task types.Slot) {
	sm.treeLock.RLock()
	defer sm.treeLock.RUnlock()
	if slot, ok := sm.slots[userID]; ok {
		return slot
	}
	if slot := runtime.NewSlot(userID, sm.logger, sm.tracer); slot != nil {
		sm.slots[userID] = slot
		return slot
	} else {
		log.Fatal("slot nil")
		return nil
	}
}

func (s *resourceManager) HandleEvent(ctx context.Context, res *metapb.ResourceObject) error {
	var err error
	typ := res.Type
	switch evt := res.Body.(type) {
	case *metapb.ResourceObject_InitConfig:
		if typ != metapb.ResourceObject_ADDED {
			s.logger.For(ctx).Warn("Unknown Init Resource Type",
				logf.Any("typ", typ),
			)
			return nil
		}
	case *metapb.ResourceObject_Stream:
		if typ != metapb.ResourceObject_ADDED {
			s.logger.For(ctx).Warn("Unknown Stream Resource Type",
				logf.Any("typ", typ),
			)
			return nil
		}

		// Deprecated: Please use handleSink\handleTopicQueue\handlePubSubQueue
		// handleSource will share topic group
		//err = s.handleSource(ctx, evt)
		//if err != nil {
		//	return err
		//}

		err = s.handleRuleTopic(ctx, evt.Stream.RuleTopic)
		if err != nil {
			return err
		}
		err = s.handlePubSubTopic(ctx, evt.Stream.PubSubTopic)
		if err != nil {
			return err
		}
		err = s.handlePubSubQueue(ctx, evt.Stream.PubSubQueue)
		if err != nil {
			return err
		}
	case *metapb.ResourceObject_Subscription:
		//err = s.handleSubscription(ctx, typ, evt)
		//if err != nil {
		//	return err
		//}
		xmetrics.ResourceSync(xmetrics.ResourceTypeSubscription, 1, 0)
		if env := utils.Log.Check(log.DebugLevel, fmt.Sprintf("Skip Subscription Resource")); env != nil {
			env.Write(logf.Any("evt", evt))
		}
	case *metapb.ResourceObject_Route:
		//err = s.handleRoute(ctx, typ, evt)
		//if err != nil {
		//	return err
		//}
		xmetrics.ResourceSync(xmetrics.ResourceTypeRoute, 1, 0)
		if env := utils.Log.Check(log.DebugLevel, fmt.Sprintf("Skip Route Resource")); env != nil {
			env.Write(logf.Any("evt", evt))
		}
	case *metapb.ResourceObject_Rule:
		xmetrics.ResourceSync(xmetrics.ResourceTypeRule, 1, 0)
		err = s.handleRule(ctx, typ, evt)
		if err != nil {
			return err
		}

	default:
		s.logger.For(ctx).Error("Unknown Resource Type",
			logf.Any("evt", res.Body),
		)
	}
	return nil
}

func (s *resourceManager) handleRuleTopic(ctx context.Context, RuleTopic *metapb.Flow) error {
	source, err := s.Source(RuleTopic.URI)
	if err != nil {
		s.logger.For(ctx).Error(
			"create source",
			logf.StatusCode(v1error.StreamOpenFail),
			logf.String("source", RuleTopic.URI),
			logf.Error(err),
		)
		return err
	} else {
		s.logger.For(ctx).Info(
			"create source",
			logf.StatusCode(v1error.StreamOpen),
			logf.String("source", RuleTopic.URI),
		)
	}
	s.sources[RuleTopic.URI] = source
	return nil
}

func (s *resourceManager) handlePubSubTopic(ctx context.Context, flow *metapb.Flow) error {
	sink, err := s.Sink(flow.URI)
	if err != nil {
		s.logger.For(ctx).Error(
			"failed to create republish sink",
			logf.StatusCode(v1error.StreamOpenFail),
			logf.String("source", flow.URI),
			logf.Error(err),
		)
	}
	// Router republish on loopback
	s.SetRePublishSink(sink)

	// Metrics queue
	s.SetMetricsSink(sink)

	// Metrics queue
	s.SetReportSink(sink)

	return nil
}

func (s *resourceManager) handlePubSubQueue(ctx context.Context, flow *metapb.Flow) error {
	sink, err := s.Sink(flow.URI)
	if err != nil {
		s.logger.For(ctx).Error(
			"failed to create republish sink",
			logf.StatusCode(v1error.StreamOpenFail),
			logf.String("source", flow.URI),
			logf.Error(err),
		)
	}
	s.SetPubsubSink(sink)
	return nil
}

//func (s *resourceManager) handleSubscription(ctx context.Context, typ metapb.ResourceObject_EventType, sub *metapb.ResourceObject_Subscription) error {
//	evt := s.pubSubFactor.Tasks(ctx, typ, sub.Subscription)
//	if evt == nil {
//		return types.ErrCreatTaskFail
//	}
//	s.logger.For(ctx).Info("parse task",
//		logf.Topic(evt.Topic),
//		logf.UserID(evt.UserId),
//		logf.Any("evt", evt),
//		logf.Any("Type", evt.Type),
//		logf.String("ID", evt.Task.ID()),
//		logf.String("Action", typ.String()),
//		logf.String("Task", evt.Task.String()))
//	if evt.UserId == "" {
//		err := types.ErrTaskUserIDEmpty
//		s.logger.Bg().Error("add task",
//			logf.StatusCode(v1error.SubscriptionCreateFailed),
//			logf.String("ID", evt.Task.ID()),
//			logf.Any("Type", evt.Type),
//			logf.String("Task", evt.Task.String()),
//			logf.Error(err))
//		return err
//	}
//
//	return s.handleTask(ctx, typ, evt)
//}

func (s *resourceManager) handleRule(ctx context.Context, typ metapb.ResourceObject_EventType, rule *metapb.ResourceObject_Rule) error {
	evt, err := s.ruleFactor.Tasks(ctx, typ, rule.Rule)
	if err != nil {
		utils.Log.For(ctx).Error("creat task fail", logf.Error(err))
		return nil
	}
	s.logger.For(ctx).Info("handle task",
		logf.Topic(evt.Topic),
		logf.UserID(evt.UserId),
		logf.Any("Action", typ),
		logf.Any("evt", evt),
		logf.Any("Type", evt.Type),
		logf.String("ID", evt.Task.ID()),
		logf.String("Task", evt.Task.String()))
	if evt.UserId == "" {
		err := types.ErrTaskUserIDEmpty
		s.logger.Bg().Error("handle task",
			logf.StatusCode(v1error.SubscriptionCreateFailed),
			logf.String("ID", evt.Task.ID()),
			logf.Any("Type", evt.Type),
			logf.String("Task", evt.Task.String()),
			logf.Error(err))
		return err
	}
	return s.handleTask(ctx, typ, evt)
}

//func (s *resourceManager) handleRoute(ctx context.Context, typ metapb.ResourceObject_EventType, route *metapb.ResourceObject_Route) error {
//	evt := s.routeFactor.Tasks(ctx, typ, route.Route)
//	if evt == nil {
//		return types.ErrCreatTaskFail
//	}
//	s.logger.For(ctx).Info("add task",
//		logf.Topic(evt.Topic),
//		logf.UserID(evt.UserId),
//		logf.Any("evt", evt),
//		logf.Any("Type", evt.Type),
//		logf.String("ID", evt.Task.ID()),
//		logf.String("Action", "PUT"),
//		logf.String("Task", evt.Task.String()))
//	if evt.UserId == "" {
//		err := types.ErrTaskUserIDEmpty
//		s.logger.Bg().Error("add task",
//			logf.StatusCode(v1error.SubscriptionCreateFailed),
//			logf.String("ID", evt.Task.ID()),
//			logf.Any("Type", evt.Type),
//			logf.String("Task", evt.Task.String()),
//			logf.Error(err))
//		return err
//	}
//	return s.handleTask(ctx, typ, evt)
//}

func (s *resourceManager) handleTask(ctx context.Context, typ metapb.ResourceObject_EventType, evt *types.TaskEvent) error {
	switch typ {
	case metapb.ResourceObject_ADDED:
		ret := s.Slot(evt.UserId).AddTask(ctx, evt)
		s.logger.For(ctx).Info("add task",
			logf.StatusCode(v1error.RuleCreate),
			logf.String("ID", evt.Task.ID()),
			logf.Any("Type", typ),
			logf.Bool("Status", ret))
	case metapb.ResourceObject_DELETED:
		ret := s.Slot(evt.UserId).DelTask(ctx, evt)
		s.logger.For(ctx).Info("del task",
			logf.StatusCode(v1error.RuleDelete),
			logf.String("ID", evt.Task.ID()),
			logf.Any("Type", typ),
			logf.Bool("Status", ret))
	default:
		s.logger.For(ctx).Warn("Unknown Resource Type",
			logf.Any("typ", typ),
			logf.Any("evt", evt),
		)
	}
	return nil
}
