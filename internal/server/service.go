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

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	pb "github.com/tkeel-io/rule-rulex/internal/api/v1"
	conn "github.com/tkeel-io/rule-rulex/internal/network"
	"github.com/tkeel-io/rule-rulex/internal/resource"
	"github.com/tkeel-io/rule-rulex/internal/runtime/rule/stream/functions"
	"github.com/tkeel-io/rule-rulex/internal/runtime/task/rule"
	"github.com/tkeel-io/rule-rulex/internal/transport/grpc"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	metapb "github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/metadata/v1error"
	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-util/pkg/tracing"
	xutils "github.com/tkeel-io/rule-util/pkg/utils"
	xstream "github.com/tkeel-io/rule-util/stream"
)

type RuleService struct {
	id     string
	status types.STATUS

	slotIdx      int
	slotCapacity int
	loopback     xstream.Source
	cliManager   grpc.JobManagerClient
	cliPubSub    grpc.PubSubClient
	cliRule      grpc.RuleActionClient

	logger          log.Factory
	tracer          opentracing.Tracer
	resourceManager types.ResourceManager
	syncManager     types.SyncManager
	//pubSubFactor    *pubsub.Factor
	ruleFactor *rule.Factor
	config     *types.ServiceConfig

	//shutdown chan error
}

func New(ctx context.Context, slotNum int, c *types.ServiceConfig) *RuleService {
	s := &RuleService{
		id:           "slot_" + xutils.GenerateUUID(),
		slotIdx:      0,
		config:       c,
		slotCapacity: slotNum,
		logger:       c.Logger,
		tracer:       c.Tracer,
	}

	if loopbackStream, err := xstream.OpenSource(types.LoopbackStream); err != nil {
		s.logger.Bg().Error("open loppback stream",
			logf.StatusCode(v1error.ServiceStartFailed),
			logf.Topic(types.LoopbackStream),
			logf.Error(err))
	} else {
		if env := utils.Log.Check(log.DebugLevel, fmt.Sprintf("open loppback stream")); env != nil {
			env.Write(logf.StatusCode(v1error.ServiceStartFailed),
				logf.Topic(types.LoopbackStream))
		}
		s.loopback = loopbackStream
	}

	if resourceManager, err := resource.NewManager(ctx, s.config); err != nil {
		s.logger.For(ctx).Error("Creat sync service fail",
			logf.Error(err))
		s.logger.For(ctx).Fatal("exit")
	} else {
		s.resourceManager = resourceManager
	}
	if err := s.initSyncManager(ctx, c); err != nil {
		c.Logger.For(ctx).Error("Creat sync service fail",
			logf.Error(err))
	}

	return s
}

func (s *RuleService) initSyncManager(ctx context.Context, c *types.ServiceConfig) error {
	if syncManager, err := conn.NewWatcher(ctx, c, s.resourceManager); err != nil {
		return err
	} else {
		syncManager.SetReceiver(ctx, s.handleEvent)
		s.syncManager = syncManager
	}
	return nil
}

/*

func (s *watcher) transform(resp ){
	switch body := resp.(type) {
	case *metapb.RestoreResponse_InitConfig:
		if s.status == types.STARTING {
			s.logger.For(ctx).Error("init double",
				logf.Error(err))
		}
		s.status = types.STARTING
		s.logger.For(ctx).Debug("recive RestoreResponse_InitConfig")
		continue
	case *metapb.RestoreResponse_Resource:
		res := body.Resource
		s.logger.For(ctx).Debug(
			"recive restore resource response",
			logf.Any("res", res),
		)
		if err := s.deploy(ctx, &types.SinkEvent{res.Sink.URI}); err != nil {
			return ERROR_VERSION, types.ErrMetadataNotReachable
		}
		for _, sURL := range res.Source {
			if err := s.deploy(ctx, &types.SourceEvent{sURL.URI}); err != nil {
				return ERROR_VERSION, types.ErrMetadataNotReachable
			}
		}
		continue
	case *metapb.RestoreResponse_Subscription:
		rsp := body.Subscription
		rev = rsp.StartRevision
		for _, value := range rsp.Subscriptions {
			task, err := s.pubSubInvoke(ctx, value)
			if task == nil {
				return ERROR_VERSION, types.ErrCreatTaskFail
			}
			if err != nil {
				return ERROR_VERSION, types.ErrDeployTaskFail
			}
			if rev < value.ResourceVersion {
				rev = value.ResourceVersion
			}
		}
	case *metapb.RestoreResponse_Rules:
		rsp := body.Rules
		rev = rsp.StartRevision
		for _, value := range rsp.Rules {
			task, err := s.ruleInvoke(ctx, value)
			if task == nil {
				return ERROR_VERSION, types.ErrCreatTaskFail
			}
			if err != nil {
				return ERROR_VERSION, types.ErrDeployTaskFail
			}
			if rev < value.ResourceVersion {
				rev = value.ResourceVersion
			}
		}
	}
}
func (s *watcher) pubSubInvoke(ctx context.Context, value *metapb.SubscriptionValue) (interface{}, error) {
	task := s.pubSubFactor.Tasks(ctx, value)
	if task == nil {
		return task, types.ErrCreatTaskFail
	}
	if err := s.deploy(ctx, task); err != nil {
		return task, types.ErrDeployTaskFail
	}
	return task, nil
}

func (s *watcher) ruleInvoke(ctx context.Context, value *metapb.RuleValue) (interface{}, error) {
	task := s.ruleFactor.Tasks(ctx, value)
	if task == nil {
		return task, types.ErrCreatTaskFail
	}
	if err := s.deploy(ctx, task); err != nil {
		return task, types.ErrDeployTaskFail
	}
	return task, nil
}

*/

func (s *RuleService) handleEvent(ctx context.Context, evt *metapb.ResourceObject) error {
	return s.resourceManager.HandleEvent(ctx, evt)
}

//func (s *RuleService) handleEvent_old(ctx context.Context, evt interface{}) error {
//	return s.resourceManager.HandleEvent(ctx, evt)
//}

func (s *RuleService) Start(ctx context.Context) error {
	//metrics.InitMetrics(types.LoopbackStream)
	s.syncManager.Run()
	s.logger.For(ctx).Info("sync event",
		logf.StatusCode(v1error.SyncStart))
	go func() {
		s.logger.Bg().Info("starting cloud event")
		ctx := context.Background()
		if err := s.loopback.StartReceiver(ctx, s.handleMessage); err != nil {
			log.Error("starting loopback",
				logf.StatusCode(v1error.ServiceStartFailed),
				logf.Error(err))
			log.Fatal("open loopback fail")
		}
	}()

	for _, source := range s.resourceManager.Sources() {
		go func(source xstream.Source) {
			ctx := context.Background()
			if err := source.StartReceiver(ctx, s.handleMessage); err != nil {
				log.Error("OpenStream",
					logf.StatusCode(v1error.StreamOpenFail),
					logf.String("Stream", source.String()),
					logf.Error(err))
			} else {
				log.Info("OpenStream",
					logf.StatusCode(v1error.StreamOpen),
					logf.String("Stream", source.String()))
			}
		}(source)
	}

	return nil
}

//@TODO
func (s *RuleService) Stop(ctx context.Context) {
	s.logger.For(ctx).Info("stop")
}

//@TODO
func (s *RuleService) Destroy(ctx context.Context) {
	s.logger.For(ctx).Info("destroy")
	s.Stop(ctx)
}

func Interface2string(in interface{}) (out string) {
	switch inString := in.(type) {
	case string:
		out = inString
	default:
		out = ""
	}
	return
}

func SubscribeID2Topic(subscribeID string) (topic string) {

	itmes := strings.Split(subscribeID, "_")
	if len(itmes) != 3 {
		return
	}
	topic = fmt.Sprintf("rulex/rule-%s", itmes[1])
	return
}

func (s *RuleService) handleMessage(ctx context.Context, m interface{}) error {
	//	metrics.MsgMetrics.Mark(1)
	//	msgCtx := xmetrics.NewMsgContext()
	//defer msgCtx.Observe(nil)

	span, ctx := tracing.AddSpan(ctx, "Handle Message")
	defer span.Finish()
	// TODO

	message, ok := m.(xstream.PublishMessage)
	//	xmetrics.MsgReceived(len(message.Data()))
	if !ok {
		s.logger.For(ctx).Error("message decode fail",
			logf.Any("message", m),
			logf.Any("message type", reflect.TypeOf(m)))

		if dataByte, ok := m.([]byte); ok {

			msg := &pb.TopicEventRequest{}
			if err := json.Unmarshal(dataByte, msg); err == nil {
				switch kv := msg.Data.AsInterface().(type) {
				case map[string]interface{}:
					subID := Interface2string(kv["subscribe_id"])
					domain := Interface2string(kv["owner"])
					if domain == "" {
						domain = "admin"
					}
					topic := SubscribeID2Topic(subID)
					if topic == "" {
						return types.ErrDecode
					}
					msgData, _ := json.Marshal(kv)
					message = xstream.NewMessage()
					message.SetData(msgData)
					message.SetDomain(domain)
					message.SetTopic(topic)
				}

			}
		} else {
			return types.ErrDecode
		}

	}

	span.SetTag("message.entity", message.Entity())
	span.SetTag("message.topic", message.Topic())
	span.SetTag("message.domain", message.Domain())

	if env := utils.Log.Check(log.DebugLevel, fmt.Sprintf("handle")); env != nil {
		env.Write(logf.String("domain", message.Domain()),
			logf.String("topic", message.Topic()),
			logf.Any("message", message),
			logf.Time("time", time.Now()))
	}

	userID := message.Domain()
	if userID == "" {
		s.logger.For(ctx).Error("userID empty",
			logf.Message(message))
	}

	entityID := message.Entity()
	if entityID == "" {
		s.logger.For(ctx).Error("entityID empty",
			logf.Message(message))
	}

	s.Debug(ctx, message)

	//if message.TargetEntity() != "" {
	//	targetEntity := message.TargetEntity()
	//	if env := utils.Log.Check(log.DebugLevel, fmt.Sprintf("Close")); env != nil {
	//		//env.Write(logf.Any("messages", message))
	//	}
	//	s.logger.For(ctx).Debug(
	//		"Direct Send",
	//		logf.String("target_entity", targetEntity),
	//	)
	//
	//	func(message stream.Message) {
	//		if err := s.resourceManager.PubsubSink().Send(ctx, message); err != nil {
	//			s.logger.For(ctx).Error(
	//				"send message fail",
	//				logf.Error(err),
	//			)
	//		}
	//		s.logger.For(ctx).Debug(
	//			"send message ok",
	//			logf.Any("message", message),
	//		)
	//	}(message.Copy())
	//	message.SetTargetEntity("")
	//	ctx = context.WithValue(ctx, types.DirectEntityKey, targetEntity)
	//	metrics.Inc(metrics.MetricName(userID, entityID, metrics.MtcMessageAPIDown))
	//} else {
	//	metrics.Inc(metrics.MetricName(userID, entityID, metrics.MtcMessageUp))
	//}

	vCtx := functions.NewMessageContext(message.(xstream.PublishMessage))
	globalSlot := s.resourceManager.Slot("*")
	globalSlot.Invoke(ctx, vCtx, message)
	userSlot := s.resourceManager.Slot(userID)
	userSlot.Invoke(ctx, vCtx, message)
	return nil
}

func (s *RuleService) Debug(ctx context.Context, message xstream.PublishMessage) {
	userID := message.Domain()
	if message.Topic() == "/mdmp/debug/tree" {
		func(message xstream.Message) {
			globalSlot := s.resourceManager.Slot("*")
			tree := "global:\n" + globalSlot.Tree().String()
			message.SetData([]byte(tree))
			message.SetTargetEntity(message.Entity())
			if err := s.resourceManager.PubsubSink().Send(ctx, message); err != nil {
				s.logger.For(ctx).Error(
					"send message fail",
					logf.Error(err),
				)
			}
			s.logger.For(ctx).Debug(
				"send message ok",
				logf.Any("message", message),
			)
		}(message.Copy())
		func(message xstream.Message) {
			userSlot := s.resourceManager.Slot(userID)
			tree := userID + ":\n" + userSlot.Tree().String()
			message.SetData([]byte(tree))
			message.SetTargetEntity(message.Entity())
			if err := s.resourceManager.PubsubSink().Send(ctx, message); err != nil {
				s.logger.For(ctx).Error(
					"send message fail",
					logf.Error(err),
				)
			}
			s.logger.For(ctx).Debug(
				"send message ok",
				logf.Any("message", message),
			)
		}(message.Copy())
	}
}
