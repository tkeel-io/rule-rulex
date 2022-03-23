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

package republish

import (
	"encoding/json"
	"fmt"
	"github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/runtime/rule/stream/functions"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/tkeel-io/rule-rulex/pkg/sink"
	"github.com/tkeel-io/rule-util/stream"
	"reflect"
)

const (
	EntityType = "republish"
)

type republishOption struct {
	topic string `json:"topic,omitempty"`
}

var dstTopicField = v1.RULETYPE_REPUBLISH_DSTTOPIC

type republishAction struct {
	option     map[string]string
	entityType string
	entityID   string
}

var loopbackStream stream.Sink = nil

func SetRepublishStream(sinkStream stream.Sink) error {
	loopbackStream = sinkStream
	return nil
}

func init() {
	sink.Registered(EntityType, newRepublish)
}

func newRepublish(entityType, entityID string) types.Action {
	if env := utils.Log.Check(log.DebugLevel, "New action"); env != nil {
		env.Write(
			logf.Any("entityType", entityType),
			logf.Any("entityID", entityID),
		)
	}
	return &republishAction{
		entityType: entityType,
		entityID:   entityID,
	}
}

func (this *republishAction) ID() string {
	return fmt.Sprintf("republish:%s:%s", this.entityID, this.entityType)
}

func (this *republishAction) Setup(actx types.ActionContent, metadata map[string]string) error {
	//ctx := actx.Context()
	opt, err := parseOption(metadata)
	if err != nil {
		utils.Log.Bg().Error("metadata",
			logf.Any("metadata", metadata),
			logf.Error(err))
		return err
	}
	this.option = opt
	return nil
}

func (this *republishAction) Invoke(actx types.ActionContent, m v1.Message) error {
	ctx := actx.Context()
	if loopbackStream == nil {
		utils.Log.For(ctx).Error("loopback stream nil",
			logf.Error(ErrRepublishSinkStreamError))
		return ErrRepublishSinkStreamError
	}
	message, ok := m.(stream.PublishMessage)
	if !ok {
		utils.Log.For(ctx).Error("message decode fail",
			logf.Any("message type", reflect.TypeOf(m)))
		return types.ErrDecode
	}

	ttl := message.TTL()
	if ttl > 3 {
		return fmt.Errorf("republish error(ttl)")
	}
	message.SetTTL(ttl + 1)
	evalCtx := functions.NewMessageContext(message.Copy().(stream.PublishMessage))
	topic, ok := this.option[dstTopicField]
	if !ok || topic == "" {
		utils.Log.For(ctx).Error("republish topic empty",
			logf.Any("action", this),
			logf.Any("out_message", message),
			logf.Error(ErrRepublishTopicEmpty))
		return ErrRepublishTopicEmpty
	}
	dstTopic := fmt.Sprintf("%v", Execute(evalCtx, topic))
	if dstTopic == "" {
		log.For(ctx).Error("republish new topic empty",
			logf.Any("action", this),
			logf.Any("out_message", message),
			logf.Error(ErrRepublishNewTopicEmpty))
		return ErrRepublishNewTopicEmpty
	}
	message.SetTopic(dstTopic)
	if err := loopbackStream.Send(ctx, message); err != nil {
		log.For(ctx).Error("republish send error",
			logf.Any("action", this),
			logf.Any("out_message", message),
			logf.Error(ErrRepublishSendError))
		return ErrRepublishSendError
	}
	return nil
}

func parseOption(metadata map[string]string) (map[string]string, error) {
	if s, ok := metadata["option"]; ok {
		opt := make(map[string]string)
		err := json.Unmarshal([]byte(s), &opt)
		if err != nil {
			return nil, err
		} else {
			return opt, nil
		}
	}
	return nil, types.ErrOptionParseFail
}

func (this *republishAction) Close() error {
	return nil
}
