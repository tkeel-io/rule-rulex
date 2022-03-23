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

package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/tkeel-io/rule-rulex/pkg/sink"
	xutils "github.com/tkeel-io/rule-rulex/pkg/sink/utils"
	"github.com/tkeel-io/rule-util/stream"
	"net/url"
	"strings"
)

const EntityType = "stream"

type Option struct {
	SinkURI string `json:"sink,omitempty"`
}

type metricMap struct {
	globalFields map[string]string //global globalFields
	subFields    []*keyValue
	metricNodes  map[string]*node //metric metricNodes
}

type valueMap map[string]string //field map

type keyValue struct {
	name  string
	value string
}

type node struct {
	nodeName string
	fields   []*keyValue
}

type execData1 struct {
	ts     int64
	names  []string
	values []interface{}
}

type cloudsatData []nodeData
type nodeData map[string]interface{}

func (this nodeData) Copy() nodeData {
	newMap := make(map[string]interface{})
	for k, v := range this {
		newMap[k] = v
	}
	return newMap
}

type streamSink struct {
	option      *Option
	metricMap   *metricMap
	valueMapCtx xutils.Context
	sink        stream.Sink
}

// Setup
// 1. 创建
func (this *streamSink) Setup(ctx types.ActionContent, metadata map[string]string) error {
	log.GlobalLogger().Bg().Debug("Setup", logf.Any("metadata", metadata))

	if env := utils.Log.Check(log.DebugLevel, fmt.Sprintf("Close")); env != nil {
		//env.Write(logf.Any("messages", message))
	}
	ret, err := this.parseOption(metadata)
	if err != nil {
		return err
	} else {
		this.option = ret
	}
	u, err := url.Parse(ret.SinkURI)
	if err != nil {
		return err
	}
	arr := strings.Split(u.Path, "/")
	if len(arr) != 3 && arr[0] != ""{
		fmt.Println("kafka://[user[:password]@][net[(addr)]]/{topic}/{group_id}[?partition=partition&offset=offset]")
		return fmt.Errorf("kafka: parse error ")
	}

	sink, err := stream.OpenSink(ret.SinkURI)
	if err != nil {
		log.GlobalLogger().Bg().Error("stream: open sink error ", logf.Any("error", err.Error()))
		return err
	}
	this.sink = sink
	return nil
}

// Invoke
// 1. 发送消息
// 2. 重试，错误处理
func (this *streamSink) Invoke(ctx types.ActionContent, message types.Message) error {
	log.GlobalLogger().Bg().Info("Invoke", logf.Any("messages", string(message.Data())))
	//msgCtx := NewMessageContext(message.(types.PublishMessage))
	return this.publish(ctx, message)
}

// Close
// 关闭连接
func (this *streamSink) Close() error {
	this.sink.Close(context.Background())
	log.GlobalLogger().Bg().Debug("Close")
	return nil
}

func (this *streamSink) publish(ctx types.ActionContent, message types.Message) (err error) {
	err = this.sink.Send(ctx.Context(), message)
	if err != nil {
		log.GlobalLogger().Bg().Error("Publish", logf.Any("error", err.Error()))
		return err
	}
	return nil
}

func (this *streamSink) parseOption(metadata map[string]string) (*Option, error) {
	if s, ok := metadata["option"]; ok {
		opt := Option{}
		err := json.Unmarshal([]byte(s), &opt)
		if err != nil {
			return nil, err
		}
		if opt.SinkURI == "" {
			return nil, fmt.Errorf("sink uri empty")
		}
		return &opt, nil
	}
	return nil, fmt.Errorf("option not found")
}

func init() {
	sink.Registered(EntityType, newAction)
}

func newAction(entityType, entityID string) types.Action {
	return newSink(entityType, entityID)
}
func newSink(entityType, entityID string) *streamSink {
	return &streamSink{}
}
