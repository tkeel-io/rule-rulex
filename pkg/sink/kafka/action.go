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

package kafka

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	v1 "github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/tkeel-io/rule-rulex/pkg/sink"
	"github.com/Shopify/sarama"
)

type Option struct {
	SinkURI string `json:"sink,omitempty"`
}
type nodeData map[string]interface{}

func (this nodeData) Copy() nodeData {
	newMap := make(map[string]interface{})
	for k, v := range this {
		newMap[k] = v
	}
	return newMap
}

type kafkaSink struct {
	option   *Option
	topic    string
	shutdown chan struct{}
	sink     sarama.AsyncProducer
}

// Setup
// 1. 创建
func (this *kafkaSink) Setup(ctx types.ActionContent, metadata map[string]string) error {
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

	if u.Scheme != "kafka" {
		return nil
	}

	arr := strings.Split(u.Path, "/")
	if len(arr) != 3 || arr[0] != "" {
		fmt.Println("kafka://[user[:password]@][net[(addr)]]/{topic}/{group_id}[?partition=partition&offset=offset]")
		return fmt.Errorf("kafka: parse error ")
	}

	this.topic = arr[1]
	brokers := strings.Split(u.Host, ",")

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Compression = sarama.CompressionGZIP
	this.sink, err = sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return err
	}

	this.shutdown = make(chan struct{}, 1)

	go this.onResponsed()

	return err
}

func (this *kafkaSink) onResponsed() {
	for {
		select {
		case <-this.shutdown:
			break
		case <-this.sink.Successes():
		case e := <-this.sink.Errors():
			if e != nil {
				log.GlobalLogger().Bg().Error("send failed",
					logf.Any("msg", e.Msg), logf.Any("err", e.Msg))
			}
		}
	}
}

// Invoke
// 1. 发送消息
// 2. 重试，错误处理
func (this *kafkaSink) Invoke(ctx types.ActionContent, message types.Message) error {
	log.GlobalLogger().For(ctx.Context()).Info("Invoke", logf.Any("messages", string(message.Data())))
	msg := message.(types.PublishMessage)

	msgCtx := &sarama.ProducerMessage{
		Topic:   this.topic,
		Key:     nil,
		Headers: []sarama.RecordHeader{{[]byte(v1.MATE_TOPIC), []byte(msg.Topic())}},
		Value:   sarama.StringEncoder(msg.Data()),
	}
	this.sink.Input() <- msgCtx

	return nil
}

// Close
// 关闭连接
func (this *kafkaSink) Close() error {
	log.GlobalLogger().Bg().Debug("Close")
	return nil
}

func (this *kafkaSink) parseOption(metadata map[string]string) (*Option, error) {
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
	return newKafkaSink(entityType, entityID)
}
func newKafkaSink(entityType, entityID string) *kafkaSink {
	return &kafkaSink{}
}
