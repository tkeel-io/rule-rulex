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

package device

import (
	"fmt"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/tkeel-io/rule-rulex/pkg/sink"
	"github.com/tkeel-io/rule-rulex/internal/types"
)

const (
	EntityType     = "webhook"
)

type webhook struct {
	urlTlp     string
	entityType string
	entityID   string
}

// Invoke
// 1. 发送消息
// 2. 重试，错误处理
func (m *webhook) Invoke(ctx types.ActionContent, message types.Message) error {
	if env := utils.Log.Check(log.DebugLevel, fmt.Sprintf("Invoke")); env != nil {
		env.Write(logf.Any("messages", message))
	}
	return nil
}

// Close
// 关闭连接
func (m *webhook) Close() error {
	if env := utils.Log.Check(log.DebugLevel, fmt.Sprintf("Close")); env != nil {
		//env.Write(logf.Any("messages", message))
	}
	return nil
}

// Setup
// 1. 创建
func (m *webhook) Setup(actx types.ActionContent, metadata map[string]string) error {
	ctx := actx.Context()
	log.For(ctx).Debug("Setup", logf.Any("metadata", metadata))
	return nil
}

func init() {
	sink.Registered(EntityType, func(entityType, entityID string) types.Action {
		return &webhook{
			entityType: entityType,
			entityID: entityID,
		}
	})
}

func New() *webhook {
	return &webhook{
	}
}
