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

package sink

import (
	"context"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-util/stream"
	stypes "github.com/tkeel-io/rule-util/stream/types"
	"strings"
	"time"
)

const DefaultActionEntityID = "default"

type CheckpointedFunction = stypes.CheckpointedFunction


type EntityInfo struct {
	Id       string
	Metadata map[string]string
}

type EntityMeta struct {
	LastTime time.Time
	Type     string
	Metadata map[string]string
}

type ActionManager interface {
	Invoke(ctx context.Context, entityID string, messages stream.Message) error
}

type ActionBehavior int32

const (
	ActionModePanic = iota
	ActionModeRedeliver
	ActionModeContinue
)

func UnmarshalBehavior(typ string) ActionBehavior {
	switch strings.ToLower(typ) {
	case "continue":
		return ActionModeContinue
	case "redeliver":
		return ActionModeRedeliver
	default:
		return ActionModePanic
	}
}

type checkpointedAction struct {
	action types.FlushAction
}

func (this *checkpointedAction) SnapshotState(ctx stypes.FunctionSnapshotContext) error {
	return this.action.Flush(NewContext(context.Background()))
}

func (this *checkpointedAction) InitializeState(context stypes.FunctionInitializationContext) error {
	return nil
}

func warpCheckpointedFunction(action types.FlushAction) stypes.CheckpointedFunction {
	return &checkpointedAction{action}
}
