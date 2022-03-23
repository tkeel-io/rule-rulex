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

package runtime

import (
	"context"

	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/utils"

	//"github.com/tkeel-io/rule-util/pkg/tracing"
	"sync"

	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	xmetrics "github.com/tkeel-io/rule-rulex/internal/metrices/prometheus"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/internal/utils/topic"
	"github.com/tkeel-io/rule-util/stream"
	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go"
)

// slot
type slot struct {
	id       string
	userID   string
	cache    *sync.Map
	logger   log.Factory
	tracer   opentracing.Tracer
	tireTree *topic.Tree

	taskLock sync.Mutex
}

var _ types.Slot = (*slot)(nil)

func NewSlot(userID string, logger log.Factory, tracer opentracing.Tracer) *slot {
	s := &slot{
		id:       "slot_" + uuid.New().String(),
		userID:   userID,
		tireTree: topic.New(),
		cache:    &sync.Map{},
		logger:   logger,
		tracer:   tracer,
	}
	return s
}

// find match task
// trie
// |
// + - + - task-M
// |   |
// +   task-1,...,task-N
// |
// task2,taskM
func (s *slot) Invoke(ctx context.Context, vCtx ruleql.Context, message stream.PublishMessage) error {
	flag := true
	topic := message.Topic()
	tasks := s.match(topic)
	for _, t := range tasks {
		nm := message.Copy().(stream.PublishMessage)
		//nm := message
		if err := t.Invoke(ctx, vCtx, nm); err != nil {
			s.logger.For(ctx).Error(
				"0.invoke2 msg",
				logf.Message(nm),
				logf.Error(err),
			)
			flag = false
		} else {
			if env := utils.Log.Check(log.DebugLevel, "4.invoke2 msg"); env != nil {
				env.Write(logf.Message(nm))
			}
		}
	}

	if !flag {
		xmetrics.MsgRecvFail()
	}
	return nil
}

func (s *slot) AddTask(ctx context.Context, event *types.TaskEvent) bool {
	s.taskLock.Lock()
	defer s.taskLock.Unlock()
	key := taskKey(event)
	v, ok := s.cache.Load(key)
	if ok {
		oldone := v.(*types.TaskEvent)
		log.Info("Find old task, remove it",
			logf.Any("UserId", oldone.UserId),
			logf.Any("Topic", oldone.Topic),
			logf.Any("Task", oldone.Task))
		s.cache.Delete(key)
		s.tireTree.Remove(oldone.Topic, oldone.Task)
		oldone.Task.Close(ctx)
		log.Info("Remove old task done",
			logf.Any("UserId", oldone.UserId),
			logf.Any("Topic", oldone.Topic),
			logf.Any("Task", oldone.Task))
	}
	s.cache.Store(key, event)
	return s.tireTree.Add(event.Topic, event.Task)
}

func (s *slot) DelTask(ctx context.Context, event *types.TaskEvent) bool {
	s.taskLock.Lock()
	defer s.taskLock.Unlock()
	key := taskKey(event)
	v, ok := s.cache.Load(key)
	if ok {
		task := v.(*types.TaskEvent)
		log.Info("Remove task",
			logf.Any("UserId", task.UserId),
			logf.Any("Topic", task.Topic),
			logf.Any("Task", task.Task))
		s.cache.Delete(key)
		b := s.tireTree.Remove(task.Topic, task.Task)
		task.Task.Close(ctx)
		log.Info("Remove task done",
			logf.Any("UserId", task.UserId),
			logf.Any("Topic", task.Topic),
			logf.Any("Task", task.Task))
		return b
	} else {
		log.Error("Task not found",
			logf.Any("Task", event))
		return false
	}
}

func taskKey(evt *types.TaskEvent) string {
	return evt.Task.ID()
}

func (s *slot) match(topic string) []types.Task {
	nodes := s.tireTree.Match(topic)
	ret := make([]types.Task, 0, len(nodes))
	for _, node := range nodes {
		switch node := node.(type) {
		case types.Task:
			ret = append(ret, node)
		default:
			s.logger.Bg().Error("unknown type task",
				logf.Any("node", node))
			s.logger.Bg().Fatal("unknown type task")
		}
	}
	return ret
}

func (s *slot) Tree() *topic.Tree {
	return s.tireTree
}
