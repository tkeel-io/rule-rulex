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

package report

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/tkeel-io/rule-rulex/pkg/define"
	"sync"
	"time"

	v1 "github.com/tkeel-io/rule-util/metadata/v1"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	xstream "github.com/tkeel-io/rule-util/stream"
)

var (
	version      = "mdmpv1"
	reportUser   = "system"
	reportDevice = "mdmp-report"
	once         = sync.Once{}
	reportSink   xstream.Sink
	err          error
)

func InitReportSink(sink xstream.Sink) {
	if sink == nil {
		log.Fatal("report sink nil error")
	}
	reportSink = sink
}

func TaskStatus(ctx context.Context, t types.BaseObject, status rulex.Status) {
	if err := Report(ctx, &rulex.RulexStatus{
		UserId:      t.UserID(),
		RuleId:      t.ID(),
		Status:      status,
		Timestamp:   time.Now().Unix(),
		RefreshTime: t.RefreshTime(),
	}); err != nil {
		utils.Log.For(ctx).Error("Report error", logf.Error(err))
	}
}
func TaskError(ctx context.Context, t types.BaseObject, s rulex.Status, err error) {
	if err := Report(ctx, &rulex.RulexStatus{
		UserId:    t.UserID(),
		RuleId:    t.ID(),
		Status:    s,
		Desc:      t.String(),
		Timestamp: time.Now().Unix(),
		RefreshTime: t.RefreshTime(),
		Error:     err.Error(),
	}); err != nil {
		utils.Log.For(ctx).Error("Report error", logf.Error(err))
	}
	return
}

func ActionStatus(ctx context.Context, t types.BaseObject, ac *v1.Action, s rulex.Status) {
	if err := Report(ctx, &rulex.RulexStatus{
		UserId:    t.UserID(),
		RuleId:    t.ID(),
		ActionId:  ac.Id,
		Status:    s,
		Timestamp: time.Now().Unix(),
		RefreshTime: t.RefreshTime(),
	}); err != nil {
		utils.Log.For(ctx).Error("Report error", logf.Error(err))
	}
	return
}

func jsonMarshal(v interface{}) string {
	if byt, err := json.Marshal(v); err != nil {
		return string(byt)
	}
	return ""
}

func ActionError(ctx context.Context, t types.BaseObject, ac *v1.Action, s rulex.Status, err error) {
	if err := Report(ctx, &rulex.RulexStatus{
		UserId:    t.UserID(),
		RuleId:    t.ID(),
		ActionId:  ac.Id,
		Status:    s,
		Desc:      jsonMarshal(ac),
		Timestamp: time.Now().Unix(),
		RefreshTime: t.RefreshTime(),
		Error:     err.Error(),
	}); err != nil {
		utils.Log.For(ctx).Error("Report error", logf.Error(err))
	}
	return
}

func NewMessage(data interface{}) (xstream.Message, error) {
	byt, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	event := xstream.NewMessage()
	event.SetMethod(v1.MethodPublish)
	event.SetData(byt)
	return event, nil
}

func Report(ctx context.Context, status *rulex.RulexStatus) error {
	//fmt.Println("Report",status)
	if reportSink == nil {
		err = errors.New("Report sink is nil")
		return err
	}
	if status == nil {
		err = errors.New("Event is nil")
		return err
	}
	//log.Info("metrics", logf.Any("data", data))
	event, err := NewMessage(status)
	if err != nil {
		return err
	}
	event.SetDomain(reportUser)
	event.SetEntity(reportDevice)
	event.SetTopic(fmt.Sprintf("/mdmp/status/rulex/%s", status.RuleId))

	reportSink.Send(ctx, event)
	return nil
}
