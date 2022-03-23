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

package stateful

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"github.com/tkeel-io/rule-rulex/internal/runtime/rule/stream/api"
	"github.com/tkeel-io/rule-rulex/internal/runtime/rule/stream/functions"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/tkeel-io/rule-util/stream"
	"sync"
	"time"
)

const ListKey = "x-state-list"

type State stream.Message

type WindowState struct {
	id          string
	context     context.Context
	state       State
	cnt         int
	window      *ruleql.WindowExpr
	timer       utils.Timer
	exceCnt     int
	TriggerFunc api.StateFunc

	values         *sync.Map
	aggregateDatas map[string]functions.AggregateData
	td             *utils.TimerData
	removeFunc     func(*WindowState)
}

func newWindowState(ctx context.Context, key string, removeFunc func(*WindowState),
	window *ruleql.WindowExpr, timer utils.Timer, TriggerFunc api.StateFunc) *WindowState {
	ws := &WindowState{
		id:          key,
		context:     ctx,
		window:      window,
		timer:       timer,
		TriggerFunc: TriggerFunc,
		exceCnt:     0,
		removeFunc:  removeFunc,
	}
	ws.values = &sync.Map{}
	ws.aggregateDatas = map[string]functions.AggregateData{}
	//ws.run()
	return ws
}

func (ws *WindowState) GetState() (state stream.Message) {
	//fmt.Println("+GetState+", time.Now())
	return ws.state
}

func (ws *WindowState) SetState(state stream.Message) {
	//fmt.Println("+SetInvoke+", time.Now())
	ws.state = state
}

func (ws *WindowState) SetTriggerCallback(TriggerFunc api.StateFunc) {
	//fmt.Println("+SetTrigger+", time.Now())
	ws.TriggerFunc = TriggerFunc
}

func (ws *WindowState) initTrigger() {
	//fmt.Println("+initTrigger+", time.Now())
	ws.td = ws.timer.Add(time.Duration(ws.window.Interval)*time.Second, ws.OnTrigger)
}

func (ws *WindowState) OnTrigger() {
	fmt.Println("+OnTrigger#Start+", ws.id, ws.cnt, time.Now())
	if ws.cnt == 0 {
		ws.close()
		return
	}
	ws.cnt = 0

	theState := ws.snopshotState()
	ws.clean()

	//ws.state = nil
	if err := ws.TriggerFunc(ws.context, theState); err != nil {
		utils.Log.For(ws.context).Error("State Trigger Error",
			logf.Error(err))
	}

	ws.initTrigger()
	fmt.Println("+OnTrigger#End+", ws.id, time.Now(), theState)
}

func (ws *WindowState) clean() {
	for _, fn := range ws.aggregateDatas {
		fn.Clean()
	}
}
func (ws *WindowState) snopshotState() State {
	state := ws.state.Copy()
	ws.values.Range(func(key, value interface{}) bool {
		bty, err := json.Marshal(value)
		if err != nil {
			log.For(ws.context).Error("snopshot state error")
		}
		state.SetAttr(key.(string), bty)
		return true
	})
	return state
}

func (ws *WindowState) Key() string {
	return ws.id
}

func (ws *WindowState) getFunction(ac *AggregateCallExpr) functions.AggregateData {
	expr := ac.CallExpr
	id := expr.String()
	if ret, ok := ws.aggregateDatas[id]; ok {
		return ret
	}
	name := expr.FuncName()
	if fn, ok := functions.AggregateFuncs[name]; ok {
		ret := fn()
		ws.aggregateDatas[id] = ret
		return ret
	}
	fmt.Println("Error getFunction", expr)
	return nil
}

func (ws *WindowState) setValue(ac *AggregateCallExpr, val ruleql.Node) error {
	key := ac.CallExpr.String()
	ws.values.Store(key, val)
	return nil
}

func (ws *WindowState) close() {
	fmt.Println("+WindowState+Close+", ws.id, time.Now(), ws.state)
	if ws.timer != nil && ws.td != nil {
		ws.timer.Del(ws.td)
	}
	ws.removeFunc(ws)
}

func (ws *WindowState) Exce(ctx functions.Context, operator *WindowsOperator) error {
	//fmt.Println("+2. ProcessElement+", time.Now(), state, ctx)
	//fmt.Println("+2.1 Process Op+", time.Now())
	//Exce(evalCtx, windowState, msg)
	ws.cnt++
	for k, ac := range operator.aggExprs {
		if err := ws.evalAggregateCallExpr(ctx, ac); err != nil {
			fmt.Println("Exce Error", k, ac)
		}
	}
	return nil
}

func (ws *WindowState) evalAggregateCallExpr(ctx functions.Context, ac *AggregateCallExpr) error {
	if ac == nil || len(ac.Args()) == 0 {
		return errors.New("call aggregate error")
	}
	v := ws.getFunction(ac)

	args := ac.Args()
	arg0 := ruleql.EvalRuleQL(ctx, args[0])
	ret := v.Call(ac.CallExpr, []functions.Node{arg0})
	if ret.Type() == ruleql.Undefined {
		return nil
	}
	if err := ws.setValue(ac, ret); err != nil {
		fmt.Println("Exce Error", err)
		return err
	}
	return nil
}

type stateManager struct {
	lock         sync.RWMutex
	windows      map[string]*WindowState
}

func NewStateManager() *stateManager {
	return &stateManager{
		lock:    sync.RWMutex{},
		windows: make(map[string]*WindowState, 0),
	}
}

func (wm *stateManager) Get(key string) *WindowState {
	wm.lock.RLock()
	defer wm.lock.RUnlock()
	if op, ok := wm.windows[key]; ok {
		return op
	}
	return nil
}

func (wm *stateManager) Set(state *WindowState) error {
	wm.lock.Lock()
	defer wm.lock.Unlock()
	wm.windows[state.Key()] = state
	return nil
}

func (wm *stateManager) Del(state *WindowState) error {
	wm.lock.Lock()
	defer wm.lock.Unlock()
	delete(wm.windows, state.Key())
	return nil
}

func (wm *stateManager) LoadOrStore(key string, newState func() *WindowState) (state *WindowState, loaded bool) {
	wm.lock.RLock()
	defer wm.lock.RUnlock()
	if op, ok := wm.windows[key]; ok {
		return op, true
	}
	state = newState()
	wm.windows[key] = state
	return state, false
}
