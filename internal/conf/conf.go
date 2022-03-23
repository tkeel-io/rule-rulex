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

package conf

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	xutils "github.com/tkeel-io/rule-util/pkg/utils"
	api "github.com/tkeel-io/rule-util/rulex"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/BurntSushi/toml"

	"github.com/tkeel-io/rule-util/pkg/registry"

	xtime "github.com/tkeel-io/rule-rulex/pkg/time"
)

var (
	Debug = true
)

// LoadTomlFile load config file.
func LoadTomlFile(confPath string) *Config {
	conf := Default()
	bs, err := ioutil.ReadFile(confPath)
	if err != nil {
		utils.Log.Bg().Fatal("load config failed, ",
			logf.String("dir", confPath),
			logf.Error(err))
	}
	_, err = toml.Decode(string(bs), &conf)
	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		utils.Log.Bg().Fatal("load config failed, ",
			logf.String("config", string(bs)),
			logf.String("dir", dir),
			logf.Error(err))
	} else {
		utils.Log.Bg().Info("load config",
			logf.String("dir", dir),
			logf.Any("config", conf),
		)
	}

	conf.Prometheus.Endpoints = conf.Discovery.Endpoints
	return conf
}

// Default new a config with specified default value.
func Default() *Config {
	return &Config{
		Discovery: &registry.Config{
			Endpoints: []string{"127.0.0.1:2379"},
		},
		RPCClient: &RPCClientConfig{
			Dial:                  xtime.Duration(time.Second),
			Timeout:               xtime.Duration(time.Second),
			PermitWithoutStream:   true,
			BackoffMaxDelay:       xtime.Duration(time.Second * 3),
			InitialWindowSize:     1 << 30,
			InitialConnWindowSize: 1 << 30,
		},
		RPCServer: &RPCServerConfig{
			APPID:                api.APPID,
			Timeout:              xtime.Duration(time.Second),
			IdleTimeout:          xtime.Duration(time.Second * 60),
			MaxLifeTime:          xtime.Duration(time.Hour * 2),
			ForceCloseWait:       xtime.Duration(time.Second * 20),
			KeepAliveInterval:    xtime.Duration(time.Second * 60),
			KeepAliveTimeout:     xtime.Duration(time.Second * 20),
			MaxMessageSize:       1024 * 1024,
			MaxConcurrentStreams: 1024,
		},
		Slot: &SlotConfig{
			WorkerSize: 100,
			SlotNum:    1,
		},
		Base: &BaseConfig{
			LogLevel:       "info",
			WhiteList:      []string{},
			ProfEnable:     false,
			TracerEnable:   false,
			ProfPathPrefix: "debug",
		},
		Prometheus: &Prometheus{
			Zone:   "staging",
			Node:   "unkown",
			Module: api.APPID,
			Name:   "rulex" + xutils.GenerateUUID(),
			Host:   "eth0",
		},
	}
}

// Config config.
type Config struct {
	Discovery  *registry.Config
	RPCClient  *RPCClientConfig
	RPCServer  *RPCServerConfig
	Slot       *SlotConfig
	Base       *BaseConfig
	Metrics    *MetricsConfig
	Prometheus *Prometheus
}

// Core Config
type CoreConfig struct {
	LogLevel log.Level
}

// RPCServer is RPC server config.
type RPCServerConfig struct {
	APPID                string
	Addr                 string
	Timeout              xtime.Duration
	IdleTimeout          xtime.Duration
	MaxLifeTime          xtime.Duration
	ForceCloseWait       xtime.Duration
	KeepAliveInterval    xtime.Duration
	KeepAliveTimeout     xtime.Duration
	MaxMessageSize       int32
	MaxConcurrentStreams int32
	EnableOpenTracing    bool
}

// RPCClientConfig is RPC client config.
type RPCClientConfig struct {
	Dial                  xtime.Duration
	Timeout               xtime.Duration
	PermitWithoutStream   bool
	InitialWindowSize     int32
	InitialConnWindowSize int32
	BackoffMaxDelay       xtime.Duration
}

type SlotConfig struct {
	SlotNum    int
	WorkerSize int
}

type MetricsConfig struct {
	Endpoint       string
	ReportInterval int
}

type BaseConfig struct {
	ProfPathPrefix string
	LogLevel       string
	ProfEnable     bool
	TracerEnable   bool
	WhiteList      []string
	Endpoints      []string
}

type Prometheus struct {
	Zone      string
	Node      string
	Name      string
	Host      string
	Module    string
	ExitFlag  bool
	Endpoints []string
}

func (this *Prometheus) Address() string {

	var (
		err error
		ip  string
	)

	if ip, err = utils.GetIpByName(this.Host); nil != err {
		utils.Log.Bg().Error("parse addr failed.",
			logf.String("host", this.Host))
	}
	return ip
}
