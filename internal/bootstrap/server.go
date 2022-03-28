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

package bootstrap

import (
	"context"
	"net"
	"os"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/tkeel-io/rule-rulex/internal/conf"
	"github.com/tkeel-io/rule-rulex/internal/endpoint/rulex"
	xmetrics "github.com/tkeel-io/rule-rulex/internal/metrices/prometheus"
	"github.com/tkeel-io/rule-rulex/internal/server"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/tkeel-io/rule-util/metadata/v1error"
	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-util/pkg/registry"
	"github.com/tkeel-io/rule-util/stream/checkpoint"

	xgrpc "github.com/tkeel-io/rule-rulex/internal/transport/grpc"
	pb "github.com/tkeel-io/rule-util/metadata/v1"
	etcdv3 "github.com/tkeel-io/rule-util/pkg/registry/etcd3"
	grpc "google.golang.org/grpc"
	//_ "git.internal.yunify.com/MDMP2/cloudevents/pkg/cloudevents/transport/kafka22"
	//_ "git.internal.yunify.com/MDMP2/cloudevents/pkg/cloudevents/transport/qmq"
)

type listenFunc func(network string, address string) (net.Listener, error)

type patchTable struct {
	listen listenFunc
	remove func(name string) error
}

func newPatchTable() *patchTable {
	return &patchTable{
		listen: net.Listen,
		remove: os.Remove,
	}
}

type Runable interface {
	Run()
}

type Server struct {
	shutdown      chan error
	conf          *conf.Config
	logger        log.Factory
	tracer        opentracing.Tracer
	ctx           context.Context
	cancel        context.CancelFunc
	routerService *server.RuleService
	server        *grpc.Server
	listener      net.Listener
}

func NewServer(ctx context.Context, c *conf.Config, tracer opentracing.Tracer, logger log.Factory) *Server {
	utils.SetLogger(logger)
	srv, err := newServer(ctx, c, tracer, logger)
	if err != nil {
		logger.Bg().Error("creat server error",
			logf.Error(err))
	}
	return srv
}

func newServer(ctx context.Context, config *conf.Config, tracer opentracing.Tracer, logger log.Factory) (*Server, error) {
	ctx, cancel := context.WithCancel(ctx)
	pt := newPatchTable()

	var srv = &Server{
		ctx:    ctx,
		cancel: cancel,
		conf:   config,
		tracer: tracer,
		logger: logger,
		server: xgrpc.New(config.RPCServer),
	}
	srv.routerService = server.New(ctx, config.Slot.SlotNum, &types.ServiceConfig{
		Client: xgrpc.NewClient(config),
		Tracer: tracer,
		Logger: logger,
	})

	network, address := extractNetAddress(config.RPCServer.Addr)

	if network != "tcp" {
		return nil, errors.Errorf("unsupported protocol %s", address)
	}

	err := register(config, srv)

	if srv.listener, err = pt.listen(network, address); err != nil {
		return nil, errors.Errorf("unable to listen: %v", err)
	}

	if config.Discovery != nil {
		dc, err := etcdv3.New(config.Discovery)
		if err != nil {
			log.Fatal("Register error!")
		}

		if err = dc.Register(ctx, &registry.Node{
			AppID: config.RPCServer.APPID,
			Addr:  srv.listener.Addr().String(),
		}, 1); err != nil {
			log.Fatal("Register error!")
		}
	}

	return srv, nil
}

//Run ...
func (srv *Server) Run() error {
	go func() {
		log.Info("starting gRPC server",
			logf.String("Addr", srv.listener.Addr().String()))
		err := srv.server.Serve(srv.listener)
		if err != nil {
			log.Error("start gRPC server",
				logf.StatusCode(v1error.ServiceStartFailed),
				logf.Error(err))
			srv.errHandler(err)
		}
	}()
	go func() { xmetrics.Init(srv.conf) }()
	checkpoint.InitCoordinator(srv.ctx, 2*time.Second)
	return srv.routerService.Start(srv.ctx)
}

func (srv *Server) Close() error {
	utils.Log.Bg().Info("Close server")
	srv.server.GracefulStop()
	if srv.shutdown != nil {
		srv.cancel()
		srv.shutdown <- nil
		_ = srv.Wait()
	}

	return nil
}

// Wait waits for the server to exit.
func (srv *Server) Wait() error {
	if srv.shutdown == nil {
		return errors.Errorf("server(metadata) not running")
	}

	err := <-srv.shutdown
	srv.shutdown = nil

	return err
}

func register(c *conf.Config, srv *Server) (err error) {
	rulexNodeHandle := rulex.NewEndpoint(srv.ctx)
	pb.RegisterRulexNodeActionServer(srv.server, rulexNodeHandle)

	return err
}

func extractNetAddress(apiAddress string) (string, string) {
	idx := strings.Index(apiAddress, "://")
	if idx < 0 {
		return "tcp", apiAddress
	}

	return apiAddress[:idx], apiAddress[idx+3:]
}

func (srv *Server) errHandler(err error) {
	select {
	case <-srv.shutdown:
		return
	default:
	}
	select {
	case <-srv.ctx.Done():
	case srv.shutdown <- err:
	}
}
