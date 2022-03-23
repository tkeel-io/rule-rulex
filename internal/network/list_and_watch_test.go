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

package conn

import (
	"context"
	"fmt"
	"github.com/tkeel-io/rule-util/metadata"
	"github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/registry"
	"github.com/tkeel-io/rule-util/pkg/registry/etcd3"
	"github.com/tkeel-io/rule-util/pkg/utils"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"testing"
	"time"
)

var opts = []grpc.DialOption{
	grpc.WithBalancerName("round_robin"),
	grpc.WithInsecure(),
	grpc.WithUnaryInterceptor(
		grpc_opentracing.UnaryClientInterceptor(),
	),
	//grpc.WithTimeout(timeout),
	grpc.WithBackoffMaxDelay(time.Second * 3),
	grpc.WithInitialWindowSize(1 << 30),
	grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64 * 1024 * 1024)),
}

func TestListAndWatch(t *testing.T) {
	//gunit.RunSequential(new(ListAndWatch), t)
	gunit.Run(new(ListAndWatch), t)
}

type ListAndWatch struct {
	*gunit.Fixture
	watcher *watcher
	logger  log.Factory
	level   log.Level
}

func (this *ListAndWatch) Setup() {
	this.logger, this.level = log.InitLogger("IOTHub", "router")
	this.level.SetLevel(log.DebugLevel)

	discovery, err0 := etcdv3.New(&registry.Config{
		Endpoints: []string{"192.168.100.2:2379"},
	})
	if err0 != nil {
		panic(err0)
	}
	cli := newResourcesManagerClient(discovery)
	this.watcher = &watcher{
		id:         "sync_" + utils.GenerateUUID(),
		ctx:        context.Background(),
		deploy:     deploy,
		cliManager: cli,
		logger:     log.GlobalLogger(),
		callbacks:  make([]types.ResourceCallbackFunc, 0),
	}
	this.So(nil, should.BeNil)
}

func (this *ListAndWatch) TestListAndWatch() {
	ctx := context.Background()
	this.watcher.SetReceiver(ctx, func(ctx context.Context, evt *v1.ResourceObject) error {
		fmt.Println(ctx, evt)
		return nil
	})
	this.watcher.Run()
	return
}

func newResourcesManagerClient(discovery *etcdv3.Discovery) v1.ResourcesManagerClient {
	resolverBuilder := discovery.GRPCResolver()
	resolver.Register(resolverBuilder)
	conn, err1 := grpc.Dial(
		resolverBuilder.Scheme()+":///"+metadata.APPID,
		opts...
	)
	if err1 != nil {
		panic(err1)
	}
	return v1.NewResourcesManagerClient(conn)
}
