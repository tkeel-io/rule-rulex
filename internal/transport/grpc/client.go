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

package grpc

import (
	"github.com/tkeel-io/rule-util/metadata"
	pb "github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/pkg/registry/etcd3"
	"github.com/tkeel-io/rule-rulex/internal/conf"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
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

func NewClient(c *conf.Config) types.GrpcClient {
	discovery, err0 := etcdv3.New(c.Discovery)
	if err0 != nil {
		panic(err0)
	}

	return &Client{
		resource: newResourcesManagerClient(discovery),
		manager:  mewJobManagerClient(discovery),
		pubSub:   newPubSubClient(discovery),
		rule:     newRuleClient(discovery),
	}
}

func newPubSubClient(discovery *etcdv3.Discovery) pb.PubSubClient {
	//timeout := time.Duration(config.RPCClient.Timeout)
	resolverBuilder := discovery.GRPCResolver()
	resolver.Register(resolverBuilder)
	conn, err1 := grpc.Dial(
		resolverBuilder.Scheme()+":///"+metadata.APPID,
		opts...,
	)
	if err1 != nil {
		panic(err1)
	}
	return pb.NewPubSubClient(conn)
}

func newRuleClient(discovery *etcdv3.Discovery) pb.RuleActionClient {
	//timeout := time.Duration(config.RPCClient.Timeout)
	resolverBuilder := discovery.GRPCResolver()
	resolver.Register(resolverBuilder)
	conn, err1 := grpc.Dial(
		resolverBuilder.Scheme()+":///"+metadata.APPID,
		opts...,
	)
	if err1 != nil {
		panic(err1)
	}
	return pb.NewRuleActionClient(conn)
}

func mewJobManagerClient(discovery *etcdv3.Discovery) pb.JobManagerClient {
	//timeout := time.Duration(config.RPCClient.Timeout)
	resolverBuilder := discovery.GRPCResolver()
	resolver.Register(resolverBuilder)
	conn, err1 := grpc.Dial(
		resolverBuilder.Scheme()+":///"+metadata.APPID,
		opts...,
	)
	if err1 != nil {
		panic(err1)
	}
	return pb.NewJobManagerClient(conn)
}

func newResourcesManagerClient(discovery *etcdv3.Discovery) pb.ResourcesManagerClient {
	//timeout := time.Duration(config.RPCClient.Timeout)
	resolverBuilder := discovery.GRPCResolver()
	resolver.Register(resolverBuilder)
	conn, err1 := grpc.Dial(
		resolverBuilder.Scheme()+":///"+metadata.APPID,
		opts...,
	)
	if err1 != nil {
		panic(err1)
	}
	return pb.NewResourcesManagerClient(conn)
}
