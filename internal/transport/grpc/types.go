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
	pb "github.com/tkeel-io/rule-util/metadata/v1"

	"google.golang.org/grpc"
)

type Client struct {
	resource pb.ResourcesManagerClient
	manager  pb.JobManagerClient
	pubSub   pb.PubSubClient
	rule     pb.RuleActionClient
}

type ResourcesManagerClient = pb.ResourcesManagerClient
type JobManagerClient = pb.JobManagerClient
type PubSubClient = pb.PubSubClient
type RuleActionClient = pb.RuleActionClient

var MaxCallRecvMsgSize = grpc.MaxCallRecvMsgSize

func (c *Client) Manager() pb.JobManagerClient {
	return c.manager
}

func (c *Client) PubSub() pb.PubSubClient {
	return c.pubSub
}

func (c *Client) Rule() pb.RuleActionClient {
	return c.rule
}
func (c *Client) Resource() pb.ResourcesManagerClient {
	return c.resource
}
