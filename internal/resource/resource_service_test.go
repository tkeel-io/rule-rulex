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

package resource

import (
	"context"
	"fmt"
	pb "github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"github.com/tkeel-io/rule-util/stream"
	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"testing"
)

type mockcli struct{}

func (m *mockcli) Manager() pb.JobManagerClient { return nil }

func (m *mockcli) PubSub() pb.PubSubClient { return nil }

func (m *mockcli) Rule() pb.RuleActionClient { return nil }

func (m *mockcli) Resource() pb.ResourcesManagerClient { return nil }

type mockrule struct {
	id    string
	value string
}

func (s *mockrule) ID() string {
	return fmt.Sprintf("%s", s.id)
}

func (s *mockrule) String() string {
	return string(s.value)
}

func (s *mockrule) Invoke(ctx context.Context, evalCtx ruleql.Context, msg stream.PublishMessage) error {
	return nil
}

func TestTreeManager(t *testing.T) {
	//gunit.RunSequential(new(Manager), t)
	gunit.Run(new(TreeManagerUnit), t)
}

type TreeManagerUnit struct {
	*gunit.Fixture
	tm *resourceManager
}

func (this *TreeManagerUnit) Setup() {
	var _ types.GrpcClient = &mockcli{}
	tm, err := NewManager(context.Background(), &types.ServiceConfig{
		Client: &mockcli{},
		Logger: utils.Log,
	})
	this.So(tm, should.NotBeNil)
	this.So(err, should.BeNil)
	this.tm = tm
}

func (this *TreeManagerUnit) Test() {
}
