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
	"io"
	"math"
	"sync"
	"time"

	metapb "github.com/tkeel-io/rule-util/metadata/v1"
	v1 "github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/metadata/v1error"
	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-util/pkg/tracing"
	"github.com/tkeel-io/rule-util/pkg/utils"

	"github.com/tkeel-io/rule-rulex/internal/types"
)

type watcher struct {
	id         string
	logger     log.Factory
	cliManager v1.ResourcesManagerClient
	status     types.STATUS
	callbacks  []types.ResourceCallbackFunc
	deploy     func(context.Context, []types.ResourceCallbackFunc, *metapb.ResourceObject) error
	ctx        context.Context
	wg         sync.WaitGroup
}

func NewWatcher(ctx context.Context, c *types.ServiceConfig, resourceManager types.ResourceManager) (*watcher, error) {
	logger := log.GlobalLogger()
	return &watcher{
		id:         "sync_" + utils.GenerateUUID(),
		wg:         sync.WaitGroup{},
		ctx:        ctx,
		deploy:     deploy,
		cliManager: c.Client.Resource(),
		logger:     logger,
		callbacks:  make([]types.ResourceCallbackFunc, 0),
	}, nil
}

func (s *watcher) SetReceiver(ctx context.Context, fn types.ResourceCallbackFunc) {
	s.callbacks = append(s.callbacks, fn)
}

func (s *watcher) Run() {
	var (
		version int64
		err     error
	)

	span, ctx := tracing.AddSpan(context.Background(), "SyncStart")
	defer span.Finish()

	s.logger.For(ctx).Info("sync restore",
		logf.StatusCode(v1error.SyncStart))

	for {
		version, err = s.restore(ctx)
		if err == nil {
			break
		}
		if err == types.ErrMetadataNotReachable {
			s.logger.For(ctx).Error(
				"restore error",
				logf.String("id", s.id),
				logf.Error(err))
			time.Sleep(time.Second)
			continue
		}
		s.logger.Bg().Error("sync restore",
			logf.StatusCode(v1error.SyncStartFailed),
			logf.Error(err))
		panic(err)
	}

	s.init()

	s.sync(ctx, version)
	s.wg.Wait()
	return
}

func (s *watcher) restore(ctx context.Context) (int64, error) {
	s.logger.For(ctx).Info("restore")
	watch, err := s.cliManager.List(
		ctx,
		&metapb.ResourcesRequest{},
	)
	if err != nil {
		s.logger.For(ctx).Error("sync fail", logf.Error(err))
		return ERROR_VERSION, types.ErrMetadataNotReachable
	}
	var version int64 = math.MaxInt64
	for {
		var rev = ERROR_VERSION
		resp, rerr := watch.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			s.logger.For(ctx).Error("sync fail", logf.Error(err))
			return ERROR_VERSION, types.ErrMetadataNotReachable
		}
		if resp == nil || resp.Body == nil {
			s.logger.For(ctx).Error("sync body nil",
				logf.StatusCode(v1error.SyncStartFailed))
			return 0, types.ErrRestore
		}
		if rerr != nil {
			s.logger.For(ctx).Error("sync restore",
				logf.StatusCode(v1error.SyncStartFailed),
				logf.Error(rerr))
			return 0, types.ErrRestore
		}

		for _, e := range resp.Body {
			err = s.deploy(ctx, s.callbacks, e)
			if err != nil {
				err := types.ErrCreatTaskFail
				log.For(ctx).Error("creat task error",
					logf.Any("event", e),
					logf.Error(err),
				)
				continue
			}
			rev = e.ResourceVersion
		}

		if rev == ERROR_VERSION {
			s.logger.For(ctx).Error(
				"recv error(unknown type)",
				logf.StatusCode(v1error.StreamOpenFail),
				logf.Any("resp", resp),
				logf.Error(err),
			)
			return ERROR_VERSION, types.ErrRecvUnknownType
		}
		if version == math.MaxInt64 || rev > version {
			s.logger.For(ctx).Info("update_version",
				logf.Int64("version", version),
				logf.Int64("recv_version", rev))
			version = rev
		}

	}
	return version, nil
}

func (s *watcher) sync(ctx context.Context, version int64) {
	s.logger.For(ctx).Info("start sync", logf.Int64("version", version))
	watch, err := s.cliManager.Watch(
		ctx,
		&metapb.ResourcesRequest{ResourceVersion: version},
	)
	if err != nil {
		s.logger.For(ctx).Error("sync fail", logf.Error(err))
		time.AfterFunc(time.Second, func() {
			s.sync(ctx, version)
		})
		return
	}
	go func() {
		span, ctx := tracing.AddSpan(ctx, "SubscriptionWatch Subscription Change")
		defer span.Finish()

		for {

			var rev = ERROR_VERSION
			resp, err := watch.Recv()

			if err != nil {
				s.logger.For(ctx).Warn("watchSubscriptionChange", logf.Error(err))
				time.AfterFunc(time.Second, func() {
					s.sync(ctx, version)
				})
				return
			}

			for _, resource := range resp.Body {
				err = s.deploy(ctx, s.callbacks, resource)
				if err != nil {
					err := types.ErrCreatTaskFail
					log.For(ctx).Error("creat task error",
						logf.Any("resource", resource),
						logf.Error(err),
					)
					continue
				}
				rev = resource.ResourceVersion
			}
			if rev != ERROR_VERSION {
				version = rev
			}
		}
	}()
}

func (s *watcher) init() {
}

func deploy(ctx context.Context, callbacks []types.ResourceCallbackFunc, resource *metapb.ResourceObject) error {
	for _, fn := range callbacks {
		err := fn(ctx, resource)
		if err != nil {
			return err
		}
	}
	return nil
}
