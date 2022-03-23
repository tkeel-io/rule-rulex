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

package cmd

//import (
//	"flag"
//	"fmt"
//	"github.com/tkeel-io/rule-util/metadata/v1error"
//	"github.com/tkeel-io/rule-util/pkg/logfield"
//	"github.com/tkeel-io/rule-rulex/internal/metrices"
//	"github.com/opentracing/opentracing-go"
//	"github.com/spf13/cobra"
//	"os"
//	"os/signal"
//	"qingcloud.com/qing-cloud-mq/common/proto"
//	"qingcloud.com/qing-cloud-mq/consumer"
//	"qingcloud.com/qing-cloud-mq/utils"
//	"syscall"
//
//	"github.com/tkeel-io/rule-util/pkg/debug"
//	"github.com/tkeel-io/rule-util/pkg/log"
//	"github.com/tkeel-io/rule-util/pkg/pprof"
//	"github.com/tkeel-io/rule-util/pkg/tracing"
//	"github.com/tkeel-io/rule-rulex/internal/conf"
//	"github.com/tkeel-io/rule-rulex/pkg/version"
//)
//
//func init() {
//	rootCmd.AddCommand(benchmarkCmd)
//}
//
//var benchmarkCmd = &cobra.Command{
//	Use:   "benchmark",
//	Short: "benchmark message.",
//	Long:  `benchmark message.`,
//	Run: func(cmd *cobra.Command, args []string) {
//		version := fmt.Sprintln(
//			fmt.Sprintf("Build Date:%s", version.BuildDate),
//			fmt.Sprintf("Git Commit:%s", version.GitCommit),
//			fmt.Sprintf("Version:%s", version.Version),
//			fmt.Sprintf("Go Version:%s", version.GoVersion),
//			fmt.Sprintf("OS / Arch:%s", version.OsArch),
//		)
//		logger.Bg().Info("Start",
//			logf.String("version", version),
//			logf.StatusCode(v1error.ServiceIniting))
//		flag.Parse()
//		defer log.Flush()
//
//		cfg := conf.LoadTomlFile(confPath)
//
//		if cfg.Base.TracerEnable {
//			tracer := tracing.Init("router", metricsFactory, logger)
//			opentracing.SetGlobalTracer(tracer)
//		}
//		if cfg.Base.LogLevel != "" {
//			level.SetLevel(log.GetLevel(cfg.Base.LogLevel))
//		}
//		if len(cfg.Base.Endpoints) > 0 {
//			handles := []*debug.HandleFunc{}
//			if cfg.Base.ProfEnable {
//				handles = append(handles, pprof.HandleFunc()...)
//			}
//			handles = append(handles, []*debug.HandleFunc{
//				{"/log/level", level.ServeHTTP},
//			}...)
//			debug.Init(
//				cfg.Base.Endpoints,
//				handles...,
//			)
//		}
//
//		//ctx := context.Background()
//		testEtcdString := "192.168.200.3:2379"
//		topic := "mdmp-topic"
//
//		opt := consumer.NewOptions()
//		opt.GroupID = "broad"
//		//opt.CommitMode = consumer.MANUAL
//		//opt.SubMode = consumer.BROADCAST
//		opt.OffsetPath = os.TempDir()
//		opt.ConsumeMode = consumer.CONSUME_FROM_LAST
//		opt.NSServer = testEtcdString
//		opt.ConsumerID = utils.GetUUID()
//		opt.MsgMode = consumer.PUSH
//		c, _ := consumer.NewConsumer(opt)
//
//		listener := &simpleListener{id: "id0"}
//		_ = c.BindQueueWithListener(topic, listener)
//		c.Start()
//		defer func() {
//			fmt.Println("listener.cnt", listener.id, listener.cnt)
//		}()
//
//		signalChan := make(chan os.Signal, 1)
//		signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
//
//		select {
//		case s := <-signalChan:
//			logger.Bg().Info("Captured signal",
//				logf.Any("signal", s))
//			logger.Bg().Info("Stop",
//				logf.StatusCode(v1error.ServiceStop))
//			os.Exit(0)
//		}
//	},
//}
//
//type simpleListener struct {
//	id  string
//	cnt int
//}
//
//func (s *simpleListener) OnReceived(msg *proto.Message, ctx consumer.Context) error {
//	s.cnt++
//	metrics.MsgMetrics.Mark(1)
//	err := ctx.Ack()
//	return err
//}
