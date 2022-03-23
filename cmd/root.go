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

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/tkeel-io/rule-util/rulex"
	"github.com/uber/jaeger-lib/metrics"

	goflag "flag"

	flag "github.com/spf13/pflag"
	"github.com/uber/jaeger-lib/metrics/expvar"
	prom "github.com/uber/jaeger-lib/metrics/prometheus"
)

var (
	metricsBackend string
	metricsFactory metrics.Factory
)

var rootCmd = &cobra.Command{
	Use:   "rulex",
	Short: "This is rulex service.",
	Long:  `This is rulex service.`,
}

//Execute execute the root command
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&metricsBackend, "metrics", "m", "expvar", "Metrics backend (expvar|prometheus)")
	rootCmd.PersistentFlags().StringVar(&confPath, "conf", "../../conf/router-example.toml", "default config path")

	rand.Seed(int64(time.Now().Nanosecond()))

	cobra.OnInitialize(onInitialize)
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)
}

// onInitialize is called before the command is executed.
func onInitialize() {
	switch metricsBackend {
	case "expvar":
		metricsFactory = expvar.NewFactory(10) // 10 buckets for histograms
		logger.Bg().Info("Using expvar as metrics backend")
	case "prometheus":
		metricsFactory = prom.New().Namespace(metrics.NSOptions{Name: rulex.APPID, Tags: nil})
		logger.Bg().Info("Using Prometheus as metrics backend")
	default:
		logger.Bg().Fatal("unsupported metrics backend " + metricsBackend)
	}
}
