package prometheus

import (
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	xmetrics "github.com/tkeel-io/rule-util/pkg/metrics/prometheus"
	"github.com/tkeel-io/rule-util/pkg/metrics/prometheus/option"
	"github.com/tkeel-io/rule-rulex/internal/conf"
	xutils "github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/prometheus/client_golang/prometheus"
)

var rulexMetric *RulexMetrics
var logger = xutils.Logger

type RulexMetrics struct {
	name   string
	node   string
	zone   string
	module string
	reg    *prometheus.Registry

	//消息的输入输出统计
	msgInOut *prometheus.GaugeVec
	//消息处理延迟
	msgDelay *prometheus.HistogramVec
	//消息延迟处理时间分布
	msgDelaySum *prometheus.SummaryVec
	//接受消息的速率（B/s）
	msgReceivedRate prometheus.Gauge
	//消息发送的速率
	msgSentRate prometheus.Gauge
	//资源（rule,route,subscription）同步速度（单位：个）
	resourceSync *prometheus.GaugeVec
	//资源（rule,route,subscription）同步速度（单位：byte）
	resourceSyncSent *prometheus.GaugeVec
}

func GetIns() *RulexMetrics {
	return rulexMetric
}

func (this *RulexMetrics) register() {

	labels := option.WithBindLabels(
		option.NewBaseBindLabels(
			this.name,
			this.module,
			this.zone)...,
	)
	opts := []xmetrics.Option{labels.Append([]option.BindLabel{option.NewBindLabel("node", this.node)})}

	xmetrics.Setup(this.reg, opts...).Register(this.msgInOut)
	xmetrics.Setup(this.reg, opts...).Register(this.msgReceivedRate)
	xmetrics.Setup(this.reg, opts...).Register(this.msgSentRate)
	xmetrics.Setup(this.reg, opts...).Register(this.msgDelay)
	xmetrics.Setup(this.reg, opts...).Register(this.msgDelaySum)
	xmetrics.Setup(this.reg, opts...).Register(this.resourceSync)
	xmetrics.Setup(this.reg, opts...).Register(this.resourceSyncSent)
	xmetrics.Setup(this.reg, opts...).Register(prometheus.NewGoCollector())
}

func (this *RulexMetrics) exposed(c *conf.Config) {
	if err := xmetrics.ExposedMetrics(this.reg, &xmetrics.ExposedConf{
		Addr: c.Prometheus.Address(),
		Etcd: c.Prometheus.Endpoints,
		KV:   NewrulexFmt(this.name),
	}); nil != err {
		if c.Prometheus.ExitFlag {
			panic(err)
		} else {
			logger.Bg().Error("start prometheus exporter failure.", logf.Any("args", c))
		}
	}
}

func Init(c *conf.Config) {
	rulexMetric = &RulexMetrics{
		name:   c.Prometheus.Name,
		node:   c.Prometheus.Node,
		zone:   c.Prometheus.Zone,
		module: c.Prometheus.Module,
		reg:    prometheus.NewRegistry(),

		msgInOut: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mdmp",
			Name:      "inout",
			Help:      "rece ived messages and sent messages.",
		}, inoutLabels),

		msgReceivedRate: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "mdmp",
			Subsystem: "recv",
			Name:      "rate",
			Help:      "received message rate.",
		}),

		msgSentRate: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "mdmp",
			Subsystem: "sent",
			Name:      "rate",
			Help:      "send message rate.",
		}),

		msgDelay: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "mdmp",
			Subsystem: "msg",
			Name:      "delay",
			Help:      "message delay histogram.",
			Buckets:   []float64{5, 10, 20, 50, 100, 200, 300, 500, 1000, 1500, 3000}, //延迟以ms为单位
		}, []string{"status"}),

		msgDelaySum: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:  "mdmp",
			Subsystem:  "msg",
			Name:       "delay_summary",
			Help:       "message delay summary.",
			Objectives: map[float64]float64{0.5: 0.01, 0.6: 0.01, 0.7: 0.01, 0.8: 0.01, 0.9: 0.01, 0.95: 0.001, 0.99: 0.001},
		}, []string{"status"}),

		resourceSync: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mdmp",
			Name:      "sync",
			Help:      "count for sync resource.",
		}, resSyncLabels),

		resourceSyncSent: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mdmp",
			Name:      "sync_sent",
			Help:      "count for sync resource(bytes).",
		}, resSyncLabels),
	}

	rulexMetric.register()

	//expose
	rulexMetric.exposed(c)
}
