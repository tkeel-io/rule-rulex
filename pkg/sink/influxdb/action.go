package influxdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/pkg/sink"
	"github.com/tkeel-io/rule-util/pkg/log"
)

type tsItem struct {
	Ts    int64   `json:"ts"`
	Value float64 `json:"value"`
}

type tsMsg struct {
	Telemetry map[string]tsItem `json:"telemetry"`
}

type deviceMsg struct {
	Id         string `json:"id"`
	Properties tsMsg  `json:"properties"`
}

// Influx allows writing to InfluxDB.
type Influx struct {
	cfg       *InfluxConfig
	client    influxdb2.Client
	writeAPI  api.WriteAPIBlocking
	modelName string
	tags      map[string]string
	metadata  map[string]string
}

type InfluxConfig struct {
	URL    string `json:"url"`
	Token  string `json:"token"`
	Org    string `json:"org"`
	Bucket string `json:"bucket"`
}

type Option struct {
	Tags map[string]string `json:"tags"`
	Urls []string          `json:"urls"`
}

func (i *Influx) Setup(ctx types.ActionContent, metadata map[string]string) error {
	i.metadata = metadata
	i.modelName = "keel"
	return i.setup()
}

func (i *Influx) parseOption() (err error) {
	s, ok := i.metadata["option"]
	if !ok {
		return errors.New("option not found")
	}
	opt := Option{}
	err = json.Unmarshal([]byte(s), &opt)
	if err != nil {
		return errors.New(fmt.Sprintf("option unmarshal error(%s)", err.Error()))
	}
	i.tags = opt.Tags
	if len(opt.Urls) != 1 {
		return errors.New("url error")
	}
	url := opt.Urls[0]
	items1 := strings.Split(url, "@")
	if len(items1) != 2 {
		return errors.New("url error")
	}
	items2 := strings.SplitN(items1[0], ":", 2)
	if len(items2) != 2 {
		return errors.New("url error")
	i.cfg = &InfluxConfig{}
	i.cfg.Token = items2[1]

	items3 := strings.SplitN(items1[1], "/", 2)
	if len(items3) != 2 {
		return errors.New("url error")
	i.cfg.URL = "http://" + items3[0]

	items4 := strings.SplitN(items3[1], "?", 2)
	if len(items4) != 2 {
		return errors.New("url error")
	i.cfg.Org = items4[0]
	i.cfg.Bucket = items4[1]
	return
}

func (i *Influx) setup() (err error) {
	client := influxdb2.NewClient(i.cfg.URL, i.cfg.Token)
	i.client = client
	i.writeAPI = i.client.WriteAPIBlocking(i.cfg.Org, i.cfg.Bucket)
	return nil
}

func makeKVString(req map[string]string) string {
	ress := make([]string, 0)
	for k, v := range req {
		ress = append(ress, k+"="+v)
	}
	return strings.Join(ress, ",")
}

func makeKVSFloat(req map[string]tsItem) (string, int64) {
	ress := make([]string, 0)
	var ts int64
	for k, v := range req {
		ress = append(ress, fmt.Sprintf("%s=%f", k, v.Value))
		ts = v.Ts * 1e6
	}
	return strings.Join(ress, ","), ts
}

// 模型名称 作为measurement, 模型id和设备id作为默认tag
func (i *Influx) makeDataString(data []byte) []string {
	msg := deviceMsg{}
	err := json.Unmarshal(data, &msg)
	if err != nil {
		log.GlobalLogger().Bg().Error(err.Error())
		return nil
	}
	if len(msg.Properties.Telemetry) == 0 {
		return nil
	}
	valueString, ts := makeKVSFloat(msg.Properties.Telemetry)

	ss := fmt.Sprintf("%s,%s %s %d", i.modelName, makeKVString(i.tags), valueString, ts)
	return []string{ss}
}

func (i *Influx) Invoke(ctx types.ActionContent, message types.Message) error {
	points := i.makeDataString(message.Data())
	return i.writeAPI.WriteRecord(context.Background(), points...)
}

// Close
// 关闭连接
func (i *Influx) Close() error {
	log.GlobalLogger().Bg().Debug("Close")
	return nil
}

func init() {
	sink.Registered(EntityType, newAction)
}

func newAction(entityType, entityID string) types.Action {
	return newInflux(entityType, entityID)
}

func newInflux(entityType, entityID string) *Influx {
	return &Influx{}
}
