package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"go.uber.org/atomic"
	"net/url"
	"strconv"
	"strings"

	//"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/tkeel-io/rule-rulex/pkg/sink"
	plugin "github.com/tkeel-io/rule-rulex/pkg/sink/plugin/redis"
	xutils "github.com/tkeel-io/rule-rulex/pkg/sink/utils"
	"sync"
	"time"

	xredis "github.com/go-redis/redis"
)

const (
	ready_insert_message   = 50000
	interval_consumer_time = 3 * time.Second
	max_run_pool           = 5
)

type Option struct {
	//Addrs     string `json:"addrs,omitempty"`
	Urls   []string         `json:"urls"`
	Key string           `json:"Key,omitempty"`
	Fields map[string]Field `json:"fields,omitempty"`
}
type Field struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}
type Message struct {
	Data     map[string]*Data `json:"data"`
	DeviceId string           `json:"device_id"`
}
type Data struct {
	Value interface{} `json:"value"`
	Time  int64       `json:"ts"`
}

type Fields struct {
	fields   map[string]string //global fields
	fieldArr []*field
	nodes    map[string]*node //metric nodes
}
type node struct {
	nodeName string
	fields   map[string]string //metric nodes fields
	fieldArr []*field
}

type execNode struct {
	ts     int64
	fields []string
	args   []interface{}
	typ []string
}

type field struct {
	name  string `json:"urls"`
	typ   string `json:"urls"`
	value string `json:"urls"`
}

//redis plugin
type redis struct {
	entityType, entityID string
	balance              plugin.LoadBalance
	//db     *sqlx.DB
	option *Option
	fields []*field //映射关系
	actionContent types.ActionContent
	ch    chan types.Message
	wg    *sync.WaitGroup
	queue xutils.BatchSink
	metadata      map[string]string
	inited        *atomic.Bool
}

func (a *redis) Flush(ctx types.ActionContent) error {
	if a.queue != nil {
		a.queue.Flush(ctx.Context())
	}
	return nil
}

func (a *redis) Setup(ctx types.ActionContent, metadata map[string]string) (err error) {
	a.actionContent = ctx
	a.metadata = metadata
	a.inited.Store(false)
	err =a.setup()

	return err
}

func (a *redis) setup()(err error){
	log.Info("redis setup ", logf.Any("metadata", a.metadata))
	opt, err := a.parseOption(a.metadata)
	if err != nil {
		return err
	}
	servers := make([]*plugin.Server, len(opt.Urls))
	for k, v := range opt.Urls {
		u,_ :=url.Parse(v)
		p,_ :=u.User.Password()
		DB,err :=strconv.Atoi(strings.Replace(u.Path,"/","",-1))
		db :=xredis.NewClient(&xredis.Options{
			Addr : u.Host,
			Password: p,
			DB: DB,
			DialTimeout: 30 * time.Second,

		})
		log.Info("redis init " + v)
		if err != nil {
			log.Error("open redis", logf.Any("error", err))
			return err
		}
		if _,err = db.Ping().Result(); err != nil {
			log.Error("ping redis", logf.Any("error", err))
			return err
		}
		servers[k] = &plugin.Server{db, v, 1}

	}
	a.option = opt
	a.balance = plugin.NewLoadBalanceRandom(servers)
	a.fields = make([]*field, 0, len(opt.Fields))
	for key, fd := range opt.Fields {
		a.fields = append(a.fields, &field{
			key, fd.Type, fd.Value,
		})
	}
	return
}
func (a *redis) initTask()(err error){
	sopt := xutils.SinkBatchOptions{
		MaxBatching:             max_run_pool,
		MaxPendingMessages:      ready_insert_message,
		BatchingMaxPublishDelay: interval_consumer_time,
	}

	a.queue, err = xutils.NewBatchSink("redis", &sopt,
		func(msgs []types.Message) (err error) {
			//t := time.Now()
			if err := a.Insert(sink.NewContext(context.Background()), msgs); err != nil {
				a.onError(err)
			}
			 utils.Log.Bg().Info("insert message",
				logf.Int("len(msgs)", len(msgs)))
			return nil
		})
	return err
}
func (a *redis) Invoke(ctx types.ActionContent, message types.Message) (err error) {
	log.Info("redis Invoke ", logf.Any("message", message))
	if !a.inited.Load() {
		if err = a.initTask(); err != nil {
			return err
		}
		a.inited.Store(true)
	}

	a.queue.Send(ctx.Context(), message)
	return
}

func (a *redis) Insert(ctx types.ActionContent, messages []types.Message) (err error) {
	var (
		tx xredis.Pipeliner
	)
	server := a.balance.Select([]*xredis.Client{})
	if server == nil {
		return fmt.Errorf("get database failed, can't insert")
	}
	rows := make([]*execNode, 0)
	for _, message := range messages {

		data := new(execNode)
		utils.Log.Bg().Info("Invoke", logf.Any("messages", string(message.Data())))
		msgCtx := NewMessageContext(message.(types.PublishMessage))
		evalCtx := NewContext("", string(message.Data()), msgCtx)
		data.fields = make([]string, 0, len(a.fields))
		data.args = make([]interface{}, 0, len(a.fields))
		for _, field := range a.fields {
			name, v, typ := field.name, field.value, field.typ
			val := Execute(evalCtx, v)
			key,err :=keyGenerator(message,name)//动态生成key
			if err !=nil {
				return err
			}
			data.fields=append(data.fields, key)
			if err := fillExecNode(val, typ, data, name); err != nil {
				return err
			}
		}
		if len(data.fields) > 0 && len(data.fields) == len(data.args) {
			rows = append(rows, data)
		} else {
			utils.Log.Bg().Warn("rows is empty",
				logf.Any("message", message),
				logf.Any("args", data.args),
				logf.Any("fields", data.fields),
				logf.Any("option", a.option),
			)
		}

	}

	//var mv map[string]interface{}
	if len(rows) > 0 {
		tx =server.DB.TxPipeline()
		//db :=server.DB
		for _, row := range rows {
			for k,t :=range row.typ{
				if t == "String" {
					r :=make(map[string]string)
					so :=row.args[k]  //获得要插入的value
					by,_ :=so.(string)
					json.Unmarshal([]byte(by),&r)//将value转化为map类型
					key :=row.fields[k]
					tx.Set(key,row.args[k],-1)

				}else if t =="Hash" {
					so :=row.args[k]  //获得要插入的value
					rr:=so.(map[string]interface{})
					rstr :=make(map[string]string)
					result :=make(map[string]interface{})
					for a,b :=range rr{//将value全部作为string类型存储
						str,_ :=json.Marshal(b)
						rstr[a]=string(str)
					}
					for ke,va:=range rstr{//将map[string]string转化为map[string]interface{}
						result[ke] = va

					}
					key :=row.fields[k]//获得key的值

					tx.HMSet(key,result)

				}else {

					utils.Log.Bg().Error("args type is error",
							logf.String("type", t),
							logf.Any("args", row.args),
							logf.Any("fields", row.fields),
							logf.String("error", "args type is error"))
						return fmt.Errorf("%s","args type is error")
				}
			}

			if _,err =tx.Exec();err != nil {
				utils.Log.Bg().Error("db Exec error",
					logf.Any("args", row.args),
					logf.Any("fields", row.fields),
					logf.String("error", err.Error()))
				return err
			}

		//fmt.Println(row.value)
            //tx.HSet("","","")
			//utils.Log.Bg().Debug("preURL",
			//	logf.Int64("ts", row.ts),
			//	logf.Any("args", row.args),
			//	logf.String("preURL", preURL))
			//if _, err = stmt.Exec(row.args...); err != nil {
			//	utils.Log.Bg().Error("db Exec error",
			//		logf.String("preURL", preURL),
			//		logf.Any("args", row.args),
			//		logf.Any("fields", row.fields),
			//		logf.String("error", err.Error()))
			//	return err
			//}
		}

	}
	return nil
}
func fillExecNode(val interface{}, typ string, data *execNode, fieldName string) error {
	switch typ {
	case "String":
		if arg, err := ToString(val); err == nil {
			data.args = append(data.args, arg)
			data.typ=append(data.typ, typ)
		} else {
			return errors.New(fmt.Sprintf("field(%s) type error,want %s,got (%v)", fieldName, typ, val))
		}
	case "Hash":
		if arg, err := ToHash(val); err == nil {
			data.args = append(data.args, arg)
			data.typ=append(data.typ, typ)
		} else {
			return errors.New(fmt.Sprintf("field(%s) type error,want %s,got (%v)", fieldName, typ, val))
		}

	default:
		if arg, err := ToString(val); err == nil {
			data.args = append(data.args, arg)
			data.typ=append(data.typ, "String")
		} else {
			return errors.New(fmt.Sprintf("field(%s) type error,want %s,got (%v)", fieldName, typ, val))
		}
	}
	return nil
}
//func (a *mysql) parseFields(metadata map[string]string) []*field {
	//if s, ok := metadata["fields"]; ok {
		//fields := make(map[string]string)
		//err := json.Unmarshal([]byte(s), &fields)
		//if err == nil {
		//	ret := make([]*field, 0, len(fields))
		//	for n, v := range fields {
			//	ret = append(ret, &field{n, v})
		//	}
		//	return ret
	//	}
	//}
//	return nil
//}

func (a *redis) parseOption(metadata map[string]string) (*Option, error) {
	s, ok := metadata["option"]
	if !ok {
		return nil, errors.New("option not found")
	}

	opt := Option{}
	err := json.Unmarshal([]byte(s), &opt)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("option unmarshal error(%s)", err.Error()))
	}
	if opt.Fields == nil {
		return nil, errors.New(fmt.Sprintf("field not found"))
	}

	for key, field := range opt.Fields {
		if key == "" {
			return nil, errors.New(fmt.Sprintf("field name is empty"))
		}
		if field.Type == "" {
			return nil, errors.New(fmt.Sprintf("field(%s) types is empty", key))
		}
		if field.Value == "" {
			//return nil, errors.New(fmt.Sprintf("field(%s) types is empty", key))
		}
	}
	return &opt, nil
}

func (a *redis) Close() (err error) {
	log.Debug("close queue...")
	if a.queue != nil {
		a.queue.Close()
		return
	}
	log.Debug("close db...")
	if a.balance != nil {
		a.balance.Close()
		return
	}
	log.Debug("close db done...")
	return nil
}

func (a *redis) genSql(row *execNode) string {
	return  Redis_SSQL_TLP
}
func (a *redis) onError(err error) {
	a.actionContent.Nack(err)
}
func keyGenerator(message types.Message,key string) (string,error){
	data :=make(map[string]interface{})
	err :=json.Unmarshal(message.Data(),&data)
	if err!=nil{

		    return "",errors.New(fmt.Sprintf("key build error,err:%s", err))

	}else{
		k :=data[key].(string)
		if len(k)==0{
			return "",errors.New(fmt.Sprintf("key build error,key:%s 无效", key))
		}
		keyresult :=fmt.Sprintf("mdmp.rulex.%s",k)
		return keyresult,nil
	}
}