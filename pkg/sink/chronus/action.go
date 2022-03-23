package chronus

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/tkeel-io/rule-util/pkg/errors"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/tkeel-io/rule-rulex/pkg/sink"
	"github.com/jmoiron/sqlx"
	"go.uber.org/atomic"
	"strings"
	"sync"
	"time"

	ck "github.com/tkeel-io/rule-rulex/pkg/go-clickhouse"
	plugin "github.com/tkeel-io/rule-rulex/pkg/sink/plugin/clickhouse"
	xutils "github.com/tkeel-io/rule-rulex/pkg/sink/utils"
	//ck "github.com/ClickHouse/clickhouse-go"
)

const (
	ready_insert_message   = 50000
	interval_consumer_time = 3 * time.Second
	max_run_pool           = 5
)

type Option struct {
	//Addrs     string `json:"addrs,omitempty"`
	Urls   []string         `json:"urls"`
	DbName string           `json:"dbName,omitempty"`
	Table  string           `json:"table,omitempty"`
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
}

type field struct {
	name  string `json:"urls"`
	typ   string `json:"urls"`
	value string `json:"urls"`
}

//chronus plugin
type chronus struct {
	entityType, entityID string
	balance              plugin.LoadBalance
	//db     *sqlx.DB
	option *Option
	fields []*field //映射关系

	ch    chan types.Message
	wg    *sync.WaitGroup
	queue xutils.BatchSink

	actionContent types.ActionContent
	metadata      map[string]string
	inited        *atomic.Bool
}

func (a *chronus) Flush(ctx types.ActionContent) error {
	if a.queue != nil {
		a.queue.Flush(ctx.Context())
	}
	return nil
}

func (a *chronus) Setup(ctx types.ActionContent, metadata map[string]string) error {
	a.actionContent = ctx
	a.metadata = metadata
	a.inited.Store(false)
	err := a.setup()
	//switch err := err.(type) {
	//case nil:
	//case *ck.Exception:
	//	switch err.Code {
	//	case 60:
	//		return errors.NewCodeError(errors.TableNotExisted,"")
	//	default:
	//	}
	//default:
	//}
	return err
}

func (a *chronus) setup() (err error) {
	ctx := a.actionContent.Context()
	log.Info("chronus setup ", logf.Any("metadata", a.metadata))
	opt, err := a.parseOption(a.metadata)
	if err != nil {
		return err
	}
	servers := make([]*plugin.Server, len(opt.Urls))
	for k, v := range opt.Urls {
		log.Info("chronus init " + v)
		db, err := sqlx.Open("chronus", v)
		if err != nil {
			log.Error("open chronus", logf.Any("error", err))
			return err
		}
		if err = db.PingContext(ctx); err != nil {
			log.Error("ping chronus", logf.Any("error", err))
			return err
		}
		if _, err = db.Query(fmt.Sprintf("desc %s.%s;", opt.DbName, opt.Table)); err != nil {
			log.Error("check chronus table", logf.Any("error", err))
			return err
		}
		db.SetConnMaxLifetime(30 * time.Second)
		db.SetMaxOpenConns(5)
		servers[k] = &plugin.Server{db, v, 1}
	}
	a.option = opt

	a.fields = make([]*field, 0, len(opt.Fields))
	for key, fd := range opt.Fields {
		a.fields = append(a.fields, &field{
			key, fd.Type, fd.Value,
		})
	}

	//a.db = db
	a.balance = plugin.NewLoadBalanceRandom(servers)

	return
}

func (a *chronus) initTask() (err error) {
	sopt := xutils.SinkBatchOptions{
		MaxBatching:             max_run_pool,
		MaxPendingMessages:      ready_insert_message,
		BatchingMaxPublishDelay: interval_consumer_time,
	}
	a.queue, err = xutils.NewBatchSink("chronus", &sopt,
		func(msgs []types.Message) (err error) {
			log.Info("chronus NewBatchSink", logf.Any("msgs", len(msgs)))
			//t := time.Now()
			if err := a.Insert(sink.NewContext(context.Background()), msgs); err != nil {
				a.onError(err)
			}
			utils.Log.Bg().Info("insert message",
				logf.Int("len(msgs)", len(msgs)))
			//fmt.Println("len(msgs)", len(msgs), time.Now().Sub(t), t, string(msgs[0].Data()))
			return nil
		})
	return err
}

func (a *chronus) Invoke(ctx types.ActionContent, message types.Message) (err error) {
	log.Info("chronus Invoke ", logf.Any("message", message))
	if !a.inited.Load() {
		if err = a.initTask(); err != nil {
			return err
		}
		a.inited.Store(true)
	}
	a.queue.Send(ctx.Context(), message)
	return
}

func (a *chronus) Insert(ctx types.ActionContent, messages []types.Message) (err error) {
	//log.Info("chronus Insert ", logf.Any("messages", messages))
	var (
		tx *sql.Tx
	)
	rows := make([]*execNode, 0)
	for _, message := range messages {

		data := new(execNode)

		//fmt.Println(string(message.Data()))
		utils.Log.Bg().Info("Invoke", logf.Any("messages", string(message.Data())))
		//jsonCtx := utils.NewJSONContext(string(message.Data()))
		msgCtx := NewMessageContext(message.(types.PublishMessage))
		evalCtx := NewContext("", string(message.Data()), msgCtx)

		data.fields = make([]string, 0, len(a.fields))
		data.args = make([]interface{}, 0, len(a.fields))
		for _, field := range a.fields {
			name, v, typ := field.name, field.value, field.typ
			val := Execute(evalCtx, v)
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
	if len(rows) > 0 {
		preURL := a.genSql(rows[0])
		server := a.balance.Select([]*sqlx.DB{})
		if server == nil {
			return fmt.Errorf("get database failed, can't insert")
		}
		if tx, err = server.DB.BeginTx(ctx.Context(), nil); err != nil {
			utils.Log.Bg().Error("pre URL error",
				logf.String("preURL", preURL),
				logf.Any("row", rows[0]),
				logf.String("error", err.Error()))
			return err
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			}
		}()
		stmt, err := tx.Prepare(preURL)
		if err != nil {
			utils.Log.Bg().Error("pre URL error",
				logf.String("preURL", preURL),
				logf.String("error", err.Error()))
			return err
		}
		for _, row := range rows {
			utils.Log.Bg().Debug("preURL",
				logf.Int64("ts", row.ts),
				logf.Any("args", row.args),
				logf.String("preURL", preURL))
			if _, err := stmt.Exec(row.args...); err != nil {
				utils.Log.Bg().Error("db Exec error",
					logf.String("preURL", preURL),
					logf.Any("args", row.args),
					logf.Any("fields", row.fields),
					logf.String("error", err.Error()))
				return err
			}
		}
		err = tx.Commit()
		if err != nil {
			row := rows[0]
			utils.Log.Bg().Error("tx Commit error",
				logf.Int64("ts", row.ts),
				logf.Any("args", row.args),
				logf.Any("fields", row.fields),
				logf.String("preURL", preURL),
				logf.String("error", err.Error()))
			return err
		}
		_ = stmt.Close()
	}
	return nil
}

func fillExecNode(val interface{}, typ string, data *execNode, fieldName string) error {
	switch typ {
	case "DateTime":
		if val, err := ToInt64(val); err == nil {
			ts := time.Unix(val, 0)
			data.args = append(data.args, ts)
		} else {
			return errors.New(fmt.Sprintf("field(%s) type error,want %s,got %v", fieldName, typ, val))
		}
	case "Date":
		if arg, err := ToInt64(val); err == nil {
			ts := time.Unix(arg, 0)
			data.args = append(data.args, ck.Date(ts))
		} else {
			return errors.New(fmt.Sprintf("field(%s) type error,want %s,got (%v)", fieldName, typ, val))
		}
	case "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64":
		if arg, err := ToInt64(val); err == nil {
			data.args = append(data.args, arg)
		} else {
			return errors.New(fmt.Sprintf("field(%s) type error,want %s,got (%v)", fieldName, typ, val))
		}
	case "Float32", "Float64":
		if arg, err := ToFloat64(val); err == nil {
			data.args = append(data.args, arg)
		} else {
			return errors.New(fmt.Sprintf("field(%s) type error,want %s,got (%v)", fieldName, typ, val))
		}
	case "String":
		if arg, err := ToString(val); err == nil {
			data.args = append(data.args, arg)
		} else {
			return errors.New(fmt.Sprintf("field(%s) type error,want %s,got (%v)", fieldName, typ, val))
		}
	default:
		if arg, err := ToFloat64(val); err == nil {
			data.args = append(data.args, arg)
		} else {
			return errors.New(fmt.Sprintf("field(%s) type error,want %s,got (%v)", fieldName, typ, val))
		}
	}
	data.fields = append(data.fields, fieldName)
	return nil
}

func (a *chronus) parseOption(metadata map[string]string) (*Option, error) {
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

func (a *chronus) Close() (err error) {
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

func (a *chronus) genSql(row *execNode) string {
	stmts := strings.Repeat("?,", len(row.fields))
	if len(stmts) > 0 {
		stmts = stmts[:len(stmts)-1]
	}
	return fmt.Sprintf(CLICKHOUSE_SSQL_TLP,
		a.option.DbName,
		a.option.Table,
		strings.Join(row.fields, ","),
		stmts)
}

func (a *chronus) onError(err error) {
	a.actionContent.Nack(err)
}
