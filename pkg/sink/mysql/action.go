package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"go.uber.org/atomic"

	//"github.com/tkeel-io/rule-rulex/internal/utils"

	"github.com/jmoiron/sqlx"
	"github.com/tkeel-io/rule-rulex/pkg/sink"
	plugin "github.com/tkeel-io/rule-rulex/pkg/sink/plugin/mysql"
	xutils "github.com/tkeel-io/rule-rulex/pkg/sink/utils"

	_ "github.com/go-sql-driver/mysql"
)

const (
	ready_insert_message   = 50000
	interval_consumer_time = 3 * time.Second
	max_run_pool           = 5
)

type Option struct {
	// Addrs     string `json:"addrs,omitempty"`
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
	fields   map[string]string // global fields
	fieldArr []*field
	nodes    map[string]*node // metric nodes
}

type node struct {
	nodeName string
	fields   map[string]string // metric nodes fields
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

// mysql plugin
type mysql struct {
	entityType, entityID string
	balance              plugin.LoadBalance
	// db     *sqlx.DB
	option        *Option
	fields        []*field // 映射关系
	actionContent types.ActionContent
	ch            chan types.Message
	wg            *sync.WaitGroup
	queue         xutils.BatchSink
	metadata      map[string]string
	inited        *atomic.Bool
}

func (a *mysql) Flush(ctx types.ActionContent) error {
	if a.queue != nil {
		a.queue.Flush(ctx.Context())
	}
	return nil
}

func (a *mysql) Setup(ctx types.ActionContent, metadata map[string]string) (err error) {
	a.actionContent = ctx
	a.metadata = metadata
	a.inited.Store(false)
	err = a.setup()

	return err
}

func (a *mysql) setup() (err error) {
	ctx := a.actionContent.Context()
	log.Info("mysql setup ", logf.Any("metadata", a.metadata))
	opt, err := a.parseOption(a.metadata)
	if err != nil {
		return err
	}
	servers := make([]*plugin.Server, len(opt.Urls))
	for k, v := range opt.Urls {
		log.Info("mysql init " + v)
		db, err := sqlx.Open("mysql", v)
		if err != nil {
			log.Error("open mysql", logf.Any("error", err))
			return err
		}
		if err = db.PingContext(ctx); err != nil {
			log.Error("ping mysql", logf.Any("error", err))
			return err
		}
		if _, err = db.Query(fmt.Sprintf("desc %s.%s;", opt.DbName, opt.Table)); err != nil {
			log.Error("check mysql table", logf.Any("error", err))
			return err
		}
		db.SetConnMaxLifetime(30 * time.Second)
		db.SetMaxOpenConns(5)
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

func (a *mysql) initTask() (err error) {
	sopt := xutils.SinkBatchOptions{
		MaxBatching:             max_run_pool,
		MaxPendingMessages:      ready_insert_message,
		BatchingMaxPublishDelay: interval_consumer_time,
	}

	a.queue, err = xutils.NewBatchSink("mysql", &sopt,
		func(msgs []types.Message) (err error) {
			// t := time.Now()
			if err := a.Insert(sink.NewContext(context.Background()), msgs); err != nil {
				a.onError(err)
			}
			utils.Log.Bg().Info("insert message",
				logf.Int("len(msgs)", len(msgs)))
			return nil
		})
	return err
}

func (a *mysql) Invoke(ctx types.ActionContent, message types.Message) (err error) {
	log.Info("mysql Invoke ", logf.Any("message", message))
	if !a.inited.Load() {
		if err = a.initTask(); err != nil {
			return err
		}
		a.inited.Store(true)
	}
	// a.queue.Send(ctx.Context(), message)

	return a.Insert(sink.NewContext(context.Background()), []types.Message{message})
}

func (a *mysql) Insert(ctx types.ActionContent, messages []types.Message) (err error) {
	var tx *sql.Tx
	server := a.balance.Select([]*sqlx.DB{})
	if server == nil {
		return fmt.Errorf("get database failed, can't insert")
	}
	rows := make([]*execNode, 0)
	for _, message := range messages {

		data := new(execNode)
		tqlNode := ruleql.NewJSONContext(string(message.Data()))

		utils.Log.Bg().Info("Invoke", logf.Any("messages", string(message.Data())))

		data.fields = make([]string, 0, len(a.fields))
		data.args = make([]interface{}, 0, len(a.fields))
		for _, field := range a.fields {
			name, v, typ := field.name, field.value, field.typ
			val := xutils.GetValue(tqlNode, v)
			if err := fillExecNode(val, typ, data, name); err != nil {
				utils.Log.Bg().Error("fillExecNode failed", logf.Any("error", err))
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
			if _, err = stmt.Exec(row.args...); err != nil {
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
	var err error
	lowerType := strings.ToLower(typ)
	switch lowerType {
	case "tinyint", "smallint", "mediumint", "int", "bigint", "int64":
		val, err = ToInt64(val)
	case "float":
		val, err = ToFloat32(val)
	case "double":
		val, err = ToFloat64(val)
	case "char", "varchar", "text", "date", "time", "datetime", "timestamp", "string":
		val, err = ToString(val)
	default:
		err = fmt.Errorf("field(%s) type error,want %s,got (%v)", fieldName, typ, val)
	}
	if err != nil {
		return err
	}
	data.args = append(data.args, val)
	data.fields = append(data.fields, fieldName)
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

func (a *mysql) parseOption(metadata map[string]string) (*Option, error) {
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
			// return nil, errors.New(fmt.Sprintf("field(%s) types is empty", key))
		}
	}
	return &opt, nil
}

func (a *mysql) Close() (err error) {
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

func (a *mysql) genSql(row *execNode) string {
	stmts := strings.Repeat("?,", len(row.fields))
	if len(stmts) > 0 {
		stmts = stmts[:len(stmts)-1]
	}
	//	fmt.Println("row.fields=",row.fields)
	return fmt.Sprintf(MYSQL_SSQL_TLP,
		a.option.DbName,
		a.option.Table,
		strings.Join(row.fields, ","),
		stmts)
}

func (a *mysql) onError(err error) {
	a.actionContent.Nack(err)
}
