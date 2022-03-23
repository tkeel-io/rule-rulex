package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	gv1 "github.com/tkeel-io/rule-util/gateway/v1"
	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/types"
	ck "github.com/tkeel-io/rule-rulex/pkg/go-clickhouse"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"

	//"github.com/jmoiron/sqlx"
	//ck "github.com/mailru/go-clickhouse"
	"time"
)

//property model
type PropertyDB struct {
	balance LoadBalance
	dbName  string
	table   string
	log     *zap.Logger
}

func NewPropertyDB(dbName, table string, urls []string, log *zap.Logger) (property types.DBImpl, err error) {
	servers := make([]*Server, len(urls))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for k, v := range urls {
		//tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000
		//url := fmt.Sprintf("%s/%s?max_execution_time=10", v, dbName)
		log.Info("clickhouse init " + v)
		db, err := sqlx.Open("mysql", v)
		if err != nil {
			log.Error("open mysql", logf.Any("error", err))
			return nil, err
		}
		if err = db.PingContext(ctx); err != nil {
			log.Error("ping mysql", logf.Any("error", err))
			return nil, err
		}
		//_, err := db.Exec(CLICKHOUSE_IOT_THING_CREATE)
		//if err != nil {
		//	log.For(ctx).Warn(err.Error(), logf.String("table", table))
		//}
		db.SetConnMaxLifetime(30 * time.Second)
		db.SetMaxOpenConns(5)
		servers[k] = &Server{db, v, 1}
	}
	log.Info("connect mysql ", zap.Any("config", servers))
	property = &PropertyDB{
		balance: NewLoadBalanceRandom(servers),
		dbName:  dbName,
		table:   table,
		log:     log,
	}
	return property, nil
}
func (p *PropertyDB) BatchCreate(ctx context.Context, sqlCreate string, values interface{}) (err error) {
	var (
		tx     *sql.Tx
		stmt   *sql.Stmt
		preURL string
		ok     bool
		data   []*gv1.DeviceModel
	)
	if data, ok = values.([]*gv1.DeviceModel); !ok {
		return errors.New("values type error")
	}
	start := time.Now()
	server := p.balance.Select([]*sqlx.DB{})
	if server == nil {
		return fmt.Errorf("get database failed, can't insert")
	}
	if tx, err = server.DB.BeginTx(ctx, nil); err != nil {
		log.Error("begin tx", logf.Any("error", err))
		return
	}
	defer func() {
		if err != nil {
			log.Error("Rollback error", logf.Error(err))
			_ = tx.Rollback()
		}
		if stmt != nil {
			_ = stmt.Close()
		}
		log.Info("Invoke lost", logf.Int("len", len(data)), logf.Int32("lost", int32(time.Since(start)/1e6)))
		//fmt.Println("Invoke lost:", time.Now().Sub(start))
	}()
	preURL = fmt.Sprintf(sqlCreate, p.dbName, p.table)
	log.Debug("preURL",
		logf.String("preURL", preURL))
	if stmt, err = tx.Prepare(preURL); err != nil {
		log.Error("tx prepare", logf.Any("error", err))
		return
	}
	for _, v := range data {
		_, err = stmt.Exec(v.Time,
			v.UserId,
			v.DeviceId,
			v.SourceId,
			v.ThingId,
			v.Identifier,
			v.ValueInt32,
			v.ValueFloat,
			v.ValueDouble,
			v.ValueString,
			v.ValueEnum,
			v.ValueBool,
			v.ValueStringEx,
			ck.Array(v.ValueArrayString),
			ck.Array(v.ValueArrayStruct),
			ck.Array(v.ValueArrayInt32),
			ck.Array(v.ValueArrayFloat),
			ck.Array(v.ValueArrayDouble),
			v.Tags,
			ck.Date(v.ActionDate),
			v.ActionTime)
		if err != nil {
			log.Error("stmt error", logf.Error(err))
			return err
		}
	}
	err = tx.Commit()
	if err != nil {
		log.Error("commit error", logf.Error(err))
	}
	return err
}
func (p *PropertyDB) Close() error {
	p.balance.Close()
	return nil
}
