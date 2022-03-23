package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	gv1 "github.com/tkeel-io/rule-util/gateway/v1"
	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/jmoiron/sqlx"
	//ck "github.com/mailru/go-clickhouse"
	//ck "github.com/ClickHouse/clickhouse-go"
	ck "github.com/tkeel-io/rule-rulex/pkg/go-clickhouse"
	"time"
)

//property model
type EventDB struct {
	balance LoadBalance
	dbName  string
	table   string
}

func NewEventDB(dbName, table string, urls []string) (event types.DBImpl, err error) {
	var (
		db *sqlx.DB
	)
	servers := make([]*Server, len(urls))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for k, v := range urls {
		db, err = sqlx.Open("clickhouse", v)
		if err != nil {
			log.For(ctx).Error("open clickhouse", logf.Any("error", err))
			return
		}
		if err = db.PingContext(ctx); err != nil {
			log.For(ctx).Error("ping clickhouse", logf.Any("error", err))
			return
		}
		//_, err := db.Exec(CLICKHOUSE_IOT_DEVICE_EVENT_DATA)
		//if err != nil {
		//	log.For(ctx).Warn(err.Error(), logf.String("table", table))
		//}
		db.SetConnMaxLifetime(30 * time.Second)
		db.SetMaxOpenConns(5)
		servers[k] = &Server{db, v, 1}
	}
	event = &EventDB{
		balance: NewLoadBalanceRandom(servers),
		dbName:  dbName,
		table:   table,
	}
	return event, nil
}
func (e *EventDB) BatchCreate(ctx context.Context, sqlCreate string, values interface{}) (err error) {
	var (
		tx     *sql.Tx
		stmt   *sql.Stmt
		preURL string
		ok     bool
		data   []*gv1.EventThingStore
	)
	if data, ok = values.([]*gv1.EventThingStore); !ok {
		return errors.New("values type error")
	}
	start := time.Now()
	server := e.balance.Select([]*sqlx.DB{})
	if server == nil {
		return fmt.Errorf("get database failed, can't insert")
	}
	if tx, err = server.DB.BeginTx(ctx, nil); err != nil {
		log.For(ctx).Error("begin tx", logf.Any("error", err))
		return
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
		if stmt != nil {
			_ = stmt.Close()
		}
		log.Info("CreateEx finshed",
			logf.Any("time lost", time.Since(start)/1e6))
	}()
	preURL = fmt.Sprintf(sqlCreate, e.dbName, e.table)
	log.For(ctx).Debug("preURL",
		logf.String("preURL", preURL))
	if stmt, err = tx.Prepare(preURL); err != nil {
		log.For(ctx).Error("tx prepare", logf.Any("error", err))
		return
	}
	for _, v := range data {
		_, err = stmt.Exec(
			v.DeviceId,
			v.UserId,
			v.ThingId,
			v.SourceId,
			v.Identifier,
			v.EventId,
			v.Type,
			v.Metadata,
			v.Time,
			ck.Date(v.ActionDate),
			v.ActionTime)
		if err != nil {
			log.GlobalLogger().For(ctx).Error("stmt error", logf.Error(err))
			return err
		}
	}
	err = tx.Commit()
	return err
}
func (e *EventDB) Close() error {
	e.balance.Close()
	return nil
}
