package redis

import (
	"context"
	//"database/sql"
	"errors"
	"fmt"
	gv1 "github.com/tkeel-io/rule-util/gateway/v1"
	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/go-redis/redis"
	//"github.com/jmoiron/sqlx"
	"net/url"
	"strconv"
	//"github.com/garyburd/redigo/redis"
	//ck "github.com/mailru/go-clickhouse"
	//ck "github.com/ClickHouse/clickhouse-go"
	"time"
)

//property model
type EventDB struct {
	balance LoadBalance
	dbName  string
	table   string
}

func NewEventDB( urls []string) (event types.DBImpl, err error) {
	var (
		db *redis.Client
	)
	servers := make([]*Server, len(urls))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for k, v := range urls {//redis://[:password]@host:port/db
		u,err :=url.Parse(v)
		if err != nil {
			log.For(ctx).Error("open redis", logf.Any("error", err))
			return nil, err
		}
		p,_ :=u.User.Password()
		DB,_ :=strconv.Atoi(u.Path)
		db =redis.NewClient(&redis.Options{
			Addr : u.Host,
			Password: p,
			DB: DB,
			DialTimeout: 30 * time.Second,

		})

		if _,err = db.Ping().Result(); err != nil {
			log.For(ctx).Error("ping redis", logf.Any("error", err))
			return nil,err
		}
		//_, err := db.Exec(CLICKHOUSE_IOT_DEVICE_EVENT_DATA)
		//if err != nil {
		//	log.For(ctx).Warn(err.Error(), logf.String("table", table))
		//}
		//db.SetConnMaxLifetime(30 * time.Second)
		//db.SetMaxOpenConns(5)
		servers[k] = &Server{db, v, 1}
	}
	event = &EventDB{
		balance: NewLoadBalanceRandom(servers),
	}
	return event, nil
}
func (e *EventDB) BatchCreate(ctx context.Context, sqlCreate string, values interface{}) (err error) {
	var (
		tx     redis.Pipeliner
		preURL string
		ok     bool
		data   []*gv1.EventThingStore
	)
	if data, ok = values.([]*gv1.EventThingStore); !ok {
		fmt.Println(data)
		return errors.New("values type error")
	}
	start := time.Now()
	server := e.balance.Select([]*redis.Client{})
	if server == nil {
		return fmt.Errorf("get database failed, can't insert")
	}
	tx =server.DB.TxPipeline()

	//if tx, err = s; err != nil {
	//	log.For(ctx).Error("begin tx", logf.Any("error", err))
	//	return
	//}


	defer func() {
		log.Info("CreateEx finshed",
			logf.Any("time lost", time.Since(start)/1e6))
	}()
	//preURL = fmt.Sprintf(sqlCreate, e.key, e.value)
	log.For(ctx).Debug("preURL",
		logf.String("preURL", preURL))
	if _, err = tx.Exec(); err != nil {
		log.For(ctx).Error("tx prepare", logf.Any("error", err))
		return
	}
	//for _, v := range data {

		//if err != nil {
			//log.GlobalLogger().For(ctx).Error("stmt error", logf.Error(err))
			//return err
		//}
	//}

	return err
}
func (e *EventDB) Close() error {
	e.balance.Close()
	return nil
}
