package clickhouse

import (
	"fmt"
	"github.com/tkeel-io/rule-util/pkg/log"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"runtime/debug"
	"sync"
	"testing"
)

func TestNewLoadBalanceRandom(t *testing.T) {
	db, err := sqlx.Connect("mysql", "root:123456@tcp(127.0.0.1:3306)/ac")
	if err != nil {
		log.Fatal(err.Error())
	}
	open, err := sqlx.Connect("postgres", "postgres://postgres@127.0.0.1:5432/iot?sslmode=disable")
	if err != nil {
		log.Fatal(err.Error())
	}
	servers := []*Server{{
		DB:     db,
		Name:   "mysql",
		Weight: 0,
	}, {
		DB:     open,
		Name:   "pg",
		Weight: 1,
	}}
	lb := NewLoadBalanceRandom(servers)
	debug.SetMaxThreads(100000 / 2)
	var s sync.WaitGroup
	s.Add(100000 / 2)
	nilCount := 0
	m := make(map[string]int)
	m["pg"] = 0
	m["mysql"] = 0
	for i := 0; i < 100000 / 2; i++ {
		go func() {
			c := lb.Select(nil)
			if c == nil {
				nilCount++
			}else {
				if c.Name == "pg" {
					m["pg"]++
				}else {
					m["mysql"]++
				}
			}
			s.Done()
		}()
		//time.Sleep(time.Second)
	}
	//fmt.Println(count)
	s.Wait()

	fmt.Println("================>", nilCount)
	fmt.Println("================>", m)

	lb.mutex.Lock()
	fmt.Println("活跃数量", len(lb.servers), "不活跃数量", len(lb.notActiveServers))
	fmt.Println("活跃的")
	for _, server := range lb.servers {
		fmt.Println(server.Name, "---", server.DB.DriverName())
	}
	fmt.Println("不活跃的")
	for _, server := range lb.notActiveServers {
		fmt.Println(server.Name, "---", server.DB.DriverName())
	}
	lb.mutex.Unlock()

	for _, server := range lb.servers {
		if server.DB.Ping() == nil {
			fmt.Println(server.Name)
		}
	}

	//time.Sleep(100 * time.Second)
}
