package grpc

import (
	"context"
	"fmt"
	"github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/pkg/registry"
	"github.com/tkeel-io/rule-util/pkg/registry/etcd3"
	"os"
	"sync"
	"testing"
	"time"
)

type client struct {
	clis  []v1.PubSubClient
	count int
	idx   int
}

func (c *client) get() v1.PubSubClient {
	c.idx++
	return c.clis[c.idx%c.count]
}

var cli client

func TestMain(m *testing.M) {
	clis := make([]v1.PubSubClient, 0)
	discovery, err0 := etcdv3.New(&registry.Config{
		Endpoints: []string{
			"192.168.0.12:2379",
		},
	})
	if err0 != nil {
		panic(err0)
	}
	for i := 0; i < 1; i++ {
		clis = append(clis, newPubSubClient(discovery))
	}
	cli = client{
		clis:  clis,
		count: len(clis),
		idx:   0,
	}
	os.Exit(m.Run())
}

// BenchmarkPubsubListSubscriberFromMeta-8   	  200000	      7654 ns/op
func BenchmarkPubsubListSubscriberFromMeta(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		go func() {
			ret, err := cli.get().ListSubscriptions(ctx,
				&v1.SubscriptionRequest{
					Assert: &v1.Assert{},
					Subscription: &v1.Subscription{
						UserId:      "admin",
						Id:          "aaa",
						TopicFilter: fmt.Sprintf("aaa-%d", i),
					},
				})
			_, _ = ret, err
		}()
	}
}

// 10W => 5.064809277s
func TestPubsubListSubscriberFromMeta(t *testing.T) {
	ctx := context.Background()
	tt := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < 1*100000; i++ {
		wg.Add(1)
		go func() {
			_, err := cli.get().ListSubscriptions(ctx,
				&v1.SubscriptionRequest{
					Assert: &v1.Assert{},
					Subscription: &v1.Subscription{
						UserId:      "admin",
						Id:          "aaa",
						TopicFilter: fmt.Sprintf("aaa-%d", i),
					},
				})
			if err != nil {
				fmt.Println("err", err)
			}
			wg.Done()
		}()
	}
	fmt.Println(time.Now().Sub(tt))
	wg.Wait()
	fmt.Println(time.Now().Sub(tt))
}
