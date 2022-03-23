package stateful

import (
	"context"
	"fmt"
	"github.com/tkeel-io/rule-rulex/internal/utils"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"github.com/tkeel-io/rule-util/stream"
	"reflect"
	"testing"
	"time"
)

func Test_newWindowState(t *testing.T) {
	ctx := context.Background()
	timer := utils.NewTimer(1024)
	state := PubMessage.(stream.Message)

	expr, _ := ruleql.Parse(sql)
	window := ruleql.GetWindow(expr)

	got := newWindowState(ctx, "", func(st *WindowState){
		fmt.Println(st)
	}, window, timer,
		func(ctx context.Context, state interface{}) error {
			fmt.Println("[+]", reflect.TypeOf(state), state)
			return nil
		})
	got.SetState(state)
	fmt.Println(got)

	time.Sleep(20 * time.Second)
}
