package stateful

import (
	"fmt"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"testing"
)

func TestParseError(t *testing.T) {

	tests := []struct {
		expr   string
		hasErr interface{}
	}{
		// calc
		{"select a() from aaa", false},
		{"select * from /sys/iott-yvtT6s0WQw/+/thing/property/+/post", false},
	}

	for i, tt := range tests {
		t.Run("tt.name", func(t *testing.T) {
			_, err := ruleql.Parse(tt.expr)
			t.Log(fmt.Sprintf("++++[%d][%v],want[%v]+++++", i, err, tt.hasErr))
		})
	}
	t.Errorf("vvvvvvvvvvvvvv")
}