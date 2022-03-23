package prometheus

import (
	"fmt"

	logf "github.com/tkeel-io/rule-util/pkg/logfield"
)

const UndefineValue = "undefine"

type rulexFmt struct {
	NodeName string
}

func NewrulexFmt(name string) *rulexFmt {
	return &rulexFmt{NodeName: name}
}

func (this *rulexFmt) Key() string {
	return fmt.Sprintf("/exporters/rulex/%s", this.NodeName)
}

func (this *rulexFmt) Value(args ...string) string {
	if len(args) == 2 {
		addr := fmt.Sprintf("%s:%s", args[0], args[1])
		return fmt.Sprintf("{\"addr\":\"%s\"}", addr)
	}
	logger.Bg().Error("register prometheus exporter failure.", logf.Any("args", args))
	return UndefineValue
}
