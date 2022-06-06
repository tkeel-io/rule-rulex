package prometheus

var (
	// resource sync labels
	resSyncLabels = []string{
		"resource", // 资源类型
	}
	inoutLabels = []string{
		"inout",  // 标识message是输入还是输出
		"status", // 标识消息的处理状态
	}

	ruleExecuteLabels = []string{
		"tenant", // 租户id
		"status", // 规则执行状态
	}
)

const (
	ResourceTypeRule         = "rule"
	ResourceTypeRoute        = "route"
	ResourceTypeSubscription = "subscription"

	StatusAll     = "all"
	StatusSuccess = "success"
	StatusFailure = "failure"

	MsgInput  = "in"
	MsgOutput = "out"
)
