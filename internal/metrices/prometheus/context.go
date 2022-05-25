package prometheus

import "time"

func (this *RulexMetrics) MsgTrace(inout, status string) {
	this.msgInOut.WithLabelValues(inout, status).Inc()
}

func (this *RulexMetrics) MsgRecvRate(size int) {
	this.msgReceivedRate.Add(float64(size))
}

func (this *RulexMetrics) MsgSentRate(size int) {
	this.msgSentRate.Add(float64(size))
}

func (this *RulexMetrics) MsgDelay(status string, delay float64) {
	this.msgDelay.WithLabelValues(status).Observe(delay)
	this.msgDelaySum.WithLabelValues(status).Observe(delay)
}

func (this *RulexMetrics) ResourceSync(resource string, num, size int) {
	this.resourceSync.WithLabelValues(resource).Add(float64(num))
	this.resourceSyncSent.WithLabelValues(resource).Add(float64(size))
}

func (rule *RulexMetrics) RuleExecute(tenantId, status string) {
	rule.ruleExecute.WithLabelValues(tenantId, status).Inc()
}

//----------------Collect Interfaces---------------
func MsgReceived(num int) {
	rulexMetric.MsgTrace(MsgInput, StatusAll)
	rulexMetric.MsgRecvRate(num)
}

func MsgRecvFail() {
	rulexMetric.MsgTrace(MsgInput, StatusFailure)
}

func MsgSent(status string, num int) {
	// status = {"success" | "failure"}
	rulexMetric.MsgTrace(MsgOutput, status)
	rulexMetric.MsgSentRate(num)
}

func ResourceSync(resource string, num, size int) { rulexMetric.ResourceSync(resource, num, size) }

//-------------------Delay Context.
type msgContext struct {
	start_t time.Time
}

func NewMsgContext() *msgContext {
	return &msgContext{
		start_t: time.Now(),
	}
}

func (this *msgContext) Observe(err error) {
	status := StatusSuccess
	if err != nil {
		status = StatusFailure
	}
	rulexMetric.MsgDelay(status, float64(time.Since(this.start_t).Microseconds())/1000.0)
}
