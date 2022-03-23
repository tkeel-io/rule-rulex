package utils

import (
	"fmt"
	"sync"
	"testing"
	"time"

	log "github.com/golang/glog"
)


var N = 5000000
func TestTimer(t *testing.T) {
	timer := NewTimer(N)
	tds := make([]*TimerData, N)
	for i := 0; i < N; i++ {
		tds[i] = timer.Add(time.Duration(i)*time.Second+5*time.Minute, nil)
	}
	printTimer(timer)
	for i := 0; i < N; i++ {
		log.Infof("td: %s, %s, %d", tds[i].Key, tds[i].ExpireString(), tds[i].index)
		timer.Del(tds[i])
	}
	printTimer(timer)
	for i := 0; i < N; i++ {
		tds[i] = timer.Add(time.Duration(i)*time.Second+5*time.Minute, nil)
	}
	printTimer(timer)
	for i := 0; i < N; i++ {
		timer.Del(tds[i])
	}
	printTimer(timer)
	timer.Add(time.Second, nil)
	time.Sleep(time.Second * 2)
	if len(timer.timers) != 0 {
		t.FailNow()
	}
}

func printTimer(timer *Timer) {
	log.Infof("----------timers: %d ----------", len(timer.timers))
	for i := 0; i < len(timer.timers); i++ {
		log.Infof("timer: %s, %s, index: %d", timer.timers[i].Key, timer.timers[i].ExpireString(), timer.timers[i].index)
	}
	log.Infof("--------------------")
}


func TestTimerAll(t *testing.T) {
	timer := NewTimer(N)
	tds := make([]*TimerData, N)
	wg := &sync.WaitGroup{}
	t1 := time.Now()
	for i := 0; i < N; i++ {
		wg.Add(1)
		tds[i] = timer.Add(time.Duration(i%10000)*time.Microsecond, func() {
			wg.Done()
		})
	}
	fmt.Println(time.Duration(N)*time.Microsecond)
	fmt.Println(time.Now().Sub(t1))
	wg.Wait()
	fmt.Println(time.Now().Sub(t1))
}
