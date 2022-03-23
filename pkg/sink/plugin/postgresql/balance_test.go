package postgresql

import (
	"fmt"
	"testing"
)

func TestNewLoadBalanceRandom(t *testing.T) {
	//count := make([]int, 4)
	//servers := make([]*Server, 0)
	//lb := NewLoadBalanceRandom(servers)
	//
	//for i := 0; i < 100000; i++ {
	//	c := lb.Select(nil)
	//	count[c.Weight]++
	//}
	//fmt.Println(count)
	src := make([]byte, 10)
	copy(src, []byte{1, 2, 3})
	fmt.Println(src)
}
