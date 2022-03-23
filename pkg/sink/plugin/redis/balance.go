package redis

import (
	"github.com/go-redis/redis"
	"github.com/valyala/fastrand"
	//"google.golang.org/genproto/googleapis/cloud/redis/v1"
)

type Server struct {
	DB     *redis.Client
	Name   string
	Weight int
	//主机是否在线
	//Online bool
}
type LoadBalance interface {
	//选择一个后端Server
	//参数remove是需要排除选择的后端Server
	Select(remove []*redis.Client) *Server
	//更新可用Server列表
	UpdateServers(servers []*Server)
	Close()
}

type LoadBalanceRandom struct {
	servers []*Server
}

func NewLoadBalanceRandom(servers []*Server) *LoadBalanceRandom {
	new := &LoadBalanceRandom{}
	new.UpdateServers(servers)
	return new
}
func (l *LoadBalanceRandom) Close() {
	for _, v := range l.servers {
		v.DB.Close()
	}
}

//系统运行过程中，后端可用Server会更新
func (l *LoadBalanceRandom) UpdateServers(servers []*Server) {
	newServers := make([]*Server, 0)
	for _, e := range servers {
		newServers = append(newServers, e)
		//if e.Online == true {
		//	newServers = append(newServers, e)
		//}
	}
	l.servers = newServers
}

//选择一个后端Server
func (l *LoadBalanceRandom) Select(remove []*redis.Client) *Server {
	curServer := l.servers
	if len(curServer) == 0 {
		return nil
	}

	if len(remove) == 0 {
		for i := 0; i < 3; i++ {
			id := fastrand.Uint32n(uint32(len(curServer)))
			server := curServer[id]
			if _, err := server.DB.Ping().Result(); err == nil {
				return server
			}
		}
		return nil
	} else {
		tmpServers := make([]*Server, 0)
		for _, s := range curServer {
			isFind := false
			for _, v := range remove {
				if s.DB == v {
					isFind = true
					break
				}
			}
			if isFind == false {
				tmpServers = append(tmpServers, s)
			}
		}
		if len(tmpServers) == 0 {
			return nil
		}
		for i := 0; i < 3; i++ {
			id := fastrand.Uint32n(uint32(len(curServer)))
			server := tmpServers[id]
			if _, err := server.DB.Ping().Result(); err == nil {
				return server
			}
		}
		return nil
	}
}

func (l *LoadBalanceRandom) String() string {
	return "Random"
}
