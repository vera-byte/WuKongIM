package discovery

import (
	"fmt"
	"net"
	"time"
)

// NodeStatus 表示节点状态
type NodeStatus int

const (
	StatusUnknown NodeStatus = iota
	StatusOnline
	StatusOffline
)

// Node 表示发现的网络节点
type Node struct {
	Instance   string     // 节点实例名
	IP         net.IP     // 节点IP地址
	Port       int        // 节点端口号
	LastSeen   time.Time  // 最后可见时间
	Status     NodeStatus // 节点状态
	ServiceTxt []string   // 服务TXT记录
	IsLocal    bool       // 是否为本机节点
}

// String 返回节点的字符串表示
func (n *Node) String() string {
	return n.IP.String() + ":" + fmt.Sprint(n.Port)
}

// StatusString 返回状态的字符串表示
func (n *Node) StatusString() string {
	switch n.Status {
	case StatusOnline:
		return "在线"
	case StatusOffline:
		return "离线"
	default:
		return "未知"
	}
}
