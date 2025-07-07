package wkmesh

import (
	"fmt"
	"net"
	"strings"
)

// IP转节点ID (0-1023)
func HashIPTo1024(ipStr string) (uint32, error) {

	ip := net.ParseIP(strings.TrimSpace(ipStr))
	fmt.Printf("原始输入: %s\n%s\n", ipStr, ip.String()) // 调试信息

	if ip == nil {
		return 0, fmt.Errorf("invalid IP address: %s", ipStr)
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return 0, fmt.Errorf("only IPv4 supported, got: %v", ip)
	}
	return uint32(ip4[2])<<8 | uint32(ip4[3]), nil
}
