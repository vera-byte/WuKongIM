package discovery

import (
	"log"
	"net"
	"os"
	"time"
)

// 自定义日志记录器
var logger *log.Logger

func init() {
	// 创建日志记录器，可设置为 ioutil.Discard 关闭日志
	logger = log.New(os.Stdout, "", log.LstdFlags)
	// 关闭日志：logger = log.New(ioutil.Discard, "", 0)
}

// Config 包含发现服务的配置
type Config struct {
	ServiceName  string         // 服务类型，例如 "_mdns-demo._tcp"
	ServicePort  int            // 服务端口
	ScanInterval time.Duration  // 扫描间隔，默认2秒
	NodeTimeout  time.Duration  // 节点超时时间，默认5秒
	AnnounceInt  time.Duration  // 宣告间隔，默认3秒
	InstanceID   string         // 实例ID，默认为主机名+端口
	Iface        *net.Interface // 网络接口，如果为nil则自动选择
	LocalIP      net.IP         // 本地IP，如果为nil则自动选择
	Version      string         // 版本号，默认为 "1.0"
	NameSpace    string         // 名称空间，用于标识服务的唯一性
	Log          *log.Logger    // 日志记录器，默认为自定义日志记录器
}

// NewConfig 创建默认配置
func NewConfig() *Config {
	return &Config{
		ServiceName:  "_mdns-demo._tcp",
		ServicePort:  8080,
		ScanInterval: 2 * time.Second,
		NodeTimeout:  5 * time.Second,
		AnnounceInt:  3 * time.Second,
		Version:      "1.0",
		NameSpace:    "default",
		InstanceID:   "", // 默认为空，后续会自动设置为主机名+端口
		Log:          logger,
	}
}
