package wkmesh

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/server"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"go.uber.org/zap"
)

type WKMesh struct {
	Discovery *Discovery   // 服务发现模块
	LOG       *wklog.WKLog // 日志记录器
	Server    *server.Server
}

// MyListener 实现 Listener 接口，处理节点事件
type MyListener struct {
	LOG    *wklog.WKLog // 日志记录器
	WKMesh *WKMesh
}

func (l *MyListener) OnNodeUpdate(node NodeInfo) {
	eventType := "节点更新"
	if node.Status == NodeStatusOnline {
		eventType = "节点上线"
	} else if node.Status == NodeStatusOffline {
		eventType = "节点下线"
	}

	l.LOG.Info(eventType,
		zap.String("name", node.Name),
		zap.Uint32("nodeId", node.NodeId),
		zap.String("ip", node.IP),
		zap.String("port", node.Port),
		zap.String("version", node.Version),
		zap.Duration("latency", node.Latency),
		zap.String("status", string(node.Status)),
	)

	if l.WKMesh.Server != nil {
		// n := &pb.Node{
		// 	Id:            uint64(node.NodeId),
		// 	ClusterAddr:   fmt.Sprintf("%s:%s", node.IP, node.Port),
		// 	ApiServerAddr: fmt.Sprintf("%s:%s", node.IP, getEnv("REST_PORT", "11110")),
		// 	Online:        node.Status == NodeStatusOnline,
		// }

		// if node.Status == NodeStatusOnline {
		// 	l.WKMesh.Server.GetClusterConfigServer().ProposeJoin(n)
		// } else {
		// 	l.WKMesh.Server.GetClusterConfigServer().ProposeLeave(n)
		// }
	}

	// 输出所有节点
	nodes := l.WKMesh.Discovery.ListNodes()
	l.LOG.Info("当前所有节点",
		zap.Int("count", len(nodes)),
	)
}

func (l *MyListener) OnNodeDelete(name string) {
	l.LOG.Info("节点已删除", zap.String("name", name))
	// 输出所有节点
	nodes := l.WKMesh.Discovery.ListNodes()
	l.LOG.Info("当前剩余节点",
		zap.Int("count", len(nodes)),
	)
}

func (m *WKMesh) WithSetServer(server *server.Server) {
	m.Server = server
	opts := server.GetClusterConfigServer().Options()
	m.LOG.Info("正在添加静态节点", zap.Any("nodes", opts.InitNodes))
	for _, n := range opts.InitNodes {
		m.Discovery.AddStaticNode(n, "11110")
	}
}

// NewMesh 创建并初始化WKMesh实例
func NewMesh() *WKMesh {
	log := wklog.NewWKLog("WKMesh")
	log.Info("Starting WuKongIM Mesh")

	// 获取本地IP和生成节点名
	localIP := GetLocalIP()
	name := fmt.Sprintf("node-%s", localIP)

	// 创建发现模块
	discovery := newDiscovery(name, "11110")
	if discovery == nil {
		log.Error("发现模块创建失败")
		return nil
	}
	log.Info("发现模块创建成功", zap.String("node", name))

	mesh := &WKMesh{
		Discovery: discovery,
		LOG:       log,
	}
	mesh.Discovery.RegisterListener(&MyListener{
		LOG:    mesh.Discovery.Log,
		WKMesh: mesh,
	})

	// 添加优雅关闭处理
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
		<-c
		mesh.LOG.Info("接收到关闭信号，开始优雅关闭")
		mesh.Discovery.Shutdown()
		time.Sleep(2 * time.Second) // 等待服务注销完成
		os.Exit(0)
	}()

	return mesh
}

// 获取环境变量，如果不存在则返回默认值
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
