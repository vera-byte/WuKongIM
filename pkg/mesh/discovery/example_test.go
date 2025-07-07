package discovery_test

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/mesh/discovery"
)

func Test_discovery(t testing.T) {
	// 创建配置
	config := discovery.NewConfig()
	config.ServiceName = "_myapp._tcp"
	config.ServicePort = 8080
	config.InstanceID, _ = os.Hostname()

	// 创建发现管理器
	manager, err := discovery.NewDiscoveryManager(*config)
	if err != nil {
		log.Fatalf("创建发现管理器失败: %v", err)
	}

	// 注册事件处理
	manager.RegisterEventHandler(func(node *discovery.Node, status discovery.NodeStatus) {
		log.Printf("节点状态变更: %s -> %s", node.Instance, node.StatusString())
	})

	// 启动服务
	if err := manager.Start(); err != nil {
		log.Fatalf("启动服务失败: %v", err)
	}
	defer manager.Stop()

	// 定期打印节点状态
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for range ticker.C {
			nodes := manager.GetNodes()
			config.Log.Println("\n当前节点列表:")
			for _, node := range nodes {
				config.Log.Printf("- %s (%s:%d) [%s] 最后活跃: %v\n",
					node.Instance,
					node.IP,
					node.Port,
					node.StatusString(),
					time.Since(node.LastSeen).Round(time.Second))
			}
		}
	}()

	// 等待终止信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	t.Log("正在关闭...")
}
