package wkmesh

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"go.uber.org/zap"
)

type WKMesh struct {
	Discovery *Discovery   // 服务发现模块
	LOG       *wklog.WKLog // 日志记录器
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

	// 添加优雅关闭处理
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		mesh.Discovery.Shutdown()
		time.Sleep(1 * time.Second) // 等待下线通知发送
		os.Exit(0)
	}()

	return mesh
}
