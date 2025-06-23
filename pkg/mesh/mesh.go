package wkmesh

import (
	"fmt"

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

	return &WKMesh{
		Discovery: discovery,
		LOG:       log,
	}
}
