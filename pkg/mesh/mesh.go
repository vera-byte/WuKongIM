package wkmesh

import (
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
)

type WKMesh struct {
	Discovery *Discovery // 服务发现模块
	LOG       wklog.Log  // 日志记录器

}

// NewMesh 创建一个新的 WKMesh 实例
func NewMesh() *WKMesh {
	log := wklog.NewWKLog("wkmesh")
	log.Info("Starting WuKongIM Mesh")
	localIP := GetLocalIP()
	name := fmt.Sprintf("node-%s", localIP)
	discovery := newDiscovery(name, "11110")
	if discovery == nil {
		log.Error("Failed to create discovery instance")
		return nil
	} else {
		log.Info("Discovery instance created successfully")
	}
	return &WKMesh{
		Discovery: discovery,
		LOG:       log,
	}
}
