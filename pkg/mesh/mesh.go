package wkmesh

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/WuKongIM/WuKongIM/internal/server"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/version"
	"github.com/vera-byte/vera-mesh/discovery"
	"go.uber.org/zap"
)

type WKMesh struct {
	Log    *wklog.WKLog // 日志记录器
	Server *server.Server
}

func NewMesh(s *server.Server) *WKMesh {

	log := wklog.NewWKLog("wkmesh")
	mesh := &WKMesh{
		Log:    log,
		Server: s,
	}
	// 创建配置
	config := discovery.NewConfig()
	config.ServiceName = "_myapp._tcp"
	config.ServicePort = 8080
	config.Version = version.Version

	// 创建发现管理器
	manager, err := discovery.NewDiscoveryManager(*config)
	if err != nil {
		log.Fatal("创建发现管理器失败:", zap.Error(err))
	}
	// 注册事件处理
	manager.RegisterEventHandler(func(node *discovery.Node, status discovery.NodeStatus) {
		log.Info("节点状态变更 ", zap.String("实列：", node.Instance), zap.String("状态：", node.StatusString()))
		if s != nil {
			switch status {
			case discovery.StatusOnline:
				// 节点上线 加入集群
				if !node.IsLocal {
					NodeId, _ := HashIPTo1024(node.IP.String())
					log.Info("节点ID", zap.Uint32("ID", NodeId), zap.String("IP", node.IP.String()))
					s.GetClusterConfigServer().ProposeJoin(&types.Node{
						Id: uint64(NodeId),
						// ClusterAddr:   fmt.Sprintf("%s:%d", node.IP.String(), 11110),
						// ApiServerAddr: fmt.Sprintf("%s:%d", node.IP.String(), 5001),
						Join:        true,
						Online:      true,
						AllowVote:   true,
						Status:      types.NodeStatus_NodeStatusWillJoin,
						CreatedAt:   node.LastSeen.Unix(),
						LastOffline: node.LastSeen.Unix(),
						Role:        types.NodeRole_NodeRoleReplica,
					})
				} else {
					log.Info("本地节点上线，跳过集群加入", zap.String("IP", node.IP.String()))
				}

			}
		}
	})
	// 启动服务
	if err := manager.Start(); err != nil {
		log.Fatal("启动服务失败:", zap.Error(err))
	}
	defer manager.Stop()

	// 等待终止信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Info("正在关闭...")
	return mesh
}
