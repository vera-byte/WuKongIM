package wkmesh

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/WuKongIM/WuKongIM/internal/server"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/mesh/discovery"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/version"
	"go.uber.org/zap"
)

type WKMesh struct {
	Log              *wklog.WKLog // 日志记录器
	Server           *server.Server
	DiscoveryManager *discovery.DiscoveryManager // 发现管理器
}

func NewMesh(s *server.Server, nanmeSpace string) *WKMesh {

	log := wklog.NewWKLog("wkmesh")

	// 创建配置
	config := discovery.NewConfig()
	config.ServiceName = fmt.Sprintf("_%s._tcp", nanmeSpace)
	config.ServicePort = 8080
	config.NameSpace = nanmeSpace
	config.Version = version.Version

	// 创建发现管理器
	manager, err := discovery.NewDiscoveryManager(*config)
	if err != nil {
		log.Fatal("创建发现管理器失败:", zap.Error(err))
	}
	manager.GetNodes()
	// 注册事件处理
	manager.RegisterEventHandler(func(node *discovery.Node, status discovery.NodeStatus) {
		isInit := s.GetClusterConfigServer().IsInitialized()
		log.Info("集群是否初始化", zap.Bool("isInit", isInit))
		if !isInit {
			log.Info("集群未初始化，跳过节点状态变更处理", zap.String("IP", node.IP.String()), zap.String("Instance", node.Instance), zap.String("Status", node.StatusString()))
			return
		}
		log.Info("节点状态变更 ", zap.String("实列IP：", node.IP.String()), zap.String("实列：", node.Instance), zap.String("状态：", node.StatusString()))
		NodeId, err := GetLastIPSegment(node.IP.String())
		if err != nil {
			log.Error("IP转换失败", zap.Error(err))
			return
		}
		log.Info("当前领导节点是", zap.Any("Leader", s.GetClusterConfigServer().LeaderId()))
		if s != nil {
			switch status {
			case discovery.StatusOnline:
				// 节点上线 加入集群
				if !node.IsLocal {
					log.Info("节点ID", zap.Uint16("ID", NodeId), zap.String("IP", node.IP.String()))
					err = s.GetClusterConfigServer().ProposeJoin(&types.Node{
						Id:            uint64(NodeId),
						ClusterAddr:   fmt.Sprintf("%s:%d", node.IP.String(), 11110),
						ApiServerAddr: fmt.Sprintf("http://%s:%d", node.IP.String(), 5001),
						// Join:          true,
						Online:      true,
						AllowVote:   true,
						Status:      types.NodeStatus_NodeStatusWillJoin,
						CreatedAt:   node.LastSeen.Unix(),
						LastOffline: node.LastSeen.Unix(),
						Role:        types.NodeRole_NodeRoleReplica,
					})
					if err != nil {
						log.Error("节点加入集群失败", zap.Uint16("ID", NodeId), zap.String("IP", node.IP.String()), zap.Error(err))
					}

				} else {
					log.Info("自身节点上线，跳过集群加入", zap.String("IP", node.IP.String()))
				}
			case discovery.StatusOffline:
				err = s.GetClusterConfigServer().ProposeNodeOnlineStatus(uint64(NodeId), true)
				if err != nil {
					log.Error("节点状态下线变更", zap.Uint16("ID", NodeId), zap.String("IP", node.IP.String()), zap.Error(err))
				}
			}

		}
	})
	mesh := &WKMesh{
		Log:              log,
		Server:           s,
		DiscoveryManager: manager,
	}
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
