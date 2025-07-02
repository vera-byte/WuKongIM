package wkmesh

import (
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/version"
	"github.com/hashicorp/mdns"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
)

type UDPBody struct {
	Name      string `json:"name"`      // 节点名称
	IP        string `json:"ip"`        // 节点IP地址
	Port      string `json:"port"`      // 节点端口
	Version   string `json:"version"`   // 节点版本
	RestPort  string `json:"rest_port"` // REST端口
	Timestamp int64  `json:"timestamp"` // 时间戳
	Announce  bool   `json:"announce"`  // 是否为上线通知
	Shutdown  bool   `json:"shutdown"`  // 是否为下线通知
}

type NodeStatus string

const (
	NodeStatusOnline  NodeStatus = "online"  // 在线状态
	NodeStatusOffline NodeStatus = "offline" // 离线状态
)

// NodeInfo 节点信息结构
type NodeInfo struct {
	NodeId    uint32        `json:"node_id"`
	Name      string        `json:"name"`
	IP        string        `json:"ip"`
	Port      string        `json:"port"`
	Version   string        `json:"version"`
	LastSeen  time.Time     `json:"-"`
	Reachable bool          `json:"reachable"`
	Latency   time.Duration `json:"latency"`
	Status    NodeStatus    `json:"status"` // 状态: "online", "offline"
}

// Listener 节点监听器接口
type Listener interface {
	OnNodeUpdate(node NodeInfo)
	OnNodeDelete(name string)
}

// Discovery 服务发现核心结构
type Discovery struct {
	SelfName     string
	SelfPort     string
	Version      string
	Log          *wklog.WKLog
	nodes        map[string]NodeInfo
	mu           sync.RWMutex
	listeners    []Listener
	shutdown     bool // 关闭标志
	hasAnnounced bool // 是否已发送上线通知
	isK8sEnv     bool // 是否在 Kubernetes 环境中
	k8sClient    *kubernetes.Clientset
	mdnsServer   *mdns.Server // mDNS服务器实例
	serviceName  string       // mDNS服务名称
}

func newDiscovery(selfName, selfPort string) *Discovery {
	d := &Discovery{
		Log:         wklog.NewWKLog("WKMesh.Discovery"),
		SelfName:    selfName,
		SelfPort:    selfPort,
		Version:     version.Version,
		nodes:       make(map[string]NodeInfo),
		listeners:   make([]Listener, 0),
		isK8sEnv:    os.Getenv("KUBERNETES_SERVICE_HOST") != "",
		serviceName: getEnv("MDNS_SERVICE_NAME", "_wukongim._tcp"),
	}

	// 初始化自身节点信息
	selfIP := GetLocalIP()
	nodeId, _ := HashIPTo1024(selfIP)
	selfNode := NodeInfo{
		NodeId:    nodeId,
		Name:      selfName,
		IP:        selfIP,
		Port:      selfPort,
		Version:   version.Version,
		LastSeen:  time.Now(),
		Reachable: true,
		Latency:   0,
		Status:    NodeStatusOnline,
	}
	d.nodes[d.SelfName] = selfNode

	d.Log.Info("节点初始化完成",
		zap.String("name", selfName),
		zap.String("ip", selfIP),
		zap.String("port", selfPort),
		zap.Bool("k8s", d.isK8sEnv),
	)

	// 启动核心协程
	go d.cleanupLoop()

	// Kubernetes 环境特殊处理
	if d.isK8sEnv {
		d.initK8s()
	} else {
		// 非 Kubernetes 环境使用mDNS
		go d.setupMDNS()
	}

	// 延迟发送上线通知
	go func() {
		time.Sleep(1 * time.Second)
		d.Announce()
	}()

	return d
}

// 添加或更新节点
func (d *Discovery) addOrUpdateNode(node NodeInfo) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 如果节点不存在或信息有变化，则更新
	existing, exists := d.nodes[node.Name]
	if !exists || existing.IP != node.IP || existing.Port != node.Port || existing.Status != node.Status {
		if exists {
			d.Log.Debug("更新节点信息",
				zap.String("name", node.Name),
				zap.Any("old", existing),
				zap.Any("new", node),
			)
		} else {
			d.Log.Info("发现新节点",
				zap.String("name", node.Name),
				zap.String("ip", node.IP),
				zap.String("port", node.Port),
			)
		}
		d.nodes[node.Name] = node

		// 通知监听器
		for _, l := range d.listeners {
			go l.OnNodeUpdate(node)
		}
	}
}

// 删除节点
func (d *Discovery) deleteNode(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.nodes[name]; exists {
		delete(d.nodes, name)
		d.Log.Info("删除节点", zap.String("name", name))

		// 通知监听器
		for _, l := range d.listeners {
			go l.OnNodeDelete(name)
		}
	}
}

// 单播广播循环
func (d *Discovery) unicastLoop() {
	d.Log.Info("启动单播广播循环", zap.String("interval", "5s"))

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for !d.shutdown {
		<-ticker.C
		// 获取所有已知节点
		nodes := d.GetAllNodes()

		// 向所有节点发送更新
		for _, node := range nodes {
			if node.Name == d.SelfName {
				continue // 跳过自身
			}

			msg := &UDPBody{
				Name:      d.SelfName,
				IP:        GetLocalIP(),
				Port:      d.SelfPort,
				Version:   d.Version,
				RestPort:  getEnv("REST_PORT", "11110"),
				Timestamp: time.Now().UnixMilli(),
			}

			if err := d.sendUDPMessage(node.IP, 11110, msg); err != nil {
				d.Log.Debug("单播发送失败",
					zap.String("target", node.IP),
					zap.Error(err),
				)
			}
		}

		// 更新自身节点最后可见时间
		d.mu.Lock()
		if node, ok := d.nodes[d.SelfName]; ok {
			node.LastSeen = time.Now()
			d.nodes[d.SelfName] = node
		}
		d.mu.Unlock()
	}
}

// 发送UDP消息
func (d *Discovery) sendUDPMessage(ip string, port int, msg *UDPBody) error {
	addr := fmt.Sprintf("%s:%d", ip, port)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("解析UDP地址失败: %w", err)
	}

	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return fmt.Errorf("建立UDP连接失败: %w", err)
	}
	defer conn.Close()

	b, _ := json.Marshal(msg)
	if _, err := conn.Write(b); err != nil {
		return fmt.Errorf("发送消息失败: %w", err)
	}

	return nil
}

// Announce 发送上线通知
func (d *Discovery) Announce() {
	if d.hasAnnounced || d.shutdown {
		return
	}

	d.Log.Info("发送上线通知")

	if d.isK8sEnv {
		// 向所有已知节点发送上线通知
		nodes := d.GetAllNodes()
		for _, node := range nodes {
			if node.Name == d.SelfName {
				continue
			}

			msg := &UDPBody{
				Name:      d.SelfName,
				IP:        GetLocalIP(),
				Port:      d.SelfPort,
				Version:   d.Version,
				Announce:  true,
				Timestamp: time.Now().UnixMilli(),
			}

			if err := d.sendUDPMessage(node.IP, 11110, msg); err != nil {
				d.Log.Warn("上线通知发送失败",
					zap.String("target", node.IP),
					zap.Error(err),
				)
			}
		}
	} else {
		// 对于mDNS，服务发布已经处理了"上线通知"
		d.Log.Info("mDNS服务已发布，上线通知已完成")
	}

	d.hasAnnounced = true
}

// Shutdown 节点关闭时调用，通知其他节点自己即将下线
func (d *Discovery) Shutdown() {
	if d.shutdown {
		return
	}
	d.shutdown = true

	d.Log.Info("节点正在关闭，发送下线通知")

	if d.isK8sEnv {
		// 向所有已知节点发送下线通知
		nodes := d.GetAllNodes()
		for _, node := range nodes {
			if node.Name == d.SelfName {
				continue
			}

			msg := &UDPBody{
				Name:      d.SelfName,
				IP:        GetLocalIP(),
				Port:      d.SelfPort,
				Version:   d.Version,
				Shutdown:  true,
				Timestamp: time.Now().UnixMilli(),
			}

			if err := d.sendUDPMessage(node.IP, 11110, msg); err != nil {
				d.Log.Warn("下线通知发送失败",
					zap.String("target", node.IP),
					zap.Error(err),
				)
			}
		}
	} else {
		// 关闭mDNS服务器
		if d.mdnsServer != nil {
			d.mdnsServer.Shutdown()
			d.Log.Info("mDNS服务已关闭")
		}
	}

	d.Log.Info("下线通知已发送")

	// 更新自身状态为下线
	d.mu.Lock()
	if node, ok := d.nodes[d.SelfName]; ok {
		node.Status = NodeStatusOffline
		d.nodes[d.SelfName] = node
	}
	d.mu.Unlock()

	// 等待一小段时间确保消息发送
	time.Sleep(500 * time.Millisecond)
}

// 清理过期节点
func (d *Discovery) cleanupLoop() {
	d.Log.Info("启动节点清理循环", zap.String("interval", "10s"))

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for !d.shutdown {
		<-ticker.C
		now := time.Now()
		removed := []string{}

		d.mu.Lock()
		for name, node := range d.nodes {
			if name == d.SelfName {
				continue // 忽略自身
			}

			if node.Status == NodeStatusOffline || now.Sub(node.LastSeen) > 30*time.Second {
				removed = append(removed, name)
			}
		}
		d.mu.Unlock()

		// 删除过期节点
		for _, name := range removed {
			d.deleteNode(name)
		}
	}
	d.Log.Info("清理循环退出")
}

// 注册节点监听器
func (d *Discovery) RegisterListener(l Listener) {
	d.listeners = append(d.listeners, l)
	d.Log.Debug("注册节点监听器", zap.Int("count", len(d.listeners)))
}

// 添加静态节点
func (d *Discovery) AddStaticNode(ip, port string) {
	reachable, latency := testRESTPing(ip, getEnv("REST_PORT", "11110"))
	name := fmt.Sprintf("static-%s:%s", ip, port)
	nodeId, _ := HashIPTo1024(ip)

	node := NodeInfo{
		NodeId:    nodeId,
		Name:      name,
		IP:        ip,
		Port:      port,
		Version:   "manual",
		LastSeen:  time.Now(),
		Reachable: reachable,
		Latency:   latency,
		Status:    NodeStatusOnline,
	}

	d.addOrUpdateNode(node)
	d.Log.Info("添加静态节点", zap.String("ip", ip), zap.String("port", port))
}

// 获取所有节点列表
func (d *Discovery) ListNodes() []NodeInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()

	list := make([]NodeInfo, 0, len(d.nodes))
	for _, n := range d.nodes {
		list = append(list, n)
	}
	return list
}

// 获取所有节点副本
func (d *Discovery) GetAllNodes() map[string]NodeInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()

	nodesCopy := make(map[string]NodeInfo, len(d.nodes))
	maps.Copy(nodesCopy, d.nodes)
	return nodesCopy
}

// 获取本地IP地址
func GetLocalIP() string {
	if ip := os.Getenv("POD_IP"); ip != "" {
		return ip
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return "127.0.0.1"
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		// 优先 eth0/ens33
		if iface.Name != "eth0" && iface.Name != "ens33" {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			switch v := addr.(type) {
			case *net.IPNet:
				if ip := v.IP.To4(); ip != nil {
					return ip.String()
				}
			case *net.IPAddr:
				if ip := v.IP.To4(); ip != nil {
					return ip.String()
				}
			}
		}
	}

	// fallback: 任意非回环地址
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			switch v := addr.(type) {
			case *net.IPNet:
				if ip := v.IP.To4(); ip != nil {
					return ip.String()
				}
			case *net.IPAddr:
				if ip := v.IP.To4(); ip != nil {
					return ip.String()
				}
			}
		}
	}

	return "127.0.0.1"
}

// 测试节点REST可达性
func testRESTPing(ip, port string) (bool, time.Duration) {
	scheme := getEnv("REST_SCHEME", "http")
	path := getEnv("HEALTH_CHECK_PATH", "/health")
	url := fmt.Sprintf("%s://%s:%s%s", scheme, ip, port, path)
	client := &http.Client{Timeout: 2 * time.Second}

	start := time.Now()
	resp, err := client.Get(url)
	if err != nil {
		return false, 0
	}
	defer resp.Body.Close()

	// 仅当返回200时认为成功
	if resp.StatusCode != http.StatusOK {
		return false, 0
	}

	return true, time.Since(start)
}

// IP转节点ID (0-1023)
func HashIPTo1024(ipStr string) (uint32, error) {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return 0, fmt.Errorf("invalid IP address: %s", ipStr)
	}
	ip = ip.To4()
	if ip == nil {
		return 0, fmt.Errorf("only IPv4 supported")
	}
	// 使用IP地址的最后两个字节生成节点ID
	return uint32(ip[2])<<8 | uint32(ip[3]), nil
}

// 获取环境变量，如果不存在则返回默认值
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
