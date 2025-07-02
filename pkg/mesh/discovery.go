package wkmesh

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/version"
	"github.com/hashicorp/mdns"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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

type NodeStatus string

const (
	NodeStatusOnline  NodeStatus = "online"  // 在线状态
	NodeStatusOffline NodeStatus = "offline" // 离线状态
)

// ... [其他结构体定义保持不变] ...

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
		d.Log.Info("运行在Kubernetes环境中")

		// 初始化 Kubernetes 客户端
		if err := d.initK8sClient(); err != nil {
			d.Log.Error("Kubernetes客户端初始化失败", zap.Error(err))
		} else {
			// 启动 Kubernetes 发现循环
			go d.k8sDiscoveryLoop()

			// 立即执行一次发现
			go d.discoverK8sPods()
		}

		// 启动单播循环
		go d.unicastLoop()
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

// 设置mDNS服务
func (d *Discovery) setupMDNS() {
	d.Log.Info("设置mDNS服务发现")

	// 获取本地IP
	ip := GetLocalIP()

	// 转换端口为整数
	portInt, err := strconv.Atoi(d.SelfPort)
	if err != nil {
		d.Log.Error("端口转换失败", zap.Error(err))
		return
	}

	// 创建服务信息
	restPort := getEnv("REST_PORT", "11110")
	info := []string{
		d.SelfName,                               // 节点名称
		restPort,                                 // REST端口
		strconv.FormatInt(time.Now().Unix(), 10), // 启动时间戳
	}

	// 强制使用IPv4地址
	ipAddr := net.ParseIP(ip)
	if ipAddr.To4() == nil {
		d.Log.Warn("非IPv4地址，强制使用IPv4回环地址", zap.String("ip", ip))
		ipAddr = net.ParseIP("127.0.0.1")
	}

	service, err := mdns.NewMDNSService(
		d.SelfName,
		d.serviceName,
		"",               // 域名
		"",               // 主机名
		portInt,          // 端口
		[]net.IP{ipAddr}, // IP地址（强制IPv4）
		info,             // 附加信息
	)
	if err != nil {
		d.Log.Error("创建mDNS服务失败", zap.Error(err))
		return
	}

	// 创建mDNS服务器
	server, err := mdns.NewServer(&mdns.Config{
		Zone:  service,
		Iface: getIPv4Interface(), // 仅使用IPv4接口
	})
	if err != nil {
		d.Log.Error("创建mDNS服务器失败", zap.Error(err))
		return
	}

	d.mdnsServer = server
	d.Log.Info("mDNS服务已启动",
		zap.String("name", d.SelfName),
		zap.String("ip", ip),
		zap.Int("port", portInt),
	)

	// 启动mDNS发现循环（仅IPv4）
	go d.mdnsDiscoveryLoop()
}

// mDNS服务发现循环（仅IPv4）
func (d *Discovery) mdnsDiscoveryLoop() {
	// 从环境变量获取查询间隔，默认为30秒
	interval := 30
	if val := getEnv("MDNS_QUERY_INTERVAL", ""); val != "" {
		if i, err := strconv.Atoi(val); err == nil && i > 0 {
			interval = i
		}
	}
	d.Log.Info("启动mDNS发现循环", zap.Int("interval_seconds", interval))

	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for !d.shutdown {
		// 创建缓冲通道处理mDNS条目
		entriesCh := make(chan *mdns.ServiceEntry, 16)
		doneCh := make(chan struct{})

		// 处理条目的协程
		go func() {
			defer close(doneCh)
			for entry := range entriesCh {
				d.handleMDNSEntry(entry)
			}
		}()

		// 执行mDNS查询 - 强制使用IPv4
		params := &mdns.QueryParam{
			Service:   d.serviceName,
			Domain:    "local",
			Timeout:   5 * time.Second,
			Entries:   entriesCh,
			Interface: getIPv4Interface(), // 仅查询IPv4接口
		}

		// 执行查询
		err := mdns.Query(params)
		if err != nil {
			d.Log.Warn("mDNS查询失败", zap.Error(err))
		}

		// 关闭通道并等待处理完成
		close(entriesCh)
		<-doneCh

		// 等待下一次查询
		select {
		case <-ticker.C:
			// 继续下一次查询
		case <-d.shutdownChan():
			// 收到关闭信号
			return
		}
	}
	d.Log.Info("mDNS发现循环退出")
}

// 获取关闭信号通道
func (d *Discovery) shutdownChan() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		d.mu.RLock()
		defer d.mu.RUnlock()
		for !d.shutdown {
			time.Sleep(100 * time.Millisecond)
		}
		close(ch)
	}()
	return ch
}

// 处理mDNS发现结果
func (d *Discovery) handleMDNSEntry(entry *mdns.ServiceEntry) {
	// 忽略自身节点
	if entry.Name == d.SelfName+"."+d.serviceName+".local." {
		return
	}

	// 提取IP地址 - 优先IPv4
	ip := ""
	if len(entry.AddrV4) > 0 {
		ip = entry.AddrV4.String()
	} else if entry.Addr != nil && entry.Addr.To4() != nil {
		ip = entry.Addr.String()
	} else if len(entry.AddrV6) > 0 {
		// 如果没有IPv4地址，则使用IPv6
		ip = entry.AddrV6.String()
	}

	if ip == "" {
		d.Log.Warn("mDNS条目缺少IP地址", zap.String("name", entry.Name))
		return
	}

	// 节点名称从info字段获取
	name := ""
	restPort := getEnv("REST_PORT", "11110")
	if len(entry.InfoFields) > 0 {
		name = entry.InfoFields[0]
	}
	if name == "" {
		// 如果info字段没有，则从服务名解析
		name = entry.Name
		// 去除服务后缀
		suffix := "." + d.serviceName + ".local."
		if len(name) > len(suffix) && name[len(name)-len(suffix):] == suffix {
			name = name[:len(name)-len(suffix)]
		}
	}

	// 获取REST端口
	if len(entry.InfoFields) > 1 {
		restPort = entry.InfoFields[1]
	}

	// 测试节点可达性
	reachable, latency := testRESTPing(ip, restPort)

	nodeId, _ := HashIPTo1024(ip)
	node := NodeInfo{
		NodeId:    nodeId,
		Name:      name,
		IP:        ip,
		Port:      strconv.Itoa(entry.Port),
		Version:   version.Version, // 假设版本一致
		LastSeen:  time.Now(),
		Reachable: reachable,
		Latency:   latency,
		Status:    NodeStatusOnline,
	}

	d.addOrUpdateNode(node)
}

// 获取IPv4网络接口
func getIPv4Interface() *net.Interface {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}

	for _, iface := range ifaces {
		// 跳过回环和未启用的接口
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			// 检查是否为IPv4地址
			if ip.To4() != nil {
				return &iface
			}
		}
	}

	return nil
}

// 初始化 Kubernetes 客户端
func (d *Discovery) initK8sClient() error {
	// 创建集群内配置
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("创建集群内配置失败: %w", err)
	}

	// 创建客户端
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("创建Kubernetes客户端失败: %w", err)
	}

	d.k8sClient = clientset
	d.Log.Info("Kubernetes客户端初始化成功")
	return nil
}

// 动态发现 Kubernetes Pod
func (d *Discovery) discoverK8sPods() {
	if d.k8sClient == nil {
		return
	}

	// 获取命名空间
	namespace := getEnv("POD_NAMESPACE", "default")
	if namespace == "" {
		namespace = "default"
	}

	// 获取标签选择器
	selector := getEnv("MESH_POD_SELECTOR", "app=wukong-im")

	d.Log.Debug("发现Kubernetes Pod",
		zap.String("namespace", namespace),
		zap.String("selector", selector),
	)

	// 获取 Pod 列表
	pods, err := d.k8sClient.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		d.Log.Error("获取Pod列表失败", zap.Error(err))
		return
	}

	d.Log.Info("发现Kubernetes Pod", zap.Int("count", len(pods.Items)))

	// 处理发现的 Pod
	for _, pod := range pods.Items {
		// 跳过自身
		if pod.Status.PodIP == GetLocalIP() {
			continue
		}

		// 跳过非运行状态的 Pod
		if pod.Status.Phase != corev1.PodRunning {
			d.Log.Debug("跳过非运行状态Pod",
				zap.String("name", pod.Name),
				zap.String("phase", string(pod.Status.Phase)),
			)
			continue
		}

		// 获取端口
		port := getEnv("MESH_PORT", "11110")

		// 检查容器中是否有自定义端口设置
		if len(pod.Spec.Containers) > 0 {
			for _, env := range pod.Spec.Containers[0].Env {
				if env.Name == "MESH_PORT" {
					port = env.Value
					break
				}
			}
		}

		// 添加或更新节点
		d.addOrUpdateNode(NodeInfo{
			Name:      pod.Name,
			IP:        pod.Status.PodIP,
			Port:      port,
			Version:   version.Version,
			LastSeen:  time.Now(),
			Reachable: true, // 假设可达，后续会检查
		})
	}
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

// 定期发现循环
func (d *Discovery) k8sDiscoveryLoop() {
	d.Log.Info("启动Kubernetes发现循环", zap.String("interval", "30s"))

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for !d.shutdown {
		<-ticker.C
		d.discoverK8sPods()
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
		removed := []NodeInfo{}

		d.mu.Lock()
		for name, node := range d.nodes {
			if name == d.SelfName {
				continue // 忽略自身
			}

			if node.Status == NodeStatusOffline || now.Sub(node.LastSeen) > 30*time.Second {
				removed = append(removed, node)
				delete(d.nodes, name)
			}
		}
		d.mu.Unlock()

		// 通知监听器
		if len(removed) > 0 {
			d.Log.Info("清理过期节点", zap.Int("count", len(removed)))
			for _, node := range removed {
				for _, l := range d.listeners {
					go l.OnNodeDelete(node.Name)
				}
			}
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
