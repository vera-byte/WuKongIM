package discovery

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/vera-byte/mdns"
)

// EventHandler 节点事件处理函数
type EventHandler func(node *Node, status NodeStatus)

// DiscoveryManager 管理mDNS服务发现
type DiscoveryManager struct {
	mu          sync.RWMutex
	config      Config
	server      *mdns.Server
	nodes       map[string]*Node
	discoveryCh chan *mdns.ServiceEntry
	iface       *net.Interface
	localIP     net.IP
	shutdown    bool
	announceTxt []string
	offline     bool
	handlers    []EventHandler
}

// NewDiscoveryManager 创建新的发现管理器
func NewDiscoveryManager(config Config) (*DiscoveryManager, error) {
	// 如果未提供实例ID，则生成默认

	if config.InstanceID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "unknown-host"
		}
		config.InstanceID = fmt.Sprintf("%s-%d", hostname, config.ServicePort)
	}

	// 获取网络信息
	iface, localIP := getNetworkInfoForLinux()
	if config.Iface != nil {
		iface = config.Iface
	}
	if config.LocalIP != nil {
		localIP = config.LocalIP
	}
	if iface == nil || localIP == nil {
		return nil, fmt.Errorf("no suitable network interface or IP address found")
	}

	// 初始化宣告TXT记录
	announceTxt := []string{
		config.NameSpace,
		"version=" + config.Version,
		"hostname=" + config.InstanceID,
	}

	return &DiscoveryManager{
		config:      config,
		nodes:       make(map[string]*Node),
		discoveryCh: make(chan *mdns.ServiceEntry, 128),
		iface:       iface,
		localIP:     localIP,
		announceTxt: announceTxt,
	}, nil
}

// RegisterEventHandler 注册节点事件处理函数
func (m *DiscoveryManager) RegisterEventHandler(handler EventHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers = append(m.handlers, handler)
}

// Start 启动发现服务
func (m *DiscoveryManager) Start() error {
	// 启动mDNS服务
	if err := m.startServer(); err != nil {
		return err
	}

	// 启动服务发现
	m.startDiscovery()

	// 添加自身节点
	m.addSelfNode()

	// 启动协程
	go m.announcePresence()
	go m.checkNodeTimeouts()

	return nil
}

// Stop 停止发现服务
func (m *DiscoveryManager) Stop() {
	m.shutdown = true
	m.offlineAnnouncement()
	m.stopDiscovery()
	m.stopServer()
}

// GetNodes 获取当前节点列表（拷贝）
func (m *DiscoveryManager) GetNodes() []*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]*Node, 0, len(m.nodes))
	for _, node := range m.nodes {
		// 拷贝节点，避免外部修改内部状态
		n := *node
		nodes = append(nodes, &n)
	}
	return nodes
}

// 触发事件处理
func (m *DiscoveryManager) triggerEvent(node *Node, status NodeStatus) {
	for _, handler := range m.handlers {
		handler(node, status)
	}
}

// 添加自身节点
func (m *DiscoveryManager) addSelfNode() {
	m.mu.Lock()
	defer m.mu.Unlock()

	selfKey := fmt.Sprintf("%s:%d", m.localIP, m.config.ServicePort)

	if _, exists := m.nodes[selfKey]; !exists {
		selfNode := &Node{
			Instance:   m.config.InstanceID,
			IP:         m.localIP,
			Port:       m.config.ServicePort,
			Status:     StatusOnline,
			LastSeen:   time.Now(),
			ServiceTxt: m.announceTxt,
			IsLocal:    true,
		}
		m.nodes[selfKey] = selfNode
		m.triggerEvent(selfNode, StatusOnline)
	}
}

// 发送离线宣告
func (m *DiscoveryManager) offlineAnnouncement() {
	if m.offline {
		return
	}
	m.offline = true

	// 创建离线服务宣告 (TTL=0)
	service, err := mdns.NewMDNSService(
		m.config.InstanceID,
		m.config.ServiceName,
		"local.",
		"",
		m.config.ServicePort,
		[]net.IP{m.localIP},
		append(m.announceTxt, "status=offline"),
	)
	if err != nil {
		return
	}

	// 配置服务器
	config := &mdns.Config{
		Zone:  service,
		Iface: m.iface,
	}

	// 创建临时服务器发送离线宣告
	server, err := mdns.NewServer(config)
	if err != nil {
		return
	}

	// 短暂运行以发送宣告
	time.Sleep(200 * time.Millisecond)
	server.Shutdown()

	// 关闭主服务器
	if m.server != nil {
		m.server.Shutdown()
	}
}

// 启动mDNS服务
func (m *DiscoveryManager) startServer() error {
	// 添加启动时间戳到TXT记录
	txt := append(m.announceTxt, fmt.Sprintf("started=%d", time.Now().Unix()))

	// 创建mDNS服务
	service, err := mdns.NewMDNSService(
		m.config.InstanceID,
		m.config.ServiceName,
		"local.",
		"",
		m.config.ServicePort,
		[]net.IP{m.localIP},
		txt,
	)
	if err != nil {
		return err
	}

	// 配置mDNS服务器
	config := &mdns.Config{
		Zone:  service,
		Iface: m.iface,
	}

	// 创建并启动服务器
	server, err := mdns.NewServer(config)
	if err != nil {
		return err
	}

	m.server = server
	return nil
}

// 停止mDNS服务
func (m *DiscoveryManager) stopServer() {
	if m.server != nil {
		m.server.Shutdown()
	}
}

// 定期宣告服务存在
func (m *DiscoveryManager) announcePresence() {
	ticker := time.NewTicker(m.config.AnnounceInt)
	defer ticker.Stop()

	for !m.shutdown {
		<-ticker.C

		m.mu.Lock()
		// 更新TXT记录，添加最后可见时间戳
		txt := append(m.announceTxt, fmt.Sprintf("last_seen=%d", time.Now().Unix()))

		// 创建新的服务实例
		service, err := mdns.NewMDNSService(
			m.config.InstanceID,
			m.config.ServiceName,
			"local.",
			"",
			m.config.ServicePort,
			[]net.IP{m.localIP},
			txt,
		)
		if err != nil {
			m.mu.Unlock()
			continue
		}

		// 配置服务器
		config := &mdns.Config{
			Zone:  service,
			Iface: m.iface,
		}

		// 重启服务器以更新宣告信息
		if m.server != nil {
			m.server.Shutdown()
		}

		server, err := mdns.NewServer(config)
		if err != nil {
		} else {
			m.server = server
			// 更新自身节点信息
			selfKey := fmt.Sprintf("%s:%d", m.localIP, m.config.ServicePort)
			if node, exists := m.nodes[selfKey]; exists {
				node.LastSeen = time.Now()
				node.ServiceTxt = txt
			}
		}
		m.mu.Unlock()
	}
}

// 启动服务发现
func (m *DiscoveryManager) startDiscovery() {
	go m.processEntries() // 启动条目处理协程

	go func() {
		for !m.shutdown {
			params := mdns.DefaultParams(m.config.ServiceName)
			params.Entries = m.discoveryCh
			params.Timeout = m.config.ScanInterval / 2
			params.Interface = m.iface
			params.DisableIPv6 = false

			_ = mdns.Query(params)
			time.Sleep(m.config.ScanInterval)
		}
	}()
}

// 停止服务发现
func (m *DiscoveryManager) stopDiscovery() {
	close(m.discoveryCh)
}

// 处理发现的服务条目
func (m *DiscoveryManager) processEntries() {
	for entry := range m.discoveryCh {
		// 忽略无效条目
		if len(entry.AddrV4) == 0 || entry.Port == 0 {
			continue
		}

		// 解析实例名称
		instanceName := ""
		if len(entry.Name) > 0 {
			dotIndex := strings.Index(entry.Name, ".")
			if dotIndex != -1 {
				instanceName = entry.Name[:dotIndex]
			} else {
				instanceName = entry.Name
			}
		}

		// 过滤非目标服务
		if !strings.Contains(entry.Name, m.config.ServiceName) {
			continue
		}

		// 检查应用标识
		hasAppIdentifier := false
		for _, info := range entry.InfoFields {
			if info == m.config.NameSpace {
				hasAppIdentifier = true
				break
			}
		}
		if !hasAppIdentifier {
			continue
		}

		// 跳过自身实例
		if instanceName == m.config.InstanceID {
			continue
		}

		// 检查是否为离线宣告
		isOffline := false
		for _, info := range entry.InfoFields {
			if info == "status=offline" {
				isOffline = true
				break
			}
		}

		// 使用IP:端口作为唯一键
		key := fmt.Sprintf("%s:%d", entry.AddrV4, entry.Port)

		m.mu.Lock()
		node, exists := m.nodes[key]

		if !exists {
			// 如果是离线宣告，不创建新节点
			if isOffline {
				m.mu.Unlock()
				continue
			}

			// 创建新节点
			node = &Node{
				Instance:   instanceName,
				IP:         entry.AddrV4,
				Port:       entry.Port,
				ServiceTxt: entry.InfoFields,
				Status:     StatusOnline,
				LastSeen:   time.Now(),
			}
			m.nodes[key] = node
			m.triggerEvent(node, StatusOnline)
		} else {
			// 更新现有节点
			node.LastSeen = time.Now()
			node.ServiceTxt = entry.InfoFields

			if isOffline {
				// 收到离线宣告，立即标记为离线
				if node.Status != StatusOffline {
					node.Status = StatusOffline
					m.triggerEvent(node, StatusOffline)
				}
			} else {
				// 正常上线宣告
				if node.Status != StatusOnline {
					node.Status = StatusOnline
					m.triggerEvent(node, StatusOnline)
				}
			}
		}
		m.mu.Unlock()
	}
}

// 检查节点超时
func (m *DiscoveryManager) checkNodeTimeouts() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if m.shutdown {
			return
		}

		m.mu.Lock()
		now := time.Now()
		for _, node := range m.nodes {
			// 跳过自身节点和已离线节点
			if node.IsLocal || node.Status == StatusOffline {
				continue
			}

			// 标记超时节点为离线
			if now.Sub(node.LastSeen) > m.config.NodeTimeout {
				node.Status = StatusOffline
				m.triggerEvent(node, StatusOffline)
			}
		}
		m.mu.Unlock()
	}
}

// getNetworkInfoForLinux 针对Linux系统优化的网络信息获取
func getNetworkInfoForLinux() (*net.Interface, net.IP) {
	ifaces, err := net.Interfaces()
	if err != nil {
		logger.Printf("获取网络接口失败: %v", err)
		return nil, nil
	}

	// 优先选择活动连接
	preferredOrder := []net.Flags{
		net.FlagUp | net.FlagBroadcast, // 活动有线连接
		net.FlagUp,                     // 其他活动连接
	}

	for _, flags := range preferredOrder {
		for _, iface := range ifaces {
			// 跳过回环接口
			if iface.Flags&net.FlagLoopback != 0 {
				continue
			}

			// 检查标志匹配
			if iface.Flags&flags != flags {
				continue
			}

			addrs, err := iface.Addrs()
			if err != nil {
				logger.Printf("获取接口 %s 地址失败: %v", iface.Name, err)
				continue
			}

			for _, addr := range addrs {
				ipNet, ok := addr.(*net.IPNet)
				if !ok {
					continue
				}

				ip := ipNet.IP
				// 使用非回环IPv4地址
				if ip.To4() != nil && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
					logger.Printf("选择接口: %s, IP: %s", iface.Name, ip)
					return &iface, ip
				}
			}
		}
	}

	// 如果首选方法失败，尝试回退到原始方法
	logger.Println("使用回退方法获取网络信息")
	return getNetworkInfoFallback()
}

// getNetworkInfoFallback 回退网络信息获取方法
func getNetworkInfoFallback() (*net.Interface, net.IP) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, nil
	}

	for _, iface := range ifaces {
		// 跳过回环接口和未启用的接口
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		// 查找IPv4地址
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}

			ip := ipNet.IP
			// 使用非回环IPv4地址
			if ip.To4() != nil && !ip.IsLoopback() {
				return &iface, ip
			}
		}
	}

	return nil, nil
}
