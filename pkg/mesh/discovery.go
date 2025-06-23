package wkmesh

import (
	"encoding/binary"
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
	"go.uber.org/zap"
	"golang.org/x/net/ipv4"
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
	Status    string        `json:"status"` // 状态: "online", "offline"
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
}

func newDiscovery(selfName, selfPort string) *Discovery {
	selfIP := GetLocalIP()
	nodeId, _ := IPv4ToUint32(selfIP)

	// 创建自身节点信息
	selfNode := NodeInfo{
		NodeId:    nodeId,
		Name:      selfName,
		IP:        selfIP,
		Port:      selfPort,
		Version:   version.Version,
		LastSeen:  time.Now(),
		Reachable: true,
		Latency:   0,
		Status:    "online",
	}

	d := &Discovery{
		Log:       wklog.NewWKLog("WKMesh.Discovery"),
		SelfName:  selfName,
		SelfPort:  selfPort,
		Version:   version.Version,
		nodes:     make(map[string]NodeInfo),
		listeners: make([]Listener, 0),
	}

	// 添加自身节点
	d.mu.Lock()
	d.nodes[d.SelfName] = selfNode
	d.mu.Unlock()

	d.Log.Info("节点初始化完成",
		zap.String("name", selfName),
		zap.String("ip", selfIP),
		zap.String("port", selfPort),
	)

	// 启动核心协程
	go d.broadcastLoop()
	go d.listenLoop()
	go d.cleanupLoop()

	// 延迟发送上线通知
	go func() {
		time.Sleep(500 * time.Millisecond)
		d.Announce()
	}()

	return d
}

// Announce 发送上线通知
func (d *Discovery) Announce() {
	if d.hasAnnounced || d.shutdown {
		return
	}

	d.Log.Info("发送上线通知")

	broadcastIP, err := GetBroadcastIP()
	if err != nil {
		broadcastIP = "255.255.255.255"
	}

	addr, _ := net.ResolveUDPAddr("udp", broadcastIP+":11110")
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		d.Log.Error("创建上线通知连接失败", zap.Error(err))
		return
	}
	defer conn.Close()

	msg := map[string]interface{}{
		"name":      d.SelfName,
		"ip":        GetLocalIP(),
		"port":      d.SelfPort,
		"version":   d.Version,
		"announce":  true, // 上线通知标志
		"timestamp": time.Now().UnixMilli(),
	}
	b, _ := json.Marshal(msg)

	if _, err := conn.Write(b); err != nil {
		d.Log.Warn("发送上线通知失败", zap.Error(err))
	} else {
		d.Log.Info("上线通知已发送")
		d.hasAnnounced = true
	}
}

// Shutdown 节点关闭时调用，通知其他节点自己即将下线
func (d *Discovery) Shutdown() {
	if d.shutdown {
		return
	}
	d.shutdown = true

	d.Log.Info("节点正在关闭，发送下线通知")

	// 发送下线广播
	broadcastIP, err := GetBroadcastIP()
	if err != nil {
		broadcastIP = "255.255.255.255"
	}
	addr, _ := net.ResolveUDPAddr("udp", broadcastIP+":11110")
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		d.Log.Error("创建下线通知连接失败", zap.Error(err))
		return
	}
	defer conn.Close()

	msg := map[string]interface{}{
		"name":      d.SelfName,
		"ip":        GetLocalIP(),
		"port":      d.SelfPort,
		"version":   d.Version,
		"shutdown":  true, // 下线标志
		"timestamp": time.Now().UnixMilli(),
	}
	b, _ := json.Marshal(msg)

	if _, err := conn.Write(b); err != nil {
		d.Log.Warn("发送下线通知失败", zap.Error(err))
	} else {
		d.Log.Info("下线通知已发送")
	}

	// 更新自身状态为下线
	d.mu.Lock()
	if node, ok := d.nodes[d.SelfName]; ok {
		node.Status = "offline"
		d.nodes[d.SelfName] = node
	}
	d.mu.Unlock()

	// 等待一小段时间确保消息发送
	time.Sleep(500 * time.Millisecond)
}

// 注册节点监听器
func (d *Discovery) RegisterListener(l Listener) {
	d.listeners = append(d.listeners, l)
	d.Log.Debug("注册节点监听器", zap.Int("count", len(d.listeners)))
}

// 广播循环 - 向网络发送节点信息
func (d *Discovery) broadcastLoop() {
	// 获取子网广播地址
	broadcastIP, err := GetBroadcastIP()
	if err != nil {
		d.Log.Error("获取广播地址失败", zap.Error(err))
		broadcastIP = "255.255.255.255" // 回退到全局广播
	}

	addr, _ := net.ResolveUDPAddr("udp", broadcastIP+":11110")
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		d.Log.Error("创建广播连接失败", zap.Error(err))
		return
	}
	defer conn.Close()

	d.Log.Info("启动广播循环",
		zap.String("broadcast", broadcastIP),
		zap.String("interval", "5s"),
	)

	// 首次广播时发送上线通知
	go d.Announce()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if d.shutdown {
				d.Log.Info("广播循环退出")
				return
			}

			ip := GetLocalIP()
			msg := map[string]interface{}{
				"name":      d.SelfName,
				"ip":        ip,
				"port":      d.SelfPort,
				"version":   d.Version,
				"rest_port": "11110",
				"timestamp": time.Now().UnixMilli(),
			}
			b, _ := json.Marshal(msg)

			if _, err := conn.Write(b); err != nil {
				d.Log.Warn("广播发送失败", zap.Error(err))
			}

			// 更新自身节点最后可见时间
			d.mu.Lock()
			node := d.nodes[d.SelfName]
			node.LastSeen = time.Now()
			d.nodes[d.SelfName] = node
			d.mu.Unlock()
		}
	}
}

// 监听循环 - 接收网络中的节点信息
func (d *Discovery) listenLoop() {
	group := net.IPv4(224, 0, 0, 250)
	port := 11110

	// 获取多播接口
	iface := getMulticastInterface()
	if iface == nil {
		d.Log.Error("未找到有效的多播接口")
		os.Exit(1)
	}
	d.Log.Info("使用网络接口",
		zap.String("name", iface.Name),
		zap.Strings("ips", getInterfaceIPs(iface)),
	)

	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: port,
	})
	if err != nil {
		d.Log.Error("创建监听连接失败", zap.Error(err))
		os.Exit(1)
	}
	defer udpConn.Close()

	p := ipv4.NewPacketConn(udpConn)
	if err := p.JoinGroup(iface, &net.UDPAddr{IP: group}); err != nil {
		d.Log.Error("加入多播组失败", zap.Error(err))
		os.Exit(1)
	}

	_ = p.SetControlMessage(ipv4.FlagDst, true)
	_ = udpConn.SetReadBuffer(2048)

	d.Log.Info("开始监听节点广播",
		zap.String("group", group.String()),
		zap.Int("port", port),
	)

	buf := make([]byte, 2048)
	for !d.shutdown {
		n, _, _, err := p.ReadFrom(buf)
		if err != nil {
			if d.shutdown {
				break
			}
			d.Log.Warn("读取消息失败", zap.Error(err))
			continue
		}
		go d.handleMessage(buf[:n])
	}
	d.Log.Info("监听循环退出")
}

// 处理接收到的节点消息
func (d *Discovery) handleMessage(data []byte) {
	var msg map[string]interface{}
	if err := json.Unmarshal(data, &msg); err != nil {
		d.Log.Warn("消息解析失败", zap.Error(err))
		return
	}

	name, _ := msg["name"].(string)
	// 忽略自身消息
	if name == d.SelfName {
		return
	}

	// 检查是否为上线通知
	if announce, ok := msg["announce"].(bool); ok && announce {
		d.Log.Info("收到上线通知", zap.String("name", name))

		// 如果这是新节点，立即回复
		d.mu.RLock()
		_, exists := d.nodes[name]
		d.mu.RUnlock()

		if !exists {
			d.Log.Info("回复上线通知", zap.String("name", name))
			go d.Announce()
		}
	}

	// 检查是否为下线通知
	if shutdown, ok := msg["shutdown"].(bool); ok && shutdown {
		d.Log.Info("收到下线通知", zap.String("name", name))

		d.mu.Lock()
		if node, exists := d.nodes[name]; exists {
			node.Status = "offline"                           // 标记为下线状态
			node.LastSeen = time.Now().Add(-30 * time.Second) // 立即触发清理
			d.nodes[name] = node
		}
		d.mu.Unlock()

		// 立即触发节点删除通知
		for _, l := range d.listeners {
			go l.OnNodeDelete(name)
		}
		return
	}

	ip, _ := msg["ip"].(string)
	port, _ := msg["port"].(string)
	version, _ := msg["version"].(string)
	restPort := "11110" // 默认REST端口

	// 检测节点可达性
	reachable, latency := testRESTPing(ip, restPort)
	nodeId, _ := IPv4ToUint32(ip)

	node := NodeInfo{
		NodeId:    nodeId,
		Name:      name,
		IP:        ip,
		Port:      port,
		Version:   version,
		LastSeen:  time.Now(),
		Reachable: reachable,
		Latency:   latency,
		Status:    "online", // 默认为在线状态
	}

	d.mu.Lock()
	existed := false
	if existingNode, ok := d.nodes[name]; ok {
		// 保留现有状态（如果存在）
		node.Status = existingNode.Status
		existed = true
	}
	d.nodes[name] = node
	d.mu.Unlock()

	if !existed {
		d.Log.Info("发现新节点",
			zap.String("name", node.Name),
			zap.String("ip", node.IP),
			zap.Duration("latency", node.Latency),
			zap.Bool("reachable", node.Reachable),
		)
		for _, l := range d.listeners {
			go l.OnNodeUpdate(node)
		}
	} else {
		d.Log.Debug("更新节点信息", zap.String("name", name))
	}
}

// 清理过期节点
func (d *Discovery) cleanupLoop() {
	d.Log.Info("启动节点清理循环", zap.String("interval", "10s"))

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for !d.shutdown {
		select {
		case <-ticker.C:
			now := time.Now()
			removed := []string{}

			d.mu.Lock()
			for name, node := range d.nodes {
				if name == d.SelfName {
					continue // 忽略自身
				}

				// 对于标记为下线的节点，立即清理
				if node.Status == "offline" {
					delete(d.nodes, name)
					removed = append(removed, name)
					continue
				}

				// 正常节点超时清理
				if now.Sub(node.LastSeen) > 15*time.Second {
					delete(d.nodes, name)
					removed = append(removed, name)
				}
			}
			d.mu.Unlock()

			// 通知监听器
			if len(removed) > 0 {
				d.Log.Info("清理过期节点", zap.Strings("nodes", removed))
				for _, name := range removed {
					for _, l := range d.listeners {
						go l.OnNodeDelete(name)
					}
				}
			}
		}
	}
	d.Log.Info("清理循环退出")
}

// 添加静态节点
func (d *Discovery) AddStaticNode(ip, port string) {
	reachable, latency := testRESTPing(ip, "11110")
	name := fmt.Sprintf("static-%s:%s", ip, port)
	nodeId, _ := IPv4ToUint32(ip)

	node := NodeInfo{
		NodeId:    nodeId,
		Name:      name,
		IP:        ip,
		Port:      port,
		Version:   "manual",
		LastSeen:  time.Now(),
		Reachable: reachable,
		Latency:   latency,
		Status:    "online",
	}

	d.mu.Lock()
	d.nodes[name] = node
	d.mu.Unlock()

	d.Log.Info("添加静态节点", zap.String("ip", ip), zap.String("port", port))

	for _, l := range d.listeners {
		go l.OnNodeUpdate(node)
	}
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
		return ip // Kubernetes环境优先使用POD_IP
	}

	ifaces, _ := net.Interfaces()
	for _, iface := range ifaces {
		// 跳过本地回环和非活动接口
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}

		addrs, _ := iface.Addrs()
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

// 获取子网广播地址
func GetBroadcastIP() (string, error) {
	ifaces, _ := net.Interfaces()
	for _, iface := range ifaces {
		// 跳过本地回环和非活动接口
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}

		addrs, _ := iface.Addrs()
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok || ipNet.IP.To4() == nil {
				continue
			}

			// 计算广播地址: IP OR (NOT mask)
			mask := ipNet.Mask
			ip := ipNet.IP.To4()
			broadcast := net.IP(make([]byte, 4))
			for i := range ip {
				broadcast[i] = ip[i] | ^mask[i]
			}
			return broadcast.String(), nil
		}
	}
	return "", fmt.Errorf("未找到有效接口")
}

// 获取多播网络接口
func getMulticastInterface() *net.Interface {
	// 优先选择Kubernetes环境常见接口
	preferred := []string{"eth0", "en0", "en1", "enp0s1"}

	for _, name := range preferred {
		if iface, err := net.InterfaceByName(name); err == nil {
			if iface.Flags&net.FlagUp != 0 && iface.Flags&net.FlagMulticast != 0 {
				return iface
			}
		}
	}

	// 回退到所有可用接口
	ifaces, _ := net.Interfaces()
	for _, iface := range ifaces {
		if iface.Flags&net.FlagLoopback == 0 &&
			iface.Flags&net.FlagUp != 0 &&
			iface.Flags&net.FlagMulticast != 0 {
			return &iface
		}
	}
	return nil
}

// 获取接口IP列表
func getInterfaceIPs(iface *net.Interface) []string {
	addrs, _ := iface.Addrs()
	ips := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok {
			if ip := ipNet.IP.To4(); ip != nil {
				ips = append(ips, ip.String())
			}
		}
	}
	return ips
}

// 测试节点REST可达性
func testRESTPing(ip, port string) (bool, time.Duration) {
	url := fmt.Sprintf("http://%s:%s/health", ip, port)
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

// IP转uint32
func IPv4ToUint32(ipStr string) (uint32, error) {
	ip := net.ParseIP(ipStr).To4()
	if ip == nil {
		return 0, fmt.Errorf("无效的IPv4地址: %s", ipStr)
	}
	return binary.BigEndian.Uint32(ip), nil
}

// uint32转IP
func Uint32ToIPv4(n uint32) string {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, n)
	return ip.String()
}
