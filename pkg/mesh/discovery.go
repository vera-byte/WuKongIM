package wkmesh

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/version"
	"golang.org/x/net/ipv4"
)

// NodeInfo represents node metadata
type NodeInfo struct {
	Name      string        `json:"name"`
	IP        string        `json:"ip"`
	Port      string        `json:"port"`
	Version   string        `json:"version"`
	LastSeen  time.Time     `json:"-"`
	Reachable bool          `json:"reachable"`
	Latency   time.Duration `json:"latency"`
}

// Listener interface to receive node events
type Listener interface {
	OnNodeUpdate(node NodeInfo)
	OnNodeDelete(name string)
}

// Discovery is the core structure for service discovery
type Discovery struct {
	SelfName string
	SelfPort string
	Version  string

	nodes     map[string]NodeInfo
	mu        sync.RWMutex
	listeners []Listener
}

func newDiscovery(selfName, selfPort string) *Discovery {
	// 添加自身节点到本地节点列表中
	selfIP := GetLocalIP()
	selfNode := NodeInfo{
		Name:      selfName,
		IP:        selfIP,
		Port:      selfPort,
		Version:   version.Version,
		LastSeen:  time.Now(),
		Reachable: true,
		Latency:   0,
	}

	discovery := &Discovery{
		SelfName:  selfName,
		SelfPort:  selfPort,
		Version:   version.Version,
		nodes:     make(map[string]NodeInfo),
		listeners: make([]Listener, 0),
	}
	discovery.mu.Lock()
	discovery.nodes[discovery.SelfName] = selfNode
	discovery.mu.Unlock()

	go discovery.broadcastLoop()
	go discovery.listenLoop()
	go discovery.cleanupLoop()
	return discovery
}

func (d *Discovery) RegisterListener(l Listener) {
	d.listeners = append(d.listeners, l)
}

func (d *Discovery) broadcastLoop() {
	addr, _ := net.ResolveUDPAddr("udp", "255.255.255.255:11111")
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		fmt.Println("广播失败:", err)
		return
	}
	defer conn.Close()

	ip := GetLocalIP()
	for {
		msg := map[string]interface{}{
			"name":      d.SelfName,
			"ip":        ip,
			"port":      d.SelfPort,
			"version":   d.Version,
			"rest_port": "18080",
			"timestamp": time.Now().UnixMilli(),
		}
		b, _ := json.Marshal(msg)
		conn.Write(b)

		// 更新自身节点 LastSeen，避免被清除
		d.mu.Lock()
		node := d.nodes[d.SelfName]
		node.LastSeen = time.Now()
		d.nodes[d.SelfName] = node
		d.mu.Unlock()

		time.Sleep(5 * time.Second)
	}
}

func (d *Discovery) listenLoop() {
	group := net.IPv4(224, 0, 0, 250)
	port := 11111

	iface := getMulticastInterface()
	if iface == nil {
		fmt.Println("未找到有效的多播接口")
		os.Exit(1)
	}
	fmt.Println("使用网络接口:", iface.Name)

	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: port,
	})
	if err != nil {
		fmt.Println("ListenUDP失败:", err)
		os.Exit(1)
	}

	p := ipv4.NewPacketConn(udpConn)
	if err := p.JoinGroup(iface, &net.UDPAddr{IP: group, Zone: iface.Name}); err != nil {
		fmt.Println("加入多播组失败:", err)
		os.Exit(1)
	}

	_ = p.SetControlMessage(ipv4.FlagDst, true)
	_ = udpConn.SetReadBuffer(2048)

	buf := make([]byte, 2048)
	for {
		n, _, _, err := p.ReadFrom(buf)
		if err != nil {
			fmt.Println("读取失败:", err)
			continue
		}
		go d.handleMessage(buf[:n], nil)
	}
}

func (d *Discovery) handleMessage(data []byte, _ *net.UDPAddr) {
	var msg map[string]interface{}
	if err := json.Unmarshal(data, &msg); err != nil {
		return
	}
	name, _ := msg["name"].(string)
	if name == d.SelfName {
		return
	}
	ip, _ := msg["ip"].(string)
	port, _ := msg["port"].(string)
	version, _ := msg["version"].(string)

	restPort := "18080"
	if val, ok := msg["rest_port"].(string); ok && val != "" {
		restPort = val
	}

	reachable, latency := testRESTPing(ip, restPort)

	node := NodeInfo{
		Name:      name,
		IP:        ip,
		Port:      port,
		Version:   version,
		LastSeen:  time.Now(),
		Reachable: reachable,
		Latency:   latency,
	}
	d.mu.Lock()
	_, existed := d.nodes[name]
	d.nodes[name] = node
	d.mu.Unlock()

	if !existed {
		for _, l := range d.listeners {
			// 打印节点和延迟信息
			fmt.Printf("[发现节点] %s (%s:%s) 版本: %s, 可达: %t, 延迟: %v\n",
				node.Name, node.IP, node.Port, node.Version, node.Reachable, node.Latency.Milliseconds())
			go l.OnNodeUpdate(node)
		}
	}
}

func (d *Discovery) cleanupLoop() {
	for {
		time.Sleep(10 * time.Second)
		now := time.Now()
		d.mu.Lock()
		for name, node := range d.nodes {
			if name == d.SelfName {
				continue // 忽略自己
			}
			if now.Sub(node.LastSeen) > 15*time.Second {
				delete(d.nodes, name)
				for _, l := range d.listeners {
					go l.OnNodeDelete(name)
				}
			}
		}
		d.mu.Unlock()
	}
}

func (d *Discovery) AddStaticNode(ip, port string) {
	reachable, latency := testRESTPing(ip, "18080")
	name := fmt.Sprintf("static-%s:%s", ip, port)
	node := NodeInfo{
		Name: name, IP: ip, Port: port, Version: "manual",
		LastSeen: time.Now(), Reachable: reachable, Latency: latency,
	}
	d.mu.Lock()
	d.nodes[name] = node
	d.mu.Unlock()

	for _, l := range d.listeners {
		go l.OnNodeUpdate(node)
	}
}

func (d *Discovery) ListNodes() []NodeInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()
	list := make([]NodeInfo, 0, len(d.nodes))
	for _, n := range d.nodes {
		list = append(list, n)
	}
	return list
}

func GetLocalIP() string {
	addrs, _ := net.InterfaceAddrs()
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
			return ipNet.IP.String()
		}
	}
	return "127.0.0.1"
}

func getMulticastInterface() *net.Interface {
	preferred := []string{"en0", "en1"}
	for _, name := range preferred {
		iface, err := net.InterfaceByName(name)
		if err == nil && iface.Flags&net.FlagUp != 0 && iface.Flags&net.FlagMulticast != 0 {
			return iface
		}
	}
	ifaces, _ := net.Interfaces()
	for _, iface := range ifaces {
		if iface.Flags&net.FlagLoopback == 0 && iface.Flags&net.FlagUp != 0 && iface.Flags&net.FlagMulticast != 0 {
			return &iface
		}
	}
	return nil
}

func testRESTPing(ip, port string) (bool, time.Duration) {
	url := fmt.Sprintf("http://%s:%s/ping", ip, port)
	client := &http.Client{Timeout: 2 * time.Second}
	start := time.Now()
	resp, err := client.Get(url)
	if err != nil {
		return false, 0
	}
	defer resp.Body.Close()
	latency := time.Since(start)
	return true, latency
}
