package wkmesh

import (
	"net"
	"strconv"
	"time"

	"github.com/WuKongIM/WuKongIM/version"
	"github.com/hashicorp/mdns"
	"go.uber.org/zap"
)

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
