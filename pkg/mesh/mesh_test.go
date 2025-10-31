package wkmesh

import (
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

func TestMesh(t *testing.T) {

	NewMesh(nil, "test")
}

func TestIp(t *testing.T) {
	ipList, err := wkutil.GetIntranetIP()
	if err != nil {
		t.Fatalf("获取内网IP失败: %v", err)
	}
	fmt.Println("内网IP列表:")
	fmt.Println(ipList)
	mapper := NewIPMapper()

	// 示例IP列表
	ips := []string{
		"192.168.1.1",
		"10.0.0.1",
		"172.16.0.1",
		"8.8.8.8",
		"1.1.1.1",
	}

	// 正向转换
	fmt.Println("正向转换:")
	for _, ip := range ips {
		short, err := mapper.IPToShort(ip)
		if err != nil {
			fmt.Printf("错误: %v\n", err)
			continue
		}
		fmt.Printf("%-15s → %d\n", ip, short)
	}

	// 反向转换
	fmt.Println("\n反向转换:")
	for _, ip := range ips {
		short, _ := mapper.IPToShort(ip)
		restored, err := mapper.ShortToIP(short)
		if err != nil {
			fmt.Printf("错误: %v\n", err)
			continue
		}
		fmt.Printf("%-3d → %s\n", short, restored)
	}

	// 保存和加载演示
	fmt.Println("\n持久化演示:")
	if err := mapper.SaveToFile("ip_mappings.json"); err == nil {
		fmt.Println("映射已保存到文件")

		newMapper := NewIPMapper()
		if err := newMapper.LoadFromFile("ip_mappings.json"); err == nil {
			fmt.Println("从文件加载映射成功")
			restored, _ := newMapper.ShortToIP(0)
			fmt.Printf("数字 0 对应的IP: %s\n", restored)
		}
	}
}
