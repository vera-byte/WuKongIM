package wkmesh

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
)

func GetLastIPSegment(ipStr string) (uint16, error) {
	ip := net.ParseIP(ipStr).To4()
	if ip == nil {
		return 0, fmt.Errorf("无效的IPv4地址: %s", ipStr)
	}
	return uint16(ip[3]), nil
}

// IPMapper 管理IP与短数字的双向映射
type IPMapper struct {
	sync.RWMutex
	IPToNumber map[string]uint16 // IP -> 短数字
	NumberToIP map[uint16]string // 短数字 -> IP
	nextNumber uint16            // 下一个可用数字
}

// NewIPMapper 创建新的映射器
func NewIPMapper() *IPMapper {
	return &IPMapper{
		IPToNumber: make(map[string]uint16),
		NumberToIP: make(map[uint16]string),
		nextNumber: 0,
	}
}

// IPToShort 将IPv4转换为短数字
func (m *IPMapper) IPToShort(ipStr string) (uint16, error) {
	ip := net.ParseIP(ipStr)
	if ip == nil || ip.To4() == nil {
		return 0, errors.New("无效的IPv4地址")
	}
	normalizedIP := ip.To4().String()

	m.Lock()
	defer m.Unlock()

	// 检查是否已有映射
	if num, exists := m.IPToNumber[normalizedIP]; exists {
		return num, nil
	}

	// 生成SHA-256哈希
	hash := sha256.Sum256([]byte(normalizedIP))

	// 尝试使用前10位作为数字
	shortNum := binary.BigEndian.Uint16(hash[:2]) & 0x3FF // 取10位(0-1023)

	// 处理冲突
	for attempts := 0; attempts < 1024; attempts++ {
		if _, exists := m.NumberToIP[shortNum]; !exists {
			break
		}
		// 冲突时使用线性探测
		shortNum = (shortNum + 1) % 1024
	}

	// 如果仍冲突，使用顺序分配
	if _, exists := m.NumberToIP[shortNum]; exists {
		for m.nextNumber < 1024 {
			if _, exists := m.NumberToIP[m.nextNumber]; !exists {
				shortNum = m.nextNumber
				break
			}
			m.nextNumber++
		}
		if m.nextNumber >= 1024 {
			return 0, errors.New("数字池已满")
		}
	}

	// 创建映射
	m.IPToNumber[normalizedIP] = shortNum
	m.NumberToIP[shortNum] = normalizedIP
	m.nextNumber = shortNum + 1

	return shortNum, nil
}

// ShortToIP 将短数字转换回IPv4
func (m *IPMapper) ShortToIP(num uint16) (string, error) {
	m.RLock()
	defer m.RUnlock()

	if ip, exists := m.NumberToIP[num]; exists {
		return ip, nil
	}
	return "", errors.New("未知的数字")
}

// SaveToFile 保存映射到文件
func (m *IPMapper) SaveToFile(filename string) error {
	m.RLock()
	defer m.RUnlock()

	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0644)
}

// LoadFromFile 从文件加载映射
func (m *IPMapper) LoadFromFile(filename string) error {
	m.Lock()
	defer m.Unlock()

	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, m)
}
