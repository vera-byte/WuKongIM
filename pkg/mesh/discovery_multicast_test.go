package wkmesh_test

import (
	"encoding/json"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type UDPBody struct {
	Name      string `json:"name"`
	IP        string `json:"ip"`
	Port      string `json:"port"`
	Version   string `json:"version"`
	Timestamp int64  `json:"timestamp"`
	Announce  bool   `json:"announce"`
	Shutdown  bool   `json:"shutdown"`
}

func selectInterface() (*net.Interface, net.IP, error) {
	ifaces, _ := net.Interfaces()
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, _ := iface.Addrs()
		for _, a := range addrs {
			switch v := a.(type) {
			case *net.IPNet:
				if ip := v.IP.To4(); ip != nil {
					return &iface, ip, nil
				}
			case *net.IPAddr:
				if ip := v.IP.To4(); ip != nil {
					return &iface, ip, nil
				}
			}
		}
	}
	return nil, nil, os.ErrNotExist
}

func TestMulticastSendAndReceive(t *testing.T) {
	group := net.IPv4(224, 0, 0, 250)
	port := 11110
	addr := &net.UDPAddr{IP: group, Port: port}

	iface, _, err := selectInterface()
	assert.NoError(t, err)

	recvConn, err := net.ListenMulticastUDP("udp4", iface, addr)
	assert.NoError(t, err)
	defer recvConn.Close()
	recvConn.SetReadBuffer(2048)

	sendConn, err := net.DialUDP("udp4", nil, addr)
	assert.NoError(t, err)
	defer sendConn.Close()

	msg := UDPBody{
		Name:      "test-node",
		IP:        "192.168.0.1",
		Port:      "11110",
		Version:   "v1.0.0",
		Announce:  true,
		Timestamp: time.Now().UnixMilli(),
	}

	b, _ := json.Marshal(msg)

	_, err = sendConn.Write(b)
	assert.NoError(t, err)

	recvConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 2048)
	n, src, err := recvConn.ReadFromUDP(buf)
	assert.NoError(t, err)
	assert.NotEmpty(t, src.String())

	var recvMsg UDPBody
	err = json.Unmarshal(buf[:n], &recvMsg)
	assert.NoError(t, err)
	assert.Equal(t, msg.Name, recvMsg.Name)
	assert.Equal(t, msg.Port, recvMsg.Port)
	assert.Equal(t, msg.Version, recvMsg.Version)
}
