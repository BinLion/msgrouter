package utils

import (
	"net"
	"testing"
)

// 测试获取本机ip
func TestGetLocalIp(t *testing.T) {
	ip, err := GetLocalIp()
	if err != nil || ip == "" {
		t.Errorf("get local ip failed. ip:%v, err:%v", ip, err)
		return
	}

	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		t.Errorf("dial to 8.8.8.8 failed. err:%v", err)
		return
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)

	if ip != localAddr.IP.String() {
		t.Errorf("get local ip error. ip:%v, localAddrIp:%v", ip, localAddr.IP)
	}
}
