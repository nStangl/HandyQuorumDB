package util

import (
	"net"
	"strings"
)

func CopyAddr(a *net.TCPAddr) *net.TCPAddr {
	ip := make([]byte, len(a.IP))
	copy(ip, a.IP)

	return &net.TCPAddr{
		IP:   ip,
		Port: a.Port,
		Zone: strings.Clone(a.Zone),
	}
}
