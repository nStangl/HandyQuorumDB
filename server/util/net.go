package util

import (
	"fmt"
	"net"
)

func ParseAddress(addr net.Addr) (string, int) {
	switch a := addr.(type) {
	case *net.UDPAddr:
		return a.IP.String(), a.Port
	case *net.TCPAddr:
		return a.IP.String(), a.Port
	}

	panic(fmt.Errorf("address %v is neither ipv4 or ipv6", addr))
}
