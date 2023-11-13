package util

import (
	"encoding/hex"
	"strings"

	"crypto/md5"

	"lukechampine.com/uint128"
)

// Mathmatically correct modulo function (% as done in Python, Haskell, Ruby, etc.)
//
// modulo(-1, 5) = 4
//
// modulo(3, -5) = -2
func Modulo(x, n int) int {
	return (x%n + n) % n
}

func MD5Hash(val string) string {
	b := md5.Sum([]byte(val))
	return hex.EncodeToString(b[:])
}

func MD5HashUint128(val string) uint128.Uint128 {
	b := md5.Sum([]byte(val))
	return uint128.FromBytesBE(b[:])
}

func Uint128BigEndian(u uint128.Uint128) string {
	b := make([]byte, 16)
	u.PutBytesBE(b)

	return strings.TrimLeft(hex.EncodeToString(b), "0")
}

func BigEndianUint128(s string) uint128.Uint128 {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}

	return uint128.FromBytesBE(b)
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
