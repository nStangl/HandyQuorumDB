package util

import (
	"fmt"
	"testing"

	"lukechampine.com/uint128"
)

func TestModulo(t *testing.T) {
	tests := []struct {
		x int
		n int
		r int
	}{
		{6, 5, 1},
		{-1, 5, 4},
		{3, -5, -2},
		{1, 4, 1},
		{5, 4, 1},
		{-1, 1, 0},
	}

	for _, test := range tests {
		if res := Modulo(test.x, test.n); res != test.r {
			t.Errorf("Modulo(%d, %d) = %d but expected %d", test.x, test.n, res, test.r)
		}
	}
}

func TestMD5Hash(t *testing.T) {
	tests := []struct {
		key      string
		expected string
	}{
		{"abc", "900150983cd24fb0d6963f7d28e17f72"},
		{"def", "4ed9407630eb1000c0f6b63842defa7d"},
		{"thisIsAKey", "a8435849961afab615c0665d437c6d7e"},
		{"9YO29SKb", "3300c6b69ddf360da644cea92cc6e773"},
		{"localhost:8080", "9f5ffc7a10e0bad054458b089947ce2f"},
		{"localhost:8081", "544b0204d642d996a1a342762d3adadf"},
	}

	for _, test := range tests {
		if res := MD5Hash(test.key); res != test.expected {
			t.Errorf("MD5Hash(%s) = %s but expected %s", test.key, res, test.expected)
		}
	}

	for _, test := range tests {
		h := MD5Hash(test.key)
		hi := MD5HashUint128(test.key)

		if res := Uint128BigEndian(hi); res != test.expected || res != h {
			t.Errorf("Uint128BigEndian(hash(%s)) = %s but expected %s", test.key, res, test.expected)
		}

		if res := BigEndianUint128(Uint128BigEndian(hi)); res != hi {
			t.Errorf("BigEndianUint128(Uint128BigEndian((hash(%s))) = %s but expected %s", test.key, res, test.expected)
		}
	}
}

func TestMD5ToUint128(t *testing.T) {
	tests := []struct {
		key      string
		expected string
	}{
		{"HKOVRjmBFF", "135689515723221422458089658349367448353"},
	}

	for _, test := range tests {
		u, err := uint128.FromString(test.expected)
		fmt.Printf("u: %v\n", u)
		if err != nil {
			t.Errorf("uint128.FromString(%s) conversion failed: %v", test.expected, err)
		}

		if res := MD5HashUint128(test.key); !u.Equals(res) {
			t.Errorf("MD5HashUint128(%s) = %s but expected %s", test.key, res, test.expected)
		}
	}
}

func TestUint128ToHex(t *testing.T) {
	tests := []struct {
		u        string
		expected string
	}{
		{"1", "1"},
		{"10", "a"},
		{"191415658344158766168031473277922803570", "900150983cd24fb0d6963f7d28e17f72"},
		{"12345678909876543", "2bdc545def293f"},
		{"18446744073709551615", "ffffffffffffffff"},
		{"994467440737095516159999", "d296301fd412663fffff"},
		{"340282366920938463463374607431768211455", "ffffffffffffffffffffffffffffffff"},
	}

	for _, test := range tests {
		u, err := uint128.FromString(test.u)
		fmt.Printf("u: %v\n", u)
		if err != nil {
			t.Errorf("Uint128BigEndian(%s) conversion failed: %v", test.u, err)
		}

		if res := Uint128BigEndian(u); res != test.expected {
			t.Errorf("Uint128BigEndian(%v) = %s but expected %s", test.u, res, test.expected)
		}
	}
}

func TestHexToUint128(t *testing.T) {
	tests := []struct {
		u        string
		expected string
	}{
		{"a18d1ff984573a42640efebfd292591d", "214738469701892518451414057112204106013"},
		{"fe2c5f992dd5fc96c487f999f9c0e474", "337854310956150895183421562257649230964"},
	}

	for _, test := range tests {
		u, err := uint128.FromString(test.expected)
		fmt.Printf("u: %v\n", u)
		if err != nil {
			t.Errorf("Uint128BigEndian(%s) conversion failed: %v", test.u, err)
		}

		if res := BigEndianUint128(test.u); res != u {
			t.Errorf("BigEndianUint128(%v) = %s but expected %s", test.u, res, test.expected)
		}
	}
}
