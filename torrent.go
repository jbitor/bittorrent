package bittorrent

import (
	"errors"
	"net"

	"github.com/jbitor/bencoding"
)

// Decodes a compact peer address from a 6-byte bencoding.String to a net.TCPAddr.
// Returns an error if the string is the wrong length.
func DecodePeerAddress(encoded bencoding.String) (addr net.TCPAddr, err error) {
	if len(encoded) != 6 {
		err = errors.New("encoded address has wrong length (should be 6)")
	} else {
		addr = net.TCPAddr{
			IP:   net.IPv4(encoded[0], encoded[1], encoded[2], encoded[3]),
			Port: int(encoded[4])<<8 + int(encoded[5]),
		}
	}

	return
}
