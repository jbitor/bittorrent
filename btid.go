package torrent

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	weakrand "math/rand"
)

// A BTID is a 20-byte string, used to identify Torrents (their "info hash")
// and DHT nodes (their node ID).
type BTID string

// Verify() returns an error if the BTID is of the wrong length.
// (Unknown/empty values are not valid.)
func (id BTID) Verify() (err error) {
	if len(id) != 20 {
		return errors.New(fmt.Sprintf("BTID has length %v, should be 20.", len(id)))
	} else {
		return nil
	}
}

// String() represents a BTID in hexadecimal.
func (id BTID) String() (str string) {
	if id != "" {
		return hex.EncodeToString([]byte(id))
	} else {
		return "unknown"
	}
}

// Converts the id to a big-endian [5]uint32 value.
func (id BTID) Uint32s() (result [5]uint32) {
	var idBytes [20]byte
	copy(idBytes[:], []byte(id))

	result[0] = uint32(idBytes[0])<<24 + uint32(idBytes[1])<<16 + uint32(idBytes[2])<<8 + uint32(idBytes[3])
	result[1] = uint32(idBytes[4])<<24 + uint32(idBytes[5])<<16 + uint32(idBytes[6])<<8 + uint32(idBytes[7])
	result[2] = uint32(idBytes[8])<<24 + uint32(idBytes[9])<<16 + uint32(idBytes[10])<<8 + uint32(idBytes[11])
	result[3] = uint32(idBytes[12])<<24 + uint32(idBytes[13])<<16 + uint32(idBytes[14])<<8 + uint32(idBytes[15])
	result[4] = uint32(idBytes[16])<<24 + uint32(idBytes[17])<<16 + uint32(idBytes[18])<<8 + uint32(idBytes[19])

	return result
}

func (id BTID) XoredUint32s(other BTID) (result [5]uint32) {
	own := id.Uint32s()
	others := other.Uint32s()

	for i := 0; i < len(own); i++ {
		result[i] = own[i] ^ others[i]
	}

	return result
}

// BTIDFromHex() returns a valid BTID for a given hex string, or an error.
func BTIDFromHex(hexStr string) (id BTID, err error) {
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return "", err
	}

	id = BTID(bytes)

	err = id.Verify()
	if err != nil {
		return "", err
	}

	return id, nil
}

// SecureRandomBTID returns a BTID instantiated using a cryptographically
// secure random number generator, or an error if it was not possible to
// generate sufficient secure bytes.
func SecureRandomBTID() (id BTID, err error) {
	bytes := new([20]byte)
	n, err := rand.Read(bytes[:])

	if err != nil {
		return "", err
	}

	if n < 20 {
		return "", errors.New("Failed to generate 20 secure random bytes for BTID")
	}

	return BTID(bytes[:]), nil
}

// WeakRandomBTID returns a BTID instantiated using the standard random
// number generator. (Seeding the RNG is the responsibility of the user.)
func WeakRandomBTID() (id BTID) {
	bytes := new([20]byte)

	for i := range bytes {
		bytes[i] = byte(weakrand.Int() & 0xFF)
	}

	return BTID(bytes[:])
}
