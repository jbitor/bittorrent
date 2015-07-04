package bittorrent

import (
	"encoding/binary"
	"io"

	"github.com/jbitor/bencoding"
)

// Types of messages supported by the BitTorrent peer protocol
type messageType byte

const peerProtocolHeader = "\x13BitTorrent protocol"

const (
	msgChoke         messageType = 0
	msgUnchoke                   = 1
	msgInterested                = 2
	msgNotInterested             = 3
	msgHave                      = 4
	msgBitfield                  = 5
	msgRequest                   = 6
	msgPiece                     = 7
	msgCancel                    = 8

	// BEP-10 Extension Protocl
	msgExtended = 20
)

type PeerMessage interface {
	Type() messageType
	Encode() (data []byte)
	Decode(data []byte) (err error)
}

type peerMessage struct {
	t       messageType
	rawBody string
}

func writeHandshake(w io.Writer, peerId BTID, infohash BTID) {
	header := []byte(peerProtocolHeader)
	extensionFlags := make([]byte, 8)
	extensionFlags[5] |= 0x10 // indicate extension protocol support
	w.Write(header)
	w.Write(extensionFlags)
	w.Write([]byte(infohash))
	w.Write([]byte(peerId))

	// If we had any pieces, we would need to indicate so here, but we don't.
	// writeMessage(w, msgBitfield, piecesBitfield)
}

func writeKeepAlive(w io.Writer) {
	binary.Write(w, binary.BigEndian, int32(0))
}

// Writes a non-keepalive message.
func writeMessage(w io.Writer, msgType messageType, body []byte) {
	var messageLength int32
	if body != nil {
		messageLength = 1 + int32(len(body))
	} else {
		messageLength = 1
	}

	binary.Write(w, binary.BigEndian, messageLength)
	w.Write([]byte{byte(msgType)})

	if body != nil {
		w.Write(body)
	}
}

// Writes an BEP-10 extension handshake message.
func writeExtensionHandshake(w io.Writer, data bencoding.Dict) {
	body := make([]byte, 0)
	// TODO
	_ = body
	writeMessage(w, msgExtended, nil)
}
