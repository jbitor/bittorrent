package bittorrent

import (
	"encoding/binary"
	"fmt"
	"io"

	"net"

	"github.com/jbitor/bencoding"

	"bytes"
	"time"
)

// Types of messages supported by the BitTorrent peer protocol
type peerMessageType byte

const peerProtocolHeader = "\x13BitTorrent protocol"

const (
	msgChoke         peerMessageType = 0
	msgUnchoke                       = 1
	msgInterested                    = 2
	msgNotInterested                 = 3
	msgHave                          = 4
	msgBitfield                      = 5
	msgRequest                       = 6
	msgPiece                         = 7
	msgCancel                        = 8

	// BEP-10 Extension Protocl
	msgExtended = 20
)

type swarmPeer struct {
	swarm Swarm
	addr  net.TCPAddr
	conn  *net.TCPConn
	karma int
	// TODO: peerId
	gotHandshake bool
}

func (p *swarmPeer) String() string {
	return fmt.Sprintf("<swarmPeer %v:%v for %s>", p.addr.IP, p.addr.Port, p.swarm.InfoHash())
}

// If we're not already connected to this peer, block until we've opened a connection.
func (p *swarmPeer) connect() {
	if p.conn != nil {
		return
	}

	logger.Printf("Attempting to connect to %v for %v.", p, p.swarm)

	// TODO(jre): We really need to dial on the same port as we're
	// listening, but net doesn't directly let us do that.
	conn, err := net.DialTCP("tcp", nil, &p.addr)
	if err != nil {
		logger.Printf("Failed to connect to %v for %v: %v", p, p.swarm, err)
		return
	}

	logger.Printf("Sending handshake to %v...", p)
	writeHandshake(conn, p.swarm.Client().PeerId(), p.swarm.InfoHash())

	p.gotHandshake = false
	p.conn = conn
	go p.listen()
}

// Listening for and processing messages from this peer.
func (p *swarmPeer) listen() {
	defer func() {
		p.conn.Close()
		p.conn = nil
	}()

	// Called to handle each non-keepalive message.
	gotMessage := func(messageType peerMessageType, body string) {
		switch messageType {

		case msgExtended:
			extendedType := body[0]
			if extendedType != 0 {
				logger.Printf("Got unsupported extension message type: %v", extendedType)
				return
			}
			bencoded := body[1:len(body)]
			logger.Printf("Got an extension handshake message")
			data, err := bencoding.Decode([]byte(bencoded))

			if err != nil {
				logger.Printf("Error decoding message: %v", err)
				return
			}

			// Check if the peer supports metadata exchange
			if dataM, hasM := data.(bencoding.Dict)["m"]; hasM {
				if mdxId, hasMdx := dataM.(bencoding.Dict)["ut_metadata"]; hasMdx {
					logger.Printf("Peer %v supports metadata exchange, using extension ID %v.", p, mdxId)
				} else {
					logger.Printf("Peer %v does not support metadata exchange!", p)
					return
				}
			} else {
				logger.Printf("Peer %v does not support any extensions!", p)
				return
			}

		default:
			logger.Printf("Got message of unsupported type %v: %v", messageType, body)
		}

	}

	// All unprocessed data. May contain multiple messages and/or span multiple chunks.
	unprocessedBuffer := make([]byte, 0)

	// Called to handle each chunk of data we get from the peer's connection.
	gotChunk := func(chunk string) {
		unprocessedBuffer = append(unprocessedBuffer, []byte(chunk)...)

		// Process all complete messages in the buffer.
		for len(unprocessedBuffer) > 0 {
			if !p.gotHandshake {
				handshakeSize := 20 + 8 + 20 + 20

				if len(unprocessedBuffer) >= handshakeSize {
					handshake := unprocessedBuffer[0:handshakeSize]

					if string(handshake[0:len(peerProtocolHeader)]) != peerProtocolHeader {
						logger.Printf("Peer protocol header missing for %v!", p)
						break
					}
					// TODO: veriffy the rest of the handshake

					unprocessedBuffer = unprocessedBuffer[handshakeSize:len(unprocessedBuffer)]
					p.gotHandshake = true
					continue
				} else {
					break
				}
			} else {
				if len(unprocessedBuffer) < 4 {
					// Too short to even have the length prefix.
					break
				}
				length := uint32(0)
				buf := bytes.NewBuffer([]byte(chunk[0:4]))
				binary.Read(buf, binary.BigEndian, &length)

				if uint32(len(unprocessedBuffer)) >= length+4 {
					message := unprocessedBuffer[4 : 4+length]
					unprocessedBuffer = unprocessedBuffer[4+length : len(unprocessedBuffer)]

					if length == 0 {
						logger.Printf("Got keepalive message from %s.", p)
						continue
					}

					messageType := peerMessageType(message[0])
					messageBody := message[1:len(message)]
					gotMessage(messageType, string(messageBody))
				} else {
					// Still waiting for some of message.
					break
				}
			}
		}
	}

	// Buffer into which we read each chunk as we get them.
	chunkBuffer := make([]byte, 32768)
	for {
		readLength, err := p.conn.Read(chunkBuffer)

		if err == io.EOF {
			logger.Printf("Remote peer disconnected: %v", p)
			p.karma -= 1
			break
		} else if err != nil {
			logger.Printf("Unkonwn error reading from %v: %v", p, err)
			time.Sleep(6 * time.Second)
			continue
		} else if readLength > 0 {
			gotChunk(string(chunkBuffer[0:readLength]))
			continue
		} else {
			// There was no data to read.
		}
	}

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
func writeMessage(w io.Writer, messageType peerMessageType, body []byte) {
	var messageLength int32
	if body != nil {
		messageLength = 1 + int32(len(body))
	} else {
		messageLength = 1
	}

	binary.Write(w, binary.BigEndian, messageLength)
	w.Write([]byte{byte(messageType)})

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
