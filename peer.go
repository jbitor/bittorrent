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
	msgChoke         peerMessageType = 0x00
	msgUnchoke                       = 0x01
	msgInterested                    = 0x02
	msgNotInterested                 = 0x03
	msgHave                          = 0x04
	msgBitfield                      = 0x05
	msgRequest                       = 0x06
	msgPiece                         = 0x07
	msgCancel                        = 0x08

	// BEP-9 Extension Protocl
	msgDhtPort = 0x09

	// BEP-6 Fast Extension
	msgHaveAll       = 0x0E
	msgHaveNone      = 0x0F
	msgSuggestPiece  = 0x0D
	msgRejectRequest = 0x10
	msgAllowedFast   = 0x11

	// BEP-10 Extension Protocl
	msgExtended = 0x14
)

type swarmPeer struct {
	swarm Swarm
	addr  net.TCPAddr
	conn  *net.TCPConn
	karma int
	// TODO: peerId
	gotHandshake bool

	extensions struct {
		bep5dht struct {
			supported bool
		}
		bep10ExtensionsProtocol struct {
			supported bool
		}
		bep9MetadataExchange struct {
			supported bool
			id        uint8
		}
		utPex struct {
			supported bool
		}
	}
}

func (p *swarmPeer) String() string {
	return fmt.Sprintf("<swarmPeer %v:%v for %s>", p.addr.IP, p.addr.Port, p.swarm.InfoHash())
}

// If we're not already connected to this peer, block until we've opened a connection.
func (p *swarmPeer) connect() {
	if p.conn != nil {
		return
	}

	logger.Debug("Attempting to connect to %v for %v.", p, p.swarm)

	// TODO(jre): We really need to dial on the same port as we're
	// listening, but net doesn't directly let us do that.
	conn, err := net.DialTCP("tcp", nil, &p.addr)
	if err != nil {
		logger.Warning("Failed to connect to %v for %v: %v", p, p.swarm, err)
		return
	}

	logger.Debug("Sending handshake to %v...", p)
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

	onMdxMessage := func(message string) {
		// Now, we need to decode to the end of the dict, and then maybe there'll be
		// data too!

		data, remainder, err := bencoding.DecodeFirst([]byte(message))
		if err != nil {
			logger.Error("Error decoding message: %v", err)
			return
		}

		logger.Notice("got metadata exchange message %v + %v B", data, len(remainder))
	}

	// Called to handle each non-keepalive message.
	onMessage := func(messageType peerMessageType, body string) {
		switch messageType {

		case msgExtended:
			if len(body) == 0 {
				logger.Warning("got extension message with 0-length body -- what?!")
				return
			}

			extensionId := body[0]

			// THE BODY MAY NOT BE BENCODED, YOU GOOF

			// TODO: A more sensible generic way of handling extensions and extension mesages
			if extensionId == 0 {
				// Handshake!
				logger.Info("Got an extension handshake message")

				bencoded := body[1:]
				data, err := bencoding.Decode([]byte(bencoded))

				if err != nil {
					logger.Error("Error decoding message: %v", err)
					return
				}

				// Check if the peer supports metadata exchange
				if dataM, hasM := data.(bencoding.Dict)["m"]; hasM {
					if mdxIdP, hasMdx := dataM.(bencoding.Dict)["ut_metadata"]; hasMdx {
						mdxId := uint8(mdxIdP.(bencoding.Int))

						if mdxId != 0 {
							logger.Info("Peer %v supports metadata exchange, using extension ID %v.", p, mdxId)
							p.extensions.bep9MetadataExchange.supported = true
							p.extensions.bep9MetadataExchange.id = mdxId

							// TODO:
							go func() {
								requestBody, err := bencoding.Encode(bencoding.Dict{
									"msg_type": bencoding.Int(0), // request piece
									"piece":    bencoding.Int(0),
								})

								if err != nil {
									logger.Error("unable to encode extension handshake: %v", err)
									return
								}

								logger.Notice("requesting first piece of metadata!")
								writeMessage(p.conn, msgExtended, append([]byte{ourUtMetadataId}, requestBody...))
							}()
						} else {
							logger.Info("Peer %v does not support metadata exchange!", p)
							return
						}
					} else {
						logger.Info("Peer %v does not support metadata exchange!", p)
						return
					}
				} else {
					logger.Info("Peer %v does not support metadata exchange!", p)
					return
				}
			} else if p.extensions.bep9MetadataExchange.supported && extensionId == p.extensions.bep9MetadataExchange.id {
				onMdxMessage(body[1:])
			} else {
				logger.Warning("got extension message for unrecognied extension id %v from %v: %v", extensionId, p, body[1:])
			}

		case msgBitfield:
			logger.Warning("Got unsupported bitfield message from %v.", p)

		case msgChoke:
			logger.Warning("Got unsupported choke message from %v.", p)
			if len(body) != 0 {
				logger.Error("Choke message had %v B body -- should have been empty.", len(body))
			}

		case msgUnchoke:
			logger.Warning("Got unsupported unchoke message from %v.", p)
			if len(body) != 0 {
				logger.Error("Unchoke message had %v B body -- should have been empty.", len(body))
			}

		case msgInterested:
			logger.Warning("Got unsupported interested message from %v.", p)
			if len(body) != 0 {
				logger.Error("Interested message had %v B body -- should have been empty.", len(body))
			}

		case msgNotInterested:
			logger.Warning("Got unsupported not interested message from %v.", p)
			if len(body) != 0 {
				logger.Error("Unsuported message had %v B body -- should have been empty.", len(body))
			}

		case msgHave:
			logger.Warning("Got %v B unsupported have message from %v.", len(body), p)

		case msgRequest:
			logger.Warning("Got %v B unsupported request message from %v.", len(body), p)

		case msgPiece:
			logger.Warning("Got %v B unsupported piece message from %v.", len(body), p)

		case msgDhtPort:
			logger.Warning("Got %v B unsupported DHT port message from %v.", len(body), p)

		case msgHaveAll:
			logger.Warning("Got %v B unsupported have all message from %v.", len(body), p)

		case msgHaveNone:
			logger.Warning("Got %v B unsupported have none message from %v.", len(body), p)

		case msgSuggestPiece:
			logger.Warning("Got %v B unsupported suggest piece message from %v.", len(body), p)

		case msgRejectRequest:
			logger.Warning("Got %v B unsupported reject request message from %v.", len(body), p)

		case msgAllowedFast:
			logger.Warning("Got %v B unsupported allowed fast message from %v.", len(body), p)

		default:
			logger.Error("Got %v B message of unknown type 0x%02x.", len(body), messageType)
		}

	}

	// All unprocessed data. May contain multiple messages and/or span multiple chunks.
	unprocessedBuffer := make([]byte, 0)

	// Called to handle each chunk of data we get from the peer's connection.
	onChunk := func(chunk string) {
		unprocessedBuffer = append(unprocessedBuffer, []byte(chunk)...)

		// Process all complete messages in the buffer.
		for len(unprocessedBuffer) > 0 {
			if !p.gotHandshake {
				handshakeSize := 20 + 8 + 20 + 20

				if len(unprocessedBuffer) >= handshakeSize {
					handshake := unprocessedBuffer[:handshakeSize]

					if string(handshake[:len(peerProtocolHeader)]) != peerProtocolHeader {
						logger.Info("Peer protocol header missing for %v!", p)
						break
					}
					// TODO: veriffy the rest of the handshake

					unprocessedBuffer = unprocessedBuffer[handshakeSize:]
					p.gotHandshake = true
					continue
				} else {
					// Wait for the rest of the handshake.
					break
				}
			} else {
				if len(unprocessedBuffer) < 4 {
					// Too short to even have the length prefix.
					break
				}
				length := uint32(0)
				buf := bytes.NewBuffer([]byte(unprocessedBuffer[:4]))
				binary.Read(buf, binary.BigEndian, &length)

				logger.Debug("next message length is %v B, have %v B", length, len(unprocessedBuffer)-4)

				if uint32(len(unprocessedBuffer)) >= length+4 {
					message := unprocessedBuffer[4 : 4+length]
					unprocessedBuffer = unprocessedBuffer[4+length:]

					if length == 0 {
						logger.Info("Got keepalive message from %s.", p)
						continue
					}

					messageType := peerMessageType(message[0])
					messageBody := string(message[1:])
					onMessage(messageType, messageBody)
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

		if readLength > 0 {
			logger.Info("got chunk of %v bytes from %v", readLength, p)
			onChunk(string(chunkBuffer[:readLength]))
		}

		if err == io.EOF {
			logger.Info("Remote peer disconnected: %v", p)
			p.karma -= 1
			break
		} else if err != nil {
			logger.Info("Unknown error reading from %v: %v", p, err)
			time.Sleep(6 * time.Second)
		}
	}

}

const (
	extensionHandshakeId uint8 = iota
	ourUtMetadataId            = 3
	ourUtPexId                 = 4
)

func writeHandshake(w io.Writer, peerId BTID, infohash BTID) {
	// BitTorrent Handshake
	header := []byte(peerProtocolHeader)
	extensionFlags := make([]byte, 8)
	extensionFlags[5] |= 0x10 // indicate extension protocol support
	w.Write(header)
	w.Write(extensionFlags)
	w.Write([]byte(infohash))
	w.Write([]byte(peerId))

	// If we had any pieces, we would need to indicate so here, but we don't.
	// writeMessage(w, msgBitfield, piecesBitfield)

	// TODO: move this somewhere else and only fire it after we check their extension flags
	// Write Extension Protcol Handshake

	handshakeBody, err := bencoding.Encode(bencoding.Dict{
		"v": bencoding.String("jbitor 0.0.0"),
		"m": bencoding.Dict{
			"ut_metadata": bencoding.Int(ourUtMetadataId),
			"ut_pex":      bencoding.Int(ourUtPexId),
		},
		"p": bencoding.Int(PORT),
	})

	if err != nil {
		logger.Info("unable to encode extension handshake: %v", err)
		return
	}

	logger.Info("sent extension handshake")
	writeMessage(w, msgExtended, append([]byte{extensionHandshakeId}, handshakeBody...))
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
