package bittorrent

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"

	"github.com/jbitor/bencoding"
)

type Swarm interface {
	// The infohash of the torrent.
	Id() (infoHash BTID)

	// Returns the torrent's info dictionary. Blocks until
	// it's been downloaded from peers.
	GetInfo() (info *bencoding.Dict)

	AddPeer(peer *RemotePeer)
}

type swarm struct {
	infoHash BTID
	peers    []*RemotePeer
}

func OpenSwarm(infoHash BTID) (s Swarm) {
	sd := &swarm{infoHash, make([]*RemotePeer, 0)}
	s = Swarm(sd)
	return
}

func (s *swarm) Id() (infoHash BTID) {
	return s.infoHash
}

func (s *swarm) AddPeer(peer *RemotePeer) {
	s.peers = append(s.peers, peer)
	return
}

func (s *swarm) GetInfo() (info *bencoding.Dict) {
	peerIdData := []byte(WeakRandomBTID())
	peerIdData[0] = byte("-"[0])
	peerIdData[1] = byte("J"[0])
	peerIdData[2] = byte("B"[0])
	peerIdData[3] = byte("0"[0])
	peerIdData[4] = byte("0"[0])
	peerIdData[5] = byte("0"[0])
	peerIdData[6] = byte("0"[0])
	peerIdData[7] = byte("-"[0])
	peerId := BTID(peerIdData)

	var wg sync.WaitGroup

	wg.Add(1)

	for _, peer := range s.peers {
		func(peer *RemotePeer) {
			wg.Add(1)
			go func() {
				defer wg.Done()

				logger.Printf("Connecting to peer %v.\n", peer)

				conn, err := net.DialTCP("tcp", nil, &peer.Address)
				if err != nil {
					logger.Printf("Failed to connect to %v: %v", peer, err)
					return
				}
				// conn.Close()

				logger.Printf("Successfully connected to peer %v.\n", peer)

				logger.Printf("Sending handshake to %v", peer)
				writeHandshake(conn, peerId, s.infoHash)

				// Send keepalives every couple minutes
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						logger.Printf("Sending keepalive to %v", peer)
						writeKeepAlive(conn)
						time.Sleep(120 * time.Second)
					}
				}()

				// Read incoming messages
				wg.Add(1)
				go func() {
					length := int32(0)
					defer wg.Done()

					gotHandshake := false

					handleChunk := func(chunk string) {
						// XXX: This is treating a chunk as a full message. Very bad.
						logger.Printf("got chunk from %v, length %v", peer, len(chunk))

						if !gotHandshake {
							if chunk[0:len(peerProtocolHeader)] != peerProtocolHeader {
								logger.Printf("Peer protocol header missing!")
							}

							gotHandshake = true

							logger.Printf("got handshake: %v", chunk)
						} else {
							if len(chunk) < 4 {
								logger.Printf("ignoring too-short chunk")
								return
							}
							buf := bytes.NewBuffer([]byte(chunk[0:4]))
							binary.Read(buf, binary.BigEndian, &length)
							logger.Printf("message length: %v", length)

							if length < 0 {
								logger.Printf("got -length message...")
								return
							} else if length == 0 {
								logger.Printf("Got keepalive.")
								return
							} else if int32(len(chunk))-4 < length {
								logger.Printf("Ignoring too-long (chunked?) message")
								return
							}

							msgType := messageType(chunk[4])
							body := chunk[5 : 5+length-1]

							logger.Printf("got message type/body: %v/%v", msgType, body)

							switch msgType {
							case msgExtended:
								extendedType := body[0]
								if extendedType != 0 {
									logger.Printf("Got unsupported extension message type: %v", extendedType)
									return
								}
								bencoded := body[1:len(body)]
								logger.Printf("Got an extension handshake message")
								data, err := bencoding.Decode([]byte(bencoded))
								logger.Printf("error/message: %v/%v", err, data)
							default:
								logger.Printf("Got unsupported message type: %v", msgType)
							}
						}
					}

					buffer := make([]byte, 32768)
					for {
						readLength, err := conn.Read(buffer)

						if err != nil && err != io.EOF {
							logger.Printf("Error reading from %v: %v", peer, err)
							time.Sleep(6 * time.Second)
						} else if readLength > 0 {
							handleChunk(string(buffer[0:readLength]))
						} else {
							logger.Printf("Got zero-length no-error read() from %v", peer)
							time.Sleep(6 * time.Second)
						}
					}
				}()
			}()
		}(peer)

		time.Sleep(1 * time.Second)
	}

	wg.Done()
	wg.Wait()

	return
}
