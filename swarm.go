package bittorrent

import (
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
				defer conn.Close()

				logger.Printf("Successfully connected to peer %v.\n", peer)

				// TODO: save this
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

				logger.Printf("Sending handshake to %v", peer)

				conn.SetWriteDeadline(time.Now().Add(8 * time.Second))
				writeHandshake(conn, peerId, s.infoHash)

				// logger.Printf("Sending keepalive to %v", peer)
				// writeKeepAlive(conn)

				data := make([]byte, 64)

				conn.SetReadDeadline(time.Now().Add(12 * time.Second))

				for {
					_, err = conn.Read(data)
					if err != nil {
						logger.Printf("Failed to get reply from %v: %v", peer, err)
						time.Sleep(1 * time.Second)
						continue
					}

					logger.Printf("got a reply:\n%v\n", data)
					return
				}
			}()
		}(peer)

		time.Sleep(1 * time.Second)
	}

	wg.Done()
	wg.Wait()

	return
}
