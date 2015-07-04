package bittorrent

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/jbitor/bencoding"
)

// BitTorrent Client
type Client interface {
	PeerId() BTID
	// Returns a connection to a Swarm() for a given torrent, starting one if
	// none exists.
	Swarm(infoHash BTID) Swarm
}

type client struct {
	peerId   BTID
	listener *net.TCPListener
	swarms   map[BTID]Swarm
}

const PORT = 6881

// Opens a new client, listening for incoming connections.
func OpenClient() Client {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{Port: PORT})
	if err != nil {
		logger.Fatalf("Client is unable to listen for peer connections: %v", err)
	}

	c := &client{
		peerId:   genPeerId(),
		listener: listener,
		swarms:   make(map[BTID]Swarm),
	}
	go c.listen()
	return Client(c)
}

func (c *client) PeerId() BTID {
	return c.peerId
}

// Information and connections related for a specific torrent.
func (c *client) Swarm(infoHash BTID) Swarm {
	s := swarm{
		infoHash: infoHash,
		// TODO: map, deduplication:
		peers:  make([]*swarmPeer, 0),
		client: c,
		info:   nil,
	}
	return Swarm(&s)
}

// Listen for incoming connections, handing them off to the appropriate
// Swarm() instances.
//
// XXX: This isn't likely to be used yet, since we're using a different
// outgoing port, adn aren't publishing ourselves anywhere.
// PS: Actually, BEP-10 allows you to share your listening port with the
// other peer.
func (c *client) listen() {
	for {
		conn, err := c.listener.AcceptTCP()
		if err != nil {
			logger.Info("Error accepting new connection: %v", err)
		} else {
			logger.Info("Got incoming peer connection. Not implemented! Closing.")
			conn.Close()
		}
	}
}

// Generates a ranndom peer ID, using a prefix identifying the client.
func genPeerId() (peerId BTID) {
	peerIdData := []byte(WeakRandomBTID())
	peerIdData[0] = byte("-"[0])
	peerIdData[1] = byte("J"[0])
	peerIdData[2] = byte("B"[0])
	peerIdData[3] = byte("0"[0])
	peerIdData[4] = byte("0"[0])
	peerIdData[5] = byte("0"[0])
	peerIdData[6] = byte("0"[0])
	peerIdData[7] = byte("-"[0])
	peerId = BTID(peerIdData)
	return
}

type Swarm interface {
	Client() Client
	// The info-hash of the torrent this swarm is for.
	InfoHash() BTID
	// Returns the torrent metainfo dictionary, blocking until it's available.
	Info() (info bencoding.Dict)
	// Returns whether the torrent's info has been downloaded yet.
	HasInfo() bool
	// Attempts to set the info for this torrent, returning an error if it's not valid.
	SetInfo(info []byte) (err error)
	// Adds a new address to the list of known peers.
	AddPeer(addr net.TCPAddr)
}

type swarm struct {
	client   Client
	infoHash BTID
	peers    []*swarmPeer
	info     bencoding.Dict
}

func (s *swarm) String() string {
	return fmt.Sprintf("<swarm %s>", s.InfoHash())
}

func (s *swarm) InfoHash() (infoHash BTID) {
	return s.infoHash
}

func (s *swarm) HasInfo() bool {
	return s.info != nil
}

func (s *swarm) SetInfo(info []byte) (err error) {
	hashData := sha1.Sum(info)
	hash := string(hashData[:])
	if s.InfoHash() == BTID(hash) {
		logger.Info("Validated full info for torrent!")
		info, _ := bencoding.Decode(info)
		s.info = info.(bencoding.Dict)
		logger.Notice("Got info for torrent %v: %v", s.infoHash, s.info)
		return nil
	} else {
		logger.Error("Infohash invalid: %v expected != %v actual", s.InfoHash(), BTID(hash))
		return errors.New("info hash was invalid")
	}
}

func (s *swarm) Client() Client {
	return s.client
}

func (s *swarm) AddPeer(addr net.TCPAddr) {
	s.peers = append(s.peers, &swarmPeer{
		swarm:        Swarm(s),
		addr:         addr,
		karma:        0,
		conn:         nil,
		gotHandshake: false,
	})
}

func (s *swarm) Info() bencoding.Dict {
	logger.Info("Attempting to Info for %s", s)
	s.connectToAll()

	logger.Info("Finished connecting to peers. Polling until we have all of the info.")

	for !s.HasInfo() {
		time.Sleep(time.Second)
	}

	return s.info
}

// Blocks until we've attempted to connect to all available peers.
func (s *swarm) connectToAll() {
	logger.Info("Attempting to connect all peers for %v.", s)
	for _, peer := range s.peers {
		peer.connect()
	}
}
