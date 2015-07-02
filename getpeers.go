package dht

import (
	"errors"
	"time"

	"github.com/jbitor/bittorrent"
)

type GetPeersOptions struct {
	// The search isn't finished until
	TargetPeers      int           // [we've found at least TargetPeers and
	MinNodes         int           // we've contacted at least MinNodes] or
	MaxNodes         int           // we've contacted at least MaxNodes or
	Timeout          time.Duration // this much time has passed.
	MinQueryInterval time.Duration // We'll send queries at least this far apart.
}

var GetPeersDefaultOptions = GetPeersOptions{
	TargetPeers:      8,
	MinNodes:         4,
	MaxNodes:         32,
	Timeout:          10 * time.Minute,
	MinQueryInterval: 5 * time.Second,
}

type GetPeersSearch struct {
	Infohash           bittorrent.BTID
	Options            GetPeersOptions
	QueriedNodes       map[string]*RemoteNode
	PeersFound         map[string]*bittorrent.RemotePeer
	OutstandingQueries map[string]*RpcQuery
	StartTime          time.Time

	finished    bool
	localNode   *localNode
	peerReaders []chan<- []*bittorrent.RemotePeer
}

func newGetPeersSearch(target bittorrent.BTID, localNode_ *localNode) (s *GetPeersSearch) {
	s = &GetPeersSearch{
		Infohash:           target,
		Options:            GetPeersDefaultOptions,
		StartTime:          time.Now(),
		localNode:          localNode_,
		QueriedNodes:       make(map[string]*RemoteNode),
		PeersFound:         make(map[string]*bittorrent.RemotePeer),
		OutstandingQueries: make(map[string]*RpcQuery),
		peerReaders:        make([]chan<- []*bittorrent.RemotePeer, 0),
	}

	go s.run()

	return s
}

// The main loop of a GetPeers search.
func (s *GetPeersSearch) run() {
	for !s.Finished() {
		nodes := s.localNode.NodesByCloseness(s.Infohash, false)

		var remote *RemoteNode = nil
		for _, candidate := range nodes {
			// XXX(JB): .String() is not a clean way to do this
			if _, present := s.QueriedNodes[candidate.Address.String()]; present {
				continue // already queried this node for this search
			}

			if candidate.Flooded() {
				continue
			}

			remote = candidate
			s.QueriedNodes[remote.Address.String()] = remote
			break
		}

		if remote != nil {
			logger.Printf("Request peers for %v from %v.\n", s.Infohash, remote)

			go func() {
				peersResult, nodesResult, errorResult := s.localNode.GetPeers(remote, s.Infohash)

				select {
				case peers := <-peersResult:
					logger.Printf("Got peers from %v", remote)

					newPeers := make([]*bittorrent.RemotePeer, 0)

					for _, peer := range peers {
						// XXX(JB): .String() is not a clean way to do this

						if _, present := s.PeersFound[peer.Address.String()]; !present {
							newPeers = append(newPeers, peer)
							s.PeersFound[peer.Address.String()] = peer
						}
					}

					if len(newPeers) > 0 {
						logger.Printf("Got %v new peers.\n", len(newPeers))

						for _, c := range s.peerReaders {
							c <- newPeers
						}
					} else {
						logger.Printf("Got no new peers.\n")
					}
				case _ = <-nodesResult:
					// nothing to do -- nodes will already have been recorded
				case err := <-errorResult:
					logger.Printf("Error response to GetPeers: %v\n", err)
				}
			}()
		}

		time.Sleep(s.Options.MinQueryInterval)
	}
}

// Immediately causes the search to be finished.
// Any incomplete queries will be discarded.
func (s *GetPeersSearch) Terminate() {
	s.finished = true
}

// Whether this search is finished, with a result that will not change.
func (s *GetPeersSearch) Finished() bool {
	if s.finished {
		return s.finished
	}

	s.finished =
		time.Now().After(s.StartTime.Add(s.Options.Timeout)) ||
			(len(s.OutstandingQueries) == 0 &&
				len(s.QueriedNodes) >= s.Options.MinNodes &&
				(len(s.PeersFound) >= s.Options.TargetPeers ||
					len(s.QueriedNodes) >= s.Options.MaxNodes))

	if s.finished {
		for _, c := range s.peerReaders {
			close(c)
		}
	}

	return s.finished
}

// Returns a channel which is notified each time new
// peers are recieved, and is closed when the request is finished.
func (s *GetPeersSearch) ReadNewPeers() (peersSource <-chan []*bittorrent.RemotePeer) {
	peersSourceDuplex := make(chan []*bittorrent.RemotePeer)

	if s.Finished() {
		close(peersSourceDuplex)
	} else {
		s.peerReaders = append(s.peerReaders, peersSourceDuplex)
	}

	return peersSourceDuplex
}

// Blocks until we have any peers, then returns all known peers.
// Returns an error if the request fails to find any peers.
func (s *GetPeersSearch) AnyPeers() (peers []*bittorrent.RemotePeer, err error) {
	c := s.ReadNewPeers()
	_, _ = <-c

	if len(s.PeersFound) > 0 {
		peers = make([]*bittorrent.RemotePeer, 0)
		for _, peer := range s.PeersFound {
			peers = append(peers, peer)
		}
		return peers, nil
	} else {
		return nil, errors.New("No nodes found.")
	}
}

// Blocks until the request is finished, then returns all known peers.
// Returns an error if the request fails to find any peers.
func (s *GetPeersSearch) AllPeers() (peers []*bittorrent.RemotePeer, err error) {
	c := s.ReadNewPeers()
	ok := true
	for ok {
		_, ok = <-c
	}

	if len(s.PeersFound) > 0 {
		peers = make([]*bittorrent.RemotePeer, 0)
		for _, peer := range s.PeersFound {
			peers = append(peers, peer)
		}
		return peers, nil
	} else {
		return nil, errors.New("No nodes found.")
	}
}
