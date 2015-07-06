package dht

import (
	"errors"
	"io/ioutil"
	weakrand "math/rand"
	"os"
	"syscall"
	"time"

	"github.com/jbitor/bencoding"
	"github.com/jbitor/bittorrent"
)

// Any DHT queries sent to another node will time out after this long.
const QUERY_TIMEOUT = 16 * time.Second

// Information about how a client/node is connected to the DHT network.
type ConnectionInfo struct {
	GoodNodes    int
	UnknownNodes int
	BadNodes     int
}

/*
Client provides a high-level interface for interacting with the DHT.
Once a client has been opened it will continue to run asynchronously until it is closed.
*/
type Client interface {
	// Close release the client's port and unlocks its data file, leaving it unusable.
	Close() (err error)

	// Save saves the current state of the DHT to its data file.
	Save() (err error)

	// GetPeers attempts to find remote torrent peers downloading a given bittorrent.
	GetPeers(infoHash bittorrent.BTID) (search *GetPeersSearch)

	ConnectionInfo() ConnectionInfo

	Id() bittorrent.BTID
}

/*
localNodeClient implements the Client interface for this package.
*/
type localNodeClient struct {
	// The localNode instance being used by this client.
	// This will be nil if the client is not open.
	*localNode

	// The termination signal channel for the localNode.
	terminateLocalNode chan<- bool

	// The data file we read/write the node state from/to.
	openDataFile *os.File

	// A single value will be sent into this chanel when the client is
	// terminated. If the value is not nil, it will be a fatal error that
	// caused the client to be terminated.
	terminated <-chan error
}

/*
OpenClient instantiates a client whose state will be persisted at the specified path.

Existing state will be loaded if it exists, otherwise a new client will
be generated using a node a randomly-selected ID and port.

A filesystem lock will be used to ensure that only one Client may be open with
a given path at a time. The blocking parameter determines whether we block or
return an error when another Client is using the path.
*/
func OpenClient(path string, blocking bool) (c Client, err error) {
	var (
		openDataFile   *os.File
		nodeData       []byte
		nodeDict       bencoding.Bencodable
		nodeDictAsDict bencoding.Dict
		ok             bool
		lc             *localNodeClient
	)

	lc = new(localNodeClient)

	openDataFile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	lc.openDataFile = openDataFile

	flockMode := syscall.LOCK_EX

	if !blocking {
		flockMode |= syscall.LOCK_NB
	}

	err = syscall.Flock(int(openDataFile.Fd()), flockMode)
	if err != nil {
		return
	}

	nodeData, err = ioutil.ReadAll(lc.openDataFile)
	if err != nil {
		logger.Info("Unable to read existing DHT node file (%v). Creating a new one.", err)
		lc.localNode = newLocalNode()
	} else if len(nodeData) == 0 {
		logger.Info("Existing DHT node file was empty. Creating a new one.")
		lc.localNode = newLocalNode()
	} else {
		nodeDict, err = bencoding.Decode(nodeData)
		if err != nil {
			openDataFile.Close()
			return
		}

		nodeDictAsDict, ok = nodeDict.(bencoding.Dict)
		if !ok {
			err = errors.New("Node data wasn't a dict.")
			logger.Info("%v", err)
			openDataFile.Close()
			return
		}

		lc.localNode = localNodeFromBencodingDict(nodeDictAsDict)
		logger.Info("Loaded local node info from %v.", path)
	}

	terminateLocalNode := make(chan bool)
	lc.terminateLocalNode = terminateLocalNode

	c = Client(lc)

	err = lc.Run(terminateLocalNode)
	if err != nil {
		return
	}

	go func() {
		for lc.localNode != nil {
			c.Save()
			time.Sleep(15 * time.Second)
		}
	}()

	return
}

func (c *localNodeClient) Id() (id bittorrent.BTID) {
	if c.localNode != nil {
		return c.localNode.Id
	}
	// TODO(JB): is this stupid?
	return bittorrent.BTID(0)
}

func (c *localNodeClient) Close() (err error) {
	if c.localNode == nil {
		return errors.New("dht.Client is not open.")
	}

	err = c.Save()

	_ = c.openDataFile.Close()
	c.openDataFile = nil

	_ = c.localNode.Connection.Close()
	c.localNode = nil

	return
}

func (c *localNodeClient) Save() (err error) {
	var (
		nodeData []byte
	)

	if c.localNode == nil {
		err = errors.New("Client is closed.")
		return
	}

	nodeData, err = bencoding.Encode(c.localNode)
	if err != nil {
		return
	}

	err = c.openDataFile.Truncate(0)
	if err != nil {
		return
	}

	_, err = c.openDataFile.WriteAt(nodeData, 0)
	if err != nil {
		return
	}

	err = c.openDataFile.Sync()
	if err != nil {
		return
	}

	logger.Info("Saved DHT client state.")

	return
}

func (c *localNodeClient) GetPeers(target bittorrent.BTID) (s *GetPeersSearch) {
	return newGetPeersSearch(target, c.localNode)
}

func (c *localNodeClient) ConnectionInfo() ConnectionInfo {
	info := ConnectionInfo{GoodNodes: 0, UnknownNodes: 0, BadNodes: 0}

	for _, node := range c.localNode.Nodes {
		switch node.Status() {
		case STATUS_UNKNOWN:
			info.UnknownNodes++
		case STATUS_GOOD:
			info.GoodNodes++
		case STATUS_BAD:
			info.BadNodes++
		}
	}

	return info
}

func (c *localNodeClient) Run(terminate <-chan bool) (err error) {
	terminateLocalNode := make(chan bool)
	terminateNodeListMaintenance := make(chan bool)

	err = c.localNode.Run(terminateLocalNode)
	if err != nil {
		return
	}

	go func() {
		c.nodeListMaintenanceLoop(terminateNodeListMaintenance)
	}()

	go func() {
		<-terminate
		close(terminateLocalNode)
		close(terminateNodeListMaintenance)
	}()

	return
}

func (c *localNodeClient) nodeListMaintenanceLoop(terminate <-chan bool) {
	for {

		info := c.ConnectionInfo()

		logger.Info("localNode running with %v good nodes (%v unknown and %v bad).",
			info.GoodNodes, info.UnknownNodes, info.BadNodes)

		c.pingRandomNode()
		c.requestMoreNodes()

		time.Sleep(15 * time.Second)

		select {
		case _ = <-terminate:
			break
		default:
		}
	}
}

func (local *localNode) pingRandomNode() {
	var randNode *RemoteNode
	randNodeOffset := weakrand.Intn(len(local.Nodes))
	i := 0

	for _, node := range local.Nodes {
		if i == randNodeOffset {
			randNode = node
			break
		}
		i++
	}

	logger.Info("Pinging a random node: %v.", randNode)

	resultChan, errChan := local.Ping(randNode)

	timeoutChan := make(chan error)
	go func() {
		time.Sleep(10 * time.Second)
		timeoutChan <- errors.New("ping timed out")
		close(timeoutChan)
	}()

	select {
	case _ = <-resultChan:
		logger.Info("Successfully pinged %v.", randNode)

	case err := <-errChan:
		logger.Info("Failed to ping %v: %v.", randNode, err)

	case err := <-timeoutChan:
		logger.Info("Failed to ping %v: %v.", randNode, err)
	}
}

// TODO(JB): Make this a client method, and add a saving loop to it.
func (local *localNode) requestMoreNodes() {
	var randNode *RemoteNode
	randNodeOffset := weakrand.Intn(len(local.Nodes))
	i := 0

	for _, node := range local.Nodes {
		if i == randNodeOffset {
			randNode = node
			break
		}
		i++
	}

	target := bittorrent.WeakRandomBTID()

	logger.Info("Requesting new nodes around %v from %v.", target, randNode)

	resultChan, errChan := local.FindNode(randNode, target)

	timeoutChan := make(chan error)
	go func() {
		time.Sleep(10 * time.Second)
		timeoutChan <- errors.New("find nodes timed out")
		close(timeoutChan)
	}()

	select {
	case _ = <-resultChan:
		logger.Info("Successfully find nodes from %v.", randNode)

	case err := <-errChan:
		logger.Info("Failed to find nodes from %v: %v.", randNode, err)

	case err := <-timeoutChan:
		logger.Info("Failed to find nodes from %v: %v.", randNode, err)
	}
}
