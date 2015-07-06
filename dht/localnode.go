package dht

import (
	"fmt"
	"io"
	"net"
	"sort"

	"github.com/jbitor/bencoding"
	"github.com/jbitor/bittorrent"
)

const PORT = 6882

/*
localNodes is a DHT node implementation. Currently, it only supports the
client components of a node -- it does not maintain a proper routing table
and cannot respond to queries.
*/
type localNode struct {
	Id                 bittorrent.BTID
	Port               int
	Connection         *net.UDPConn
	Nodes              map[string]*RemoteNode
	OutstandingQueries map[string]*RpcQuery
}

func newLocalNode() (local *localNode) {
	id, err := bittorrent.SecureRandomBTID()
	if err != nil {
		// You used up all the entropy!
		panic(err)
	}

	local = new(localNode)
	local.Id = id
	local.Port = PORT
	local.OutstandingQueries = make(map[string]*RpcQuery)
	local.Nodes = map[string]*RemoteNode{}

	for _, addrString := range bootstrapAddresses {
		addr, err := net.ResolveUDPAddr("udp", addrString)
		if err == nil {
			local.AddOrGetRemoteNode(&RemoteNode{
				Address:       *addr,
				BootstrapOnly: true,
			})
		}
	}

	return local
}

func (local *localNode) AddOrGetRemoteNode(remote *RemoteNode) *RemoteNode {
	// If a node with the same address is already in .Nodes, returns that node.
	// Otherwise, add remote to .Nodes and return it.

	key := RemoteNodeKey(remote.Address)

	if existingRemote, ok := local.Nodes[key]; ok {
		remote = existingRemote
	} else {
		local.Nodes[key] = remote
	}

	return remote
}

type nodeOrderingByCloseness struct {
	target    bittorrent.BTID
	nodes     []*RemoteNode
	distances [][5]uint32
}

func (local *localNode) nodeOrderingByClosenessFromTarget(target bittorrent.BTID) (ordering nodeOrderingByCloseness) {
	ordering.target = target
	ordering.nodes = make([]*RemoteNode, len(local.Nodes))
	ordering.distances = make([][5]uint32, len(local.Nodes))

	i := 0
	for _, remoteNode := range local.Nodes {
		ordering.nodes[i] = remoteNode
		ordering.distances[i] = remoteNode.Id.XoredUint32s(target)
		i++
	}

	return ordering
}

func (ordering nodeOrderingByCloseness) Len() int {
	return len(ordering.nodes)
}

func (ordering nodeOrderingByCloseness) Swap(i, j int) {
	tmpNode := ordering.nodes[i]
	ordering.nodes[i] = ordering.nodes[j]
	ordering.nodes[j] = tmpNode

	tmpDistance := ordering.distances[i]
	ordering.distances[i] = ordering.distances[j]
	ordering.distances[j] = tmpDistance
}

// Orders .nodes by closeness to .target. Bootstrap nodes sort after non-bootstrap nodes.
func (ordering nodeOrderingByCloseness) Less(i, j int) bool {
	iNode, jNode := ordering.nodes[i], ordering.nodes[j]
	iDist, jDist := ordering.distances[i], ordering.distances[j]

	return (!iNode.BootstrapOnly && jNode.BootstrapOnly || iNode.BootstrapOnly == jNode.BootstrapOnly) &&
		(iDist[0] < jDist[0] || iDist[0] == jDist[0] &&
			(iDist[1] < jDist[1] || iDist[1] == jDist[1] &&
				(iDist[2] < jDist[2] || iDist[2] == jDist[2] &&
					(iDist[3] < jDist[3]))))

}

// Returns a slice of known queryable *RemoteNodes.
// If bootstrap nodes are included, they will sort after *all* non-bootstrap nodes.
func (local *localNode) NodesByCloseness(target bittorrent.BTID, includeBootstrap bool) (nodes []*RemoteNode) {
	ordering := local.nodeOrderingByClosenessFromTarget(target)
	sort.Sort(ordering)

	if !includeBootstrap {
		nodes = make([]*RemoteNode, 0, len(ordering.nodes))

		for _, node := range ordering.nodes {
			if !node.BootstrapOnly {
				nodes = append(nodes, node)
			}
		}
	} else {
		nodes = ordering.nodes[:]
	}

	return nodes
}

func (local *localNode) String() string {
	return fmt.Sprintf("<localNode %v on :%v>", local.Id, local.Port)
}

func RemoteNodeKey(addr net.UDPAddr) string {
	return fmt.Sprintf("%v:%v", addr.IP, addr.Port)
}

// Bencoding

func localNodeFromBencodingDict(dict bencoding.Dict) (local *localNode) {
	local = new(localNode)

	local.Id = bittorrent.BTID(dict["Id"].(bencoding.String))
	local.Port = int(dict["Port"].(bencoding.Int))
	local.OutstandingQueries = make(map[string]*RpcQuery)
	local.Nodes = map[string]*RemoteNode{}

	for _, nodeDict := range dict["Nodes"].(bencoding.List) {
		remote := RemoteNodeFromBencodingDict(nodeDict.(bencoding.Dict))
		local.AddOrGetRemoteNode(remote)
	}

	return local
}

func (local *localNode) MarshalBencodingDict() (dict bencoding.Dict) {
	dict = bencoding.Dict{}

	if local.Id != "" {
		dict["Id"] = bencoding.String(local.Id)
	}

	dict["Port"] = bencoding.Int(local.Port)

	nodes := make(bencoding.List, len(local.Nodes))

	i := 0
	for _, node := range local.Nodes {
		nodes[i] = node.MarshalBencodingDict()
		i++
	}

	dict["Nodes"] = nodes

	return dict
}

func (local *localNode) WriteBencodedTo(writer io.Writer) error {
	dict := local.MarshalBencodingDict()

	return dict.WriteBencodedTo(writer)
}

func (local *localNode) ToJsonable() (interface{}, error) {
	return bencoding.Dict(local.MarshalBencodingDict()).ToJsonable()
}

// Running

func (local *localNode) Run(terminate <-chan bool) (err error) {
	var conn *net.UDPConn
	// Main loop for LocalPeer's activity.
	// (Listening to replies and requests.)

	conn, err = net.ListenUDP("udp4", &net.UDPAddr{
		IP:   net.IPv4(0, 0, 0, 0),
		Port: local.Port,
	})
	if err != nil {
		return
	}

	local.Connection = conn

	rpcTerminate := make(chan bool)
	go func() {
		local.rpcListenLoop(rpcTerminate)
	}()

	go func() {
		<-terminate

		close(rpcTerminate)
		conn.Close()

	}()

	return
}
