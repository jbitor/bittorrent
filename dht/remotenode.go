package dht

import (
	"fmt"
	"net"
	"time"

	"github.com/jbitor/bencoding"
	"github.com/jbitor/bittorrent"
)

type RemoteNode struct {
	Id      bittorrent.BTID // may be unknown
	Address net.UDPAddr

	LastRequestTo    time.Time
	LastResponseTo   time.Time
	LastResponseFrom time.Time
	LastRequestFrom  time.Time

	ConsecutiveFailedQueries int

	// If specified, Source identifies the RemoteNode which gave us this Node.
	// This could be used to identify a Source which provides us with many
	// bad nodes.
	Source *RemoteNode

	// BootstrapOnly indicates that this is a bootstrap node, and should only
	// be used in order to find nodes when few are known. It should never be
	// used for queries.
	BootstrapOnly bool
}

func RemoteNodeFromAddress(address net.UDPAddr) (remote *RemoteNode) {
	// Creates a RemoteNode with a known address but an unknown ID.
	// You may want to .SendPing() this node so that it learns its ID!
	remote = new(RemoteNode)
	remote.Id = ""
	remote.Address = address

	// default values are fine for other fields

	return remote
}

func RemoteNodeFromBencodingDict(dict bencoding.Dict) (remote *RemoteNode) {
	remote = new(RemoteNode)

	remote.Id = bittorrent.BTID(dict["Id"].(bencoding.String))
	remote.Address = decodeNodeAddress(dict["Address"].(bencoding.String))
	remote.LastRequestTo = time.Unix(int64(dict["LastRequestToSec"].(bencoding.Int)), 0)
	remote.LastResponseTo = time.Unix(int64(dict["LastResponseToSec"].(bencoding.Int)), 0)
	remote.LastRequestFrom = time.Unix(int64(dict["LastRequestFromSec"].(bencoding.Int)), 0)
	remote.LastResponseFrom = time.Unix(int64(dict["LastResponseFromSec"].(bencoding.Int)), 0)
	remote.ConsecutiveFailedQueries = int(dict["ConsecutiveFailedQueries"].(bencoding.Int))
	remote.BootstrapOnly = 0 != int(dict["BootstrapOnly"].(bencoding.Int))

	return remote
}

func (remote *RemoteNode) MarshalBencodingDict() (dict bencoding.Dict) {
	var bootstrapOnly bencoding.Int
	if remote.BootstrapOnly {
		bootstrapOnly = 1
	} else {
		bootstrapOnly = 0
	}

	dict = bencoding.Dict{
		"Id": bencoding.String(remote.Id),

		"Address": encodeNodeAddress(remote.Address),

		"LastRequestToSec":    bencoding.Int(remote.LastRequestTo.Unix()),
		"LastResponseToSec":   bencoding.Int(remote.LastResponseTo.Unix()),
		"LastRequestFromSec":  bencoding.Int(remote.LastRequestFrom.Unix()),
		"LastResponseFromSec": bencoding.Int(remote.LastResponseFrom.Unix()),

		"ConsecutiveFailedQueries": bencoding.Int(remote.ConsecutiveFailedQueries),

		"BootstrapOnly": bootstrapOnly,
	}

	return dict
}

func (remote *RemoteNode) String() string {
	var typeSuffix string

	if remote.BootstrapOnly {
		typeSuffix = " (bootstrap-only)"
	} else {
		typeSuffix = ""
	}

	return fmt.Sprintf("<RemoteNode%s %v (%v) at %v:%v>",
		typeSuffix, remote.Id, remote.Status(), remote.Address.IP, remote.Address.Port)
}

func (remote *RemoteNode) Status() RemoteNodeStatus {
	switch {
	case remote.ConsecutiveFailedQueries >= 3:
		return STATUS_BAD
	case remote.LastResponseFrom.IsZero():
		return STATUS_UNKNOWN
	case time.Since(remote.LastResponseFrom) < time.Minute*15:
		return STATUS_GOOD
	case time.Since(remote.LastRequestFrom) < time.Minute*15:
		return STATUS_GOOD
	default:
		return STATUS_UNKNOWN
	}
}

// The minimum time that must elapse before we will send an additional
// query to the same node.
const MinNodeQueryInterval = 60 * time.Second

// Indicate whether we have sent a query to this node too recently
// to send another one now.
func (remote *RemoteNode) Flooded() bool {
	return time.Since(remote.LastRequestTo) < MinNodeQueryInterval
}

type RemoteNodeStatus int

const (
	STATUS_UNKNOWN RemoteNodeStatus = iota
	STATUS_GOOD
	STATUS_BAD
)

func (status RemoteNodeStatus) String() string {
	switch status {
	case STATUS_UNKNOWN:
		return "STATUS_UNKNOWN"
	case STATUS_GOOD:
		return "STATUS_GOOD"
	case STATUS_BAD:
		return "STATUS_BAD"
	default:
		panic("invalid status value")
	}
}
