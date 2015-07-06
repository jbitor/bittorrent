// Package dht provides an implementation of a "mainline" BitTorrent
// Distributed Hash Table (DHT) client, as specified in BEP 5
// (http://www.bittorrent.org/beps/bep_0005.html), and a higher-level
// client interface for querying the DHT.
package dht

var bootstrapAddresses = []string{
	"router.bittorrent.com:6881",
	"dht.transmissionbt.com:6881",
}
