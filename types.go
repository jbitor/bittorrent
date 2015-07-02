package bittorrent

import (
	"errors"
	"io"

	"github.com/jbitor/bencoding"
)

type TorrentMeta bencoding.Dict

func (metainfo TorrentMeta) WriteBencodedTo(writer io.Writer) error {
	if _, present := metainfo["name"].(bencoding.String); !present {
		return errors.New("invalid name")
	}
	if _, present := metainfo["piece length"].(bencoding.Int); !present {
		return errors.New("invalid piece length")
	}
	if _, present := metainfo["pieces"].(bencoding.String); !present {
		return errors.New("invalid pieces")
	}
	if _, present := metainfo["files"].(bencoding.List); !present {
		if _, present := metainfo["length"].(bencoding.Int); !present {
			return errors.New("invalid files or length")
		}
	}

	return bencoding.Dict(metainfo).WriteBencodedTo(writer)
}

func (metainfo TorrentMeta) ToJsonable() (interface{}, error) {
	return bencoding.Dict(metainfo).ToJsonable()
}

type TorrentFileMeta bencoding.Dict

func (filemeta TorrentFileMeta) WriteBencodedTo(writer io.Writer) error {
	if _, present := filemeta["path"].(bencoding.List); !present {
		return errors.New("invalid path")
	}
	if _, present := filemeta["length"].(bencoding.Int); !present {
		return errors.New("invalid length")
	}

	return bencoding.Dict(filemeta).WriteBencodedTo(writer)
}

func (filemeta TorrentFileMeta) ToJsonable() (interface{}, error) {
	return bencoding.Dict(filemeta).ToJsonable()
}
