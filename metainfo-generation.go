package bittorrent

import (
	"github.com/jbitor/bencoding"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

type CreationOptions struct {
	Path           string
	PieceLength    int64
	ForceMultiFile bool
}

func GenerateTorrentMetaInfo(options CreationOptions) (TorrentMeta, error) {
	fileInfo, err := os.Stat(options.Path)
	if err != nil {
		return nil, err
	}

	multiFile := fileInfo.IsDir() || options.ForceMultiFile

	pieces := make([]byte, 0)

	fileList := bencoding.List{}

	pieceHasher := newPieceHasher(options.PieceLength)

	if multiFile {
		err := filepath.Walk(options.Path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			relPath, err := filepath.Rel(options.Path, path)
			if err != nil {
				return err
			}

			pathList := bencoding.List{}

			for _, component := range filepath.SplitList(relPath) {
				pathList = append(pathList, bencoding.String(component))
			}

			fileDict := TorrentFileMeta{
				"path":   pathList,
				"length": bencoding.Int(info.Size()),
			}

			data, err := ioutil.ReadFile(path)
			if err != nil {
				return err
			}

			pieceHasher.Write(data)

			fileList = append(fileList, fileDict)

			return nil
		})

		if err != nil {
			return nil, err
		}
	} else {
		if fileInfo.Size() > 0 {
			file, err := os.Open(options.Path)
			if err != nil {
				return nil, err
			}

			defer file.Close()
			io.Copy(pieceHasher.Writer(), file)
		}
	}

	pieces = pieceHasher.Pieces()

	infoDict := TorrentMeta{
		"name":         bencoding.String(fileInfo.Name()),
		"piece length": bencoding.Int(options.PieceLength),
		"pieces":       bencoding.String(pieces),
	}

	if multiFile {
		infoDict["files"] = fileList
	} else {
		infoDict["length"] = bencoding.Int(fileInfo.Size())
	}

	return infoDict, nil
}
