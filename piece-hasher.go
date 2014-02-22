package bittorrent

import (
	"bytes"
	"crypto/sha1"
	"io"
)

type pieceHasher struct {
	PieceLength int64
	PieceHashes [][]byte
	PieceBuffer []byte
}

func newPieceHasher(pieceLength int64) pieceHasher {
	return pieceHasher{
		PieceLength: pieceLength,
		PieceHashes: [][]byte{},
		PieceBuffer: []byte{},
	}
}

type pieceHasherWrapperWriter struct {
	hasher *pieceHasher
}

func (wrapper pieceHasherWrapperWriter) Write(data []byte) (int, error) {
	return wrapper.hasher.Write(data)
}

func (self *pieceHasher) Writer() io.Writer {
	return pieceHasherWrapperWriter{hasher: self}
}

func (self *pieceHasher) Write(data []byte) (int, error) {
	written := 0
	self.PieceBuffer = bytes.Join([][]byte{self.PieceBuffer, data}, []byte(""))

	for int64(len(self.PieceBuffer)) >= self.PieceLength {
		piece := self.PieceBuffer[:self.PieceLength]

		hasher := sha1.New()
		hasher.Write(piece)
		written += len(piece)
		self.PieceHashes = append(self.PieceHashes, hasher.Sum(nil))

		self.PieceBuffer = self.PieceBuffer[self.PieceLength:]
	}

	return written, nil
}

func (self *pieceHasher) Pieces() []byte {
	piecesData := bytes.Join(self.PieceHashes, []byte(""))
	if len(self.PieceBuffer) > 0 {
		hasher := sha1.New()
		hasher.Write(self.PieceBuffer)
		piecesData = bytes.Join([][]byte{piecesData, hasher.Sum(nil)}, []byte(""))
	}
	return piecesData
}
