package zstfs

import (
	"fmt"
	"github.com/klauspost/compress/zstd"
	"github.com/melaurent/kafero"
	"os"
)

// The Fs compress its files using the ZSTD compression algorithm.
// It doesn't allow seeking.
type Fs struct {
	kafero.Fs
	level zstd.EncoderLevel
}

func NewFs(source kafero.Fs, level zstd.EncoderLevel) kafero.Fs {
	return &Fs{Fs: source, level: level}
}

func (b *Fs) Name() string {
	return "Fs"
}

func (b *Fs) OpenFile(name string, flag int, mode os.FileMode) (f kafero.File, err error) {
	sourcef, err := b.Fs.OpenFile(name, flag, mode)
	if err != nil {
		return nil, err
	}
	fmt.Println("OPENFILE", name, flag, mode, sourcef, err)
	return &File{File: sourcef, fs: b.Fs}, nil
}

func (b *Fs) Open(name string) (f kafero.File, err error) {
	sourcef, err := b.Fs.Open(name)
	if err != nil {
		return nil, err
	}
	return &File{File: sourcef, fs: b.Fs}, nil
}

func (b *Fs) Create(name string) (f kafero.File, err error) {
	sourcef, err := b.Fs.Create(name)
	if err != nil {
		return nil, err
	}
	return &File{File: sourcef, fs: b.Fs}, nil
}

// vim: ts=4 sw=4 noexpandtab nolist syn=go
