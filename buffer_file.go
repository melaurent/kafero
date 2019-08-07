package kafero

import (
	"fmt"
	"github.com/dsnet/golib/memfile"
	"os"
	"time"
)

type BufferFile struct {
	Base    File
	Buffer  memfile.File
	modTime time.Time
}

func (f *BufferFile) Close() error {
	// Copy layer to base
	// Close layer
	// Close base

	return nil
}

func (f *BufferFile) Read(b []byte) (int, error) {
	return f.Buffer.Read(b)
}

func (f *BufferFile) ReadAt(b []byte, o int64) (int, error) {
	return f.Buffer.Read(b)
}

func (f *BufferFile) Seek(o int64, w int) (int64, error) {
	return f.Buffer.Seek(o, w)
}

func (f *BufferFile) Write(b []byte) (int, error) {
	n, err := f.Buffer.Write(b)
	if err != nil {
		return 0, err
	} else {
		f.modTime = time.Now()
		return n, nil
	}
}

func (f *BufferFile) WriteAt(b []byte, o int64) (int, error) {
	n, err := f.Buffer.WriteAt(b, o)
	if err != nil {
		return 0, err
	} else {
		f.modTime = time.Now()
		return n, nil
	}
}

func (f *BufferFile) Name() string {
	return f.Base.Name()
}

func (f *BufferFile) Readdir(c int) ([]os.FileInfo, error) {
	return f.Base.Readdir(c)
}

func (f *BufferFile) Readdirnames(c int) ([]string, error) {
	return f.Base.Readdirnames(c)
}

func (f *BufferFile) Stat() (os.FileInfo, error) {
	// TODO
	baseInfo, err := f.Base.Stat()
	if err != nil {
		return nil, fmt.Errorf("error reading base file info: %v", err)
	}

	info := &BufferFileInfo{
		baseInfo: baseInfo,
		size:     int64(len(f.Buffer.Bytes())),
		modTime:  f.modTime,
	}
	return info, nil
}

func (f *BufferFile) Sync() error {
	return nil
}

func (f *BufferFile) Truncate(s int64) error {
	return f.Buffer.Truncate(s)
}

func (f *BufferFile) WriteString(s string) (int, error) {
	return f.Buffer.Write([]byte(s))
}

func (f *BufferFile) CanMmap() bool {
	return true
}

func (f *BufferFile) Mmap(offset int64, length int, prot int, flags int) ([]byte, error) {
	// TODO check if base is readonly
	return f.Buffer.Bytes(), nil
}

func (f *BufferFile) Munmap() error {
	return nil
}


type BufferFileInfo struct {
	baseInfo os.FileInfo
	size     int64
	modTime  time.Time
}

// Implements os.FileInfo
func (fi *BufferFileInfo) Name() string {
	return fi.baseInfo.Name()
}

func (fi *BufferFileInfo) Mode() os.FileMode {
	return fi.baseInfo.Mode()
}

func (fi *BufferFileInfo) ModTime() time.Time {
	return fi.modTime
}

func (fi *BufferFileInfo) IsDir() bool {
	return fi.baseInfo.IsDir()
}

func (fi *BufferFileInfo) Sys() interface{} {
	return nil
}

func (fi *BufferFileInfo) Size() int64 {
	if fi.IsDir() {
		return fi.baseInfo.Size()
	} else {
		return fi.size
	}
}