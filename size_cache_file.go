package kafero

import (
	"fmt"
	"github.com/wangjia184/sortedset"
	"io"
	"os"
	"time"
)

type SizeCacheFile struct {
	Fs    SizeCacheFS
	Base  File
	Cache File
	Flag  int
	info  *cacheFile
}

func NewSizeCacheFile(base File, cache File, flag int, Fs SizeCacheFS, info *cacheFile) File {
	return &SizeCacheFile{
		Fs:    Fs,
		Base:  base,
		Cache: cache,
		Flag:  flag,
		info:  info,
	}
}

func (f *SizeCacheFile) Close() error {
	if err := f.Sync(); err != nil {
		return fmt.Errorf("error syncing to base file: %v", err)
	}
	fstat, err := f.Base.Stat()
	if err != nil {
		return fmt.Errorf("error getting base file stat: %v", err)
	}
	if err := f.Cache.Close(); err != nil {
		return fmt.Errorf("error closing buffer file: %v", err)
	}
	if err := f.Base.Close(); err != nil {
		return fmt.Errorf("error closing base file: %v", err)
	}
	_ = f.Fs.cache.Chtimes(f.Cache.Name(), fstat.ModTime(), fstat.ModTime())
	// Each open file gets removed from the cache to prevent
	// its deletion, so we add it back
	f.info.lastAccessTime = time.Now().UnixNano() / 1000000
	f.Fs.files.AddOrUpdate(f.Cache.Name(), sortedset.SCORE(f.info.lastAccessTime), f.info)
	f.Fs.currSize += fstat.Size()

	return nil
}

func (f *SizeCacheFile) Read(b []byte) (int, error) {
	return f.Cache.Read(b)
}

func (f *SizeCacheFile) ReadAt(b []byte, o int64) (int, error) {
	return f.Cache.Read(b)
}

func (f *SizeCacheFile) Seek(o int64, w int) (int64, error) {
	return f.Cache.Seek(o, w)
}

func (f *SizeCacheFile) Write(b []byte) (int, error) {
	n, err := f.Cache.Write(b)
	if err != nil {
		return 0, err
	} else {
		return n, nil
	}
}

func (f *SizeCacheFile) WriteAt(b []byte, o int64) (int, error) {
	n, err := f.Cache.WriteAt(b, o)
	if err != nil {
		return 0, err
	} else {
		return n, nil
	}
}

func (f *SizeCacheFile) Name() string {
	return f.Base.Name()
}

func (f *SizeCacheFile) Readdir(c int) ([]os.FileInfo, error) {
	return f.Base.Readdir(c)
}

func (f *SizeCacheFile) Readdirnames(c int) ([]string, error) {
	return f.Base.Readdirnames(c)
}

func (f *SizeCacheFile) Stat() (os.FileInfo, error) {
	return f.Cache.Stat()
}

func (f *SizeCacheFile) Sync() error {
	if f.Flag == os.O_RDONLY {
		return nil
	}
	if err := f.Base.Truncate(0); err != nil {
		return fmt.Errorf("error truncating base file: %v", err)
	}
	if _, err := f.Base.Seek(0, 0); err != nil {
		return fmt.Errorf("error seeking base file to start: %v", err)
	}
	idx, err := f.Cache.Seek(0, 1)
	if err != nil {
		return fmt.Errorf("error seeking buffer file: %v", err)
	}
	if _, err := f.Cache.Seek(0, 0); err != nil {
		return fmt.Errorf("error seeking buffer file to start: %v", err)
	}
	if _, err := io.Copy(f.Base, f.Cache); err != nil {
		return fmt.Errorf("error copying buffer to base file: %v", err)
	}
	if _, err := f.Cache.Seek(idx, 0); err != nil {
		return fmt.Errorf("error seeking buffer file to start: %v", err)
	}
	if err := f.Base.Sync(); err != nil {
		return fmt.Errorf("error syncing base file: %v", err)
	}
	return nil
}

func (f *SizeCacheFile) Truncate(s int64) error {
	return f.Cache.Truncate(s)
}

func (f *SizeCacheFile) WriteString(s string) (int, error) {
	return f.Cache.Write([]byte(s))
}

func (f *SizeCacheFile) CanMmap() bool {
	return f.Cache.CanMmap()
}

func (f *SizeCacheFile) Mmap(offset int64, length int, prot int, flags int) ([]byte, error) {
	return f.Cache.Mmap(offset, length, prot, flags)
}

func (f *SizeCacheFile) Munmap() error {
	return f.Cache.Munmap()
}