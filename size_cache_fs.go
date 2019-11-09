package kafero

import (
	"os"
	"syscall"
	"time"
)

// The SizeCacheFS is a cache file system composed of a cache layer and a base layer
// the cache layer has a maximal size, and files get evicted relative to their
// last use time (read or edited).

// If the file is on cache, it is up to date ? Not necessarily

type SizeCacheFS struct {
	base      Fs
	cache     Fs
	cacheSize uint64
}

func NewSizeCacheFS(base Fs, cache Fs, cacheSize uint64) Fs {
	return &SizeCacheFS{base: base, cache: cache, cacheSize: cacheSize}
}

func (u *SizeCacheFS) cacheStatus(name string) (state cacheState, fi os.FileInfo, err error) {
	var lfi, bfi os.FileInfo
	lfi, err = u.cache.Stat(name)
	if err == nil {
		bfi, err = u.base.Stat(name)
		if err != nil {
			return cacheLocal, lfi, nil
		}
		if bfi.ModTime().After(lfi.ModTime()) {
			return cacheStale, bfi, nil
		}
		return cacheHit, lfi, nil
	} else if err == syscall.ENOENT || os.IsNotExist(err) {
		return cacheMiss, nil, nil
	} else {
		return cacheMiss, nil, err
	}
}

func (u *SizeCacheFS) copyToLayer(name string) error {
	return copyToLayer(u.base, u.cache, name)
}

func (u *SizeCacheFS) Chtimes(name string, atime, mtime time.Time) error {
	return u.base.Chtimes(name, atime, mtime)
}

func (u *SizeCacheFS) Chmod(name string, mode os.FileMode) error {
	return u.base.Chmod(name, mode)
}

func (u *SizeCacheFS) Stat(name string) (os.FileInfo, error) {
	return u.base.Stat(name)
}

func (u *SizeCacheFS) Rename(oldname, newname string) error {
	st, _, err := u.cacheStatus(oldname)
	if err != nil {
		return err
	}
	switch st {
	case cacheLocal:
	case cacheHit:
		err = u.base.Rename(oldname, newname)
	case cacheStale, cacheMiss:
		if err := u.copyToLayer(oldname); err != nil {
			return err
		}
		err = u.base.Rename(oldname, newname)
	}
	if err != nil {
		return err
	}
	return u.layer.Rename(oldname, newname)
}

func (u *CacheOnReadFs) Remove(name string) error {
	st, _, err := u.cacheStatus(name)
	if err != nil {
		return err
	}
	switch st {
	case cacheLocal:
	case cacheHit, cacheStale, cacheMiss:
		err = u.base.Remove(name)
	}
	if err != nil {
		return err
	}
	return u.layer.Remove(name)
}

func (u *CacheOnReadFs) RemoveAll(name string) error {
	st, _, err := u.cacheStatus(name)
	if err != nil {
		return err
	}
	switch st {
	case cacheLocal:
	case cacheHit, cacheStale, cacheMiss:
		err = u.base.RemoveAll(name)
	}
	if err != nil {
		return err
	}
	return u.layer.RemoveAll(name)
}

func (u *CacheOnReadFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	st, _, err := u.cacheStatus(name)
	if err != nil {
		return nil, err
	}
	switch st {
	case cacheLocal, cacheHit:
	default:
		if flag&os.O_CREATE == 0 {
			if err := u.copyToLayer(name); err != nil {
				return nil, err
			}
		}
	}
	if flag&(os.O_WRONLY|syscall.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		bfi, err := u.base.OpenFile(name, flag, perm)
		if err != nil {
			return nil, err
		}
		lfi, err := u.layer.OpenFile(name, flag, perm)
		if err != nil {
			bfi.Close() // oops, what if O_TRUNC was set and file opening in the layer failed...?
			return nil, err
		}
		return &UnionFile{Base: bfi, Layer: lfi}, nil
	} else {
		return u.layer.OpenFile(name, flag, perm)
	}
}

func (u *CacheOnReadFs) Open(name string) (File, error) {
	st, fi, err := u.cacheStatus(name)
	if err != nil {
		return nil, err
	}

	switch st {
	case cacheLocal:
		return u.layer.Open(name)

	case cacheMiss:
		bfi, err := u.base.Stat(name)
		if err != nil {
			return nil, err
		}
		if bfi.IsDir() {
			return u.base.Open(name)
		}
		if err := u.copyToLayer(name); err != nil {
			return nil, err
		}
		return u.layer.Open(name)

	case cacheStale:
		if !fi.IsDir() {
			if err := u.copyToLayer(name); err != nil {
				return nil, err
			}
			return u.layer.Open(name)
		}
	case cacheHit:
		if !fi.IsDir() {
			return u.layer.Open(name)
		}
	}
	// the dirs from cacheHit, cacheStale fall down here:
	bfile, _ := u.base.Open(name)
	lfile, err := u.layer.Open(name)
	if err != nil && bfile == nil {
		return nil, err
	}
	return &UnionFile{Base: bfile, Layer: lfile}, nil
}

func (u *CacheOnReadFs) Mkdir(name string, perm os.FileMode) error {
	err := u.base.Mkdir(name, perm)
	if err != nil {
		return err
	}
	return u.layer.MkdirAll(name, perm) // yes, MkdirAll... we cannot assume it exists in the cache
}

func (u *CacheOnReadFs) Name() string {
	return "CacheOnReadFs"
}

func (u *CacheOnReadFs) MkdirAll(name string, perm os.FileMode) error {
	err := u.base.MkdirAll(name, perm)
	if err != nil {
		return err
	}
	return u.layer.MkdirAll(name, perm)
}

func (u *CacheOnReadFs) Create(name string) (File, error) {
	bfh, err := u.base.Create(name)
	if err != nil {
		return nil, err
	}
	lfh, err := u.layer.Create(name)
	if err != nil {
		// oops, see comment about OS_TRUNC above, should we remove? then we have to
		// remember if the file did not exist before
		bfh.Close()
		return nil, err
	}
	return &UnionFile{Base: bfh, Layer: lfh}, nil
}
