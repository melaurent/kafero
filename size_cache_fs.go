package kafero

import (
	"fmt"
	"os"
	"syscall"
	"time"
)

// The SizeCacheFS is a cache file system composed of a cache layer and a base layer
// the cache layer has a maximal size, and files get evicted relative to their
// last use time (read or edited).

// If the file is on cache, it is up to date ? Not necessarily
// If you change something on the file, need to change on base and cache
// even if cache is stale, easier to just do it

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
		fmt.Println("layer time", lfi.ModTime())
		bfi, err = u.base.Stat(name)
		if err != nil {
			return cacheLocal, lfi, nil
		}
		fmt.Println("base time", bfi.ModTime())
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
	// Get size, if size over our limit, evict one file

	return copyToLayer(u.base, u.cache, name)
}

func (u *SizeCacheFS) Chtimes(name string, atime, mtime time.Time) error {
	exists, err := Exists(u.cache, name)
	if err != nil {
		return err
	}
	// If cache file exists, update to ensure consistency
	if exists {
		_ = u.cache.Chtimes(name, atime, mtime)
	}
	return u.base.Chtimes(name, atime, mtime)
}

func (u *SizeCacheFS) Chmod(name string, mode os.FileMode) error {
	exists, err := Exists(u.cache, name)
	if err != nil {
		return err
	}
	// If cache file exists, update to ensure consistency
	if exists {
		_ = u.cache.Chmod(name, mode)
	}
	return u.base.Chmod(name, mode)
}

func (u *SizeCacheFS) Stat(name string) (os.FileInfo, error) {
	return u.base.Stat(name)
}

func (u *SizeCacheFS) Rename(oldname, newname string) error {
	exists, err := Exists(u.cache, oldname)
	if err != nil {
		return err
	}
	// If cache file exists, update to ensure consistency
	if exists {
		_ = u.cache.Rename(oldname, newname)
	}
	return u.base.Rename(oldname, newname)
}

func (u *SizeCacheFS) Remove(name string) error {
	exists, err := Exists(u.cache, name)
	if err != nil {
		return err
	}
	// If cache file exists, update to ensure consistency
	if exists {
		_ = u.cache.Remove(name)
	}
	return u.base.Remove(name)
}

func (u *SizeCacheFS) RemoveAll(name string) error {
	exists, err := Exists(u.cache, name)
	if err != nil {
		return err
	}
	// If cache file exists, update to ensure consistency
	if exists {
		_ = u.cache.RemoveAll(name)
	}
	return u.base.RemoveAll(name)
}

func (u *SizeCacheFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	st, _, err := u.cacheStatus(name)
	if err != nil {
		return nil, err
	}
	switch st {
	case cacheLocal, cacheHit:
		fmt.Println("CACHE HIT", name)

	default:
		fmt.Println("CACHE MISS", name)
		if flag&(os.O_TRUNC) == 0 {
			exists, err := Exists(u.base, name)
			if err != nil {
				return nil, fmt.Errorf("error determining if base file exists: %v", err)
			}
			if exists {
				if err := u.copyToLayer(name); err != nil {
					return nil, err
				}
			}
		}
	}
	if flag&(os.O_WRONLY|syscall.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		bfi, err := u.base.OpenFile(name, flag, perm)
		if err != nil {
			return nil, err
		}

		// Force read write mode
		cacheFlag := (flag & (^os.O_WRONLY)) | os.O_RDWR

		lfi, err := u.cache.OpenFile(name, cacheFlag, perm)
		if err != nil {
			bfi.Close() // oops, what if O_TRUNC was set and file opening in the layer failed...?
			return nil, err
		}
		uf := NewBufferFile(bfi, lfi, flag, u.cache, false)
		if err != nil {
			return nil, fmt.Errorf("error creating buffer file: %v", err)
		}
		return uf, nil
	} else {
		return u.cache.OpenFile(name, flag, perm)
	}
}

func (u *SizeCacheFS) Open(name string) (File, error) {
	st, fi, err := u.cacheStatus(name)
	if err != nil {
		return nil, err
	}

	switch st {
	case cacheLocal:
		return u.cache.Open(name)

	case cacheMiss:
		fmt.Println("CACHE MISS", name)
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
		return u.cache.Open(name)

	case cacheStale:
		fmt.Println("CACHE STALE", name)
		if !fi.IsDir() {
			if err := u.copyToLayer(name); err != nil {
				return nil, err
			}
			return u.cache.Open(name)
		}
	case cacheHit:
		fmt.Println("CACHE HIT", name)
		if !fi.IsDir() {
			return u.cache.Open(name)
		}
	}
	// the dirs from cacheHit, cacheStale fall down here:
	bfile, _ := u.base.Open(name)
	lfile, err := u.cache.Open(name)
	if err != nil && bfile == nil {
		return nil, err
	}
	uf := NewBufferFile(bfile, lfile, os.O_RDONLY, u.cache, false)
	if err != nil {
		return nil, fmt.Errorf("error creating buffer file: %v", err)
	}
	return uf, nil
}

func (u *SizeCacheFS) Mkdir(name string, perm os.FileMode) error {
	err := u.base.Mkdir(name, perm)
	if err != nil {
		return err
	}
	return u.cache.MkdirAll(name, perm) // yes, MkdirAll... we cannot assume it exists in the cache
}

func (u *SizeCacheFS) Name() string {
	return "SizeCacheFS"
}

func (u *SizeCacheFS) MkdirAll(name string, perm os.FileMode) error {
	err := u.base.MkdirAll(name, perm)
	if err != nil {
		return err
	}
	return u.cache.MkdirAll(name, perm)
}

func (u *SizeCacheFS) Create(name string) (File, error) {
	bfh, err := u.base.Create(name)
	if err != nil {
		return nil, err
	}
	lfh, err := u.cache.Create(name)
	if err != nil {
		// oops, see comment about OS_TRUNC above, should we remove? then we have to
		// remember if the file did not exist before
		bfh.Close()
		return nil, err
	}
	uf, err := NewUnionFile(bfh, lfh)
	if err != nil {
		return nil, fmt.Errorf("error creating union file: %v", err)
	}
	return uf, nil
}
