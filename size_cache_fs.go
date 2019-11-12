package kafero

import (
	"encoding/json"
	"fmt"
	"github.com/wangjia184/sortedset"
	"math"
	"os"
	"syscall"
	"time"
)

// The SizeCacheFS is a cache file system composed of a cache layer and a base layer
// the cache layer has a maximal size, and files get evicted relative to their
// last use time (read or edited).

// If you change something on the file, need to change on base and cache
// even if cache is stale (invalidated), easier to just do it

type cacheFile struct {
	path           string
	size           int64
	lastAccessTime int64
}

type SizeCacheFS struct {
	base      Fs
	cache     Fs
	cacheSize int64
	currSize  int64
	files     *sortedset.SortedSet
}

func NewSizeCacheFS(base Fs, cache Fs, cacheSize int64) (Fs, error) {
	if cacheSize < 0 {
		cacheSize = 0
	}
	exists, err := Exists(cache, ".cacheindex")
	if err != nil {
		return nil, fmt.Errorf("error determining if cache index exists: %v", err)
	}
	var files []*cacheFile
	if !exists {
		err := Walk(cache, "", func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				file := &cacheFile{
					path:           path,
					size:           info.Size(),
					lastAccessTime: info.ModTime().UnixNano() / 1000000,
				}
				files = append(files, file)
			}

			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("error building cache index: %v", err)
		}
	} else {
		data, err := ReadFile(cache, ".cacheindex")
		if err != nil {
			return nil, fmt.Errorf("error reading cache index: %v", err)
		}
		if err := json.Unmarshal(data, &files); err != nil {
			return nil, fmt.Errorf("error unmarshalling files: %v", err)
		}
	}

	var currSize int64 = 0
	set := sortedset.New()
	for _, f := range files {
		set.AddOrUpdate(f.path, sortedset.SCORE(f.lastAccessTime), f)
		currSize += f.size
	}

	fs := &SizeCacheFS{
		base:      base,
		cache:     cache,
		cacheSize: cacheSize,
		currSize:  currSize,
		files:     set,
	}

	return fs, nil
}

func (u *SizeCacheFS) evict() error {
	for u.currSize > u.cacheSize {
		node := u.files.PopMin()
		// node CAN'T be nil as currSize > 0
		// we know currSize > 0 because the smallest value cache size can take is 0
		file := node.Value.(*cacheFile)
		fmt.Println("EVICTED", file.path)
		if err := u.cache.Remove(file.path); err != nil {
			return fmt.Errorf("error removing cache file: %v", err)
		}
		u.currSize -= file.size
	}

	return nil
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
		return fmt.Errorf("error determining if file exists: %v", err)
	}
	// If cache file exists, update to ensure consistency
	if exists {
		if err := u.cache.Remove(name); err != nil {
			return fmt.Errorf("error removing cache file: %v", err)
		}
		node := u.files.GetByKey(name)
		if node != nil {
			u.files.Remove(name)
			info := node.Value.(*cacheFile)
			u.currSize -= info.size
		}
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

	node := u.files.GetByKey(name)
	if node != nil {
		// Remove from set to prevent deletion while open
		u.files.Remove(name)
		info := node.Value.(*cacheFile)
		u.currSize -= info.size
	}

	var cacheFlag = flag

	if flag&(os.O_WRONLY|syscall.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		// Force read write mode
		cacheFlag = (flag & (^os.O_WRONLY)) | os.O_RDWR
	}

	bfi, err := u.base.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	lfi, err := u.cache.OpenFile(name, cacheFlag, perm)
	if err != nil {
		bfi.Close() // oops, what if O_TRUNC was set and file opening in the layer failed...?
		return nil, err
	}
	uf := NewSizeCacheFile(bfi, lfi, flag, u, info)

	return uf, nil
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

	node := u.files.GetByKey(name)
	if node != nil {
		// Remove from set to prevent deletion while open
		u.files.Remove(name)
		info := node.Value.(*cacheFile)
		u.currSize -= info.size
	}

	uf := NewSizeCacheFile(bfile, lfile, os.O_RDONLY, u, info)
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
	bfile, err := u.base.Create(name)
	if err != nil {
		return nil, err
	}
	lfile, err := u.cache.Create(name)
	if err != nil {
		// oops, see comment about OS_TRUNC above, should we remove? then we have to
		// remember if the file did not exist before
		_ = bfile.Close()
		return nil, err
	}

	info := &cacheFile{
		path:           name,
		size:           0,
		lastAccessTime: time.Now().UnixNano() / 1000000,
	}

	uf := NewSizeCacheFile(bfile, lfile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, u, info)
	return uf, nil
}

func (u *SizeCacheFS) Close() error {
	// TODO close all open files
	// Save index
	var files []*cacheFile
	nodes := u.files.GetByScoreRange(math.MinInt64, math.MaxInt64, nil)
	for _, n := range nodes {
		files = append(files, n.Value.(*cacheFile))
	}
	data, err := json.Marshal(files)
	if err != nil {
		return fmt.Errorf("error marshalling files: %v", err)
	}
	if err := WriteFile(u.cache, ".cacheindex", data, 0644); err != nil {
		return fmt.Errorf("error writing cache index: %v", err)
	}
	return nil
}
