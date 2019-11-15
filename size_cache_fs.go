package kafero

import (
	"encoding/json"
	"fmt"
	"github.com/wangjia184/sortedset"
	"io"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// The SizeCacheFS is a cache file system composed of a cache layer and a base layer
// the cache layer has a maximal size, and files get evicted relative to their
// last use time (read or edited).

// If you change something on the file, need to change on base and cache
// even if cache is stale (invalidated), easier to just do it

type cacheFile struct {
	Path           string
	Size           int64
	LastAccessTime int64
}

type SizeCacheFS struct {
	base      Fs
	cache     Fs
	cacheSize int64
	currSize  int64
	files     *sortedset.SortedSet
}

func NewSizeCacheFS(base Fs, cache Fs, cacheSize int64) (*SizeCacheFS, error) {
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
			if err != nil {
				return err
			}
			if !info.IsDir() {
				file := &cacheFile{
					Path:           path,
					Size:           info.Size(),
					LastAccessTime: info.ModTime().UnixNano() / 1000000,
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
		set.AddOrUpdate(f.Path, sortedset.SCORE(f.LastAccessTime), f)
		currSize += f.Size
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
		if err := u.cache.Remove(file.Path); err != nil {
			return fmt.Errorf("error removing cache file: %v", err)
		}
		u.currSize -= file.Size

		path := file.Path
		for path != "" && path != "." && path != "/" {
			parent := filepath.Dir(path)
			f, err := u.cache.Open(parent)
			if err != nil {
				return fmt.Errorf("error opening parent directory: %v", err)
			}
			dirs, err := f.Readdir(-1)
			if err != nil {
				return fmt.Errorf("error reading parent directory: %v", err)
			}
			_ = f.Close()

			if len(dirs) == 0 {
				if err := u.cache.Remove(parent); err != nil {
					return fmt.Errorf("error removing parent directory: %v", err)
				}
				path = parent
			} else {
				break
			}
		}
	}

	return nil
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

func (u *SizeCacheFS) copyToCache(name string) error {

	// If layer file exists, we need to remove it
	// and replace it with current file
	// TODO

	// Get size, if size over our limit, evict one file
	bfh, err := u.base.Open(name)
	if err != nil {
		if err == os.ErrNotExist {
			return err
		} else {
			return fmt.Errorf("error opening base file: %v", err)
		}
	}

	// First make sure the directory exists
	exists, err := Exists(u.cache, filepath.Dir(name))
	if err != nil {
		return err
	}
	if !exists {
		err = u.cache.MkdirAll(filepath.Dir(name), 0777) // FIXME?
		if err != nil {
			return err
		}
	}

	// Create the file on the overlay
	lfh, err := u.cache.Create(name)
	if err != nil {
		return err
	}
	n, err := io.Copy(lfh, bfh)
	if err != nil {
		// If anything fails, clean up the file
		_ = u.cache.Remove(name)
		_ = lfh.Close()
		return fmt.Errorf("error copying layer to base: %v", err)
	}

	bfi, err := bfh.Stat()
	if err != nil || bfi.Size() != n {
		_ = u.cache.Remove(name)
		_ = lfh.Close()
		return syscall.EIO
	}

	err = lfh.Close()
	if err != nil {
		_ = u.cache.Remove(name)
		_ = lfh.Close()
		return err
	}
	if err := bfh.Close(); err != nil {
		return fmt.Errorf("error closing base file: %v", err)
	}

	if err := u.cache.Chtimes(name, bfi.ModTime(), bfi.ModTime()); err != nil {
		return err
	}

	// if cache is stale and file already inside sorted set, we are just going to update it
	// Create info
	info := &cacheFile{
		Path:           name,
		Size:           bfi.Size(),
		LastAccessTime: time.Now().UnixNano() / 1000,
	}
	u.currSize += bfi.Size()
	u.files.AddOrUpdate(name, sortedset.SCORE(info.LastAccessTime), info)

	return nil
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
		node := u.files.GetByKey(oldname)
		u.files.Remove(oldname)
		info := node.Value.(*cacheFile)
		u.files.AddOrUpdate(newname, sortedset.SCORE(info.LastAccessTime), info)
		if err := u.cache.Rename(oldname, newname); err != nil {
			return err
		}
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
		u.files.Remove(name)
		info := node.Value.(*cacheFile)
		u.currSize -= info.Size
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
		err := Walk(u.cache, name, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				return u.Remove(path)
			} else {
				return nil
			}
		})
		if err != nil {
			return err
		}
		// Remove the dirs
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

	default:
		exists, err := Exists(u.base, name)
		if err != nil {
			return nil, fmt.Errorf("error determining if base file exists: %v", err)
		}
		if exists {
			if err := u.copyToCache(name); err != nil {
				return nil, err
			}
		}
	}

	node := u.files.GetByKey(name)
	var info *cacheFile
	if node == nil {
		// Need to create file
		info = &cacheFile{
			Path:           name,
			Size:           0,
			LastAccessTime: time.Now().UnixNano() / 1000,
		}
		u.files.AddOrUpdate(name, sortedset.SCORE(info.LastAccessTime), info)
	} else {
		info = node.Value.(*cacheFile)
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
	case cacheLocal, cacheHit:

	case cacheMiss:
		bfi, err := u.base.Stat(name)
		if err != nil {
			return nil, err
		}
		if !bfi.IsDir() {
			if err := u.copyToCache(name); err != nil {
				return nil, err
			}
		} else {
			return u.base.Open(name)
		}

	case cacheStale:
		if !fi.IsDir() {
			if err := u.copyToCache(name); err != nil {
				return nil, err
			}
		} else {
			return u.base.Open(name)
		}
	}
	// the dirs from cacheHit, cacheStale fall down here:
	bfile, _ := u.base.Open(name)
	lfile, err := u.cache.Open(name)
	if err != nil && bfile == nil {
		return nil, err
	}

	fi, err = u.cache.Stat(name)
	if err != nil {
		return nil, err
	}

	// No info for directories
	var info *cacheFile
	if !fi.IsDir() {
		node := u.files.GetByKey(name)
		info = node.Value.(*cacheFile)
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
		Path:           name,
		Size:           0,
		LastAccessTime: time.Now().UnixNano() / 1000,
	}
	u.files.AddOrUpdate(name, sortedset.SCORE(info.LastAccessTime), info)

	uf := NewSizeCacheFile(bfile, lfile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, u, info)
	return uf, nil
}

func (u *SizeCacheFS) Size() int64 {
	return u.currSize
}

func (u *SizeCacheFS) Close() error {
	// TODO close all open files
	// Save index
	var files []*cacheFile
	nodes := u.files.GetByScoreRange(math.MinInt64, math.MaxInt64, nil)
	for _, n := range nodes {
		f := n.Value.(*cacheFile)
		files = append(files, f)
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
