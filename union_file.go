package kafero

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// The UnionFile implements the afero.File interface and will be returned
// when reading a directory present at least in the overlay or opening a file
// for writing.
//
// The calls to
// Readdir() and Readdirnames() merge the file os.FileInfo / names from the
// base and the overlay - for files present in both layers, only those
// from the overlay will be used.
//
// When opening files for writing (Create() / OpenFile() with the right flags)
// the operations will be done in both layers, starting with the overlay. A
// successful read in the overlay will move the cursor position in the base layer
// by the number of bytes read.
type UnionFile struct {
	Base   File
	Layer  File
	Merger DirsMerger
	off    int64
	dirOff int
	files  []os.FileInfo
}

func NewUnionFile(base File, layer File) (File, error) {
	off, err := layer.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("error determining layer offset: %v", err)
	}
	uf := &UnionFile{
		Base:   base,
		Layer:  layer,
		Merger: nil,
		off:    off,
		dirOff: 0,
		files:  nil,
	}

	return uf, nil
}

func (f *UnionFile) Close() error {
	// first close base, so we have a newer timestamp in the overlay. If we'd close
	// the overlay first, we'd get a cacheStale the next time we access this file
	// -> cache would be useless ;-)
	if err := f.Base.Close(); err != nil {
		return fmt.Errorf("error closing base file: %v", err)
	}
	if err := f.Layer.Close(); err != nil {
		return fmt.Errorf("error closing layer file: %v", err)
	}
	return nil
}

func (f *UnionFile) Read(s []byte) (int, error) {
	n, err := f.Layer.Read(s)
	f.off += int64(n)
	return n, err
}

func (f *UnionFile) ReadAt(s []byte, o int64) (int, error) {
	n, err := f.Layer.ReadAt(s, o)
	f.off += int64(n)
	return n, err
}

func (f *UnionFile) Seek(o int64, w int) (pos int64, err error) {
	pos, err = f.Layer.Seek(o, w)
	f.off = pos
	return pos, err
}

func (f *UnionFile) Write(s []byte) (n int, err error) {
	n, err = f.Layer.Write(s)
	if err != nil {
		return 0, fmt.Errorf("error writing to layer file: %v", err)
	}
	if _, err := f.Base.Seek(f.off, io.SeekStart); err != nil {
		return 0, fmt.Errorf("error syncing base file: %v", err)
	}
	if _, err := f.Base.Write(s); err != nil {
		return 0, fmt.Errorf("error writing to base file: %v", err)
	}
	f.off += int64(n)
	return n, nil
}

func (f *UnionFile) WriteAt(s []byte, o int64) (n int, err error) {
	n, err = f.Layer.WriteAt(s, o)
	if err != nil {
		return 0, fmt.Errorf("error writing to layer file: %v", err)
	}
	if _, err := f.Base.Seek(f.off, io.SeekStart); err != nil {
		return 0, fmt.Errorf("error syncing base file: %v", err)
	}
	_, err = f.Base.WriteAt(s, o)
	f.off += int64(n)
	return n, err
}

func (f *UnionFile) Name() string {
	return f.Layer.Name()
}

// DirsMerger is how UnionFile weaves two directories together.
// It takes the FileInfo slices from the layer and the base and returns a
// single view.
type DirsMerger func(lofi, bofi []os.FileInfo) ([]os.FileInfo, error)

var defaultUnionMergeDirsFn = func(lofi, bofi []os.FileInfo) ([]os.FileInfo, error) {
	var files = make(map[string]os.FileInfo)

	for _, fi := range lofi {
		files[fi.Name()] = fi
	}

	for _, fi := range bofi {
		if _, exists := files[fi.Name()]; !exists {
			files[fi.Name()] = fi
		}
	}

	rfi := make([]os.FileInfo, len(files))

	i := 0
	for _, fi := range files {
		rfi[i] = fi
		i++
	}

	return rfi, nil

}

// Readdir will weave the two directories together and
// return a single view of the overlayed directories.
// At the end of the directory view, the error is io.EOF if c > 0.
func (f *UnionFile) Readdir(c int) (ofi []os.FileInfo, err error) {
	var merge DirsMerger = f.Merger
	if merge == nil {
		merge = defaultUnionMergeDirsFn
	}

	if f.dirOff == 0 {
		lfi, err := f.Layer.Readdir(-1)
		if err != nil {
			return nil, err
		}

		bfi, err := f.Base.Readdir(-1)
		if err != nil {
			return nil, err
		}

		merged, err := merge(lfi, bfi)
		if err != nil {
			return nil, err
		}
		f.files = append(f.files, merged...)
	}

	if c <= 0 {
		defer func() { f.dirOff = len(f.files) }()
		if f.dirOff >= len(f.files) {
			return nil, nil
		} else {
			return f.files[f.dirOff:len(f.files)], nil
		}
	} else {
		if f.dirOff+c > len(f.files) {
			c = len(f.files) - f.dirOff
		}

		if f.dirOff >= len(f.files) {
			return nil, io.EOF
		}

		defer func() { f.dirOff += c }()
		return f.files[f.dirOff : f.dirOff+c], nil
	}
}

func (f *UnionFile) Readdirnames(c int) ([]string, error) {
	rfi, err := f.Readdir(c)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, fi := range rfi {
		names = append(names, fi.Name())
	}
	return names, nil
}

func (f *UnionFile) Stat() (os.FileInfo, error) {
	return f.Layer.Stat()
}

func (f *UnionFile) Sync() error {
	if err := f.Layer.Sync(); err != nil {
		return fmt.Errorf("error syncing layer file: %v", err)
	}
	if err := f.Base.Sync(); err != nil {
		return fmt.Errorf("error syncing base file: %v", err)
	}

	return nil
}

func (f *UnionFile) Truncate(s int64) error {
	if err := f.Layer.Truncate(s); err != nil {
		return fmt.Errorf("error truncating layer file: %v", err)
	}
	if _, err := f.Base.Seek(f.off, io.SeekStart); err != nil {
		return fmt.Errorf("error syncing base file: %v", err)
	}
	if err := f.Base.Truncate(s); err != nil {
		return fmt.Errorf("error truncating base file :%v", err)
	}
	off, err := f.Layer.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("error seeking layer file: %v", err)
	}
	f.off = off
	return nil
}

func (f *UnionFile) WriteString(s string) (int, error) {
	n, err := f.Layer.WriteString(s)
	if err != nil {
		return 0, fmt.Errorf("error writing string to layer file: %v", err)
	}
	if _, err := f.Base.Seek(f.off, io.SeekStart); err != nil {
		return 0, fmt.Errorf("error syncing base file: %v", err)
	}
	if _, err := f.Base.WriteString(s); err != nil {
		return 0, fmt.Errorf("error writing string to base file: %v", err)
	}

	f.off += int64(n)
	return n, nil
}

func (f *UnionFile) CanMmap() bool {
	return f.Layer.CanMmap()
}

func (f *UnionFile) Mmap(offset int64, length int, prot int, flags int) ([]byte, error) {
	return f.Layer.Mmap(offset, length, prot, flags)
}

func (f *UnionFile) Munmap() error {
	return f.Layer.Munmap()
}

func copyToLayer(base Fs, layer Fs, name string) error {
	bfh, err := base.Open(name)
	if err != nil {
		if err == os.ErrNotExist {
			return err
		} else {
			return fmt.Errorf("error opening base file: %v", err)
		}
	}

	// First make sure the directory exists
	exists, err := Exists(layer, filepath.Dir(name))
	if err != nil {
		return err
	}
	if !exists {
		err = layer.MkdirAll(filepath.Dir(name), 0777) // FIXME?
		if err != nil {
			return err
		}
	}

	// Create the file on the overlay
	lfh, err := layer.Create(name)
	if err != nil {
		return err
	}
	n, err := io.Copy(lfh, bfh)
	if err != nil {
		// If anything fails, clean up the file
		_ = layer.Remove(name)
		_ = lfh.Close()
		return fmt.Errorf("error copying layer to base: %v", err)
	}

	bfi, err := bfh.Stat()
	if err != nil || bfi.Size() != n {
		_ = layer.Remove(name)
		_ = lfh.Close()
		return syscall.EIO
	}

	err = lfh.Close()
	if err != nil {
		_ = layer.Remove(name)
		_ = lfh.Close()
		return err
	}
	if err := bfh.Close(); err != nil {
		return fmt.Errorf("error closing base file: %v", err)
	}
	return layer.Chtimes(name, bfi.ModTime(), bfi.ModTime())
}
