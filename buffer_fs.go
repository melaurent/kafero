package kafero

import (
	"fmt"
	"os"
	"time"
)

type BufferFs struct {
	base  Fs
	layer Fs
}

func NewBufferFs(base Fs, layer Fs) Fs {
	return &BufferFs{
		base:  base,
		layer: layer,
	}
}

func (u *BufferFs) Chtimes(name string, atime, mtime time.Time) error {
	exists, err := Exists(u.layer, name)
	if err != nil {
		return err
	}
	if exists {
		return u.layer.Chtimes(name, atime, mtime)
	} else {
		return u.base.Chtimes(name, atime, mtime)
	}
}

func (u *BufferFs) Chmod(name string, mode os.FileMode) error {
	exists, err := Exists(u.layer, name)
	if err != nil {
		return err
	}
	if exists {
		return u.layer.Chmod(name, mode)
	} else {
		return u.layer.Chmod(name, mode)
	}
}

func (u *BufferFs) Stat(name string) (os.FileInfo, error) {
	exists, err := Exists(u.layer, name)
	if err != nil {
		return nil, err
	}
	if exists {
		return u.layer.Stat(name)
	} else {
		return u.base.Stat(name)
	}
}

func (u *BufferFs) Rename(oldname, newname string) error {
	exists, err := Exists(u.layer, oldname)
	if err != nil {
		return err
	}
	if exists {
		if err := u.layer.Rename(oldname, newname); err != nil {
			return err
		}
	}
	return u.base.Rename(oldname, newname)
}

func (u *BufferFs) Remove(name string) error {
	// It can exist in layer and base at the same time
	exists, err := Exists(u.layer, name)
	if err != nil {
		return err
	}
	if exists {
		if err := u.layer.Remove(name); err != nil {
			return err
		}
	}
	return u.base.Remove(name)
}

func (u *BufferFs) RemoveAll(name string) error {
	// It can exist in layer and base at the same time
	err1 := u.layer.RemoveAll(name)
	err2 := u.base.RemoveAll(name)
	if err1 != nil || err2 != nil {
		return fmt.Errorf("layer error: %v, base error: %v", err1, err2)
	} else {
		return nil
	}
}

func (u *BufferFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	// Open file in base, open a buffer file in layer, return a buffer file
	baseFile, err := u.base.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	// copy base file content in a new layer file
	layerFile, err := u.layer.Create(name)
	if err != nil {
		return nil, fmt.Errorf("error opening a buffer file on layer: %v", err)
	}
	// Read from base and copy to layer
	b, err := ReadFile(u.base, name)
	if err != nil {
		return nil, fmt.Errorf("error reading base file content: %v", err)
	}
	if _, err := layerFile.Write(b); err != nil {
		return nil, fmt.Errorf("error copying base file content to buffer file: %v", err)
	}
	if _, err := layerFile.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("error seeking buffer file: %v", err)
	}

	return &BufferFile{
		LayerFs: u.layer,
		Base:    baseFile,
		Buffer:  layerFile}, nil
}

func (u *BufferFs) Open(name string) (File, error) {
	baseFile, err := u.base.Open(name)
	if err != nil {
		return nil, err
	}
	// copy base file content in a new layer file
	layerFile, err := u.layer.Create(name)
	if err != nil {
		return nil, fmt.Errorf("error opening a buffer file on layer: %v", err)
	}
	// Read from base and copy to layer
	b, err := ReadFile(u.base, name)
	if err != nil {
		return nil, fmt.Errorf("error reading base file content: %v", err)
	}
	if _, err := layerFile.Write(b); err != nil {
		return nil, fmt.Errorf("error copying base file content to buffer file: %v", err)
	}
	if _, err := layerFile.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("error seeking buffer file: %v", err)
	}

	return &BufferFile{
		LayerFs: u.layer,
		Base:    baseFile,
		Buffer:  layerFile}, nil
}

func (u *BufferFs) Mkdir(name string, perm os.FileMode) error {
	err := u.base.Mkdir(name, perm)
	if err != nil {
		return err
	}
	return u.layer.MkdirAll(name, perm) // yes, MkdirAll... we cannot assume it exists in the cache
}

func (u *BufferFs) Name() string {
	return "BufferFs"
}

func (u *BufferFs) MkdirAll(name string, perm os.FileMode) error {
	err := u.base.MkdirAll(name, perm)
	if err != nil {
		return err
	}
	return u.layer.MkdirAll(name, perm)
}

func (u *BufferFs) Create(name string) (File, error) {
	baseFile, err := u.base.Create(name)
	if err != nil {
		return nil, err
	}
	layerFile, err := u.layer.Create(name)
	if err != nil {
		// oops, see comment about OS_TRUNC above, should we remove? then we have to
		// remember if the file did not exist before
		_ = baseFile.Close()
		return nil, err
	}
	return &BufferFile{
		LayerFs: u.layer,
		Base:    baseFile,
		Buffer:  layerFile}, nil
}
