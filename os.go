// Copyright © 2014 Steve Francia <spf@spf13.com>.
// Copyright 2013 tsuru authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafero

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

var _ Lstater = (*OsFs)(nil)

// OsFs is a Fs implementation that uses functions provided by the os package.
//
// For details in any method, check the documentation of the os package
// (http://golang.org/pkg/os/).
type OsFs struct{}

func NewOsFs() Fs {
	return &OsFs{}
}

func (OsFs) Name() string { return "OsFs" }

func (OsFs) Create(name string) (File, error) {
	f, e := os.Create(name)
	if f == nil {
		// while this looks strange, we need to return a bare nil (of type nil) not
		// a nil value of type *os.File or nil won't be nil
		return nil, e
	}
	return &OsFile{f: f}, e
}

func (OsFs) Mkdir(name string, perm os.FileMode) error {
	return os.Mkdir(name, perm)
}

func (OsFs) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (OsFs) Open(name string) (File, error) {
	f, e := os.Open(name)
	if f == nil {
		// while this looks strange, we need to return a bare nil (of type nil) not
		// a nil value of type *os.File or nil won't be nil
		return nil, e
	}
	return &OsFile{f: f}, e
}

func (OsFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f, e := os.OpenFile(name, flag, perm)
	if f == nil {
		// while this looks strange, we need to return a bare nil (of type nil) not
		// a nil value of type *os.File or nil won't be nil
		return nil, e
	}
	return &OsFile{f: f}, e
}

func (OsFs) Remove(name string) error {
	return os.Remove(name)
}

func (OsFs) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

func (OsFs) Rename(oldname, newname string) error {
	return os.Rename(oldname, newname)
}

func (OsFs) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (OsFs) Chmod(name string, mode os.FileMode) error {
	return os.Chmod(name, mode)
}

func (OsFs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return os.Chtimes(name, atime, mtime)
}

func (OsFs) LstatIfPossible(name string) (os.FileInfo, bool, error) {
	fi, err := os.Lstat(name)
	return fi, true, err
}

func (OsFs) Walk(root string, walkFn filepath.WalkFunc) error {
	return filepath.Walk(root, walkFn)
}

type OsFile struct {
	f    *os.File
	mmap []byte
}

func (f *OsFile) Close() error {
	if f.mmap != nil {
		if err := f.Munmap(); err != nil {
			return fmt.Errorf("error unmmapping file: %v", err)
		}
	}
	return f.f.Close()
}

func (f *OsFile) Read(s []byte) (int, error) {
	return f.f.Read(s)
}

func (f *OsFile) ReadAt(s []byte, o int64) (int, error) {
	return f.f.ReadAt(s, o)
}

func (f *OsFile) Seek(o int64, w int) (int64, error) {
	return f.f.Seek(o, w)
}

func (f *OsFile) Write(s []byte) (int, error) {
	return f.f.Write(s)
}

func (f *OsFile) WriteAt(s []byte, o int64) (int, error) {
	return f.f.WriteAt(s, o)
}

func (f *OsFile) Name() string {
	return f.f.Name()
}

func (f *OsFile) Readdir(count int) ([]os.FileInfo, error) {
	return f.f.Readdir(count)
}

func (f *OsFile) Readdirnames(n int) ([]string, error) {
	return f.f.Readdirnames(n)
}

func (f *OsFile) Stat() (os.FileInfo, error) {
	return f.f.Stat()
}

func (f *OsFile) Sync() error {
	return f.f.Sync()
}

func (f *OsFile) Truncate(size int64) error {
	return f.f.Truncate(size)
}

func (f *OsFile) WriteString(s string) (ret int, err error) {
	return f.f.WriteString(s)
}

func (f *OsFile) CanMmap() bool {
	return true
}

func (f *OsFile) Mmap(offset int64, length int, prot int, flags int) ([]byte, error) {
	return nil, fmt.Errorf("memap not supported")
	/*
		fd := f.f.Fd()
		b, err := syscall.Mmap(int(fd), offset, length, prot, flags)
		if err != nil {
			return nil, fmt.Errorf("error mmaping: %v", err)
		}
		f.mmap = b
		return b, nil
	*/
}

func (f *OsFile) Munmap() error {
	return fmt.Errorf("memap not supported")
	/*
		if f.mmap == nil {
			return fmt.Errorf("file not mmapped")
		}
		if err := syscall.Munmap(f.mmap); err != nil {
			return fmt.Errorf("error unmapping file: %v", err)
		}
		f.mmap = nil
		return nil
	*/
}
