// Copyright © 2018 Mikael Rapp, github.com/zatte
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

package gcs

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/dsnet/golib/memfile"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"google.golang.org/api/iterator"
)

// GcsFs is the Afero version adapted for GCS
type GcsFile struct {
	fs        *GcsFs
	ctx       context.Context
	openFlags int
	closed    bool
	ReadDirIt *storage.ObjectIterator
	memFile   *memfile.File
	obj       *storage.ObjectHandle
	name      string
	isDir     bool
	dirty     bool
}

func NewGcsFile(
	fs *GcsFs,
	ctx context.Context,
	obj *storage.ObjectHandle,
	openFlags int,
	name string,
) (*GcsFile, error) {
	file := &GcsFile{
		fs:        fs,
		ctx:       ctx,
		openFlags: openFlags,
		closed:    false,
		ReadDirIt: nil,
		obj:       obj,
		name:      name,
		memFile:   memfile.New(nil),
		isDir:     false,
		dirty:     false,
	}

	attr, err := obj.Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			if openFlags&os.O_CREATE != 0 {
				// Create file
				writer := obj.NewWriter(ctx)
				if _, err := writer.Write([]byte("")); err != nil {
					return nil, fmt.Errorf("error writing to file: %v", err)
				}
				if err := writer.Close(); err != nil {
					return nil, fmt.Errorf("error closing writer: %v", err)
				}
			} else {
				return nil, os.ErrNotExist
			}
		} else {
			return nil, fmt.Errorf("error getting file reader: %v", err)
		}
	} else {
		if attr.Metadata["virtual_folder"] == "y" {
			file.isDir = true
			// no need to read file to memory, it's an empty virtual dir
			return file, nil
		}
	}

	// Now we should be able to get the reader
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting file reader: %v", err)
	}

	if openFlags&os.O_TRUNC == 0 {
		// if truncating the file, let memFile be empty
		if _, err := io.Copy(file.memFile, reader); err != nil {
			return nil, fmt.Errorf("error reading file: %v", err)
		}
		if err := reader.Close(); err != nil {
			return nil, fmt.Errorf("error closing reader: %v", err)
		}
		if openFlags&os.O_APPEND == 0 {
			if _, err := file.memFile.Seek(0, 0); err != nil {
				return nil, fmt.Errorf("error seeking to begining of file: %v", err)
			}
		}
	}

	return file, nil
}

func (f *GcsFile) Close() error {
	if err := f.Sync(); err != nil {
		return fmt.Errorf("error syncing file: %v", err)
	}
	f.closed = true
	return nil
}

func (f *GcsFile) Seek(newOffset int64, whence int) (int64, error) {
	if f.closed {
		return 0, ErrFileClosed
	}
	return f.memFile.Seek(newOffset, whence)
}

func (f *GcsFile) Read(p []byte) (n int, err error) {
	return f.memFile.Read(p)
}

func (f *GcsFile) ReadAt(p []byte, off int64) (n int, err error) {
	if f.closed {
		return 0, ErrFileClosed
	}

	return f.memFile.ReadAt(p, off)
}

func (f *GcsFile) Write(p []byte) (n int, err error) {
	if f.closed {
		return 0, ErrFileClosed
	}
	f.dirty = true

	return f.memFile.Write(p)
}

func (f *GcsFile) WriteAt(b []byte, off int64) (n int, err error) {
	if f.closed {
		return 0, ErrFileClosed
	}
	f.dirty = true

	return f.memFile.WriteAt(b, off)
}

func (f *GcsFile) Name() string {
	return f.name
}

func (f *GcsFile) readdir(count int) ([]*fileInfo, error) {
	if err := f.Sync(); err != nil {
		return nil, fmt.Errorf("error syncing file")
	}
	path := f.fs.ensureTrailingSeparator(normSeparators(f.Name(), f.fs.separator))
	if f.ReadDirIt == nil {
		f.ReadDirIt = f.fs.bucket.Objects(
			f.ctx, &storage.Query{
				Delimiter: "/",
				Prefix:    path,
				Versions:  false})
	}
	var res []*fileInfo
	for {
		object, err := f.ReadDirIt.Next()
		if err == iterator.Done {
			if len(res) > 0 || count <= 0 {
				return res, nil
			}
			return res, io.EOF
		}
		if err != nil {
			return res, err
		}

		tmp := fileInfo{object, f.fs}
		// Since we create "virtual folders which are empty objects they can sometimes be returned twice
		// when we do a query (As the query will also return GCS version of "virtual folders" but they only
		// have a .Prefix, and not .Name)
		if object.Name == "" {
			continue
		}

		res = append(res, &tmp)
		if count > 0 && len(res) >= count {
			break
		}
	}
	return res, nil
}

func (f *GcsFile) Readdir(count int) ([]os.FileInfo, error) {
	fi, err := f.readdir(count)
	if err != nil {
		return nil, fmt.Errorf("error reading dir: %v", err)
	}
	if len(fi) > 0 {
		sort.Sort(ByName(fi))
	}

	var res []os.FileInfo
	for _, f := range fi {
		res = append(res, f)
	}
	return res, err
}

func (f *GcsFile) Readdirnames(n int) ([]string, error) {
	fi, err := f.Readdir(n)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("error reading dir names: %v", err)
	}
	names := make([]string, len(fi))

	for i, f := range fi {
		names[i] = f.Name()
	}
	return names, err
}

func (f *GcsFile) Stat() (os.FileInfo, error) {
	if err := f.Sync(); err != nil {
		return nil, fmt.Errorf("error syncing file")
	}
	objAttrs, err := f.obj.Attrs(f.ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, os.ErrNotExist //works with os.IsNotExist check
		}
		return nil, fmt.Errorf("error getting resource attributes: %v", err)
	}
	return &fileInfo{objAttrs, f.fs}, nil
}

func (f *GcsFile) Sync() error {
	if f.isDir {
		return nil
	}
	if !f.dirty {
		return nil
	}
	prevOff, err := f.memFile.Seek(0, 1)
	if err != nil {
		return fmt.Errorf("error relative seeking memfile: %v", err)
	}
	if _, err := f.memFile.Seek(0, 0); err != nil {
		return fmt.Errorf("error seeking to beginning of file: %v", err)
	}
	writer := f.obj.NewWriter(f.ctx)
	if _, err := writer.Write(f.memFile.Bytes()); err != nil {
		return fmt.Errorf("error copying buffer to gcs file: %v", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("error closing writer: %v", err)
	}
	if _, err := f.memFile.Seek(prevOff, 0); err != nil {
		return fmt.Errorf("error seeking memfile: %v", err)
	}
	f.dirty = false
	return nil
}

func (f *GcsFile) Truncate(wantedSize int64) error {
	if f.closed {
		return ErrFileClosed
	}
	if f.openFlags&os.O_RDONLY != 0 {
		return fmt.Errorf("file is read only")
	}
	f.dirty = true
	return f.memFile.Truncate(wantedSize)
}

func (f *GcsFile) WriteString(s string) (ret int, err error) {
	return f.Write([]byte(s))
}

func (f *GcsFile) CanMmap() bool {
	return true
}

func (f *GcsFile) Mmap(offset int64, length int, prot int, flags int) ([]byte, error) {
	return f.memFile.Bytes(), nil
}

func (f *GcsFile) Munmap() error {
	return nil
}

type fileInfo struct {
	objAtt *storage.ObjectAttrs
	fs     *GcsFs
}

func (fi *fileInfo) name() string {
	return fi.objAtt.Prefix + fi.objAtt.Name
}

func (fi *fileInfo) Name() string {
	return filepath.Base(fi.name())
}

func (fi *fileInfo) Size() int64 {
	return fi.objAtt.Size
}
func (fi *fileInfo) Mode() os.FileMode {
	if fi.IsDir() {
		return 0755
	}
	return 0664
}

func (fi *fileInfo) ModTime() time.Time {
	return fi.objAtt.Updated
}

func (fi *fileInfo) IsDir() bool {
	return fi.objAtt.Metadata["virtual_folder"] == "y"
}

func (fi *fileInfo) Sys() interface{} {
	return nil
}

type ByName []*fileInfo

func (a ByName) Len() int {
	return len(a)
}

func (a ByName) Swap(i, j int) {
	a[i].objAtt, a[j].objAtt = a[j].objAtt, a[i].objAtt
}

func (a ByName) Less(i, j int) bool {
	return strings.Compare(a[i].Name(), a[j].Name()) == -1
}
