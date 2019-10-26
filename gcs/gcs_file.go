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
	ctx       context.Context
	bucket    *storage.BucketHandle
	separator string
	openFlags int
	closed    bool
	ReadDirIt *storage.ObjectIterator
	isDir     bool
	fhoffset  int64
	resource  *gcsFileResource
}

func NewGcsFile(
	ctx context.Context,
	bucket *storage.BucketHandle,
	obj *storage.ObjectHandle,
	separator string,
	openFlags int,
	name string,
) (*GcsFile, error) {
	file := &GcsFile{
		ctx:       ctx,
		bucket:    bucket,
		separator: separator,
		openFlags: openFlags,
		closed:    false,
		ReadDirIt: nil,
		isDir:     false,
		fhoffset:  0,
		resource:  nil,
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
		// If create exclusive and file exists, error
		if openFlags&os.O_CREATE != 0 && openFlags&os.O_EXCL != 0 {
			return nil, os.ErrExist
		}
		if attr.Metadata["virtual_folder"] == "y" {
			file.isDir = true
		}
	}

	file.resource = &gcsFileResource{
		ctx:  ctx,
		obj:  obj,
		name: name,

		currentGcsSize: 0,

		offset: 0,
		reader: nil,
		writer: nil,
	}

	if (openFlags&os.O_WRONLY != 0 || openFlags&os.O_RDWR != 0) && openFlags&os.O_TRUNC != 0 {
		if err := file.resource.Truncate(0); err != nil {
			return nil, fmt.Errorf("error truncating file: %v", err)
		}
	}

	if openFlags&os.O_APPEND != 0 {
		stats, err := file.Stat()
		if err != nil {
			file.fhoffset = 0
		} else {
			file.fhoffset = stats.Size()
		}
	}

	return file, nil
}

func (f *GcsFile) Close() error {
	f.closed = true
	return f.resource.Close()
}

func (f *GcsFile) Seek(newOffset int64, whence int) (int64, error) {
	if f.closed {
		return 0, ErrFileClosed
	}

	//Since this is an expensive operation; let's make sure we need it
	if (whence == 0 && newOffset == f.fhoffset) || (whence == 1 && newOffset == 0) {
		return f.fhoffset, nil
	}

	//Fore the reader/writers to be reopened (at correct offset)
	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("error syncing file: %v", err)
	}
	stat, err := f.Stat()
	if err != nil {
		return 0, nil
	}

	switch whence {
	case 0:
		f.fhoffset = newOffset
	case 1:
		f.fhoffset += newOffset
	case 2:
		f.fhoffset = stat.Size() + newOffset
	}
	return f.fhoffset, nil
}

func (f *GcsFile) Read(p []byte) (n int, err error) {
	return f.ReadAt(p, f.fhoffset)
}

func (f *GcsFile) ReadAt(p []byte, off int64) (n int, err error) {
	if f.closed {
		return 0, ErrFileClosed
	}

	read, err := f.resource.ReadAt(p, off)
	f.fhoffset += int64(read)
	return read, err
}

func (f *GcsFile) Write(p []byte) (n int, err error) {
	return f.WriteAt(p, f.fhoffset)
}

func (f *GcsFile) WriteAt(b []byte, off int64) (n int, err error) {
	if f.closed {
		return 0, ErrFileClosed
	}

	if f.openFlags == os.O_RDONLY {
		return 0, fmt.Errorf("file is opened as read only")
	}

	written, err := f.resource.WriteAt(b, off)
	f.fhoffset = off + int64(written)
	return written, err
}

func (f *GcsFile) Name() string {
	return f.resource.name
}

func (f *GcsFile) readdir(count int) ([]*FileInfo, error) {
	if err := f.Sync(); err != nil {
		return nil, fmt.Errorf("error syncing file")
	}
	path := strings.Replace(strings.Replace(f.Name(), "\\", f.separator, -1), "/", f.separator, -1)
	if len(path) > 0 && !strings.HasSuffix(path, f.separator) {
		path = path + f.separator
	}
	if f.ReadDirIt == nil {
		f.ReadDirIt = f.bucket.Objects(
			f.ctx, &storage.Query{
				Delimiter: "/",
				Prefix:    path,
				Versions:  false})
	}
	var res []*FileInfo
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

		tmp := FileInfo{object}
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
	if !f.isDir {
		return nil, fmt.Errorf("not a directory")
	}
	fi, err := f.readdir(count)
	if err != nil {
		return nil, err
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
	if !f.isDir {
		return nil, fmt.Errorf("not a directory")
	}
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
	objAttrs, err := f.resource.obj.Attrs(f.ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, os.ErrNotExist //works with os.IsNotExist check
		}
		return nil, fmt.Errorf("error getting resource attributes: %v", err)
	}
	return &FileInfo{objAttrs}, nil
}

func (f *GcsFile) Sync() error {
	return f.resource.maybeCloseIo()
}

func (f *GcsFile) Truncate(wantedSize int64) error {
	if f.closed {
		return ErrFileClosed
	}
	if f.openFlags&os.O_RDONLY != 0 {
		return fmt.Errorf("file is read only")
	}
	return f.resource.Truncate(wantedSize)
}

func (f *GcsFile) WriteString(s string) (ret int, err error) {
	return f.Write([]byte(s))
}

func (f *GcsFile) CanMmap() bool {
	return false
}

func (f *GcsFile) Mmap(offset int64, length int, prot int, flags int) ([]byte, error) {
	return nil, fmt.Errorf("mmap not supported")
}

func (f *GcsFile) Munmap() error {
	return fmt.Errorf("mmap not supported")
}

type FileInfo struct {
	ObjAtt *storage.ObjectAttrs
}

func (fi *FileInfo) name() string {
	return fi.ObjAtt.Prefix + fi.ObjAtt.Name
}

func (fi *FileInfo) Name() string {
	return filepath.Base(fi.name())
}

func (fi *FileInfo) Size() int64 {
	return fi.ObjAtt.Size
}
func (fi *FileInfo) Mode() os.FileMode {
	if fi.IsDir() {
		return 0755
	}
	return 0664
}

func (fi *FileInfo) ModTime() time.Time {
	return fi.ObjAtt.Updated
}

func (fi *FileInfo) IsDir() bool {
	return fi.ObjAtt.Metadata["virtual_folder"] == "y"
}

func (fi *FileInfo) Sys() interface{} {
	return nil
}

type ByName []*FileInfo

func (a ByName) Len() int {
	return len(a)
}

func (a ByName) Swap(i, j int) {
	a[i].ObjAtt, a[j].ObjAtt = a[j].ObjAtt, a[i].ObjAtt
}

func (a ByName) Less(i, j int) bool {
	return strings.Compare(a[i].Name(), a[j].Name()) == -1
}
