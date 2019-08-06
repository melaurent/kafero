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
	"log"
	"os"
	"sort"

	"google.golang.org/api/iterator"
)

// GcsFs is the Afero version adapted for GCS
type GcsFile struct {
	openFlags int
	fileMode  os.FileMode
	fhoffset  int64 //File handle specific offset
	closed    bool
	ReadDirIt *storage.ObjectIterator
	resource  *gcsFileResource
}

func NewGcsFile(
	ctx context.Context,
	fs *GcsFs,
	obj *storage.ObjectHandle,
	openFlags int,
	fileMode os.FileMode,
	name string,
) *GcsFile {
	return &GcsFile{
		openFlags: openFlags,
		fileMode:  fileMode,
		fhoffset:  0,
		closed:    false,
		ReadDirIt: nil,
		resource: &gcsFileResource{
			ctx: ctx,
			fs:  fs,

			obj:  obj,
			name: name,

			currentGcsSize: 0,

			offset: 0,
			reader: nil,
			writer: nil,
		},
	}
}

func NewGcsFileFromOldFH(
	openFlags int,
	fileMode os.FileMode,
	oldFile *gcsFileResource,
) *GcsFile {
	return &GcsFile{
		openFlags: openFlags,
		fileMode:  fileMode,
		fhoffset:  0,
		closed:    false,
		ReadDirIt: nil,

		resource: oldFile,
	}
}

func (f *GcsFile) Close() error {
	// There shouldn't be a case where both are open at the same time
	// but the check is omitted at this time.
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
	log.Printf("WARNING; Seek behavior triggerd, highly inefficent. Offset before seek is at %d\n", f.fhoffset)

	//Force the reader/writers to be reopened (at correct offset)
	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("error syncing file: %v", err)
	}
	stat, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("error getting file stat: %v", err)
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

	if f.openFlags & os.O_RDONLY != 0 {
		return 0, fmt.Errorf("file is opend as read only")
	}

	written, err := f.resource.WriteAt(b, off)
	f.fhoffset += int64(written)
	return written, err
}

func (f *GcsFile) Name() string {
	return f.resource.name
}

func (f *GcsFile) readdir(count int) ([]*fileInfo, error) {
	if err := f.Sync(); err != nil {
		return nil, fmt.Errorf("error syncing file")
	}
	//normSeparators should maybe not be here; adds
	path := f.resource.fs.ensureTrailingSeparator(normSeparators(f.Name(), f.resource.fs.separator))
	if f.ReadDirIt == nil {
		//log.Printf("Querying path : %s\n", path)
		f.ReadDirIt = f.resource.fs.bucket.Objects(
			f.resource.ctx, &storage.Query{f.resource.fs.separator, path, false})
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

		tmp := fileInfo{object, f.resource.fs}

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
	objAttrs, err := f.resource.obj.Attrs(f.resource.ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, os.ErrNotExist //works with os.IsNotExist check
		}
		return nil, fmt.Errorf("error getting resource attributes: %v", err)
	}
	return &fileInfo{objAttrs, f.resource.fs}, nil
}

func (f *GcsFile) Sync() error {
	return f.resource.maybeCloseIo()
}

func (f *GcsFile) Truncate(wantedSize int64) error {
	if f.closed {
		return ErrFileClosed
	}
	if f.openFlags & os.O_RDONLY != 0 {
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

