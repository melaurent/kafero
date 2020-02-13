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

package kafero

import (
	"context"
	"fmt"
	"github.com/melaurent/kafero/gcs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// TODO walk returns folder file ???

// GcsFs is a Fs implementation that uses functions provided by google cloud storage
type GcsFs struct {
	ctx       context.Context
	client    *storage.Client
	bucket    *storage.BucketHandle
	separator string
}

func NewGcsFs(ctx context.Context, cl *storage.Client, bucket string, folderSep string) *GcsFs {
	return &GcsFs{
		ctx:       ctx,
		client:    cl,
		bucket:    cl.Bucket(bucket),
		separator: folderSep,
	}
}

// normSeparators will normalize all "\\" and "/" to the provided separator
func normSeparators(s string, to string) string {
	return strings.Replace(strings.Replace(s, "\\", to, -1), "/", to, -1)
}

func (fs *GcsFs) ensureTrailingSeparator(s string) string {
	if len(s) > 0 && !strings.HasSuffix(s, fs.separator) {
		return s + fs.separator
	}
	return s
}

func (fs *GcsFs) trimRoot(s string) string {
	if len(s) > 0 && string(s[0]) == fs.separator {
		return s[1:]
	} else {
		return s
	}
}

func (fs *GcsFs) getObj(name string) *storage.ObjectHandle {
	return fs.bucket.Object(normSeparators(name, fs.separator)) //normalize paths for ll oses
}

func (fs *GcsFs) Name() string { return "GcsFs" }

func (fs *GcsFs) Create(name string) (File, error) {
	return fs.OpenFile(name, os.O_RDWR|os.O_CREATE, 0)
}

func (fs *GcsFs) Mkdir(name string, perm os.FileMode) error {
	base := path.Base(name)
	if base == "." || base == ".." {
		return nil
	}
	name = fs.trimRoot(name)
	name = filepath.Clean(normSeparators(name, fs.separator))
	obj := fs.getObj(name)
	w := obj.NewWriter(fs.ctx)
	if err := w.Close(); err != nil {
		return err
	}
	meta := make(map[string]string)
	meta["virtual_folder"] = "y"
	_, err := obj.Update(fs.ctx, storage.ObjectAttrsToUpdate{Metadata: meta})
	//fmt.Printf("Created virtual folder: %v\n", name)
	return err
}

func (fs *GcsFs) MkdirAll(path string, perm os.FileMode) error {

	exists, err := Exists(fs, path)
	if err != nil {
		return fmt.Errorf("error determining if file exists: %v", err)
	}

	if exists {
		return nil
	}

	path = fs.trimRoot(path)

	root := ""
	folders := strings.Split(normSeparators(path, fs.separator), fs.separator)
	for _, f := range folders {
		// Don't force a delimiter prefix
		if root != "" {
			root = root + fs.separator + f
		} else {
			root = f
		}

		if err := fs.Mkdir(root, perm); err != nil {
			return err
		}
	}
	return nil
}

func (fs *GcsFs) Open(name string) (File, error) {
	return fs.OpenFile(name, os.O_RDONLY, 0)
}

func (fs *GcsFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	// No distinction between root and cwd !!TODO ?
	name = fs.trimRoot(name)
	dir := filepath.Dir(name)

	// If create flag, ensure directory exists
	if flag&os.O_CREATE != 0 && dir != "." {
		if _, err := fs.Stat(dir); err == os.ErrNotExist {
			return nil, fmt.Errorf("create %s: no such file or directory", name)
		}
	}

	file, err := gcs.NewGcsFile(fs.ctx, fs.bucket, fs.getObj(name), fs.separator, flag, name)
	if err != nil {
		// Don't decorate error, as implementations depend on knowing
		// if err is ErrExists or ErrNotExists etc..
		return nil, err
	}

	return file, nil
}

func (fs *GcsFs) Remove(name string) error {
	name = fs.trimRoot(name)
	obj := fs.getObj(name)
	if _, err := fs.Stat(name); err != nil {
		return err
	}
	return obj.Delete(fs.ctx)
}

func (fs *GcsFs) RemoveAll(path string) error {
	path = fs.trimRoot(path)
	path = fs.ensureTrailingSeparator(path)

	it := fs.bucket.Objects(fs.ctx, &storage.Query{
		Delimiter: fs.separator,
		Prefix:    path,
		Versions:  false})
	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("error iterating objects: %v", err)
		}
		if objAttrs.Name != "" {
			if err := fs.Remove(objAttrs.Name); err != nil {
				return err
			}
		} else if objAttrs.Prefix != "" {
			if err := fs.RemoveAll(objAttrs.Prefix); err != nil {
				return err
			}
		}
	}

	// TODO delete the folder file
	return nil
}

func (fs *GcsFs) Rename(oldname, newname string) error {
	oldname = fs.trimRoot(oldname)
	newname = fs.trimRoot(newname)

	src := fs.bucket.Object(oldname)
	dst := fs.bucket.Object(newname)

	if _, err := dst.CopierFrom(src).Run(fs.ctx); err != nil {
		return err
	}
	return src.Delete(fs.ctx)
}

func (fs *GcsFs) Stat(name string) (os.FileInfo, error) {
	name = fs.trimRoot(name)

	obj := fs.getObj(name)
	objAttrs, err := obj.Attrs(fs.ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, os.ErrNotExist //works with os.IsNotExist check
		}
		return nil, err
	}
	return &gcs.FileInfo{objAttrs}, nil
}

func (fs *GcsFs) Chmod(name string, mode os.FileMode) error {
	return fmt.Errorf("chmod not implemented")
}

func (fs *GcsFs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return fmt.Errorf("chtimes not implemented: Create, Delete, Updated times are read only fields in GCS and set implicitly")
}

func (fs *GcsFs) Walk(root string, walkFn filepath.WalkFunc) error {

	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	it := fs.bucket.Objects(ctx, &storage.Query{
		Prefix: root,
	})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		var info *gcs.FileInfo
		fName := ""
		if attrs != nil {
			fName = attrs.Name
			info = &gcs.FileInfo{
				ObjAtt: attrs,
			}
		} else {
			fmt.Println("NIL ATTRIBUTE", err)
		}

		if err := walkFn(fName, info, err); err != nil {
			return err
		}
	}

	return nil
}
