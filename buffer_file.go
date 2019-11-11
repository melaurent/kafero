package kafero

import (
	"fmt"
	"io"
	"os"
)

type BufferFile struct {
	LayerFs Fs
	Base    File
	Buffer  File
	Flag    int
	delete  bool
}

func NewBufferFile(base File, buffer File, flag int, layerFs Fs, delete bool) File {
	return &BufferFile{
		LayerFs: layerFs,
		Base:    base,
		Buffer:  buffer,
		Flag:    flag,
		delete:  delete,
	}
}

func (f *BufferFile) Close() error {
	if err := f.Sync(); err != nil {
		return fmt.Errorf("error syncing to base file: %v", err)
	}
	fstat, err := f.Base.Stat()
	if err != nil {
		return fmt.Errorf("error getting base file stat: %v", err)
	}
	if err := f.Buffer.Close(); err != nil {
		return fmt.Errorf("error closing buffer file: %v", err)
	}
	if err := f.Base.Close(); err != nil {
		return fmt.Errorf("error closing base file: %v", err)
	}
	if f.delete {
		_ = f.LayerFs.Remove(f.Buffer.Name())
	} else {
		_ = f.LayerFs.Chtimes(f.Buffer.Name(), fstat.ModTime(), fstat.ModTime())
	}
	return nil
}

func (f *BufferFile) Read(b []byte) (int, error) {
	return f.Buffer.Read(b)
}

func (f *BufferFile) ReadAt(b []byte, o int64) (int, error) {
	return f.Buffer.Read(b)
}

func (f *BufferFile) Seek(o int64, w int) (int64, error) {
	return f.Buffer.Seek(o, w)
}

func (f *BufferFile) Write(b []byte) (int, error) {
	n, err := f.Buffer.Write(b)
	if err != nil {
		return 0, err
	} else {
		return n, nil
	}
}

func (f *BufferFile) WriteAt(b []byte, o int64) (int, error) {
	n, err := f.Buffer.WriteAt(b, o)
	if err != nil {
		return 0, err
	} else {
		return n, nil
	}
}

func (f *BufferFile) Name() string {
	return f.Base.Name()
}

func (f *BufferFile) Readdir(c int) ([]os.FileInfo, error) {
	return f.Base.Readdir(c)
}

func (f *BufferFile) Readdirnames(c int) ([]string, error) {
	return f.Base.Readdirnames(c)
}

func (f *BufferFile) Stat() (os.FileInfo, error) {
	return f.Buffer.Stat()
}

func (f *BufferFile) Sync() error {
	if f.Flag == os.O_RDONLY {
		return nil
	}
	if err := f.Base.Truncate(0); err != nil {
		return fmt.Errorf("error truncating base file: %v", err)
	}
	if _, err := f.Base.Seek(0, 0); err != nil {
		return fmt.Errorf("error seeking base file to start: %v", err)
	}
	idx, err := f.Buffer.Seek(0, 1)
	if err != nil {
		return fmt.Errorf("error seeking buffer file: %v", err)
	}
	if _, err := f.Buffer.Seek(0, 0); err != nil {
		return fmt.Errorf("error seeking buffer file to start: %v", err)
	}
	if _, err := io.Copy(f.Base, f.Buffer); err != nil {
		return fmt.Errorf("error copying buffer to base file: %v", err)
	}
	if _, err := f.Buffer.Seek(idx, 0); err != nil {
		return fmt.Errorf("error seeking buffer file to start: %v", err)
	}
	if err := f.Base.Sync(); err != nil {
		return fmt.Errorf("error syncing base file: %v", err)
	}
	return nil
}

func (f *BufferFile) Truncate(s int64) error {
	return f.Buffer.Truncate(s)
}

func (f *BufferFile) WriteString(s string) (int, error) {
	return f.Buffer.Write([]byte(s))
}

func (f *BufferFile) CanMmap() bool {
	return f.Buffer.CanMmap()
}

func (f *BufferFile) Mmap(offset int64, length int, prot int, flags int) ([]byte, error) {
	return f.Buffer.Mmap(offset, length, prot, flags)
}

func (f *BufferFile) Munmap() error {
	return f.Buffer.Munmap()
}
