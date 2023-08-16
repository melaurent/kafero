package zstfs

import (
	"github.com/klauspost/compress/zstd"
	"github.com/melaurent/kafero"
	"io"
	"syscall"
)

type File struct {
	kafero.File
	flag          int
	fs            kafero.Fs
	reader        *zstd.Decoder
	writer        *zstd.Encoder
	readOffset    int
	isdir, closed bool
}

func (f *File) Close() error {
	f.closed = true
	if f.writer != nil {
		if err := f.writer.Close(); err != nil {
			return err
		}
		f.writer = nil
	}
	if f.reader != nil {
		f.reader.Close()
		f.reader = nil
	}
	if err := f.File.Close(); err != nil {
		return err
	}
	f.closed = true
	return nil
}

func (f *File) Read(p []byte) (n int, err error) {
	if f.closed {
		return 0, kafero.ErrFileClosed
	}
	// Cannot read from a writer
	if f.writer != nil {
		return 0, syscall.EPERM
	}
	if f.reader == nil {
		f.reader, err = zstd.NewReader(f.File)
		if err != nil {
			return 0, err
		}
	}
	n, err = f.reader.Read(p)
	if err != nil {
		return n, err
	}
	// progress
	f.readOffset += n
	return n, nil
}

func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, syscall.EPERM
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	// Allow seek if it would result in a seek to the current position.
	switch whence {
	case io.SeekStart:
		if offset == 0 && f.readOffset == 0 {
			return 0, nil
		} else {
			return 0, syscall.EPERM
		}
	case io.SeekCurrent:
		if offset == 0 {
			return 0, nil
		} else if offset > 0 {
			// read and discard
			buf := make([]byte, offset)
			n, err := f.Read(buf)
			if err != nil {
				return 0, err
			}
			return int64(n), nil
		} else {
			return 0, syscall.EPERM
		}
	case io.SeekEnd:
		return 0, syscall.EPERM
	}
	return 0, syscall.EPERM
}

func (f *File) WriteString(s string) (ret int, err error) {
	return f.Write([]byte(s))
}

func (f *File) Write(p []byte) (n int, err error) {
	if f.flag&syscall.O_WRONLY == 0 && f.flag&syscall.O_RDWR == 0 {
		return 0, syscall.EPERM
	}
	if f.closed {
		return 0, kafero.ErrFileClosed
	}
	// Cannot write to a reader
	if f.reader != nil {
		return 0, syscall.EPERM
	}
	if f.writer == nil {
		f.writer, err = zstd.NewWriter(f.File)
		if err != nil {
			return 0, err
		}
	}
	return f.writer.Write(p)
}

func (f *File) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, syscall.EPERM
}

func (f *File) Truncate(size int64) error {
	return syscall.EPERM
}

func (f *File) CanMmap() bool {
	return false
}

func (f *File) Mmap(off int64, len int, prot, flags int) ([]byte, error) {
	return nil, syscall.EPERM
}

func (f *File) Munmap() error {
	return syscall.EPERM
}
