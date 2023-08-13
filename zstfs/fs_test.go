package zstfs

import (
	"github.com/klauspost/compress/zstd"
	"github.com/melaurent/kafero"
	"github.com/melaurent/kafero/tests"
	"testing"
)

func TestWrite(t *testing.T) {
	fs := kafero.NewMemMapFs()
	zfs := NewFs(fs, zstd.SpeedBetterCompression)
	// TODO
	tests.TestWriteFile(t, zfs, "file.txt", 1000)
}
