package kafero_test

import (
	"github.com/melaurent/kafero"
	"github.com/melaurent/kafero/tests"
	"github.com/melaurent/kafero/zstfs"
	"testing"
)

var tmpCacheFs, _ = kafero.NewSizeCacheFS(&kafero.MemMapFs{}, &kafero.MemMapFs{}, 0, 0)
var zstFs = zstfs.NewFs(&kafero.MemMapFs{}, 0)
var Fss = []kafero.Fs{&kafero.MemMapFs{}, &kafero.OsFs{}, tmpCacheFs, zstFs} //gcsFs}

type TestConfig struct {
	Fs          kafero.Fs
	CanSeek     bool
	CanTruncate bool
}

var testConfigs = []TestConfig{
	{Fs: &kafero.MemMapFs{}, CanSeek: true, CanTruncate: true},
	{Fs: &kafero.OsFs{}, CanSeek: true, CanTruncate: true},
	{Fs: tmpCacheFs, CanSeek: true, CanTruncate: true},
	{Fs: zstFs, CanSeek: false, CanTruncate: false},
}

func TestRead0(t *testing.T) {
	for _, config := range testConfigs {
		if config.CanSeek {
			tests.TestRead0(t, config.Fs)
		}
	}
}

func TestOpenFile(t *testing.T) {
	for _, config := range testConfigs {
		tests.TestOpenFile(t, config.Fs)
	}
}

func TestCreate(t *testing.T) {
	for _, config := range testConfigs {
		tests.TestCreate(t, config.Fs)
	}
}

func TestRename(t *testing.T) {
	for _, config := range testConfigs {
		tests.TestRename(t, config.Fs)
	}
}

func TestRemove(t *testing.T) {
	for _, config := range testConfigs {
		tests.TestRemove(t, config.Fs)
	}
}

func TestTruncate(t *testing.T) {
	for _, config := range testConfigs {
		if config.CanTruncate {
			tests.TestTruncate(t, config.Fs)
		}
	}
}

func TestSeek(t *testing.T) {
	for _, config := range testConfigs {
		if config.CanSeek {
			tests.TestSeek(t, config.Fs)
		}
	}
}

func TestReadAt(t *testing.T) {
	for _, config := range testConfigs {
		if config.CanSeek {
			tests.TestReadAt(t, config.Fs)
		}
	}
}

func TestWriteAt(t *testing.T) {
	for _, config := range testConfigs {
		if config.CanSeek {
			tests.TestWriteAt(t, config.Fs)
		}
	}
}

func TestReadDirNames(t *testing.T) {
	for _, config := range testConfigs {
		tests.TestReadDirNames(t, config.Fs)
	}
}

func TestReadDirSimple(t *testing.T) {
	for _, config := range testConfigs {
		tests.TestReadDirSimple(t, config.Fs)
	}
}

func TestReadDir(t *testing.T) {
	for _, config := range testConfigs {
		tests.TestReadDir(t, config.Fs)
	}
}

func TestReadDirRegularFiles(t *testing.T) {
	for _, config := range testConfigs {
		tests.TestReadDirRegularFiles(t, config.Fs)
	}
}

func TestReadDirAll(t *testing.T) {
	for _, config := range testConfigs {
		tests.TestReadDirAll(t, config.Fs)
	}
}
