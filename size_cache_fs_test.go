package kafero

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestSizeCacheFS_Size(t *testing.T) {
	// Write 10 10 bytes files, check if size is 100
	var cacheFs, _ = NewSizeCacheFS(&MemMapFs{}, &MemMapFs{}, 1e+9, 0)
	for i := 0; i < 10; i++ {
		f, err := cacheFs.Create(fmt.Sprintf("%d.txt", i))
		if err != nil {
			t.Fatalf("error creating test file: %v", err)
		}
		if _, err := f.WriteString("0123456789"); err != nil {
			t.Fatalf("error writing string: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("error closing file: %v", err)
		}
	}

	if cacheFs.currSize != 100 {
		t.Fatalf("was expecting a cache of size 100, got %d", cacheFs.currSize)
	}

	// Delete 5 files, check if size is 50
	for i := 0; i < 5; i++ {
		err := cacheFs.Remove(fmt.Sprintf("%d.txt", i))
		if err != nil {
			t.Fatalf("error removing test file: %v", err)
		}
	}

	if cacheFs.currSize != 50 {
		t.Fatalf("was expecting a cache of size 50, got %d", cacheFs.currSize)
	}
}

func TestSizeCacheFS_Evict(t *testing.T) {
	// Write 11 10 bytes files, check if size is 100
	var cacheFs, _ = NewSizeCacheFS(&MemMapFs{}, &MemMapFs{}, 100, 0)
	for i := 0; i < 11; i++ {
		f, err := cacheFs.Create(fmt.Sprintf("%d.txt", i))
		if err != nil {
			t.Fatalf("error creating test file: %v", err)
		}
		if _, err := f.WriteString("0123456789"); err != nil {
			t.Fatalf("error writing string: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("error closing file: %v", err)
		}
	}

	if cacheFs.currSize != 100 {
		t.Fatalf("was expecting a cache of size 100, got %d", cacheFs.currSize)
	}

	// Delete 5 files, check if size is 60 because file 0.txt has been evicted
	for i := 0; i < 5; i++ {
		err := cacheFs.Remove(fmt.Sprintf("%d.txt", i))
		if err != nil {
			t.Fatalf("error removing test file: %v", err)
		}
	}

	if cacheFs.currSize != 60 {
		t.Fatalf("was expecting a cache of size 60, got %d", cacheFs.currSize)
	}
}

func TestSizeCacheFS_EvictOpen(t *testing.T) {
	// Write 11 10 bytes files, check if size is 100
	var cacheFs, _ = NewSizeCacheFS(&MemMapFs{}, &MemMapFs{}, 100, 0)

	// Create first file
	f, err := cacheFs.Create(fmt.Sprintf("%d.txt", 0))
	if err != nil {
		t.Fatalf("error creating test file: %v", err)
	}

	// Create new files to evict first one
	for i := 1; i < 11; i++ {
		f, err := cacheFs.Create(fmt.Sprintf("%d.txt", i))
		if err != nil {
			t.Fatalf("error creating test file: %v", err)
		}
		if _, err := f.WriteString("0123456789"); err != nil {
			t.Fatalf("error writing string: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("error closing file: %v", err)
		}
	}

	if cacheFs.currSize != 100 {
		t.Fatalf("was expecting a cache of size 100, got %d", cacheFs.currSize)
	}

	if _, err := f.WriteString("0123456789"); err != nil {
		t.Fatalf("error writing string: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("error closing file: %v", err)
	}

	if cacheFs.currSize != 100 {
		t.Fatalf("was expecting a cache of size 110, got %d", cacheFs.currSize)
	}

	// Delete 5 files, check if size is 60 because file 0.txt has been evicted
	for i := 0; i < 5; i++ {
		err := cacheFs.Remove(fmt.Sprintf("%d.txt", i))
		if err != nil {
			t.Fatalf("error removing test file: %v", err)
		}
	}

	if cacheFs.currSize != 60 {
		t.Fatalf("was expecting a cache of size 60, got %d", cacheFs.currSize)
	}
}

func TestSizeCacheFS_Update(t *testing.T) {
	var cacheFs, _ = NewSizeCacheFS(&MemMapFs{}, &MemMapFs{}, 100, 0)

	// Create file
	f, err := cacheFs.Create(fmt.Sprintf("%d.txt", 0))
	if err != nil {
		t.Fatalf("error creating test file: %v", err)
	}
	if _, err := f.WriteString("0123456789"); err != nil {
		t.Fatalf("error writing string: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("error closing file: %v", err)
	}

	if cacheFs.currSize != 10 {
		t.Fatalf("was expecting a cache of size 10, got %d", cacheFs.currSize)
	}

	f, err = cacheFs.OpenFile(fmt.Sprintf("%d.txt", 0), os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("error opening test file: %v", err)
	}
	if cacheFs.currSize != 0 {
		t.Fatalf("was expecting a cache of size 0, got %d", cacheFs.currSize)
	}
	if _, err := f.WriteString("0123456789"); err != nil {
		t.Fatalf("error writing string: %v", err)
	}
	if cacheFs.currSize != 0 {
		t.Fatalf("was expecting a cache of size 0, got %d", cacheFs.currSize)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("error closing file: %v", err)
	}
	if cacheFs.currSize != 20 {
		t.Fatalf("was expecting a cache of size 20, got %d", cacheFs.currSize)
	}
}

func TestSizeCacheFS_Index(t *testing.T) {
	base := &MemMapFs{}
	cache := &MemMapFs{}

	// Write 10 10 bytes files, check if size is 100
	var cacheFs, _ = NewSizeCacheFS(cache, base, 100, 0)
	for i := 0; i < 10; i++ {
		f, err := cacheFs.Create(fmt.Sprintf("%d.txt", i))
		if err != nil {
			t.Fatalf("error creating test file: %v", err)
		}
		if _, err := f.WriteString("0123456789"); err != nil {
			t.Fatalf("error writing string: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("error closing file: %v", err)
		}
	}

	// Test index building
	cacheFs, _ = NewSizeCacheFS(cache, base, 100, 0)
	if cacheFs.currSize != 100 {
		t.Fatalf("was expecting cache size of 100, got %d", cacheFs.currSize)
	}

	if err := cacheFs.Close(); err != nil {
		t.Fatalf(err.Error())
	}

	// Test index marshal/unmarshal
	cacheFs, _ = NewSizeCacheFS(cache, base, 100, 0)
	if cacheFs.currSize != 100 {
		t.Fatalf("was expecting cache size of 100, got %d", cacheFs.currSize)
	}
}

func TestSizeCacheFS_RemoveAll(t *testing.T) {
	base := &MemMapFs{}
	cache := &MemMapFs{}

	// Write 10 10 bytes files, check if size is 100
	var cacheFs, _ = NewSizeCacheFS(cache, base, 100, 0)

	// Keep one file open
	openF, err := cacheFs.Create("open.txt")
	if err != nil {
		t.Fatalf("error creating test file: %v", err)
	}
	if _, err := openF.WriteString("0123456789"); err != nil {
		t.Fatalf("error writing string: %v", err)
	}

	for i := 0; i < 10; i++ {
		f, err := cacheFs.Create(fmt.Sprintf("tmp/tmp/%d.txt", i))
		if err != nil {
			t.Fatalf("error creating test file: %v", err)
		}
		if _, err := f.WriteString("0123456789"); err != nil {
			t.Fatalf("error writing string: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("error closing file: %v", err)
		}
	}

	for i := 0; i < 10; i++ {
		f, err := cacheFs.Create(fmt.Sprintf("tmp/tip/%d.txt", i))
		if err != nil {
			t.Fatalf("error creating test file: %v", err)
		}
		if _, err := f.WriteString("0123456789"); err != nil {
			t.Fatalf("error writing string: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("error closing file: %v", err)
		}
	}

	if err := cacheFs.RemoveAll("tmp"); err != nil {
		t.Fatalf("error removing all: %v", err)
	}

	if cacheFs.currSize != 0 {
		t.Fatalf("was expecting size of 0, got %d", cacheFs.currSize)
	}

	if err := openF.Close(); err != nil {
		t.Fatalf("error closing file: %v", err)
	}

	if cacheFs.currSize != 10 {
		t.Fatalf("was expecting size of 10, got %d", cacheFs.currSize)
	}
}

func TestSizeCacheFS_ReadEvicted(t *testing.T) {
	base := &MemMapFs{}
	cache := &MemMapFs{}

	// Write 2 10 bytes files
	var cacheFs, _ = NewSizeCacheFS(cache, base, 10, 0)
	for i := 0; i < 2; i++ {
		f, err := cacheFs.Create(fmt.Sprintf("%d.txt", i))
		if err != nil {
			t.Fatalf("error creating test file: %v", err)
		}
		if _, err := f.WriteString("0123456789"); err != nil {
			t.Fatalf("error writing string: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("error closing file: %v", err)
		}
	}

	evicted, err := cacheFs.Open("0.txt")
	if err != nil {
		t.Fatalf("error opening evicted file: %v", err)
	}

	data, err := ioutil.ReadAll(evicted)
	if err != nil {
		t.Fatalf("error reading evicted file: %v", err)
	}

	if string(data) != "0123456789" {
		t.Fatalf("evicted file has wrong content")
	}

	if err := evicted.Close(); err != nil {
		t.Fatalf("error closing evicted file: %v", err)
	}
}

func TestSizeCacheFSProfile(t *testing.T) {
	memFs := &MemMapFs{}
	if err := memFs.Mkdir("tmp", 0744); err != nil {
		t.Fatal(err)
	}
	base := NewBasePathFs(memFs, "tmp")
	cache := &MemMapFs{}

	// Write 100 10 bytes files, check if size is 100
	var cacheFs, _ = NewSizeCacheFS(base, cache, 100, 0)
	for i := 0; i < 100; i++ {
		f, err := cacheFs.Create(fmt.Sprintf("%d.txt", i))
		if err != nil {
			t.Fatalf("error creating test file: %v", err)
		}
		if _, err := f.WriteString("0123456789"); err != nil {
			t.Fatalf("error writing string: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("error closing file: %v", err)
		}
	}
	_ = cacheFs.Close()
}
