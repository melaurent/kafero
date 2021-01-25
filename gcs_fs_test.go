package kafero

// TODO

import (
	"fmt"
	"io/ioutil"
	"testing"
)

func TestGcsFs_Create(t *testing.T) {
	fs, err := NewTestGcsFs()
	if err != nil {
		t.Fatal(err)
	}

	file, err := fs.Create("test.txt")
	if err != nil {
		t.Fatalf("error creating file: %v", err)
	}

	if _, err := file.Write([]byte("hello bb")); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("hello bb")); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	file2, err := fs.Open("test.txt")
	b, err := ioutil.ReadAll(file2)
	fmt.Println(string(b))
}
