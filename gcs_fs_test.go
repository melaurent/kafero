package kafero

// TODO

/*
import (
	"fmt"
	"os"
	"testing"
)

func TestGcsFs_Create(t *testing.T) {
	fs, err := NewTestGcsFs()
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Create("dede/dedede/odkoadoadwak.tick")
	if err != nil {
		t.Fatalf("error creating file: %v", err)
	}

	err = Walk(fs, "dede", func(path string, info os.FileInfo, err error) error {
		if info != nil && !info.IsDir() {
			fmt.Println("walk", path)
		}
		return nil
	})

	if err != nil {
		t.Fatalf("error walking dir: %v", err)
	}
}

*/
