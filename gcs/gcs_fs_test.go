package gcs

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/melaurent/kafero"
	"google.golang.org/api/option"
	"os"
	"testing"
)

func TestGcsFs_Create(t *testing.T) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile("test-fixtures/service-account.json"))
	if err != nil {
		t.Fatalf("error creating client")
	}
	fs := NewGcsFs(ctx, client, "patrick-data", "/")
	/*
	_, err = fs.Create("dede/dedede/odkoadoadwak.tick")
	if err != nil {
		t.Fatalf("error creating file: %v", err)
	}


	 */
	err = kafero.Walk(fs, "data/", func(path string, info os.FileInfo, err error) error {
		if info != nil && !info.IsDir() {
			fmt.Println("walk", path)
		}
		return nil
	})

	if err != nil {
		t.Fatalf("error walking dir: %v", err)
	}
}
