package kafero

import (
	"cloud.google.com/go/storage"
	"context"
	"google.golang.org/api/option"
)

func NewTestGcsFs() (*GcsFs, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile("gcs/test-fixtures/gcs-service-account.json"))
	if err != nil {
		return nil, err
	}
	fs := NewGcsFs(ctx, client, "kafero", "/")
	return fs, nil
}
