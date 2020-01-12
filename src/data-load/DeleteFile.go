package pp

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
)

// DeleteFile удаляет файл
func DeleteFile(ctx context.Context, bucket string, name string) error {
	// Создаём клиента для Storage
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}

	// Удаляем файл
	err = client.Bucket(bucket).Object(name).Delete(ctx)
	if err != nil {
		return fmt.Errorf("client.Bucket.Object.Delete: %v", err)
	}

	// Закрываем клиента и выходим
	_ = client.Close()
	return nil
}
