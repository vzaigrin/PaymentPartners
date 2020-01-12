package pp

import (
	"context"
	"fmt"
	"os"
	"strings"

	"cloud.google.com/go/bigquery"
)

// LoadFileToTable загружает файл в таблицу
func LoadFileToTable(ctx context.Context, projectID string, datasetID string, tableID string, filename string) error {
	// Создаём клиента для BigQuery
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: #{err}")
	}

	var loader *bigquery.Loader

	// Определяем источник по префиксу имени файла
	if strings.HasPrefix(filename, "gs://") {
		// Создаем gcsRef
		source := bigquery.NewGCSReference(filename)
		// Устанавливаем параметры
		source.SourceFormat = bigquery.CSV
		source.AutoDetect = true   // Allow BigQuery to determine schema.
		source.SkipLeadingRows = 1 // CSV has a single header line.
		loader = client.Dataset(datasetID).Table(tableID).LoaderFrom(source)
	} else {
		// Читаем локальный файл
		fo, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("open file #{filename}: #{err}")
		}
		source := bigquery.NewReaderSource(fo)
		// Устанавливаем параметры
		source.SourceFormat = bigquery.CSV
		source.AutoDetect = true   // Allow BigQuery to determine schema.
		source.SkipLeadingRows = 1 // CSV has a single header line.
		loader = client.Dataset(datasetID).Table(tableID).LoaderFrom(source)
	}

	// Загружаем файл в таблицу, перезаписывая её
	loader.WriteDisposition = bigquery.WriteTruncate

	job, err := loader.Run(ctx)
	if err != nil {
		return fmt.Errorf("loader.Run: #{err}")
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("job.Wait: #{err}")
	}

	if status.Err() != nil {
		return fmt.Errorf("loader: #{status.Err()}")
	}

	// Закрываем клиента и выходим
	_ = client.Close()
	return nil
}
