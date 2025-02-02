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
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}

	// Получаем схему таблицы
	md, err := client.Dataset(datasetID).Table(tableID).Metadata(ctx)
	if err != nil {
		return fmt.Errorf("bigquery.Dataset.Table.Metadata: %v", err)
	}
	schema := md.Schema

	// Создаём загрузчик
	var loader *bigquery.Loader

	// Определяем источник по префиксу имени файла
	if strings.HasPrefix(filename, "gs://") {
		// Создаем gcsRef
		source := bigquery.NewGCSReference(filename)
		// Устанавливаем параметры
		source.SourceFormat = bigquery.CSV
		source.Schema = schema
		source.SkipLeadingRows = 1 // CSV has a single header line.
		source.FieldDelimiter = ","
		loader = client.Dataset(datasetID).Table(tableID).LoaderFrom(source)
	} else {
		// Читаем локальный файл
		fo, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("open file %v: %v", filename, err)
		}
		source := bigquery.NewReaderSource(fo)
		// Устанавливаем параметры
		source.SourceFormat = bigquery.CSV
		source.Schema = schema
		source.SkipLeadingRows = 1 // CSV has a single header line.
		source.FieldDelimiter = ","
		loader = client.Dataset(datasetID).Table(tableID).LoaderFrom(source)
	}

	// Загружаем файл в таблицу, перезаписывая её
	loader.WriteDisposition = bigquery.WriteTruncate

	job, err := loader.Run(ctx)
	if err != nil {
		return fmt.Errorf("loader.Run: %v", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("job.Wait: %v", err)
	}

	if status.Err() != nil {
		return fmt.Errorf("loader: %v", status.Err())
	}

	// Закрываем клиента и выходим
	_ = client.Close()
	return nil
}
