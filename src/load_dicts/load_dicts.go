package pp

import (
	"context"
	"fmt"
	"log"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
)

// GCSEvent is the payload of a GCS event. Please refer to the docs for
// additional information regarding GCS events.
type GCSEvent struct {
	Bucket string `json:"bucket"`
	Name   string `json:"name"`
}

func LoadDicts(ctx context.Context, e GCSEvent) error {
	log.Printf("Processing bucket: %s", e.Bucket)
	log.Printf("Processing file: %s", e.Name)

	projectID := "my-project-1530001957977"
	datasetID := "PP"
	splitName := strings.Split(e.Name, "/")
	fileName := splitName[len(splitName)-1]

	// Определяем папку, в которой появился файл
	// Загружаем только файлы, которые появились в папке dicts
	// Если файл появился в папке inbox (реестр партнёра) - ничего не делаем
	// Реестры обрабатываются другой процедурой по расписанию
	if splitName[0] != "dicts" {
		return nil
	}

	// По имени файла получаем название справочника (имя таблицы)
	tableID := strings.ToUpper(strings.Split(fileName, ".")[0])

	// Создаём клиента для BigQuery
	clientBQ, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}

	gcsRef := bigquery.NewGCSReference("gs://" + e.Bucket + "/" + e.Name)
	gcsRef.SourceFormat = bigquery.CSV
	gcsRef.AutoDetect = true
	gcsRef.SkipLeadingRows = 1

	// Загружаем файл в таблицу в области Stage, перезаписывая её
	loader := clientBQ.Dataset(datasetID).Table("STG_" + tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteTruncate

	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	if status.Err() != nil {
		return fmt.Errorf("Load job completed with error: %v", status.Err())
	}

	// Если файл загружен успешно в область Stage, то вызываем процедуру для перегрузки данных в области ODS и DDS
	q := clientBQ.Query("CALL " + datasetID + ".LOAD_" + tableID + "();")
	job, err = q.Run(ctx)
	if err != nil {
		return fmt.Errorf("Call procedure job completed with error: %v", err)
	}

	// Создаём клиента для Storage
	clientS, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}

	// Удаляем файл
	src := clientS.Bucket(e.Bucket).Object(e.Name)
	err = src.Delete(ctx)
	if err != nil {
		return fmt.Errorf("Delete file job completed with error: %v", err)
	}

	return nil
}
