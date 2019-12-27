package pp

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// PubSubMessage is the payload of a Pub/Sub event.
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// GoPubSub consumes a Pub/Sub message.
func GoPubSub(ctx context.Context, m PubSubMessage) error {
	bucket := "pps"
	projectID := "my-project-1530001957977"
	datasetID := "PP"

	name := string(m.Data)
	if name != "go" {
		return nil
	}

	log.Printf("Let's %s!", name)

	// Создаём клиента для BigQuery
	clientBQ, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}

	// Создаём клинта для Storage
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close() // Closing the client safely cleans up background resources.

	// Читаем папку inbox
	it := client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: "inbox"})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		log.Printf(attrs.Name)

		splitName := strings.Split(attrs.Name, "/")

		// Пропускаем папки
		if len(splitName) < 3 {
			continue
		}

		fileName := splitName[len(splitName)-1]
		tableID := strings.ToUpper(splitName[1])

		// Загрузаем только файлы CSV
		splitFile := strings.Split(fileName, ".")
		if len(splitFile) < 2 && strings.ToUpper(splitFile[1]) != "CSV" {
			continue
		}

		gcsRef := bigquery.NewGCSReference("gs://" + bucket + "/" + attrs.Name)
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
		currentTime := time.Now()
		year := currentTime.Format("2006")
		month := currentTime.Format("1")

		q := clientBQ.Query("CALL " + datasetID + ".LOAD_" + tableID + "('" + fileName + "'," + year + ", " + month + ")")
		job, err = q.Run(ctx)
		if err != nil {
			return fmt.Errorf("Call procedure job completed with error: %v", err)
		}

		// Удаляем файл
		src := client.Bucket(bucket).Object(attrs.Name)
		err = src.Delete(ctx)
		if err != nil {
			return fmt.Errorf("Delete file job completed with error: %v", err)
		}
	}

	return nil
}
