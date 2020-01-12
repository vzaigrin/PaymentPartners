package pp

import (
	"context"
	"log"
	"os"
	"strings"
)

// GCSEvent is the payload of a GCS event.
type GCSEvent struct {
	Bucket string `json:"bucket"`
	Name   string `json:"name"`
}

// LoadDicts загружает новые справочники из папки dicts в DWH
func LoadDicts(ctx context.Context, e GCSEvent) error {
	log.Printf("Processing bucket: %v", e.Bucket)
	log.Printf("Processing file: %v", e.Name)

	//projectID := "my-project-1530001957977"
	projectID := os.Getenv("GCP_PROJECT")
	datasetID := "PP"
	splitName := strings.Split(e.Name, "/")
	fileName := splitName[len(splitName)-1]

	// Определяем папку, в которой появился файл
	// Загружаем только файлы, которые появились в папке dicts
	if splitName[0] != "dicts" {
		return nil
	}

	// По имени файла получаем название справочника (имя таблицы)
	// Имя файла должно совпадать с именем таблицы
	tableID := strings.ToUpper(strings.Split(fileName, ".")[0])

	// Загружаем файл в таблицу в области Stage
	err := LoadFileToTable(ctx, projectID, datasetID, "STG_"+tableID, "gs://"+e.Bucket+"/"+e.Name)
	if err != nil {
		log.Printf("Error loading table: %v", err)
		return err
	}

	// Если файл загружен успешно в область Stage, то вызываем процедуру для перегрузки данных в области ODS и DDS
	query := "CALL " + datasetID + ".LOAD_" + tableID + "();"
	err = RunQuery(ctx, projectID, query)
	if err != nil {
		log.Printf("Error running query '%v': %v", query, err)
		return err
	}

	// Если файл успешно загружен в таблицу, то удаляем его
	err = DeleteFile(ctx, e.Bucket, e.Name)
	if err != nil {
		log.Printf("Error deleting file %v: %v", e.Name, err)
		return err
	}

	return nil
}
