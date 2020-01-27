package pp

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// PubSubMessage is the payload of a Pub/Sub event
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// LoadData загружает данные по сообщению в Pub/Sub
func LoadData(ctx context.Context, m PubSubMessage) error {
	name := string(m.Data)
	log.Printf("Processing message: %v", name)
	if name != "go" {
		log.Printf("This is not a message to process")
		return nil
	}

	//projectID := "my-project-1530001957977"
	projectID := os.Getenv("GCP_PROJECT")
	datasetID := "PP"
	bucket := "pps"

	// Вычисляем предыдущий месяц по текущей дате
	time1 := time.Now().AddDate(0, -1, 0)
	year, month, _ := time1.Date()

	// Создаём клинта для Storage
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Printf("storage.NewClient: %v", err)
		return err
	}

	// Читаем папку inbox
	it := client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: "inbox"})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("iterator %v: %v", attrs.Name, err)
			return err
		}
		log.Printf("Processing %v", attrs.Name)

		splitName := strings.Split(attrs.Name, "/")

		// Пропускаем папки
		if len(splitName) < 3 {
			continue
		}

		fileName := splitName[len(splitName)-1]
		partner := strings.ToUpper(splitName[1])
		tableID := "STG_" + partner

		// Загружаем только файлы CSV
		splitFile := strings.Split(fileName, ".")
		if len(splitFile) < 2 || strings.ToUpper(splitFile[1]) != "CSV" {
			continue
		}

		// Загружаем файл в таблицу в области Stage, перезаписывая её
		err = LoadFileToTable(ctx, projectID, datasetID, tableID, "gs://"+bucket+"/"+attrs.Name)
		if err != nil {
			log.Printf("Error loading file %v into table '%v': %v", attrs.Name, tableID, err)
			return err
		}

		// Если файл загружен успешно в область Stage, то вызываем процедуру для перегрузки данных в области ODS и DDS
		query := "CALL " + datasetID + ".LOAD_" + partner + "('" + fileName + "'," + strconv.Itoa(year) + ", " + strconv.Itoa(int(month)) + ");"
		err = RunQuery(ctx, projectID, query)
		if err != nil {
			log.Printf("Error running query '%v': %v", query, err)
			return err
		}

		// Если файл успешно загружен в таблицу, то удаляем его
		err = DeleteFile(ctx, bucket, attrs.Name)
		if err != nil {
			log.Printf("Error deleting file %v: %v", attrs.Name, err)
			return err
		}
	}

	// Вызываем процедуру формирования отчёта
	query := "CALL " + datasetID + ".DATA2DM(" + strconv.Itoa(year) + ", " + strconv.Itoa(int(month)) + ");"
	err = RunQuery(ctx, projectID, query)
	if err != nil {
		log.Printf("Error running query '%v': %v", query, err)
		return err
	}

	// Закрываем клиента и выходим
	_ = client.Close()
	return nil
}
