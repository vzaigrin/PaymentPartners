package pp

import (
	"context"
	"log"
	"os"
	shared "pp/shared"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// PubSubMessage is the payload of a Pub/Sub event.
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// GoPubSub загружает данные по сообщению в Pub/Sub
func GoPubSub(ctx context.Context, m PubSubMessage) error {
	name := string(m.Data)
	log.Printf("Processing message: #{name}")
	if name != "go" {
		return nil
	}

	//projectID := "my-project-1530001957977"
	projectID := os.Getenv("GCP_PROJECT")
	datasetID := "PP"
	bucket := "pps"

	// Создаём клинта для Storage
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Printf("storage.NewClient: #{err}")
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
			log.Printf("iterator #{attrs.Name}: #{err}")
			return err
		}
		log.Printf("Processing #{attrs.Name}")

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

		// Загружаем файл в таблицу в области Stage, перезаписывая её
		err = shared.LoadFileToTable(ctx, projectID, datasetID, "STG_"+tableID, "gs://" + bucket + "/" + attrs.Name)
		if err != nil {
			log.Printf("Error loading file #{attrs.Name} into table 'STG_#{tableID}': #{err}")
			return err
		}

		// Если файл загружен успешно в область Stage, то вызываем процедуру для перегрузки данных в области ODS и DDS
		// Вычисляем предыдущий месяц по текущей дате
		time1 := time.Now().AddDate(0, -1, 0)
		year, month, _ := time1.Date()

		query := "CALL " + datasetID + ".LOAD_" + tableID + "('" + fileName + "'," + strconv.Itoa(year) + ", " + strconv.Itoa(int(month)) + ");"
		err = shared.RunQuery(ctx, projectID, query)
		if err != nil {
			log.Printf("Error running query '#{query}': #{err}")
			return err
		}

		// Если файл успешно загружен в таблицу, то удаляем его
		err = shared.DeleteFile(ctx, bucket, attrs.Name)
		if err != nil {
			log.Printf("Error deleting file #{attrs.Name}: #{err}")
			return err
		}
	}

	// Закрываем клиента и выходим
	_ = client.Close()
	return nil
}
