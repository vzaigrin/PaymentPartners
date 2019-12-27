package main
 
import (
    "os"
    "context"
    "fmt"
    "io/ioutil"
    "path/filepath"
    "strings"

    "cloud.google.com/go/bigquery"
)

func main() {
    basedir := "../../data/2019/hist"
    projectID := "my-project-1530001957977"
    datasetID := "PP"

    // Создаём клиента для BigQuery
    ctx := context.Background()
    client, err := bigquery.NewClient(ctx, projectID)
    if err != nil {
    	fmt.Errorf("bigquery.NewClient: %v", err)
    }


    // Читаем базовый каталог
    dirs, err := ioutil.ReadDir(basedir)
    if err != nil {
        fmt.Errorf("Error reading basedir: %v", err)
    }

    // Ищем папки - названия партнёров
    for _, d := range dirs {
        if d.IsDir() {
            fmt.Println(d.Name())

            // Читаем содержимое папки
            files, err := ioutil.ReadDir(basedir + "/" + d.Name())
            if err != nil {
                fmt.Errorf("Error reading partner's folder: %v", err)
            }

            // Ищем CSV файлы в папке
            for _, f := range files {
                matched, err := filepath.Match("*.csv", f.Name())

                if err != nil {
                    fmt.Println(err)
                }

                if matched {
                    fmt.Println(f.Name())
                    tableID := strings.ToUpper(d.Name())

                    // Загружаем файл в область Stage
                    fo, err := os.Open(basedir + "/" + d.Name() + "/" + f.Name())
                    if err != nil {
                        fmt.Errorf("Error reading file: %v", err)
                    }
                    source := bigquery.NewReaderSource(fo)
                    source.AutoDetect = true   // Allow BigQuery to determine schema.
                    source.SkipLeadingRows = 1 // CSV has a single header line.
            
                    loader := client.Dataset(datasetID).Table("STG_" + tableID).LoaderFrom(source)
            
                    job, err := loader.Run(ctx)
                    if err != nil {
                        fmt.Errorf("Error running load job: %v", err)
                    }
                    status, err := job.Wait(ctx)
                    if err != nil {
                        fmt.Errorf("Error running load job: %v", err)
                    }
                    if err := status.Err(); err != nil {
                        fmt.Errorf("Error running load job: %v", err)
                    }

                    // Вызываем процедуру перегрузки из области Stage
                    num := strings.Split(strings.Split(f.Name(), ".")[0], "-")[1]
                    query := "CALL " + datasetID + ".LOAD_" + tableID + "('" + f.Name() + "', 2019, " + num + ")"
                    fmt.Println(query)
                    
                    q := client.Query(query)

                    job, err = q.Run(ctx)
                    if err != nil {
                        fmt.Errorf("Error running query job: %v", err)
                    }

                    status, err = job.Wait(ctx)
                    if err != nil {
                        fmt.Errorf("Error running query job: %v", err)
                    }

                    if err = status.Err(); err != nil {
                        fmt.Errorf("Error running query job: %v", err)
                    }
                }
            } 
        }
    }

}
