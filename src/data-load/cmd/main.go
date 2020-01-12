package main

import (
    "context"
    "fmt"
    "io/ioutil"
    "log"
    "os"
    "path/filepath"
    "pp"
    "strings"
)

// Загружаем исторические данные в DWH
// Стартуем с папки с номером года, которая содержит папки с названиями партнёров,
// которые содержат файлы с данными, имена которых оканчиваются на номер месяца после тире
func main() {
    ctx := context.Background()

    // Читаем аргументы коммандной строки
    // Если каталог задан в аргументах, читаем его
    args := os.Args[1:]

    var basedir string
    if len(args) == 1 {
        basedir = args[0]
    } else {
        basedir = "../../../data/2019"
    }

    projectID := "my-project-1530001957977"
    datasetID := "PP"
    bds := strings.Split(basedir, "/")
    year := bds[len(bds)-1]

    // Читаем базовый каталог
    dirs, err := ioutil.ReadDir(basedir)
    if err != nil {
        fmt.Printf("Error reading #{basedir}: #{err}\n")
        os.Exit(-1)
    }

    // Ищем папки - названия партнёров
    for _, d := range dirs {
        if d.IsDir() {
            fmt.Println(d.Name())

            // Читаем содержимое папки
            files, err := ioutil.ReadDir(basedir + "/" + d.Name())
            if err != nil {
                fmt.Printf("Error reading partner's folder #{basedir}/#{d.Name()}: #{err}\n")
                continue
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
                    filename := basedir + "/" + d.Name() + "/" + f.Name()

                    // Загружаем файл в области Stage
                    err := pp.LoadFileToTable(ctx, projectID, datasetID, "STG_" + tableID, filename)
                    if err != nil {
                        log.Printf("Error loading file #{filename} into table STG_#{tableID}: #{err}")
                        continue
                    }

                    // Если файл загружен успешно в область Stage, то вызываем процедуру для перегрузки данных в области ODS и DDS
                    num := strings.Split(strings.Split(f.Name(), ".")[0], "-")[1]
                    query := "CALL " + datasetID + ".LOAD_" + tableID + "('" + f.Name() + "', " + year + ", " + num + ");"
                    err = pp.RunQuery(ctx, projectID, query)
                    if err != nil {
                        log.Printf("Error running query '#{query}': #{err}")
                        continue
                    }
                }
            }
        }
    }
}
