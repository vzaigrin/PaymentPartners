package main

import (
    "context"
    "fmt"
    "os"
    "pp"
    "strings"
)

// Загружаем исторические данные в DWH
func main() {
    ctx := context.Background()

    // Читаем аргументы коммандной строки
    // Должны быть заданы: название партнёра, год, месяц, имя файла
    args := os.Args[1:]

    if len(args) != 4 {
        fmt.Printf("Вызов: %v партнер год месяц файл\n", os.Args[0])
        os.Exit(-1)
    }

    projectID := "my-project-1530001957977"
    datasetID := "PP"
    partner := args[0]
    year := args[1]
    month := args[2]
    filename := args[3]
    tableID := strings.ToUpper(partner)

    // Загружаем файл в области Stage
    err := pp.LoadFileToTable(ctx, projectID, datasetID, "STG_" + tableID, filename)
    if err != nil {
        fmt.Printf("Error loading file %v into table STG_%v: %v\n", filename, tableID, err)
        os.Exit(-1)
    }

    // Если файл загружен успешно в область Stage, то вызываем процедуру для перегрузки данных в области ODS и DDS
    query := "CALL " + datasetID + ".LOAD_" + tableID + "('" + filename + "', " + year + ", " + month + ");"
    err = pp.RunQuery(ctx, projectID, query)
    if err != nil {
        fmt.Printf("Error running query '%v': %v\n", query, err)
        os.Exit(-1)
    }
}
