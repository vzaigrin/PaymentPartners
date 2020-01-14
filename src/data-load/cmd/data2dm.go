package main

import (
    "context"
    "fmt"
    "os"
    "pp"
)

// Вызываем процедуру заполнения DM по DDS за конкретный период
func main() {
    ctx := context.Background()

    // Читаем аргументы коммандной строки
    // Если каталог задан в аргументах, читаем его
    args := os.Args

    if len(args) != 3 {
        fmt.Printf("Usage: %v: year month\n", args[0])
        os.Exit(-1)
    }

    projectID := "my-project-1530001957977"
    datasetID := "PP"

    query := "CALL " + datasetID + ".DATA2DM(" + args[1] + ", " + args[2] + ");"
    err := pp.RunQuery(ctx, projectID, query)
    if err != nil {
        fmt.Printf("Error running query '%v': %v\n", query, err)
        os.Exit(-1)
    }
}
