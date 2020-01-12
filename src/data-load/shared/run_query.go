package pp

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
)

func RunQuery(ctx context.Context, projectID string, query string) error {
	// Создаём клиента для BigQuery
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: #{err}")
	}

	// Вызываем query
	_, err = client.Query(query).Run(ctx)
	if err != nil {
		return fmt.Errorf("client.Query.Run: #{err}")
	}

	// Закрываем клиента и выходим
	_ = client.Close()
	return nil
}
