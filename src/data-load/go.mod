module pp

require (
	cloud.google.com/go/bigquery v1.3.0
	cloud.google.com/go/storage v1.5.0
	google.golang.org/api v0.15.0
	github.com/vzaigrin/PaymentPartners/src/pp/shared v0.0.0
)

replace pp/shared => ./shared

go 1.11
