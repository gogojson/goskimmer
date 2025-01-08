package main

import (
	"fmt"
	"log"

	"github.com/xitongsys/parquet-go/local"
	"github.com/xitongsys/parquet-go/reader"
)

func main() {
	// Replace with the path to your Parquet file
	filePath := "path/to/your/file.parquet"

	// Open the Parquet file

	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer fr.Close()

	// Create a Parquet reader
	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		log.Fatalf("Failed to create Parquet reader: %v", err)
	}
	defer pr.ReadStop()

	// Print the schema
	schema := pr.SchemaHandler
	fmt.Println(schema)
}
