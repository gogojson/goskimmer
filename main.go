package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"reflect"
	"strings"

	"github.com/parquet-go/parquet-go"
)

func main() {
	ctx := context.Background()

	if err := run(ctx); err != nil {
		slog.Error("Error while running", "err", err.Error())
	}

}

func run(ctx context.Context) error {
	// Get file from flag
	fp := flag.String("file", "test/test.parquet.gzip", "Input file path for the file to skim")
	flag.Parse()

	// Now, we can read from the file.
	rf, err := os.Open(*fp)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return err
	}
	defer rf.Close()

	parquetReader(rf)

	return nil
}

func parquetReader(input io.ReaderAt) error {
	pf := parquet.NewReader(input)
	defer pf.Close()

	// Get the schema
	schema := pf.Schema()

	// Map of string representations to reflect.Type
	typeMap := map[string]reflect.Type{
		"INT":        reflect.TypeOf(int(0)),
		"INT8":       reflect.TypeOf(int8(0)),
		"INT16":      reflect.TypeOf(int16(0)),
		"INT32":      reflect.TypeOf(int32(0)),
		"INT64":      reflect.TypeOf(int64(0)),
		"UINT":       reflect.TypeOf(uint(0)),
		"UINT8":      reflect.TypeOf(uint8(0)),
		"UINT16":     reflect.TypeOf(uint16(0)),
		"UINT32":     reflect.TypeOf(uint32(0)),
		"UINT64":     reflect.TypeOf(uint64(0)),
		"FLOAT32":    reflect.TypeOf(float32(0)),
		"FLOAT64":    reflect.TypeOf(float64(0)),
		"STRING":     reflect.TypeOf(""),
		"BOOL":       reflect.TypeOf(true),
		"BYTE_ARRAY": reflect.TypeOf(byte(0)),
		"DOUBLE":     reflect.TypeOf(byte(0)),
		"TIMESTAMP(isAdjustedToUTC=false,unit=MICROS)": reflect.TypeOf(int64(0)),
	}

	// Create a struct type dynamically based on the schema fields
	fields := make([]reflect.StructField, len(schema.Fields()))

	for i, field := range schema.Fields() {
		fieldType, ok := typeMap[field.Type().String()]
		if !ok {
			fmt.Println("Unsupported field type:", field.Type().Kind().String())
			return fmt.Errorf("Unsupported field type: %s", field.Type().Kind().String())
		}

		// Capitalize the first letter of the field name to make it exported
		fieldName := strings.ToUpper(field.Name())

		fields[i] = reflect.StructField{
			Name: fieldName,
			Type: fieldType,
			Tag:  reflect.StructTag(fmt.Sprintf(`parquet:"%s"`, field.Name())),
		}
	}
	rowType := reflect.StructOf(fields)

	fmt.Printf("%+v", rowType)
	return nil
}
