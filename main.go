package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"text/tabwriter"

	"github.com/parquet-go/parquet-go"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var lineDivider = "--------------------------------------------------------------------------------"

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

	pf := parquet.NewReader(rf)
	defer pf.Close()

	schema, err := schemaPrinter(pf)
	if err != nil {
		return err
	}

	rowPrinter(pf)

	_ = schema

	return nil
}

func schemaPrinter(pf *parquet.Reader) (reflect.Type, error) {
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
			return nil, fmt.Errorf("Unsupported field type: %s", field.Type().Kind().String())
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
	fmt.Println(lineDivider)
	fmt.Println("Schema of the given file")

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.Debug)
	fmt.Fprintln(w, "Field\tType\tTag")
	fmt.Fprintln(w, "-----\t----\t---")

	for i := 0; i < rowType.NumField(); i++ {
		field := rowType.Field(i)
		fmt.Fprintf(w, "%s\t%s\t%s\n", field.Name, field.Type, field.Tag)
	}

	w.Flush()
	return rowType, nil
}

func rowPrinter(pf *parquet.Reader) {
	fmt.Println(lineDivider)
	p := message.NewPrinter(language.English)

	p.Printf("Row count: %d\n", pf.NumRows())
}

func timestampPrinter(pf *parquet.Reader, schema reflect.Type) {
	for i := 0; i < schema.NumField(); i++ {
		field := schema.Field(i)
		if strings.Contains(strings.ToLower(string(field.Tag)), "timestamp") {
		}
	}

	// min := 0
	// max := 0

	// pf.Read()
}
