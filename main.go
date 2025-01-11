package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"
)

func main() {
	// Get file from flag
	fp := flag.String("file", "test/test.parquet.gzip", "Input file path for the file to skim")
	flag.Parse()

	// Now, we can read from the file.
	rf, _ := os.Open(*fp)
	pf := parquet.NewReader(rf)

	// fmt.Println(pf.Schema().Columns())
	fmt.Printf("%+v", pf.Schema().GoType())
	// parquet.
	// fmt.Printf("%+v\n", pf.Schema().GoType())
	// Read the rows
	// num := int(pf.NumRows())
	// rows := parquet.GenericReader[]{}

	// for {
	// 	if err := pf.Read(rows); err == io.EOF {
	// 		break
	// 	} else if err != nil {
	// 		fmt.Println(err.Error())
	// 	}
	// }
	// fmt.Println(rows)

}

type Schema struct {
}

type Contact struct {
	Name string `parquet:"name"`
	// "zstd" specifies the compression for this column
	PhoneNumber string `parquet:"phoneNumber,optional,zstd"`
}

type AddressBook struct {
	Owner             string    `parquet:"owner,zstd"`
	OwnerPhoneNumbers []string  `parquet:"ownerPhoneNumbers,gzip"`
	Contacts          []Contact `parquet:"contacts"`
}
