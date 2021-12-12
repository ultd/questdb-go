package main

import (
	"fmt"
	"time"

	"github.com/ultd/questdb-go"
)

type User struct {
	Name       *string   `qdb:"name,string"`
	Email      string    `qdb:"email,symbol"`
	Age        int16     `qdb:"age,short"`
	LongNumber int32     `qdb:"long_num,long"`
	Birthday   time.Time `qdb:"birthday,date"`
}

func (u User) TableName() string {
	return "active_users"
}

func main() {
	// name := "Ahmad"

	a := &User{
		Name:       nil,
		Email:      "ahmad@syndica.io",
		Age:        29,
		LongNumber: 2540382,
		Birthday:   time.Now(),
	}

	line, err := questdb.StructToLine(a)
	if err != nil {
		panic(err)
	}

	fmt.Println(line.String())

	// client := questdb.Default()

	// if err := client.Connect(); err != nil {
	// 	panic(err)
	// }

	// if err := client.Write(a); err != nil {
	// 	panic(err)
	// }

}
