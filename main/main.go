package main

import (
	"time"

	"github.com/ultd/questdb-go"
)

type User struct {
	Name       string    `qdb:"name,string"`
	Email      string    `qdb:"email,symbol"`
	Age        int16     `qdb:"age,short"`
	LongNumber int64     `qdb:"long_num,long"`
	Birthday   time.Time `qdb:"birthday,date"`
}

func (u User) TableName() string {
	return "active_users"
}

func main() {

	a := User{
		Name:       "Ahmad",
		Email:      "ahmad@syndica.io",
		Age:        29,
		LongNumber: 254038235352352353,
		Birthday:   time.Now(),
	}

	client := questdb.Default()

	if err := client.Connect(); err != nil {
		panic(err)
	}

	if err := client.Write(a); err != nil {
		panic(err)
	}

}
