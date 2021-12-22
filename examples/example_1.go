package main

import (
	"fmt"
	"time"

	"github.com/ultd/questdb-go"
)

type User struct {
	IgnoredField string        `qdb:"-"`
	Name         string        `qdb:"name;string"`
	Email        string        `qdb:"email;symbol"`
	EmailME2     string        `qdb:"email_me_2;string"`
	Age          int16         `qdb:"age;short"`
	LongNumber   int           `qdb:"long_num;long"`
	Birthday     time.Time     `qdb:"birthday;timestamp"`
	TS           time.Time     `qdb:"ts;timestamp;designatedTS:true"`
	Body         questdb.Bytes `qdb:"body;binary"`
	Options      Options       `qdb:"options;embedded;embeddedPrefix:opts_"`
}

type Options struct {
	MaxAge    int    `qdb:"max_age;long"`
	LengthMax string `qdb:"length_max;string"`
}

// func (u User) TableName() string {
// 	return "users"
// }

func (u User) CreateTableOptions() questdb.CreateTableOptions {
	return questdb.CreateTableOptions{
		PartitionBy:        questdb.Day,
		MaxUncommittedRows: 40000,
		CommitLag:          "240s",
	}
}

func main() {
	user := &User{
		Name:       "john Appleseed",
		Email:      "john.appleseed@syndica.io",
		EmailME2:   "john.appleseed.2@syndica.io",
		Age:        45,
		LongNumber: 24325313426134,
		Birthday:   time.Now(),
		TS:         time.Now().AddDate(+3, 0, 0),
		Body:       []byte(`{"key_1":"value_1"}`),
		Options: Options{
			LengthMax: "455",
			MaxAge:    4325,
		},
	}

	// create a model from struct
	model, err := questdb.NewModel(user)
	if err != nil {
		panic(err)
	}

	// fmt.Println(model.CreateTableIfNotExistStatement())

	// instantiate a QuestDB cleint
	client := questdb.Default()

	// attempt to connect to influx line and pg wire protocols
	if err := client.Connect(); err != nil {
		panic(err)
	}
	defer client.Close()

	// execute create table if not exists statement
	_, err = client.DB().Exec(model.CreateTableIfNotExistStatement())
	if err != nil {
		panic(err)
	}

	// write user struct to QuestDB
	if err := client.Write(user); err != nil {
		panic(err)
	}

	row := client.DB().QueryRow(fmt.Sprintf("SELECT %s FROM users ORDER BY ts DESC;", model.Columns()))
	if err := row.Err(); err != nil {
		panic(err)
	}

	out := &User{}
	// use helper ScanInto helper func to scan row into struct fields
	err = questdb.ScanInto(row, out)
	if err != nil {
		panic(err)
	}

	// print our user
	fmt.Printf("User: %v\n", out)

}
