package main

import (
	"fmt"
	"time"

	"github.com/ultd/questdb-go"
)

type User struct {
	Name       *string   `qdb:"name;string"`
	Email      string    `qdb:"email;symbol"`
	EmailME2   string    `qdb:"email_me_2;string"`
	Age        int16     `qdb:"age;short"`
	LongNumber int32     `qdb:"long_num;long"`
	Birthday   time.Time `qdb:"birthday;timestamp;designatedTS:true"`
	Body       []byte    `qdb:"body;binary"`
	Options    Options   `qdb:"options;embedded;embeddedPrefix:opts_"`
}

type Options struct {
	MaxAge    int    `qdb:"max_age;long"`
	LengthMax string `qdb:"length_max;string"`
}

func (u User) TableName() string {
	return "active_users_2"
}

func (u User) CreateTableOptions() questdb.CreateTableOptions {
	return questdb.CreateTableOptions{
		PartitionBy:        questdb.Day,
		MaxUncommittedRows: 40000,
		CommitLag:          "240s",
	}
}

func main() {

	a := &User{
		Name:       nil,
		Email:      "ahmad@syndica.io",
		EmailME2:   "3",
		Age:        29,
		LongNumber: 2540382,
		Birthday:   time.Now(),
		Body:       []byte(`{"key_1":"value_1"}`),
	}

	model, err := questdb.NewModelFromStruct(a)
	if err != nil {
		panic(err)
	}

	fmt.Println(model.MarshalLineMessage())
	fmt.Println(model.CreateTableIfNotExistStatement())

	client := questdb.Default()

	if err := client.Connect(); err != nil {
		panic(err)
	}

	res, err := client.DB().Exec(model.CreateTableIfNotExistStatement())
	if err != nil {
		panic(err)
	}

	fmt.Printf("%+v\n", res)

	if err := client.Write(a); err != nil {
		panic(err)
	}

}
