package main

import "github.com/ultd/questdb-go"

type User struct {
	Name  string `qdb:"name,string"`
	Email string `qdb:"email,symbol"`
	Age   int    `qdb:"age,short"`
}

func (u User) TableName() string {
	return "active_users"
}

var a User = User{
	Name:  "Ahmad",
	Email: "ahmad@syndica.io",
	Age:   29,
}

func main() {

	questdb.StructToLine(a)
}
