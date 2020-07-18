package main

import (
	"log"

	_ "github.com/go-sql-driver/mysql"
	"xorm.io/xorm"
)

// - b1: Tao 1 db mysql có tên là `test`. Với các bảng `user`, `point`

// user(id string, name string, birth int64, created int64, updated_at int64)
// point(user_id string, points int64, max_points int64)

// Tạo các struct ứng với các bảng: (User, Point)

// yc:
// 1. Viết hàm: Chỉ tạo db, và tạo model(struct) ánh xạ struct thành table (CreateTable, Sync2)

type Db struct {
	engine *xorm.Engine
}

var tables []interface{}

func (a *Db) Connect() error {
	var err error
	a.engine, err = xorm.NewEngine("mysql", "root:1@tcp(0.0.0.0:3306)/test")
	if err != nil {
		log.Println(err)
	} else {
		log.Println("success")
	}
	return err

}

func Init() {
	tables = append(tables, new(User), new(Point))
}

func (a *Db) Createtable() error {
	Init()

	err := a.engine.CreateTables(tables...)
	if err != nil {
		return err
	}
	return nil
}

// func (a *Db) Syn2() error {
// 	Init()
// 	err := a.engine.Sync2(tables...)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }
