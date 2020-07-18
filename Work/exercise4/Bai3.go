package main

import (
	"fmt"
	"log"
	"strconv"
	"sync"
)

type Data struct {
	iden int
	user User
}

//insert 100 bản ghi vào user:
func InsertUser() {
	user := User{}
	for i := 0; i < 100; i++ {
		user.ID = strconv.FormatInt(int64(i+3), 10)
		user.Name = "Person" + user.ID
		err := db.InsertUser(user)
		if err != nil {
			log.Println(err)
		}
	}
}

func (db *Db) ScanforRow(buffchan chan *Data, wg *sync.WaitGroup) error {
	rows, err := db.engine.Rows(&User{})
	defer rows.Close()
	if err != nil {
		return err
	}
	user := new(User)
	i := 1
	for rows.Next() {
		err2 := rows.Scan(user)
		if err2 == nil {

			dataUser := &Data{iden: i, user: *user}
			i++
			buffchan <- dataUser
			wg.Add(1)
		}

	}
	return nil

}
func PrintUser(buffchan chan *Data, wg *sync.WaitGroup) {
	for {
		select {
		case data := <-buffchan:
			fmt.Printf("Line %v - %v - %v\n", data.iden, data.user.ID, data.user.Name)
			wg.Done()
		}
	}
}

func GetNameOfUser() error {
	buffchan := make(chan *Data, 100)
	defer close(buffchan)
	var wg sync.WaitGroup

	for i := 0; i < 2; i++ {
		go PrintUser(buffchan, &wg)
	}
	err := db.ScanforRow(buffchan, &wg)
	if err != nil {
		return err
	}
	wg.Wait()
	return nil

}
