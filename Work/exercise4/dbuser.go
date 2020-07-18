package main

import (
	"errors"
	"log"
)

type User struct {
	ID       string
	Name     string
	Birth    int64
	Created  int64
	UpdateAt int64
}

func (db *Db) InsertUser(u User) error {
	insert, err := db.engine.Insert(&u)
	if err != nil {
		return err
	}
	if insert == 0 {
		return errors.New(" Insert fail")
	}
	return nil
}

func (db *Db) UpdateUser(user, condiUser *User) error {
	update, err := db.engine.Update(user, condiUser)
	if err != nil {
		return err
	}
	if update == 0 {
		return errors.New("Update fail")
	}
	return nil
}

func (db *Db) ListUser() ([]*User, error) {
	var list []*User
	err := db.engine.Find(&list)
	if err != nil {
		return nil, errors.New("List User fail")
	}
	return list, nil
}

func (db *Db) GetUser(id string) (*User, error) {
	user := &User{ID: id}
	Find, err := db.engine.Get(user)
	if err != nil {
		log.Println("Fail")
		return nil, err
	}
	if !Find {
		return nil, errors.New("Not Found")
	}
	return user, err
}
