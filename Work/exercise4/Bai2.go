package main

import (
	"errors"
	"time"
)

// - b2: tạo 1 transaction khi update `birth` thành công thì cộng 10 điểm vào `point` sau đó sửa lại `name ` thành `$name + "updated "`
// 		  nếu 1 quá trình fail thì rollback, xong commit (CreateSesson)

func (db *Db) UpdateBirthUser(id string, birth int64) error {
	session := db.engine.NewSession()
	defer session.Close()

	session.Begin()

	user := &User{ID: id}
	find, err := session.Get(user)
	if err != nil {
		session.Rollback()
		return err
	}
	if !find {
		session.Rollback()
		return errors.New("Not found user!")
	}

	user.Birth = birth
	_, err1 := session.Update(user, &User{ID: id})
	if err1 != nil {
		session.Rollback()
		return err1
	}

	point := &Point{UserID: user.ID}
	_, err2 := session.Get(point)
	if err2 != nil {
		session.Rollback()
		return err2
	}
	point.Points += 10
	_, err = session.Update(point, &Point{UserID: user.ID})
	if err != nil {
		session.Rollback()
		return err
	}

	user.Name = user.Name + "updated"
	user.UpdateAt = time.Now().UnixNano()
	_, err1 = session.Update(user, &User{ID: id})
	if err1 != nil {
		session.Rollback()
		return err1
	}

	session.Commit()
	return nil
}
