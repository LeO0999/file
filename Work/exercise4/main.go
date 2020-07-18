package main

import (
	"fmt"

	_ "github.com/rs/xid"
)

var db *Db = new(Db)

func main() {

	// ket noi Database
	if err := db.Connect(); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success")
	}

	// tao database
	// db.Createtable()
	// if err2 := db.Syn2; err2 != nil {
	// 	fmt.Println("fail")
	// } else {
	// 	fmt.Println("create database success")
	// }

	// now := time.Now().UnixNano()
	// user1 := User{"2", "Nguyen Van A", 10101998, now, now}
	// err := db.InsertUser(user1)
	// if err != nil {
	// 	panic(err)
	// }

	// user2 := &User{}
	// user2.Name = "Nguyen Thi B"

	// condiuser := &User{ID: "1"}

	// err1 := db.UpdateUser(user2, condiuser)
	// if err1 != nil {
	// 	panic(err1)
	// }

	///Lay List User
	list, err := db.ListUser()

	if err != nil {
		panic(err)
	}
	if len(list) > 0 {
		for  _,va := range list {
			fmt.Println(va)
		}
		
	} else {
		fmt.Println("ko tim dc list user")
	}
	

	//Get user by ID
	// _, err := db.GetUser("1")
	// if err != nil {
	// 	panic(err)
	// }

	/// Viết hàm: sau khi tạo user thì insert user_id vào user_point với số điểm 10.
	// user2 := User{"3", "Nguyen Van D", 10111998, now, now}
	// err := InsertUsertoPoint(user2)
	// if err != nil {
	// 	panic(err)
	// }

	/// Bai 2: tạo 1 transaction khi update `birth` thành công thì cộng 10 điểm vào `point` sau đó sửa lại `name ` thành `$name + "updated "` nếu 1 quá trình fail thì rollback, xong commit (CreateSesson)

	// err := db.UpdateBirthUser("3", 22022002)
	// if err != nil {
	// 	panic(err)
	// }

	/// Bai3: insert 100 bản ghi vào user sau đó viết 1 workerpool scantableuser lấy ra tên của các user inra màn hình
	//insert 100 bản ghi vào user:
	// InsertUser()

	// err := GetNameOfUser()
	// if err != nil {
	// 	panic(err)
	// }
}
