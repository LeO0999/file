package main

//  Viết hàm: sau khi tạo user thì insert user_id vào user_point với số điểm 10.
func InsertUsertoPoint(user User) error {
	err := db.InsertUser(user)
	if err != nil {
		return err
	}

	point := Point{UserID: user.ID, Points: 10}
	err = db.InsertPoint(point)
	if err != nil {
		return err
	}
	return nil
}
