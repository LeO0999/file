### XORM (xorm)[https://gitea.com/xorm/xorm]

- Đây là bài đặc thù, vì nó chả có con mẹ gì nhưng nó là cần. Bài này thằng nào làm sai khi commit thì phạt 5k.
- Nội dung được biên soạn bởi [maTe](fb.com/0.anhsang.0)


- b1: Tao 1 db mysql có tên là `test`. Với các bảng `user`, `point`

user(id string, name string, birth int64, created int64, updated_at int64)
point(user_id string, points int64, max_points int64)


Tạo các struct ứng với các bảng: (User, Point)

yc:
1. Viết hàm: Chỉ tạo db, và tạo model(struct) ánh xạ struct thành table (CreateTable, Sync2)
2. Viết hàm: insert và update user, viết hàm list user hoặc đọc user theo id(4 hàm)
3. Viết hàm: sau khi tạo user thì insert user_id vào user_point với số điểm 10.


- b2: tạo 1 transaction khi update `birth` thành công thì cộng 10 điểm vào `point` sau đó sửa lại `name ` thành `$name + "updated "` nếu 1 quá trình fail thì rollback, xong commit (CreateSesson)

```go
session := engine.NewSession()
defer session.Close()

// add Begin() before any action
if err := session.Begin(); err != nil {
    // if returned then will rollback automatically
    return err
}
```

- b3: insert 100 bản ghi vào user:
sau đó viết 1 workerpool scantableuser lấy ra tên của các user inra màn hình
(Dùng scan theo row)
dùng 2 worker
và thiết lập bộ đếm `${counter} - ${id} - ${name}`

```go
defer rows.Close()
bean := new(Struct)
for rows.Next() {
    err = rows.Scan(bean)
}
```