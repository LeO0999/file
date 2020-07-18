package db

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	pb "gitlab.com/mexchange/header/users"
	"gitlab.com/mexchange/user/utils"
	"xorm.io/xorm"
)

type DB struct {
	engine *xorm.Engine
}

// ConnectDb open connection to db
func (d *DB) ConnectDb(sqlPath, dbName string) error {
	sqlConnStr := fmt.Sprintf("%s/%s?charset=utf8", sqlPath, dbName)
	engine, err := xorm.NewEngine("mysql", sqlConnStr)
	log.Print("Connected to: ", sqlConnStr)
	d.engine = engine
	return err
}
func (d *DB) listUsersQuery(rq *pb.UserRequest) *xorm.Session {
	ss := d.engine.Table(tblUser)
	if rq.GetUsername() != "" {
		ss.And("username = ?", rq.GetUsername())
	}
	if len(rq.GetIds()) != 0 {
		ss.In("id", rq.GetIds())
	}
	if rq.GetState() != "" {
		ss.And("state = ?", rq.GetState())
	}
	return ss
}

// ListUsers ...
func (d *DB) ListUsers(rq *pb.UserRequest) ([]*pb.User, error) {

	ss := d.listUsersQuery(rq)
	if rq.GetLimit() != 0 {
		ss.Limit(int(rq.GetLimit()))
	}
	if rq.GetAnchor() != "" {
		ss.And("id < ?", rq.GetAnchor())
	}
	users := make([]*pb.User, 0)
	err := ss.Desc("id").Find(&users)
	if err != nil {
		return nil, err
	}
	return users, nil
}

// CountUsers ...
func (d *DB) CountUsers(rq *pb.UserRequest) (int64, error) {
	ss := d.listUsersQuery(rq)
	return ss.Count()
}

// FindUser get signle user - oke
func (d *DB) FindUser(id string) (*pb.User, error) {
	user := &pb.User{Id: id}
	ishas, err := d.engine.Get(user)
	if err != nil {
		return nil, err
	}
	if !ishas {
		return nil, errors.New("not found")
	}
	return user, nil
}

// FindUserWithId get signle user - oke
func (d *DB) FindUserWithId(id string) (interface{}, error) {
	return d.FindUser(id)
}

// FindUserWithUsername get signle user - oke
func (d *DB) FindUserWithUsername(username string) (*pb.User, error) {
	user := &pb.User{Username: username}
	ishas, err := d.engine.Get(user)
	if err != nil {
		return nil, err
	}
	if !ishas {
		return nil, errors.New("not found")
	}
	return user, nil
}

func (d *DB) IsUserExisted(u *pb.User) bool {
	ss := d.engine.Table(tblUser)
	if u.GetUsername() != "" {
		ss = ss.Or("username = ?", u.GetUsername())
	}
	if u.GetPhone() != "" {
		ss = ss.Or("phone = ?", u.GetPhone())
	}
	if u.GetEmail() != "" {
		ss = ss.Or("email = ?", u.GetEmail())
	}
	any, err := ss.Exist()
	if err != nil {
		return false
	}
	return any
}

// FindUserWithPhone get signle user
func (d *DB) FindUserWithPhone(phone string) (*pb.User, error) {
	user := &pb.User{Phone: phone}
	ishas, err := d.engine.Get(user)
	if err != nil {
		return nil, err
	}
	if !ishas {
		return nil, errors.New("not found")
	}
	return user, nil
}

// FindUserWithEmail get signle user
func (d *DB) FindUserWithEmail(email string) (*pb.User, error) {
	user := &pb.User{Email: email}
	ishas, err := d.engine.Get(user)
	if err != nil {
		return nil, err
	}
	if !ishas {
		return nil, errors.New("not found")
	}
	return user, nil
}

//InsertUser new user
func (d *DB) InsertUser(user *pb.User) error {
	c, err := d.engine.Insert(user)
	log.Print("errr", err)
	if c == 0 {
		return errors.New("can not insert")
	}
	return err
}

//ListUserWithIds new user
func (d *DB) ListUserWithIds(uq *pb.UserRequest) ([]*pb.User, error) {
	var users []*pb.User
	err := d.engine.In("id", uq.GetIds()).Find(&users)
	return users, err
}

// UpdateUser get signle user
func (d *DB) UpdateUser(selector, updator *pb.User) error {
	c, err := d.engine.Update(updator, selector)
	if c == 0 {
		return errors.New("can not update")
	}
	return err
}

// ------------------- USER POINT ------------------

func (d *DB) InsertUserPoint(up *pb.UserPoint) error {
	c, err := d.engine.Insert(up)
	if err != nil {
		return err
	}
	if c == 0 {
		return errors.New("can not insert")
	}
	return err
}

func (d *DB) UpdateUserPoint(selector, updator *pb.UserPoint) error {
	c, err := d.engine.Update(updator, selector)
	if err != nil {
		return err
	}
	if c == 0 {
		return errors.New("can not update")
	}
	return err
}
func (d *DB) IsUserPointExisted(userId, pointType string) (bool, error) {
	pc := &pb.UserPoint{UserId: userId, PointType: pointType}
	isExisted, err := d.engine.Exist(pc)
	if err != nil {
		return false, err
	}
	if isExisted {
		return true, nil
	}
	return false, nil
}

func (d *DB) ReadUserPoint(userid, pointType string) (*pb.UserPoint, error) {
	up := &pb.UserPoint{UserId: userid, PointType: pointType}
	ishas, err := d.engine.Get(up)
	if err != nil {
		return nil, err
	}
	if !ishas {
		return nil, errors.New("not found")
	}
	return up, nil
}

func (d *DB) ListUserPoints(req *pb.UserPointRequest) ([]*pb.UserPoint, error) {
	ups := make([]*pb.UserPoint, 0)
	ss := d.engine.Where("user_id=?", req.GetUserId())
	if req.GetState() != "" {
		ss.And("state=?", req.GetState())
	}
	err := ss.Find(&ups)
	return ups, err
}

func (d *DB) listPointExchangesQuery(req *pb.PointsExchangeRequest) *xorm.Session {
	ss := d.engine.Table(tblPointsExchange)
	if req.GetReceiverId() != "" {
		ss.And("receiver_id=?", req.GetReceiverId())
	}
	if req.GetState() != "" {
		ss.And("state=?", req.GetState())
	}
	if req.GetPointType() != "" {
		ss.And("point_type=?", req.GetPointType())
	}
	// from , to number of day
	if req.GetFrom() > 0 && req.GetTo() > 0 {
		ss.And("created >= ?", req.GetFrom()*24*3600*1e9)
		if req.GetTo() == req.GetFrom() {
			ss.And("created <= ?", req.GetTo()*24*3600*1e9+23*3600*1e9)
		}
		if req.GetTo() > req.GetFrom() {
			ss.And("created <= ?", req.GetTo()*24*3600*1e9)
		}
	}
	return ss
}

func (d *DB) CountPointExchanges(req *pb.PointsExchangeRequest) (int64, error) {
	return d.listPointExchangesQuery(req).Count()
}

func (d *DB) ListPointsExchanges(req *pb.PointsExchangeRequest) ([]*pb.PointsExchange, error) {
	pEs := make([]*pb.PointsExchange, 0)
	ss := d.listPointExchangesQuery(req)
	if req.GetAnchor() != "" {
		ss.And("id>?", req.GetAnchor())
	}
	err := ss.Desc("id").Limit(int(req.GetLimit())).Find(&pEs)
	return pEs, err
}

func (d *DB) InsertPointsExchange(point *pb.PointsExchange) error {
	count, err := d.engine.Table(tblPointsExchange).Insert(point)
	if err != nil {
		return err
	}
	if count == 0 {
		return errors.New("can not insert")
	}
	return nil
}

func (d *DB) MakePointsExchange(pointsEx *pb.PointsExchange) (*pb.PointsExchange, error) {
	sess := d.engine.NewSession()
	defer sess.Close()
	// Start transcation.
	if err := sess.Begin(); err != nil {
		return nil, err
	}
	userPointBefore := &pb.UserPoint{UserId: pointsEx.GetReceiverId(), PointType: pointsEx.GetPointType()}
	if ishas, err := sess.Get(userPointBefore); !ishas || err != nil {
		log.Print("[step 1]: reject can not read point card ", pointsEx.GetReceiverId(), pointsEx.GetPointType(), err)
		sess.Rollback()
		return nil, err
	}
	if userPointBefore.GetPoints()+pointsEx.GetPoints() < 0 {
		return nil, fmt.Errorf("%s >> have %d, addition %d",
			utils.T_error_not_enough_points, userPointBefore.GetPoints(), pointsEx.GetPoints())
	}
	totalpoint := userPointBefore.GetTotalPoints()
	if totalpoint < userPointBefore.GetPoints()+pointsEx.GetPoints() {
		totalpoint = userPointBefore.GetPoints() + pointsEx.GetPoints()
	}
	// step2 update point card with local app
	sql := "UPDATE " + tblUserPoint + " set old_points=?, points = ?, total_points = ?, updated_at  = ? WHERE user_id =? AND point_type =?"
	_, err := d.engine.Exec(sql,
		userPointBefore.GetPoints(),
		userPointBefore.GetPoints()+pointsEx.GetPoints(),
		totalpoint,
		time.Now().UnixNano(),
		pointsEx.GetReceiverId(),
		pointsEx.GetPointType())
	if err != nil {
		sess.Rollback()
		log.Print("[step 2]: reject can not update point", err)
		return nil, err
	}
	// step3 log exchange
	if count, err := sess.Insert(pointsEx); err != nil {
		log.Print("[step 3]: reject can not insert point exchange", err)
		sess.Rollback()
		return nil, err
	} else if count == 0 {
		log.Print("[step 3]: reject can not insert")
		sess.Rollback()
		return nil, errors.New("step 3: insert count = 0")
	}
	sess.Commit()
	return pointsEx, nil
}

func (d *DB) ScanTableUser(uChan chan *pb.User, wg *sync.WaitGroup) error {
	rows, err := d.engine.Rows(&pb.User{})
	if err != nil {
		log.Print("error rowwwww", err)
		return err
	}
	log.Print("start scan")
	defer rows.Close()
	bean := new(pb.User)
	for rows.Next() {
		err := rows.Scan(bean)
		if err != nil {
			log.Print("Scan fail,", err)
			continue
		}
		log.Print("Hello", bean)
		uChan <- bean
		bean = new(pb.User)
		wg.Add(1)
	}
	return nil
}

// ------------------ POINT TYPE ----------------
func (d *DB) ListPointTypes(req *pb.PointTypeRequest) ([]*pb.PointType, error) {
	var pt []*pb.PointType
	ss := d.engine.Limit(int(req.GetLimit()))
	if req.GetAnchor() != "" {
		ss.Where("created <?", req.GetAnchor())
	}
	err := ss.Desc("created").Find(&pt)
	return pt, err
}

func (d *DB) FindPointTypeWithId(pointId string) (interface{}, error) {
	pt := &pb.PointType{PointId: pointId}
	ishas, err := d.engine.Get(pt)
	if err != nil {
		return nil, err
	}
	if ishas {
		return nil, errors.New("not found")
	}
	return pt, nil
}

func (d *DB) InsertPointType(pt ...*pb.PointType) error {
	c, err := d.engine.Insert(pt)
	if err != nil {
		return err
	}
	if c == 0 {
		return errors.New("can not insert")
	}
	return nil
}

func (d *DB) UpdatePointType(selector, updator *pb.PointType) error {
	c, err := d.engine.Update(updator, selector)
	if err != nil {
		return err
	}
	if c == 0 {
		return errors.New("can not update")
	}
	return nil
}

// -------------------------- RANKING ----------------
func (d *DB) ListRankings(req *pb.RankingRequest) ([]*pb.Ranking, error) {
	var rankings []*pb.Ranking
	ss := d.engine.Limit(int(req.GetLimit()))
	if req.GetPointType() != "" {
		ss.Where("point_type=?", req.GetPointType())
	}
	err := ss.Desc("created").Find(&rankings)
	if err != nil {
		return nil, err
	}
	return rankings, nil
}

func (d *DB) FindRankingWithPoinType(pointTypeIdName string) (interface{}, error) {
	pointTypeIdNames := strings.Split(pointTypeIdName, "$$")
	ranking := &pb.Ranking{
		PointType: pointTypeIdNames[0],
		Name:      pointTypeIdNames[1],
	}
	ishas, err := d.engine.Get(ranking)
	if err != nil {
		return nil, err
	}
	if ishas {
		return nil, errors.New("not found")
	}
	return ranking, nil
}

func (d *DB) InsertRanking(in ...*pb.Ranking) error {
	c, err := d.engine.Insert(in)
	if err != nil {
		return err
	}
	if c == 0 {
		return errors.New("can not insert")
	}
	return nil
}

func (d *DB) UpdateRanking(selector, updator *pb.Ranking) error {
	c, err := d.engine.Update(updator, selector)
	if err != nil {
		return err
	}
	if c == 0 {
		return errors.New("can not update")
	}
	return nil
}

func (d *DB) ListApps(limit int) ([]*pb.App, error) {
	var apps []*pb.App
	err := d.engine.Limit(limit).Desc("created").Find(&apps)
	if err != nil {
		return nil, err
	}
	return apps, nil
}

func (d *DB) InsertApp(in ...*pb.App) error {
	c, err := d.engine.Insert(in)
	if err != nil {
		return err
	}
	if c == 0 {
		return errors.New("can not insert")
	}
	return nil
}

func (d *DB) insertPartner(in *pb.Partner) error {
	c, err := d.engine.Insert(in)
	if err != nil {
		return err
	}
	if c == 0 {
		return errors.New("can not insert")
	}
	return nil
}

func (d *DB) isPartnerExisted(id string) bool {
	b, err := d.engine.Exist(&pb.Partner{Id: id})
	if err != nil {
		return false
	}
	return b
}

func (d *DB) updatePartner(selector, updator *pb.Partner) error {
	c, err := d.engine.Update(updator, selector)
	if err != nil {
		return err
	}
	if c == 0 {
		return errors.New("can not update")
	}
	return nil
}

func (d *DB) UpsertPartner(partner *pb.Partner) error {
	if existed := d.isPartnerExisted(partner.GetId()); existed {
		err := d.updatePartner(&pb.Partner{Id: partner.GetId()}, partner)
		log.Print("upsert partner ", partner.GetId(), err)
		return err
	}
	err := d.insertPartner(partner)
	log.Print("upsert partner ", partner.GetId(), err)
	return err
}

func (d *DB) ListPartners(limit int) ([]*pb.Partner, error) {
	var partners []*pb.Partner
	err := d.engine.Limit(limit).Desc("created").Find(&partners)
	if err != nil {
		return nil, err
	}
	return partners, nil
}

// --------------- POINT RATE -----------
func (d *DB) ReadPointRate(seller, buyer string) (*pb.PointRate, error) {
	pr := &pb.PointRate{PointTypeBuyer: buyer, PointTypeSeller: seller}
	ishas, err := d.engine.Get(pr)
	if err != nil {
		return nil, err
	}
	if !ishas {
		return nil, errors.New("not found")
	}
	return pr, nil
}

func (d *DB) IshasPointRate(seller, buyer string) bool {
	pr := &pb.PointRate{PointTypeBuyer: buyer, PointTypeSeller: seller}
	b, err := d.engine.Exist(pr)
	if err != nil {
		return false
	}
	return b
}

func (d *DB) FindPointRateWithKey(key string) (interface{}, error) {
	sellerBuyer := strings.Split(key, "$$")
	if len(sellerBuyer) != 2 {
		return nil, errors.New("invalid key")
	}
	return d.ReadPointRate(sellerBuyer[0], sellerBuyer[1])
}

func (d *DB) UpdatePointRate(selector, updator *pb.PointRate) error {
	c, err := d.engine.Update(updator, selector)
	if err != nil {
		return err
	}
	if c == 0 {
		return errors.New("can not update")
	}
	return nil
}

func (d *DB) InsertPointRate(in ...*pb.PointRate) error {
	c, err := d.engine.Insert(in)
	if err != nil {
		return err
	}
	if c == 0 {
		return errors.New("can not insert")
	}
	return nil
}

func (d *DB) ListPointRates(limit int) ([]*pb.PointRate, error) {
	rates := make([]*pb.PointRate, 0)
	err := d.engine.Limit(limit).Find(&rates)
	if err != nil {
		return nil, err
	}
	return rates, nil
}

// --------------- USER POINT TRANSFER -------------

func (d *DB) ReadUserPointsTransfer(id string) (*pb.UserPointsTransfer, error) {
	userPointsTransfer := &pb.UserPointsTransfer{Id: id}
	ishas, err := d.engine.Get(userPointsTransfer)
	if err != nil {
		return nil, err
	}
	if !ishas {
		return nil, errors.New("not found")
	}
	return userPointsTransfer, nil
}

func (d *DB) UpdateUserPointsTransfer(selector, updator *pb.UserPointsTransfer) error {
	c, err := d.engine.Update(updator, selector)
	if err != nil {
		return err
	}
	if c == 0 {
		return errors.New("can not update")
	}
	return nil
}

func (d *DB) InsertUserPointsTransfer(in ...*pb.UserPointsTransfer) error {
	c, err := d.engine.Insert(in)
	if err != nil {
		return err
	}
	if c == 0 {
		return errors.New("can not insert")
	}
	return nil
}

func (d *DB) listUserPointsTransferQuery(req *pb.UserPointsTransfersRequest) *xorm.Session {
	ss := d.engine.Table(tblUserPointsTransfer)
	if req.GetSenderId() != "" {
		ss.And("sender_id=?", req.GetSenderId())
	}
	if req.GetReceiverId() != "" {
		ss.And("receiver_id=?", req.GetReceiverId())
	}
	if req.GetPointTypeBuyer() != "" {
		ss.And("point_type_buyer=?", req.GetPointTypeBuyer())
	}
	if req.GetPointTypeSeller() != "" {
		ss.And("point_type_seller=?", req.GetPointTypeSeller())
	}
	// from , to number of hours
	if req.GetFromHour() > 0 && req.GetToHour() > 0 {
		ss.And("created >= ?", req.GetFromHour()*3600*1e9)
		if req.GetToHour() == req.GetFromHour() {
			ss.And("created <= ?", req.GetToHour()*3600*1e9+23*3600*1e9)
		}
		if req.GetToHour() > req.GetFromHour() {
			ss.And("created <= ?", req.GetToHour()*3600*1e9)
		}
	}
	return ss
}

func (d *DB) ListUserPointsTransfer(req *pb.UserPointsTransfersRequest) ([]*pb.UserPointsTransfer, error) {
	ss := d.listUserPointsTransferQuery(req)
	ss.Limit(int(req.GetLimit()))
	if req.GetAnchor() != "" {
		ss = ss.And("id < ?", req.GetAnchor())
	}
	list := make([]*pb.UserPointsTransfer, 0)
	if err := ss.Desc("id").Find(&list); err != nil {
		return nil, err
	}
	return list, nil
}

func (d *DB) CountUserPointsTransfer(req *pb.UserPointsTransfersRequest) (int64, error) {
	ss := d.listUserPointsTransferQuery(req)
	return ss.Count()
}

// --------------- INSERT USER PARTNER -----------

func (d *DB) InsertUserPartner(in *pb.UserPartner) error {
	c, err := d.engine.Insert(in)
	if err != nil {
		return err
	}
	if c == 0 {
		return errors.New("can not insert")
	}
	return nil
}

func (d *DB) UpdateUserPartner(selector, updator *pb.UserPartner) error {
	c, err := d.engine.Update(updator, selector)
	if err != nil {
		return err
	}
	if c == 0 {
		return errors.New("can not update")
	}
	return nil
}

func (d *DB) listUserPartnerQuery(req *pb.UserPartnerRequest) *xorm.Session {
	ss := d.engine.Table(tblUserPartner)
	if req.GetPartnerId() != "" {
		ss.And("partner_id=?", req.GetPartnerId())
	}
	if req.GetUserId() != "" {
		ss.And("user_id=?", req.GetUserId())
	}
	// from , to number of day
	if req.GetFrom() > 0 && req.GetTo() > 0 {
		ss.And("created >= ?", req.GetFrom()*24*3600*1e9)
		if req.GetTo() == req.GetFrom() {
			ss.And("created <= ?", req.GetTo()*24*3600*1e9+23*3600*1e9)
		}
		if req.GetTo() > req.GetFrom() {
			ss.And("created <= ?", req.GetTo()*24*3600*1e9)
		}
	}
	return ss
}

func (d *DB) CounUserPartnersRequest(req *pb.UserPartnerRequest) (int64, error) {
	return d.listUserPartnerQuery(req).Count()
}

func (d *DB) ListUserPartners(req *pb.UserPartnerRequest) ([]*pb.UserPartner, error) {
	up := make([]*pb.UserPartner, 0)
	ss := d.listUserPartnerQuery(req)
	if req.GetAnchor() != "" {
		ss.And("created < ?", req.GetAnchor())
	}
	err := ss.Limit(int(req.GetLimit())).Find(&up)
	return up, err
}

func (d *DB) FindPointsTransaction(exchangeId string) (*pb.PointsTransaction, error) {
	trans := &pb.PointsTransaction{ExchangeId: exchangeId}
	ishas, err := d.engine.Get(trans)
	if err != nil {
		return nil, err
	}
	if !ishas {
		return nil, errors.New("not found")
	}
	return trans, nil
}

func (d *DB) ScanTablePointsTransaction(tran chan *pb.PointsTransaction, wg *sync.WaitGroup) error {
	rows, err := d.engine.Rows(&pb.PointsTransaction{State: pb.PointsTransaction_requested.String()})
	if err != nil {
		return err
	}
	defer rows.Close()
	bean := new(pb.PointsTransaction)
	for rows.Next() {
		err := rows.Scan(bean)
		if err != nil {
			log.Print("Scan fail,", err)
			continue
		}
		wg.Add(1)
		tran <- bean
		bean = new(pb.PointsTransaction)
	}
	return nil
}
