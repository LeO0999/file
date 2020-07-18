package main

import (
	"errors"
)

type Point struct {
	UserID    string
	Points    int64
	MaxPoints int64
}

func (db *Db) InsertPoint(p Point) error {
	insert, err := db.engine.Insert(&p)
	if err != nil {
		return err
	}
	if insert == 0 {
		return errors.New("Insert fail")
	}
	return nil
}

func (db *Db) ListPoint() ([]*Point, error) {
	points := make([]*Point, 0)
	err := db.engine.Find(&points)
	if err != nil {
		return nil, err
	}
	return points, nil
}

func (db *Db) GetPoint(id string) (*Point, error) {
	point := &Point{UserID: id}
	has, err := db.engine.Get(point)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, errors.New("Point Is Not Found")
	}
	return point, nil
}
