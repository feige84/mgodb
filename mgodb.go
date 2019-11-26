package mgodb

import (
	"fmt"
	"mgodb/mg"
	"runtime/debug"
	"time"
)

var MgoDb *MgDbLib

type DbRow map[string]interface{}

type MgDbLib struct {
	Session    *mg.Session
	Db         *mg.Database
	Collection *mg.Collection
	Debug      bool
}

func NewMgoDb(dsn, dbName string, poolLimit, poolTimeout int) (*MgDbLib, error) {
	session, err := mg.Dial(dsn) //连接数据库
	if err != nil {
		return nil, fmt.Errorf("mongodb connect error: %s\n%s", err, debug.Stack())
	}
	//defer session.Close()
	session.SetMode(mg.Monotonic, true)
	session.SetSocketTimeout(10 * time.Second)
	if poolLimit > 0 {
		session.SetPoolLimit(poolLimit)
	}
	if poolTimeout > 0 {
		session.SetPoolTimeout(time.Duration(poolTimeout) * time.Second)
	}

	if err = session.Ping(); err != nil {
		return nil, fmt.Errorf("mongodb ping error: %s\n%s", err, debug.Stack())
	}

	s := new(MgDbLib)
	s.Session = session
	s.Db = s.DB(dbName)
	return s, nil
}

func (s *MgDbLib) DB(dbName string) *mg.Database {
	s.Db = s.Session.DB(dbName)
	return s.Db
}

func (s *MgDbLib) C(collection string) *mg.Collection {
	if s.Db == nil {
		s.Db = s.DB("") // use default
	}
	s.Collection = s.Db.C(collection)
	return s.Collection
}
