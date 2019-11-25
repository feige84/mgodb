package mgodb

import (
	"fmt"
	"runtime/debug"
	"time"

	"github.com/globalsign/mgo"
)

var MgoDb *MgDbLib

type DbRow map[string]interface{}

type MgDbLib struct {
	Session    *mgo.Session
	Db         *mgo.Database
	Collection *mgo.Collection
	Debug      bool
}

func NewMgoDb(dsn, dbName string, poolLimit, poolTimeout int) (*MgDbLib, error) {
	session, err := mgo.Dial(dsn) //连接数据库
	if err != nil {
		return nil, fmt.Errorf("mongodb connect error: %s\n%s", err, debug.Stack())
	}
	//defer session.Close()
	session.SetMode(mgo.Monotonic, true)
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

func (s *MgDbLib) DB(dbName string) *mgo.Database {
	s.Db = s.Session.DB(dbName)
	return s.Db
}

func (s *MgDbLib) C(collection string) *mgo.Collection {
	if s.Db == nil {
		s.Db = s.DB("") // use default
	}
	s.Collection = s.Db.C(collection)
	return s.Collection
}
