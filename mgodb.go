package mgodb

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"runtime/debug"
	"time"
)

type DbRow map[string]interface{}

type MgDbLib struct {
	Client     *mongo.Client
	Db         *mongo.Database
	Collection *mongo.Collection
	Debug      bool
}

func NewMgoDb(dsn, dbName string, ctx context.Context, poolSize, maxConnIdle uint64, useSecond bool) (*MgDbLib, error) {
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017/dds")
	//链接mongo服务
	clientOptions.SetLocalThreshold(3 * time.Second) //只使用与mongo操作耗时小于3秒的
	if maxConnIdle > 0 {
		clientOptions.SetMaxConnIdleTime(time.Duration(maxConnIdle) * time.Second) //指定连接可以保持空闲的最大秒数
	}
	if poolSize > 0 {
		clientOptions.SetMaxPoolSize(poolSize) //使用最大的连接数
	}
	//clientOptions.SetReadConcern(readconcern.Majority()) //指定查询应返回实例的最新数据确认为，已写入副本集中的大多数成员
	if useSecond {
		want, err := readpref.New(readpref.SecondaryMode)
		if err != nil {
			panic(err)
		}
		clientOptions.SetReadPreference(want) //表示只使用辅助节点
	}
	//wc := writeconcern.New(writeconcern.WMajority())
	//clientOptions.SetWriteConcern(wc)                    //请求确认写操作传播到大多数mongod实例

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("mongodb connect error: %s\n%s", err, debug.Stack())
	}

	// Check the connection
	if err = client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("mongodb ping error: %s\n%s", err, debug.Stack())
	}

	s := new(MgDbLib)
	s.Client = client
	s.Db = s.Client.Database(dbName)
	return s, nil
}

func (s *MgDbLib) DB(dbName string) *mongo.Database {
	s.Db = s.Client.Database(dbName)
	return s.Db
}

func (s *MgDbLib) C(collection string) *mongo.Collection {
	if s.Db == nil {
		s.Db = s.DB("") // use default
	}
	s.Collection = s.Db.Collection(collection)
	return s.Collection
}
