// +build !test

package mgodb

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"runtime/debug"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type BaseEntity interface {
	GetId() string
	SetId(id string)
}

type PageFilter struct {
	SortBy     string
	SortMode   int8
	Limit      int64
	Skip       int64
	Filter     map[string]interface{}
	RegexFiler map[string]string
	Projection bson.D //only certain fields
}

type MongoClient struct {
	Client     *mongo.Client
	Database   *mongo.Database
	Collection *mongo.Collection
	Ctx        context.Context
}

func NewMongoDb(dsn, dbName string, ctx context.Context, poolSize, maxConnIdle uint64, useSecond bool) (*MongoClient, error) {
	//var once sync.Once
	//once.Do(func() {
	//ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	//defer cancel() // bug may happen

	clientOptions := options.Client().ApplyURI(dsn)
	//链接mongo服务
	//clientOptions.SetLocalThreshold(3 * time.Second) //只使用与mongo操作耗时小于3秒的
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
			return nil, err
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
	var rp *readpref.ReadPref
	if useSecond {
		rp = readpref.Secondary()
	} else {
		rp = readpref.Primary()
	}
	if err = client.Ping(ctx, rp); err != nil {
		return nil, fmt.Errorf("mongodb ping error: %s\n%s", err, debug.Stack())
	}

	mg := &MongoClient{
		Client:   client,
		Database: client.Database(dbName),
		Ctx:      ctx,
	}
	return mg, nil

	//})

}

func (m *MongoClient) DB(dbName string) *mongo.Database {
	m.Database = m.Client.Database(dbName)
	return m.Database
}

func (m *MongoClient) C(collection string) *mongo.Collection {
	if m.Database == nil {
		m.Database = m.DB("") // use default
	}
	m.Collection = m.Database.Collection(collection)
	return m.Collection
}

//func (m *MongoClient) Create(collection string, e BaseEntity) (error, string) {
//	var err error
//	defer func() {
//		if r := recover(); r != nil {
//			var ok bool
//			err, ok = r.(error)
//			if !ok {
//				debug.PrintStack()
//			}
//		}
//	}()
//	collections := m.Database.Collection(collection)
//	e.SetId(UUID())
//	if cid, err := collections.InsertOne(m.Ctx, e); err == nil {
//		return nil, cid.InsertedID.(primitive.ObjectID).Hex()
//	}
//	return err, ""
//}

func (m *MongoClient) ReplaceOne(collection string, Selector bson.M, doc interface{}, opt ...*options.ReplaceOptions) (interface{}, error) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				debug.PrintStack()
			}
		}
	}()
	collections := m.Database.Collection(collection)
	result, err := collections.ReplaceOne(m.Ctx, Selector, doc, opt...)
	if err != nil {
		return 0, err
	}
	return result.ModifiedCount, nil
}

func (m *MongoClient) InsertOne(collection string, doc interface{}) (interface{}, error) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				debug.PrintStack()
			}
		}
	}()
	collections := m.Database.Collection(collection)
	result, err := collections.InsertOne(m.Ctx, doc)
	if err != nil {
		return 0, err
	}
	return result.InsertedID, nil
}

func (m *MongoClient) InsertMany(collection string, docs []interface{}, opt ...*options.InsertManyOptions) ([]interface{}, error) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				debug.PrintStack()
			}
		}
	}()
	collections := m.Database.Collection(collection)
	result, err := collections.InsertMany(m.Ctx, docs, opt...)
	if err != nil {
		return nil, err
	}
	return result.InsertedIDs, nil
	// result, err := collections.DeleteMany(ctx, bson.M{"phone": primitive.Regex{Pattern: "456", Options: ""}})
}

func (m *MongoClient) Get(collection, id string) (e BaseEntity, err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				debug.PrintStack()
			}
		}
	}()
	collections := m.Database.Collection(collection)
	objID, _ := primitive.ObjectIDFromHex(id)
	result := collections.FindOne(m.Ctx, bson.M{"_id": objID})
	err = result.Decode(&e)
	return
}

func (m *MongoClient) GetOne(collection string, filter PageFilter) (result *mongo.SingleResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				debug.PrintStack()
			}
		}
	}()
	if filter.RegexFiler != nil {
		for k, v := range filter.RegexFiler {
			filter.Filter[k] = primitive.Regex{Pattern: v, Options: ""}
		}
	}
	collections := m.Database.Collection(collection)
	opt := options.FindOne()
	if filter.Skip > 0 {
		opt.SetSkip(filter.Skip)
	}
	if filter.SortBy != "" {
		opt.SetSort(bson.M{filter.SortBy: filter.SortMode})
	}
	if filter.Projection != nil || len(filter.Projection) > 0 {
		opt.SetProjection(filter.Projection)
	}
	result = collections.FindOne(m.Ctx, filter.Filter, opt)
	return
}

func (m *MongoClient) Count(collection string, filter PageFilter) (c int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				debug.PrintStack()
			}
		}
	}()
	if filter.RegexFiler != nil {
		for k, v := range filter.RegexFiler {
			filter.Filter[k] = primitive.Regex{Pattern: v, Options: ""}
		}
	}
	collections := m.Database.Collection(collection)
	opt := options.Count()
	if filter.Skip > 0 {
		opt.SetSkip(filter.Skip)
	}
	if filter.Limit > 0 {
		opt.SetLimit(filter.Limit)
	}
	return collections.CountDocuments(m.Ctx, filter.Filter, opt)
}

func (m *MongoClient) GetAll(collection string, filter PageFilter) (c *mongo.Cursor, err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				debug.PrintStack()
			}
		}
	}()
	if filter.RegexFiler != nil {
		for k, v := range filter.RegexFiler {
			filter.Filter[k] = primitive.Regex{Pattern: v, Options: ""}
		}
	}
	collections := m.Database.Collection(collection)
	opt := options.Find()
	if filter.Skip > 0 {
		opt.SetSkip(filter.Skip)
	}
	if filter.Limit > 0 {
		opt.SetLimit(filter.Limit)
	}
	if filter.SortBy != "" {
		opt.SetSort(bson.M{filter.SortBy: filter.SortMode})
	}
	if filter.Projection != nil || len(filter.Projection) > 0 {
		opt.SetProjection(filter.Projection)
	}
	//cursor, err = collection.Find(getContext(), bson.M{"createtime": bson.M{"$gte": 2}}, options.Find().SetLimit(2), options.Find().SetSort(bson.M{"createtime": -1}));
	return collections.Find(m.Ctx, filter.Filter, opt)
}

func (m *MongoClient) DeleteOne(collection string, Selector bson.M) (int64, error) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				debug.PrintStack()
			}
		}
	}()
	collections := m.Database.Collection(collection)
	result, err := collections.DeleteOne(m.Ctx, Selector)
	if err != nil {
		return 0, err
	}
	return result.DeletedCount, nil
}

func (m *MongoClient) DeleteMany(collection string, Selector bson.M) (int64, error) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				debug.PrintStack()
			}
		}
	}()
	collections := m.Database.Collection(collection)
	result, err := collections.DeleteMany(m.Ctx, Selector)
	if err != nil {
		return 0, err
	}
	return result.DeletedCount, nil
	// result, err := collections.DeleteMany(ctx, bson.M{"phone": primitive.Regex{Pattern: "456", Options: ""}})
}

func (m *MongoClient) UpdateInc(collection string, selector bson.M, data bson.D, opt ...*options.UpdateOptions) (int64, error) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				debug.PrintStack()
			}
		}
	}()
	collections := m.Database.Collection(collection)
	result, err := collections.UpdateOne(m.Ctx, selector, bson.D{{"$inc", data}}, opt...)
	if err != nil {
		return 0, err
	}
	return result.ModifiedCount, nil
}

func (m *MongoClient) UpdateOne(collection string, selector, data bson.M, opt ...*options.UpdateOptions) (int64, error) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				debug.PrintStack()
			}
		}
	}()
	collections := m.Database.Collection(collection)
	result, err := collections.UpdateOne(m.Ctx, selector, bson.M{"$set": data}, opt...)
	if err != nil {
		return 0, err
	}
	return result.ModifiedCount, nil
}

func (m *MongoClient) UpdateMany(collection string, selector, data bson.M, opt ...*options.UpdateOptions) (int64, error) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				debug.PrintStack()
			}
		}
	}()
	collections := m.Database.Collection(collection)
	// collections.UpdateOne
	// collections.UpdateMany
	result, err := collections.UpdateMany(m.Ctx, selector, bson.M{"$set": data}, opt...)
	if err != nil {
		return 0, err
	}
	return result.ModifiedCount, nil
}

/*
func (m *MongoClient) Modify(collection string, selector, data bson.M) (error, bool) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				debug.PrintStack()
			}
		}
	}()
	collections := m.Database.Collection(collection)
	// collections.UpdateOne
	// collections.UpdateMany
	objID, _ := primitive.ObjectIDFromHex(e.GetId())
	result, err := collections.ReplaceOne(m.Ctx, bson.M{"_id": objID}, e)
	return err, result.ModifiedCount == 1
}
*/
