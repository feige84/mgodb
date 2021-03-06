// +build !test

package mgodb

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
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
	Hint       interface{}
	SortBy     interface{}
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

func NewMongoDb(dsn, dbName string, ctx context.Context, poolSize, maxConnIdle uint64, useSecond bool, wc *writeconcern.WriteConcern) (*MongoClient, error) {
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
	if wc != nil {
		//wc := writeconcern.New(writeconcern.W(0))
		clientOptions.SetWriteConcern(wc) //请求确认写操作传播到大多数mongod实例
		//	MongoDB支持的WriteConncern选项如下
		//
		//w: 数据写入到number个节点才向用客户端确认
		//	{w: 0} 对客户端的写入不需要发送任何确认，适用于性能要求高，但不关注正确性的场景
		//	{w: 1} 默认的writeConcern，数据写入到Primary就向客户端发送确认
		//	{w: “majority”} 数据写入到副本集大多数成员后向客户端发送确认，适用于对数据安全性要求比较高的场景，该选项会降低写入性能
		//j: 写入操作的journal持久化后才向客户端确认
		//	默认为”{j: false}，如果要求Primary写入持久化了才向客户端确认，则指定该选项为true
		//wtimeout: 写入超时时间，仅w的值大于1时有效。
		//	当指定{w: }时，数据需要成功写入number个节点才算成功，如果写入过程中有节点故障，可能导致这个条件一直不能满足，从而一直不能向客户端发送确认结果，针对这种情况，客户端可设置wtimeout选项来指定超时时间，当写入过程持续超过该时间仍未结束，则认为写入失败。
	}
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

func (m *MongoClient) InsertOne(collection string, doc interface{}, opt ...*options.InsertOneOptions) (interface{}, error) {
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
	result, err := collections.InsertOne(m.Ctx, doc, opt...)
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

func (m *MongoClient) Get(collection, id string) (result *mongo.SingleResult, err error) {
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
	result = collections.FindOne(m.Ctx, bson.M{"_id": objID})
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
	if filter.Hint != nil {
		opt.SetHint(filter.Hint)
	}
	return collections.CountDocuments(m.Ctx, filter.Filter, opt)
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
	if filter.SortBy != nil {
		opt.SetSort(filter.SortBy)
	}
	if filter.Projection != nil || len(filter.Projection) > 0 {
		opt.SetProjection(filter.Projection)
	}
	if filter.Hint != nil {
		opt.SetHint(filter.Hint)
	}
	result = collections.FindOne(m.Ctx, filter.Filter, opt)
	return
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
	if filter.SortBy != nil {
		opt.SetSort(filter.SortBy) //bson.M{filter.SortBy: filter.SortMode}
	}
	if filter.Projection != nil || len(filter.Projection) > 0 {
		opt.SetProjection(filter.Projection)
	}
	if filter.Hint != nil {
		opt.SetHint(filter.Hint)
	}
	//cursor, err = collection.Find(getContext(), bson.M{"createtime": bson.M{"$gte": 2}}, options.Find().SetLimit(2), options.Find().SetSort(bson.M{"createtime": -1}));
	return collections.Find(m.Ctx, filter.Filter, opt)
}

func (m *MongoClient) Aggregate(collection string, pipeline mongo.Pipeline, opt ...*options.AggregateOptions) (c *mongo.Cursor, err error) {
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
	//pipeline := mongo.Pipeline{
	//	{
	//		{"$match", bson.M{"uid": 60689235}},
	//	},
	//	{
	//		{"$group", bson.D{
	//			{
	//				"_id", "$uid"},
	//			{
	//				"count", bson.D{
	//				{"$sum", "$digg_count"},
	//			}},
	//		}},
	//	},
	//}
	return collections.Aggregate(m.Ctx, pipeline, opt...)
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
