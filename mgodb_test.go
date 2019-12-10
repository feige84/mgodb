package mgodb

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"testing"
)

type Person struct {
	//ObjectID primitive.ObjectID `bson:"_id"`
	Id   int64  `json:"id" bson:"id"`
	Name string `json:"name" bson:"name"`
	Age  int64  `json:"age" bson:"age"`
}

func TestExecute(t *testing.T) {
	//https://www.mongodb.com/blog/post/mongodb-go-driver-tutorial
	mg, err := NewMongoDb("mongodb://127.0.0.1:27017/dds", "dds", context.TODO(), 100, 10, true, writeconcern.New(writeconcern.W(0)))
	//mg, err := NewMgoDb(fmt.Sprintf("mongodb://%s:%d/%s?readPreference=secondaryPreferred", "127.0.0.1", 27017, "dds"), "dds", context.TODO(), 100, 10, true)
	if err != nil {
		panic(err)
	}
	//c := mg.C("dy_test")
	//doc := Person{
	//	Id:   11121,
	//	Name: "xxxxxxxxxxxgggg",
	//}
	//replaceOpt := options.Replace().SetUpsert(true)
	//ret2, err := mg.ReplaceOne("dy_test", bson.M{"id": doc.Id}, doc, replaceOpt)
	//fmt.Println(ret2, err)
	//return

	//count
	num, err := mg.Count("dy_test", PageFilter{})
	//num, err := c.CountDocuments(context.TODO(), bson.D{})
	if err != nil {
		panic(err)
	}
	fmt.Println("count:", num, err)

	//xx, err := mg.UpdateInc("dy_test", bson.M{"id": 99888899}, bson.D{{"age", -1}})
	//fmt.Println(xx, err)
	//
	////findone
	//res, err := mg.GetOne("dy_test", PageFilter{Filter: bson.M{"id": 1111}, Projection: bson.D{{"id", 1}}})
	////person := Person{}
	////if err := c.FindOne(context.TODO(), bson.D{{"id", 1111}}).Decode(&person); err != nil {
	////	panic(err)
	////}
	//if err != nil {
	//	panic(err)
	//}
	//pp := Person{}
	//res.Decode(&pp)
	//
	//fmt.Println("person:", pp.Id, err)
	//	return

	//findmany
	//ret, err := mg.GetAll("dy_test", PageFilter{
	//	SortBy:     "name",
	//	SortMode:   -1,
	//	Limit:      2,
	//	Skip:       0,
	//	Filter:     bson.M{"id": bson.M{"$in": []int64{1111, 2222, 11111114}}},
	//	RegexFiler: nil,
	//})
	//arr := []Person{}
	//
	//ret.All(mg.Ctx, &arr)
	////for ret.Next(mg.Ctx) {
	////	var p Person
	////	err := ret.Decode(&p)
	////	if err != nil {
	////		panic(err)
	////	}
	////	//dd := p.(Person)
	////	fmt.Println(p.Name)
	////}
	////err = ret.All(mg.Ctx, &personDb)
	//for _, v := range arr {
	//
	//	fmt.Println(v.Name)
	//
	//}
	//result, err := c.Find(context.TODO(), bson.M{"id": bson.M{"$in": []int64{1111, 2222, 11111114}}})
	//if err != nil {
	//	panic(err)
	//}
	//err = result.All(context.TODO(), &personDb)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println("personDb2", arr, err)
	//
	////insert
	doc := Person{
		Id:   9988889922,
		Name: "xxxxxxxxxxxgggg",
		Age:  33,
	}
	retx, err := mg.InsertOne("dy_test", doc)
	fmt.Println("retx:", retx, err)
	//person := Person{
	//	Id:   12345,
	//	Name: "angel",
	//}
	//ret, err := c.InsertOne(context.TODO(), person)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println(retx, err)
	//
	////insert many
	//personDb = []interface{}{}
	//personDb = append(personDb, Person{
	//	Id:   22222333,
	//	Name: "ghjfghjghj",
	//})
	//personDb = append(personDb, Person{
	//	Id:   333333335553,
	//	Name: "xdfgsdfg",
	//})
	//insertOpt := options.InsertMany()
	//insertOpt.SetOrdered(false)
	//rets, err := mg.InsertMany("dy_test", personDb, insertOpt)
	////rets, err := c.InsertMany(context.TODO(), personDb, insertOpt)
	////if err != nil {
	////	panic(err)
	////}
	////fmt.Println(rets.InsertedIDs)
	//fmt.Println(rets, err)
	//
	////update
	//result2, err := mg.UpdateOne("dy_test", bson.M{"id": 222}, bson.M{"name": "12348765"})
	////result2, err := c.UpdateOne(context.TODO(), bson.M{"id": 222}, bson.M{"$set": bson.M{"name": "12348765"}})
	////if err != nil {
	////	panic(err)
	////}
	//fmt.Println(result2, err)
	//
	////update many
	//opt := options.Update().SetUpsert(false)
	//result3, err := mg.UpdateMany("dy_test", bson.M{"id": 22222}, bson.M{"name": "12348765"}, opt)
	//fmt.Println(result3, err)
	//
	////delete
	//result4, err := mg.DeleteOne("dy_test", bson.M{"id": 22222})
	////result4, err := c.DeleteOne(context.TODO(), bson.M{"id": 222})
	////if err != nil {
	////	panic(err)
	////}
	//fmt.Println(result4, err)
	//
	////deletemany
	//result5, err := mg.DeleteMany("dy_test", bson.M{"id": 333334})
	////result5, err := c.DeleteMany(context.TODO(), bson.M{"id": 12345})
	////if err != nil {
	////	panic(err)
	////}
	//fmt.Println(result5, err)
}
