package mgodb

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"testing"
)

type Person struct {
	Id   int64  `json:"id" bson:"id"`
	Name string `json:"name" bson:"name"`
}

func TestExecute(t *testing.T) {
	//https://www.mongodb.com/blog/post/mongodb-go-driver-tutorial
	mg, err := NewMgoDb(fmt.Sprintf("mongodb://%s:%d/%s?readPreference=secondaryPreferred", "127.0.0.1", 27017, "dds"), "dds", context.TODO(), 100, 10, true)
	if err != nil {
		panic(err)
	}

	c := mg.C("dy_test")

	//count
	num, err := c.CountDocuments(context.TODO(), bson.D{})
	if err != nil {
		panic(err)
	}
	fmt.Println("count:", num)

	//findone
	person := Person{}
	if err := c.FindOne(context.TODO(), bson.D{{"id", 1111}}).Decode(&person); err != nil {
		panic(err)
	}
	fmt.Println("person:", person)

	//findmany
	personDb := []interface{}{}
	result, err := c.Find(context.TODO(), bson.M{"id": bson.M{"$in": []int64{1111, 2222, 11111114}}})
	if err != nil {
		panic(err)
	}
	err = result.All(context.TODO(), &personDb)
	if err != nil {
		panic(err)
	}
	fmt.Println("personDb", personDb)

	//insert
	person = Person{
		Id:   12345,
		Name: "angel",
	}
	ret, err := c.InsertOne(context.TODO(), person)
	if err != nil {
		panic(err)
	}
	fmt.Println(ret.InsertedID)

	//insert many
	personDb = []interface{}{}
	personDb = append(personDb, Person{
		Id:   22222,
		Name: "xxxxxxx",
	})
	personDb = append(personDb, Person{
		Id:   3333333333,
		Name: "gggggggggg",
	})
	insertOpt := &options.InsertManyOptions{}
	insertOpt.SetOrdered(false)
	rets, err := c.InsertMany(context.TODO(), personDb, insertOpt)
	if err != nil {
		panic(err)
	}
	fmt.Println(rets.InsertedIDs)

	//update
	result2, err := c.UpdateOne(context.TODO(), bson.M{"id": 111}, bson.M{"$set": bson.M{"name": "xxxxx"}})
	if err != nil {
		panic(err)
	}
	fmt.Println(result2)

	//delete
	result3, err := c.DeleteOne(context.TODO(), bson.M{"id": 111})
	if err != nil {
		panic(err)
	}
	fmt.Println(result3.DeletedCount)

	//deletemany
	result4, err := c.DeleteMany(context.TODO(), bson.M{"id": 12345})
	if err != nil {
		panic(err)
	}
	fmt.Println(result4.DeletedCount)
}
