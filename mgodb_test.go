package mgodb

import (
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"testing"
)

type Person struct {
	NAME  string
	PHONE string
}

func TestExecute(t *testing.T) {

	session, err := mgo.Dial("127.0.0.1:27017") //连接数据库
	if err != nil {
		panic(err)
	}
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)

	db := session.DB("dds")        //数据库名称
	collection := db.C("dy_aweme") //如果该集合已经存在的话，则直接返回
	countNum, err := collection.Count()
	if err != nil {
		panic(err)
	}
	fmt.Println("Things objects count: ", countNum)

	result := []Person{}
	//普通查询
	err = collection.Find(bson.M{"name": "zhangzheHero"}).Sort("phone").All(&result)
	fmt.Println(result)

	//一次可以插入多个对象 插入两个Person对象
	//插入
	// temp := &Person{
	//		PHONE: "18811577546",
	//		NAME:  "zhangzheHero",
	//	}
	//err = collection.Insert(&Person{"Ale", "+55 53 8116 9639"}, temp)
	//if err != nil {
	//	panic(err)
	//}

	//更新
	err = collection.Update(bson.M{"name": "zhangzheHero"}, bson.M{"$set": bson.M{"phone2": "13988888888"}})
	if err != nil {
		panic(err)
	}

	//删除
	//err = collection.Remove(bson.M{"name":"Ale"})
	//if err != nil {
	//	panic(err)
	//}
}
