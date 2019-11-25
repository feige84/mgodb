package mgodb

import (
	"douyin/model"
	"fmt"
	"github.com/feige84/utils"
	"github.com/globalsign/mgo/bson"
	"testing"
)

type Person struct {
	NAME  string
	PHONE string
}

func TestExecute(t *testing.T) {

	mgoDb, err := NewMgoDb("mongodb://127.0.0.1:27017/dds", "dds", 1000, 100) //mgo.Dial("127.0.0.1:27017") //连接数据库
	if err != nil {
		panic(err)
	}
	defer mgoDb.Session.Close()

	collection := mgoDb.C("dy_aweme")

	countNum, err := collection.Count()
	if err != nil {
		panic(err)
	}
	fmt.Println("Things objects count: ", countNum)

	result := []model.DyUser{}
	//普通查询 //Sort("-total_favorited").
	err = collection.Find(bson.M{"nickname": bson.M{"$regex": "/David/"}}).Skip(0).Limit(20).All(&result)
	if err != nil {
		panic(err)
	}
	utils.Pr(result)

	//result := []bson.M{}
	//err = collection.Find(bson.M{"aweme_id": bson.M{"$in": []int64{6734525952434474252, 6729716884431785219, 11111114}}}).All(&result)
	//
	//if err != nil {
	//	panic(err)
	//}
	//for _, v := range result {
	//	fmt.Println(v["aweme_id"])
	//}
	//fmt.Println(result)
	/*
		//一次可以插入多个对象 插入两个Person对象
		//插入
		var docs []interface{}
		docs = append(docs, model.DyAweme{
			AwemeId:            11111117,
			UId:                1111,
			AuthorNickname:     "aaa",
			AuthorAvatar:       "",
			Desc:               "",
			IsAuthor:           0,
			MId:                0,
			MusicTitle:         "",
			MusicOwnerId:       0,
			MusicOwnerNickname: "",
			PoiId:              0,
			CId:                0,
			StickerId:          0,
			AdId:               0,
			CreateTime:         0,
			Year:               0,
			Month:              0,
			Day:                0,
			Hour:               0,
			DynamicCover:       "",
			Cover:              "",
			PayAddrUri:         "",
			Duration:           0,
			MusicIsOriginal:    0,
			IsTop:              0,
			HasGoods:           0,
			DiggCount:          0,
			DiggCountGrade:     0,
			DownloadCount:      0,
			CommentCount:       0,
			ShareCount:         0,
			PlayCount:          0,
			Dateline:           0,
			Updated:            0,
			IsCheck:            0,
			CheckTime:          0,
			NextCheckTime:      0,
		})
		docs = append(docs, model.DyAweme{
			AwemeId:            11111116,
			UId:                1111,
			AuthorNickname:     "xxxxxxxx",
			AuthorAvatar:       "",
			Desc:               "",
			IsAuthor:           0,
			MId:                0,
			MusicTitle:         "",
			MusicOwnerId:       0,
			MusicOwnerNickname: "",
			PoiId:              0,
			CId:                0,
			StickerId:          0,
			AdId:               0,
			CreateTime:         0,
			Year:               0,
			Month:              0,
			Day:                0,
			Hour:               0,
			DynamicCover:       "",
			Cover:              "",
			PayAddrUri:         "",
			Duration:           0,
			MusicIsOriginal:    0,
			IsTop:              0,
			HasGoods:           0,
			DiggCount:          0,
			DiggCountGrade:     0,
			DownloadCount:      0,
			CommentCount:       0,
			ShareCount:         0,
			PlayCount:          0,
			Dateline:           0,
			Updated:            0,
			IsCheck:            0,
			CheckTime:          0,
			NextCheckTime:      0,
		})
		err = collection.Insert(docs...)
		if err != nil {
			panic(err)
		}*/

	//更新
	//err = collection.Update(bson.M{"aweme_id": 6503033202339220749}, bson.M{"$set": bson.M{
	//	"is_top":           1,
	//	"digg_count":       456,
	//	"digg_count_grade": 555,
	//	"download_count":   444,
	//	"comment_count":    333,
	//	"share_count":      222,
	//	"play_count":       111,
	//	"updated":          utils.GetNow().Unix(),
	//	"is_check":         0,
	//	"check_time":       utils.GetNow().Unix(),
	//	"next_check_time":  utils.GetNow().Unix()+86400,
	//}})
	//if err != nil {
	//	panic(err)
	//}
	//删除
	err = collection.Remove(bson.M{"aweme_id": 11111116})
	if err != nil {
		panic(err)
	}
}
