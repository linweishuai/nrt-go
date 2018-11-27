package parser

import (
	"fmt"
	"regexp"
	"spider/config"
	"spider/database"
	"spider/helper"
	"strconv"
	"strings"
	"time"

	"github.com/cihub/seelog"
)
import (
	"spider/engine"
)

const movieNameRe = "<p>作品：([^<]*)</p>"
const identifierRe = "<p>番号：([^<]*)</p>"
const publishTimeRe = "<p>发行日期：([^<]*)</p>"
const movieTimeRe = "<p>播放时长：([^<]*)</p>"
const directorRe = "<p>导演：([^<]*)</p>"
const typeRe = "<p>类型：([^<]*)</p>"
const producerRe = "<p>制作商：([^<]*)</p>"
const publisherRe = "<p>发行商：([^<]*)</p>"
const keywordRe = "<p>类别：([^<]*)</p>"
const imgRe = `artCon[\s\S]*?data-original="([^\"]*)"[\s\S]*/div>`


func MovieInfo(contents []byte, db *database.DbInfo, actor_id string, link string) engine.ParseResult {

	result := engine.ParseResult{}
	
	data := make(map[string]string)
	date := time.Now().Format("2006-01-02 15:04:05")

	data["actor_id"] = actor_id
	data["name"] = movieExtractString(movieNameRe,contents)


	if data["name"] == "" {
		result.Requests = append(result.Requests, engine.Request{
			Url: config.HOST + link,
			ParserFunc: func(c []byte, info *database.DbInfo) engine.ParseResult {
				return MovieInfo(c, info, actor_id, link)
			},
		})
		seelog.Errorf("返回的内容无效重新加入队列")
		return result
	}

	data["identifier"] = movieExtractString(identifierRe,contents)

	if _,ok := db.ExistsMoviesIdentifier.Load(data["identifier"]); ok{ //番号验证
		return result
	}
	
	data["publish_time"] = movieExtractString(publishTimeRe,contents)

	publishTime := strings.Replace(data["publish_time"], "/", "-", -1)
	ptUnix, err := time.Parse("2006-01-02", publishTime)
	ptStr := strconv.FormatInt(ptUnix.Unix(), 10)
	if err != nil {
		seelog.Errorf("time parse error,identifier is %v", data["identifier"])
		ptStr = ""
	}
	data["publish_time_unix"] = ptStr

	data["movie_time"] = movieExtractString(movieTimeRe,contents)
	data["director"] = movieExtractString(directorRe,contents)
	data["type"] = movieExtractString(typeRe,contents)
	data["producer"] = movieExtractString(producerRe,contents)
	data["publisher"] = movieExtractString(publisherRe,contents)
	data["keyword"] = movieExtractString(keywordRe,contents)
	data["link"] = link
	data["img_path"] = movieExtractString(imgRe,contents)

	if strings.Contains(data["img_path"], "-small") {
		data["img_path"] = strings.Replace(data["img_path"], "-small", "", -1)
	}


	//data["img_path"],_= download.Download(data["img_path"],"movies")
	data["created_time"] = date


	//if link =="/fanhaoku/AB1SSNI-054.html"{
	//	fmt.Println(data)
	//	os.Exit(1)
	//}
	if data["img_path"] == ""{
		seelog.Infof("图片链接出错,番号为:%v",data["identifier"])
		fmt.Println(data)
	}

	imgExist := helper.ImageExist(config.MOVIES_IMG,data["img_path"])
	if(!imgExist){
		wheres := map[string]string{
			"link" : data["link"],
		}
		result.Requests = append(result.Requests, engine.Request{
			Url: config.HOST + data["img_path"],
			ParserFunc: func(c []byte, info *database.DbInfo) engine.ParseResult {
				return PicDownload(c, info, config.HOST+data["img_path"], config.MOVIES_IMG, wheres)
			},
		})
	}


	db.Lock()
	

	db.InsertData["movies"] = append(db.InsertData["movies"], data)
	db.Unlock()
	return result
}

func movieExtractString(reStr string,moviesContents []byte) string {
	re := regexp.MustCompile(reStr)
	match := re.FindSubmatch(moviesContents)

	if len(match) >= 2 {
		return string(match[1])
	}
	return ""
}
