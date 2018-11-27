package parser

import (
	"fmt"
	"github.com/cihub/seelog"
	"regexp"
	"spider/config"
	"spider/database"
	"spider/engine"
	"spider/helper"
	"strconv"
	"strings"
	"sync"
	"time"
)

const nameRe = `<h1>(.*?)</h1>`
const introduceRe = `<p>(.*?)</p>`
const otherNameRe = `<li class="long"><strong>别名：</strong>(.*?)</li>`
const movieNumberRe = `<li><strong>作品数量：</strong>[^\d]*(\d*)[^<]*</li>`
const birthdayRe = `<li class=\"long\"><strong>生日：</strong>(.*?)</li>`
const debut_timeRe = `<li><strong>出道：</strong>(.*?)</li>`
const bwhRe = `<li class="long"><strong>三围：</strong>(.*?)</li>`
const cupRe = `<li><strong>罩杯：</strong>(.*?)</li>`
const HeadRe = `  <img class="face" src="(.*?)" alt=".*?">`

const MovieRe = `<li><a href="([^\"]*)" title`
const nomMovieRe = `<li class="nom">([\w-]*).*?</li>`

var contentsGlobal = []byte{}

var nameMap = &sync.Map{}

func ParsePersonInfo(contents []byte, db *database.DbInfo, link string) engine.ParseResult {
	contentsGlobal = contents
	data := make(map[string]string)
	
	data["name"] = extractString(nameRe)
	data["head_pic"] = extractString(HeadRe)
	
	_, ok := nameMap.Load(data["name"])

	actorId, exist := db.ExistsActors.Load(data["name"])

	result := engine.ParseResult{}

	//当第一次爬取到了演员的数据时,进行处理
	if !ok && data["name"] != "" {
		nameMap.Store(data["name"], true)

		MovieReMust := regexp.MustCompile(MovieRe)
		MovieReMatches := MovieReMust.FindAllSubmatch(contents, -1)
		id := ""
		if exist {
			id = actorId.(string)
			seelog.Infof("该演员已经存在:%v",data["name"])
		}else{
			date := time.Now().Format("2006-01-02 15:04:05")
			data["other_name"] = extractString(otherNameRe)
			data["bwh"] = extractString(bwhRe)
			data["introduce"] = extractString(introduceRe)
			data["movie_number"] = extractString(movieNumberRe)
			data["debut_time"] = extractString(debut_timeRe)
			data["cup"] = extractString(cupRe)
			data["birthday"] = extractString(birthdayRe)
			data["created_time"] = date
			data["updated_time"] = date

			data["actor_link"] = link
			data["birthday_unix_time"] = dateTransfer(data["birthday"])
			data["debut_unix_time"] = dateTransfer(data["debut_time"])

			id = db.InsertActor(data)

		}

		//将获取到的电影链接放入队列
		for _, links := range MovieReMatches {
			link := string(links[1])

			if _,ok := db.ExistsMovies.Load(link);ok{
				continue
			}
			result.Requests = append(result.Requests, engine.Request{
				Url: config.HOST + link,
				ParserFunc: func(c []byte, info *database.DbInfo) engine.ParseResult {
					return MovieInfo(c, info, id, link)
				},
			})

		}

		//将获取到的不完整的电影链接封装后放入队列
		nomMovieReMust := regexp.MustCompile(nomMovieRe)
		nomMovieReMatches := nomMovieReMust.FindAllSubmatch(contents, -1)

		for _, links := range nomMovieReMatches {
			identifier := string(links[1])
			link := "/fanhaoku/"+identifier+".html"

			if _,ok := db.ExistsMovies.Load(link);ok{
				continue
			}
			result.Requests = append(result.Requests, engine.Request{
				Url: config.HOST+link,
				ParserFunc: func(c []byte, info *database.DbInfo) engine.ParseResult {
					return MovieInfo(c, info, id, link)
				},
			})

		}



		headExist := helper.ImageExist(config.HEADS_IMG,data["head_pic"])

		if !headExist {
			//将演员的头像链接放入队列
			wheres := map[string]string{
				"name":     data["name"],
				"birthday": data["birthday"],
			}

			result.Requests = append(result.Requests, engine.Request{
				Url: config.HOST + data["head_pic"],
				ParserFunc: func(c []byte, info *database.DbInfo) engine.ParseResult {
					return PicDownload(c, info, config.HOST+data["head_pic"], config.HEADS_IMG, wheres)
				},
			})
		}



	}
	return result
}

func extractString(reStr string) string {
	re := regexp.MustCompile(reStr)
	match := re.FindSubmatch(contentsGlobal)

	if len(match) >= 2 {
		return string(match[1])
	}
	return ""
}

func dateTransfer(date string) string {
	s := strings.Replace(date, "年", "-", -1)
	format := ""
	if strings.Contains(s, "日") {
		format = "2006-01-02"
		s = strings.Replace(s, "月", "-", -1)

	} else {
		format = "2006-01"
		s = strings.Replace(s, "月", "", -1)

	}

	s = strings.Replace(s, "日", "", -1)

	sArr := strings.Split(s, "-")
	if len(sArr) < 2 {
		return ""
	}
	if len(sArr[1]) == 1 {
		sArr[1] = "0" + sArr[1]
	}

	if len(sArr) == 3 && len(sArr[2]) == 1 {
		sArr[2] = "0" + sArr[2]
	}

	s = strings.Join(sArr, "-")

	formatTime, err := time.Parse(format, s)

	if err != nil {
		fmt.Println(err)
		return ""
	}

	return strconv.FormatInt(formatTime.Unix(), 10)
}
