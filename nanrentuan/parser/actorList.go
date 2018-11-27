package parser

import (
	"fmt"
	"github.com/cihub/seelog"
	"regexp"
	"spider/config"
	"spider/database"
	"spider/engine"
	"sync"
)


const personRe = `<a href="([^"]*)" title="([^"]*)"><img class="lazyload" data-original="([^"]*)" />`

const linkRe = `<li><a href="([^\"]*)">\d*</a></li>`

var actorLinkMap = &sync.Map{}


func ParseActorList(contents []byte,db *database.DbInfo) engine.ParseResult {
	personReMust := regexp.MustCompile(personRe)
	actorMatches := personReMust.FindAllSubmatch(contents, -1)


	linkReMust := regexp.MustCompile(linkRe)
	linkMatches := linkReMust.FindAllSubmatch(contents,-1)

	result := engine.ParseResult{}


	//爬取演员列表的链接,防止重复爬取
	for _,links := range  linkMatches {
		link := string(links[1])
		fmt.Println(link)
		_,ok := actorLinkMap.Load(link)
		if !ok {
			actorLinkMap.Store(link,true)
			result.Requests = append(result.Requests, engine.Request{
				Url:       config.HOST+link,
				ParserFunc: ParseActorList,
			})
		}
	}

	//将演员的链接放入队列
	for _, m := range actorMatches {

		link := string(m[1])

			result.Requests = append(result.Requests, engine.Request{
				Url:       config.HOST+link,
				ParserFunc:func(c []byte,info *database.DbInfo) engine.ParseResult {
					return ParsePersonInfo(c,info, link)
				},
			})
			if string(m[2]) == "" {
				seelog.Errorf("演员为空的相关信息为:%v,%v,%v,%v ",m[0],m[1],m[2],m[3])
			}
			result.Items = append(result.Items, "actor: "+string(m[2]))

	}

	return result
}
