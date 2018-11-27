package parser

import (
	"spider/engine"
	"regexp"
)

var userRe = regexp.MustCompile(`<th><a href="(http://album.zhenai.com/u/\d*)" target="_blank">(.*?)</a></th>`)
var moreRe = regexp.MustCompile(`(http://www.zhenai.com/zhenghun/shanghai/[^"]+)`)

func ParseCity(contents []byte) engine.ParseResult {
	matchs := userRe.FindAllSubmatch(contents, -1)
	result := engine.ParseResult{}
	for _, match := range matchs {
		name := string(match[2])
		result.Requests = append(result.Requests, engine.Request{
			Url: string(match[1]),
			ParserFunc: func(c []byte) engine.ParseResult {
				return ParseProfile(c, name)
			},
		})
		result.Items = append(result.Items, "用户: "+name)
	}

	matchs = moreRe.FindAllSubmatch(contents, -1)
	for _, match := range matchs {
		result.Requests = append(result.Requests, engine.Request{
			Url:        string(match[1]),
			ParserFunc: ParseCity,
		})
	}
	return result

}
