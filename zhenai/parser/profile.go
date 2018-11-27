package parser

import (
	"spider/engine"
	"spider/model"
	"regexp"
	"strconv"
)

//const nameRe = `<a class="name fs24">(.*?)</a>`
var genderRe = regexp.MustCompile(`<td><span class="label">性别：</span><span field="">(.*?)</span></td>`)
var ageRe = regexp.MustCompile(`<td><span class="label">年龄：</span>(.*?)岁</td>`)
var heightRe = regexp.MustCompile(`<td><span class="label">身高：</span>(.*?)CM</td>`)
var weightRe = regexp.MustCompile(`<td><span class="label">体重：</span><span field="">(.*?)KG</span></td>`)
var incomeRe = regexp.MustCompile(`<td><span class="label">月收入：</span>(.*?)元</td>`)
var marriageRe = regexp.MustCompile(`<td><span class="label">婚况：</span>(.*?)</td>`)
var eductionRe = regexp.MustCompile(`<td><span class="label">学历：</span>(.*?)</td>`)
var occupationRe = regexp.MustCompile(`<td><span class="label">职业： </span>(.*?)</td>`)
var hukouRe = regexp.MustCompile(`<td><span class="label">籍贯：</span>(.*?)</td>`)
var xinzuoRe = regexp.MustCompile(`<td><span class="label">星座：</span><span field="">(.*?)</span></td>`)
var houseRe = regexp.MustCompile(`<td><span class="label">住房条件：</span><span field="">(.*?)</span></td>`)
var carRe = regexp.MustCompile(`<td><span class="label">是否购车：</span><span field="">(.*?)</span></td>`)

func ParseProfile(contents []byte, name string) engine.ParseResult {
	profile := model.Profile{}
	profile.Age, _ = strconv.Atoi(extractString(contents, ageRe))
	profile.Weight, _ = strconv.Atoi(extractString(contents, weightRe))
	profile.Height, _ = strconv.Atoi(extractString(contents, heightRe))
	profile.Income = extractString(contents, incomeRe)
	profile.Gender = extractString(contents, genderRe)
	profile.Marriage = extractString(contents, marriageRe)
	profile.Education = extractString(contents, eductionRe)
	profile.Occupation = extractString(contents, occupationRe)
	profile.Xinzuo = extractString(contents, xinzuoRe)
	profile.Hukou = extractString(contents, hukouRe)
	profile.House = extractString(contents, houseRe)
	profile.Car = extractString(contents, carRe)
	profile.Name = name

	return engine.ParseResult{
		Items: []interface{}{profile},
	}
}
func extractString(contents []byte, re *regexp.Regexp) string {
	match := re.FindSubmatch(contents)

	if len(match) >= 2 {
		return string(match[1])
	}
	return ""
}
