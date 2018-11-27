package agent

import (
	"bytes"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/cihub/seelog"
	"golang.org/x/net/html"
	"net/http"
	"spider/database"
	"spider/fetcher"
	"strconv"
	"strings"
	"time"
	"os"
	"sync"
)
var existMap = &sync.Map{}

func  VerifyIp(db *database.DbInfo)  {

	ips := GetAgents()

	seelog.Infof("共获得ip数量为:%v",len(ips))
	if len(ips) == 0 {
		return
	}
	ch := make(chan string,len(ips))

	for _,ip := range ips {

		go func(ip string) {
			beginTime := time.Now().Unix()
			_,err := fetcher.Fetcher("http://nanrenvip.xyz/nvyouku/1-0-0-0-0-0-0.html",ip,db)
			timeDiff := time.Now().Unix() - beginTime

			if timeDiff  < 10 && err == nil{
				ch <- ip
			}else{
				ch <- ""
			}
		}(ip)

	}
	rs := make([]string,0)
	count := 1

	for  {
		select{
		case ip :=  <- ch :
			count++
			//fmt.Println(count)
			if count == len(ips) {
				goto stop
			}
			if ip != "" {
				rs = append(rs,ip)
				_ ,ok := db.ExistsIps.Load(ip)
				_,memoryOk := existMap.Load(ip)
				if !ok && !memoryOk {
					db.Lock()
					db.InsertData["ip"] = append(db.InsertData["ip"], map[string]string{"ip":ip,"created_time":time.Now().Format("2006-01-02 15:04:05")})
					db.Unlock()
					seelog.Infof("ip验证成功:%v",ip)
					existMap.Store(ip,true)
					db.AgentIpCh <- map[string]interface{}{"ip":ip,"useCount":0,"errorCount":0}

				}
			}


		}
	}
stop:


}

func WriteFile(ip string)  {
	f, err := os.OpenFile("sql/ip.txt", os.O_WRONLY|os.O_APPEND, 0600)

	defer f.Close()

	if err != nil {

	fmt.Println(err.Error())

	} else {
	_,err =f.Write([]byte(ip+"\r\n"))
		if err != nil {

			fmt.Println(err.Error())

		}
	}

}


func GetAgents() []string  {


	url:= "http://www.goubanjia.com/"

	client := &http.Client{}

	req, err := http.NewRequest("GET", url, nil)
	if err  != nil {
		fmt.Printf("代理出错:%v",err)
	}

	req.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36")
	req.Header.Add("Referer", "http://nanrenvip.org/")
	req.Header.Add("Host", "nanrenvip.org")
	req.Header.Add("Upgrade-Insecure-Requests", "1")
	req.Header.Add("Accept-Language", "zh-CN,zh;q=0.9")
	req.Header.Add("Cache-Control", "max-age=0")
	req.Header.Add("Connection", "keep-alive")
	req.Header.Add("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8")

	resp, err := client.Do(req)

	defer resp.Body.Close()



	doc, err := goquery.NewDocumentFromReader(resp.Body)


	if err!= nil {

		fmt.Println(err)
	}
	rs := make([]string,0)
	//fmt.Println(OuterHtml(doc.Children()))
	defer func() {
		if err:= recover(); err != nil {
			seelog.Errorf("获取代理ip出错,错误为:%v",err)
		}
	}()
	doc.Find("td.ip").Each(func(i int, s *goquery.Selection) {

		child := s.Children();


		ip :=""

		child.Each(func(i int, selection *goquery.Selection) {
			outerHtml,_ := OuterHtml(selection)

			if strings.Contains(outerHtml,"none") {

			}else{
				if selection.HasClass("port") {
					class,_ := selection.Attr("class")
					classArr := strings.Split(class," ")
					ip += (":"+getPort(classArr[1]))
				}else{
					ip += selection.Text()
				}

			}

		})

		rs = append(rs,"http://"+ip)
	})
	return rs
}
func getPort(port string) string {
	nArr := strings.Split(port,"")
	var rs string
	for _, value := range nArr  {
			rs += strconv.Itoa(strings.Index("ABCDEFGHIZ",value))
	}
	p ,err := strconv.Atoi(rs)
	if err != nil {
		fmt.Println("转换错误")
	}

	return strconv.Itoa( p >> 3)
}


func OuterHtml(s *goquery.Selection) (string, error) {
	var buf bytes.Buffer

	if s.Length() == 0 {
		return "", nil
	}
	n := s.Get(0)
	if err := html.Render(&buf, n); err != nil {
		return "", err
	}
	return buf.String(), nil
}