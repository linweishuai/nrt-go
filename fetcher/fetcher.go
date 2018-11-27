package fetcher

import (
	"bufio"
	"fmt"
	"github.com/cihub/seelog"
	"golang.org/x/net/html/charset"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
	"io/ioutil"
	"log"
	"net/http"
	netUrl "net/url"
	"spider/config"
	"spider/database"
	"strings"
	"sync/atomic"
	"time"
)

var timeLimiter = time.Tick(config.FETCHER_INTERVAL * time.Millisecond)

var requestCount int64
var successCount int64
var  judgeRe = `is not available because it is categorized as`
func Fetcher(url string,agentTest string,db *database.DbInfo) ([]byte, error) {



	if agentTest == "" {
		atomic.AddInt64(&requestCount, 1)

		seelog.Tracef("fetcher调用统计: %v",requestCount)
	}
	<-timeLimiter

	var proxy *netUrl.URL
	var err error

	var agentInfo map[string]interface{}

	var agent string


	if agentTest != "" { //测试代理
		proxy, err = netUrl.Parse(agentTest)
	}else{
		agentInfo = <- db.AgentIpCh
		agent = agentAssertIp(agentInfo)
		proxy, err = netUrl.Parse(agent)
	}

	if err  != nil {
		seelog.Errorf("解析代理出错:%v",err)
	}
	
	timeout := time.Duration(3 * time.Second)


	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxy),
		},
		Timeout:timeout,
	}
	req, err := http.NewRequest("GET", url, nil)

	req.Close = true

	if err  != nil {
		seelog.Errorf("代理出错:%v",err)
	}

	req.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36")
	req.Header.Add("Referer", "http://nanrenvip.org/")
	req.Header.Add("Host", "nanrenvip.org")
	req.Header.Add("Upgrade-Insecure-Requests", "1")
	req.Header.Add("Accept-Language", "zh-CN,zh;q=0.9")
	req.Header.Add("Cache-Control", "max-age=0")
	req.Header.Add("Connection", "keep-alive")
	req.Header.Add("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8")
	timeUnix:=string(time.Now().Unix())
		req.Header.Add("Cookie", "Hm_lvt_87000df3ff8fbd36b40dd3cb90ea084c=1538807289,1538977903; uv_cookie_121333=1; Hm_lvt_60852cb607c7b21f13202e5e672131ce=1538807290,1538977903; UBGLAI63GV=htxmm.1538977903; fikker-B3ZY-lUd3=FqLCRiRcXQD1LkZUleh7RwLcfUX1VAnu; Hm_lpvt_87000df3ff8fbd36b40dd3cb90ea084c="+timeUnix+"; uqcpvcouplet_fidx=2; __jclm_cpv_r_61506_cpv_plan_ids=%7C2093%7C%7C2092%7C%7C2013%7C; Hm_lpvt_60852cb607c7b21f13202e5e672131ce="+timeUnix)


	resp, err := client.Do(req)

	if err != nil {
		agentErrorRecord(agentTest,agentInfo,db)
		return nil, err
	}


	//body, err := ioutil.ReadAll(resp.Body)
	//
	//if err != nil {
	//	fmt.Println(err)
	//}
	//
	//fmt.Println(string(body),err)
	//os.Exit(1)
	defer resp.Body.Close()



	if resp.StatusCode != http.StatusOK {

		if strings.Contains(url,".jpg") && resp.StatusCode == http.StatusNotFound  {
			seelog.Infof("获取的图片返回404,url为:%v",url)
			return nil,nil
		}

		if  resp.StatusCode == http.StatusNotFound   {
			agentErrorRecord(agentTest,agentInfo,db)
			seelog.Infof("该链接返回404: %v",url)
			return nil, nil
		}


		return nil, fmt.Errorf("wrong status code: %d", resp.StatusCode)
	}



	if  strings.Contains(url,".jpg") {

		body,err := ioutil.ReadAll(resp.Body)
		if err !=nil {
			seelog.Errorf("pic download body read err: %v",err)
		}

		agentSuccess(agentInfo,db)

		return body,nil
	}

	bufioReader := bufio.NewReader(resp.Body)

	e := determineEncoding(bufioReader)

	if e == charmap.Windows1252{
		agentErrorRecord(agentTest,agentInfo,db)
		return nil,fmt.Errorf("返回的编码出错")
	}

	utf8Reader := transform.NewReader(bufioReader, e.NewDecoder())

	content ,err :=  ioutil.ReadAll(utf8Reader)

	r := judgeResponse(content)

	if !r {
		agentErrorRecord(agentTest,agentInfo,db)
		return nil,fmt.Errorf("返回的内容为没有权限")
	}

	if agentTest == ""{
		agentSuccess(agentInfo,db)
	}


	return content,err
}

func judgeResponse (contents []byte)  bool {
	s := string(contents)

	if strings.Contains(s,judgeRe) {
		return false;
	}
	return true
}

func agentErrorRecord(agentTest string,agentInfo map[string]interface{},db *database.DbInfo)  {
	if agentTest == "" {

		errorCount := agentAssertCount(agentInfo,"errorCount")

		agentInfo["errorCount"] = errorCount+1

		useCount := agentAssertCount(agentInfo,"useCount")

		agent := agentAssertIp(agentInfo)


		if	(errorCount+1 >30 && useCount == 0) || (useCount !=0 && (errorCount+1)/useCount >60) {
			db.DeleteIp(agent)
			seelog.Warnf("%v代理出错,该代理出队列,使用次数为%v,错误次数为%v",agent,useCount,errorCount+1)
		}else{

			db.AgentIpCh <- agentInfo

			seelog.Warnf("%v代理出错,该代理返回队列,使用次数为%v,错误次数为%v",agent,useCount,errorCount+1)
		}

	}
}

func agentAssertIp(agentInfo map[string]interface{}) string {
		ip,ok := agentInfo["ip"].(string)
		if!ok{
			seelog.Errorf("ip断言失败")
		}
		return ip

}
func agentAssertCount(agentInfo map[string]interface{},key string) int {
	count,ok := agentInfo[key].(int)
	if!ok{
		seelog.Errorf("%v断言失败",key)
		fmt.Println(agentInfo)
	}
	return count

}

func agentSuccess(agentInfo map[string]interface{},db *database.DbInfo)  {
	useCount := agentAssertCount(agentInfo,"useCount")

	agentInfo["useCount"] = useCount+1

	errorCount :=agentAssertCount(agentInfo,"errorCount")

	db.AgentIpCh <- agentInfo

	seelog.Infof("%v代理成功,使用次数为%v,错误次数为%v",agentAssertIp(agentInfo),useCount+1,errorCount)
	atomic.AddInt64(&successCount, 1)
	seelog.Tracef("请求成功统计: %v",successCount)
}
func determineEncoding(r *bufio.Reader) encoding.Encoding {
	bytes, err := r.Peek(1024)
	if err != nil {
		log.Printf("fetch error : %v", err)
		return unicode.UTF8
	}
	e, _, _ := charset.DetermineEncoding(bytes, "")
	return e
}
