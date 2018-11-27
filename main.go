package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/cihub/seelog"
	"os"
	"spider/agent"
	"spider/config"
	"spider/database"
	"spider/engine"
	"spider/helper"
	"spider/nanrentuan/parser"
	"spider/scheduler"
)
import (
	_ "github.com/go-sql-driver/mysql"
	"time"
)

var timeLimiter = time.Tick(config.AGENT_INTERVAL * time.Minute)

func init()  {
	flag.StringVar(&config.DATABASE, "d", "nrt", "设置数据库")
	flag.StringVar(&config.HOST, "h", "http://nanrenvip.xyz", "设置爬取的域名")
	flag.StringVar(&config.ROOT, "r", "/www/wwwroot/nrt-php.cn/storage/app/public/actors_image/", "设置存储图片的路径,在该路径下需要有heads和movies文件")
	flag.IntVar(&config.PATTERN, "p", 4, "设置代理,当为1时只爬取代理,当为2时进行爬取代理和电影图片修正,当为3时进行爬取代理和重新下载损坏的图片,当为4时爬取代理,电影图片修正,爬取数据")

}
func main() {

	//e := engine.ConcurrentEngine{
	//	Scheduler:   &scheduler.SimplerScheduler{},
	//	WorkerCount: 100,
	//}

	flag.Parse()

	helper.SetLogger("log/logConfig.xml")
	defer func() {
		seelog.Flush()
	}()



	db := &database.DbInfo{}
	db.Init()
	//
	//db.WriteMovieLog();
	//os.Exit(1);
	go func() {
		for {
			agent.VerifyIp(db);
			<-timeLimiter
		}
	}()


	if	(config.PATTERN == 1){
		select{}
	}


	e := engine.ConcurrentEngine{
		Scheduler:   &scheduler.QueuedScheduler{},
		WorkerCount: 100,
		Db: db,
	}

	if	(config.PATTERN  == 3 ) {

		go func() {
			for  {
				damageReDownload(helper.GetFileList(config.MOVIES_IMG), e, db);

				<- db.DamageCh
				fmt.Println("下一轮开始")
			}
		}()
		e.Run()
	}
	if	(config.PATTERN  == 2 ){
		picCraw(db.PicCraw,e)
	}


	if config.PATTERN == 4 {
		e.Run(engine.Request{
			Url:        "http://nanrenvip.xyz/nvyouku/1-0-0-0-0-0-0.html",
			ParserFunc: func(c []byte,info *database.DbInfo) engine.ParseResult {
			return parser.ParseActorList(c,info)
			},
		})

	}


}

func picCraw(collect []map[string]string,e engine.ConcurrentEngine )  {



	go func() {
		for _,request := range collect  {
			wheres := map[string]string{
				"link" : request["link"],
			}
			seelog.Infof("链接%v的电影海报链接放入队列",request["link"])

			go func(request map[string]string) {
				e.Scheduler.Submit(engine.Request{
					Url: config.HOST + request["img_path"],
					ParserFunc: func(c []byte, info *database.DbInfo) engine.ParseResult {
						return parser.PicDownload(c, info, config.HOST+request["img_path"], config.MOVIES_IMG, wheres)
					},
				})
			}(request)


		}
	}()
	e.Run()


}

func damageReDownload(images []string,e engine.ConcurrentEngine,d *database.DbInfo)  {
	var buffer bytes.Buffer


	images = append(images,"soe688pl.jpg")
	buffer.WriteString(" img_path in (")
	for _,image := range images{
		buffer.WriteString("'"+image+"',")
	}
	buffer.Truncate(buffer.Len() - 1)
	buffer.WriteString(")")


	basicSql := buffer.String();

	//hardDeleteSql := "delete from movie where is_deleted=1"
	//
	//hardDeleteLines, err := d.Db.Exec(hardDeleteSql)
	//hardDeleteRows, err := hardDeleteLines.RowsAffected()
	//if err != nil {
	//	seelog.Errorf("硬删除出错为%v",err )
	//}
	//seelog.Infof("硬删除的行数为%v",hardDeleteRows)


	querySql := "select distinct(link),actor_id from movie where " +basicSql

	lines, err := d.Db.Query(querySql)

	if err != nil {

		seelog.Errorf(querySql)
	}

	var count  int
	rs := make([]map[string]string,0)
	for lines.Next() {
		var link ,actor_id string


		if err := lines.Scan(&link,&actor_id); err != nil {
			seelog.Errorf("读取表movies出错 %v", err)
		}
		count++
		rs = append(rs, map[string]string{
			"link" : link,
			"actor_id":actor_id,
		})

	}
	lines.Close()

	if err != nil {
		seelog.Infof("读取表movies出错:%v","movie")
	}
	seelog.Infof("修正的数据为:%v",len(rs))
	time.Sleep(time.Second*3)
	if	len(rs) < 20{
		d.InsertEndCh <- struct{}{}
		time.AfterFunc(time.Second*60, func() {
			seelog.Infof("程序退出")
			os.Exit(1)
		})
	}

	softDeleteSql := "update movie set is_deleted=1 where" + basicSql
	softDeleteLines, err := d.Db.Exec(softDeleteSql)
	softDeleteRows, err := softDeleteLines.RowsAffected()
	if err != nil {
		seelog.Errorf("表%s获取插入行数出错%v", "movie", err)
	}
	seelog.Infof("软删除的行数为%v",softDeleteRows)

	//删除操作
	for _,request := range rs  {


		go func(request map[string]string) {
			seelog.Infof("链接%v放入队列",request)
			e.Scheduler.Submit(engine.Request{
				Url: config.HOST + request["link"],
				ParserFunc: func(c []byte, info *database.DbInfo) engine.ParseResult {
					return parser.MovieInfo(c, info, request["actor_id"], request["link"])
				},
			})
		}(request)

	}


}