package database

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/cihub/seelog"
	"spider/config"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var timeLimiter = time.Tick(100 * time.Millisecond)


type DbInfo struct {
	Db                *sql.DB
	TableFields  	  map[string][]string
	GoroutineNumber   float64             //每个goroutine操作数据的数量
	InsertEndCh                chan struct{}       //标志插入操作是否
	InsertData       map[string][]map[string]string //将要进行插入的数据
	ExistsActors 	 *sync.Map
	ExistsMovies	 *sync.Map
	ExistsMoviesIdentifier	 *sync.Map
	ExistsIps 	 *sync.Map
	InsertLevelNumber map[string]int
	actorFields  []string
	ActualInsertCount int64
	AgentIpCh  chan map[string]interface{}  //map[string]{"ip":"","useCount":"","errorCount":""}

	DamageCh chan struct{}
	PicUpdateCache []map[string]string
	PicCraw  []map[string]string

	MovieInsertLog map[string]int
	sync.Mutex
}

func (d *DbInfo) Init() {
	db, err := sql.Open("mysql", "root:65bba0f498995d6b@tcp(118.24.148.82:3306)/"+config.DATABASE)
	//db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:3306)/nrt")

	if err != nil {
		seelog.Errorf("打开数据库出错%v", err)
	}
	d.Db = db;
	d.TableFields = make(map[string][]string);
	d.actorFields = []string{"name","other_name","head_pic","actor_link","bwh","movie_number","birthday","debut_time","cup","introduce","created_time","updated_time","birthday_unix_time","debut_unix_time"}
	d.TableFields["ip"] = []string{"ip","created_time"}
	d.TableFields["movies"] = []string{"actor_id","name","publish_time","publish_time_unix","identifier","movie_time","director","type","producer","publisher","keyword","link","img_path","created_time"}
	d.GoroutineNumber =  10
	d.InsertLevelNumber = make(map[string]int)
	d.InsertLevelNumber["ip"] = 1;
	d.InsertLevelNumber["movies"] = 5;

	d.MovieInsertLog = make(map[string]int)
	d.MovieInsertLog["insert_number"] = 0;



	d.InsertData = make(map[string][]map[string]string)
	d.InsertData["movies"] = make([]map[string]string,0)
	d.InsertData["ip"] = make([]map[string]string,0)

	d.InsertEndCh = make(chan struct{})
	d.DamageCh = make(chan struct{} )
	d.ExistsActors = new(sync.Map)
	d.ExistsIps = new (sync.Map)
	d.ExistsMovies = new (sync.Map)
	d.ExistsMoviesIdentifier = new (sync.Map)
	d.ActualInsertCount = 0

	d.PicUpdateCache = make([]map[string]string,0)
	d.PicCraw = make([]map[string]string,0)


	d.getExistActor()
	d.getExistIp()
	d.getExistMovies()
	d.timeUpdatePic()
	d.work()

}

func (d *DbInfo) getExistActor()  {
	actors,err := d.Db.Query("select `id`,`name` from actors");

	if err != nil {
		seelog.Errorf("读取演员表出错%v", err)
	}
	var count int
	for actors.Next() {
		var name string
		var id []byte
		if err := actors.Scan(&id, &name); err != nil {
			seelog.Errorf("读取数据出错dms_会员 %v", err)
		}
		count++
		d.ExistsActors.Store(name, string(id))

	}

	seelog.Tracef("已有的演员数量为%v",count)
	actors.Close()

}

func (d *DbInfo) InsertActor(actor map[string]string )  string{
	table := "actors"

	var buffer bytes.Buffer

	buffer.WriteString("INSERT INTO " + table + " (")

	for _, field := range d.actorFields {

		buffer.WriteString("`" + field + "`,")

	}

	buffer.Truncate(buffer.Len() - 1)

	buffer.WriteString(")VALUES (")

	for _, field := range d.actorFields{
			transferValue := strings.Replace(actor[field], "'", "\\'", -1)
			buffer.WriteString("'"+transferValue+"'")
			buffer.WriteString(",")


	}
	buffer.Truncate(buffer.Len() - 1)

	buffer.WriteString(")")

	rs, err := d.Db.Exec(buffer.String())

	if err != nil {

		fmt.Printf("表%s插入数据出错，错误为:%v","acotr",err)
		seelog.Tracef(buffer.String())
	}

	id, err := rs.LastInsertId()

	if err != nil {
		return "0"
	}

	seelog.Infof("actors insert success,actor is %v,insert id is %v", actor["name"],id)

	return strconv.FormatInt(id,10)


}
func (d *DbInfo) getExistIp()  {
	var number int
	err :=  d.Db.QueryRow("select count(*) from ip where is_deleted=0").Scan(&number)
	if err != nil {
		seelog.Errorf("读取ip数量出错");
	}

	ipChNumber := 1500

	if number > 1000 {
		ipChNumber = number*2
	}
	d.AgentIpCh = make(chan map[string]interface{},ipChNumber)


	ips,err := d.Db.Query("select `id`,`ip` from ip where is_deleted=0");

	if err != nil {
		seelog.Errorf("读取ip表出错%v", err)
	}
	if ips == nil {
		return
	}
	var count int
	for ips.Next() {
		var ip string
		var id []byte
		if err := ips.Scan(&id, &ip); err != nil {
			seelog.Errorf("读取表ips出错 %v", err)
		}
		count++
		d.ExistsActors.Store(ip, string(id))
		d.AgentIpCh <- map[string]interface{}{"ip":ip,"useCount":0,"errorCount":0}
	}
	seelog.Tracef("已有的ip数量为%v",count)
	ips.Close()

}

func (d *DbInfo) getExistMovies()  {
	movies,err := d.Db.Query("select `img_path`,`link`,`identifier` from movies ");

	if err != nil {
		seelog.Errorf("读取movie表出错%v", err)
	}
	if movies == nil {
		return
	}
	var count ,crawCount int
	for movies.Next() {
		var link string

		var imgPath []byte

		var identifier string
		if err := movies.Scan(&imgPath, &link,&identifier); err != nil {
			seelog.Errorf("读取表movies出错 %v", err)
		}
		count++
		imgPathStr :=  string(imgPath)
		d.ExistsMovies.Store(link,imgPathStr)
		d.ExistsMoviesIdentifier.Store(identifier,true)

		if(strings.Contains(imgPathStr,"upload")){
			crawCount++
			t := map[string]string{
				"img_path":imgPathStr,
				"link" : link,
			}

			d.PicCraw = append(d.PicCraw,t)
		}
	}

	d.MovieInsertLog["before_number"] = count
	seelog.Tracef("已有的movie数量为%v",count)
	seelog.Tracef("等待爬取的电影海报数量为%v",crawCount)
	movies.Close()

}

func (d *DbInfo) DeleteIp(ip string) {
	_ ,err := d.Db.Exec("update ip set is_deleted=1 where ip='" +ip+"'");
	if err != nil {
		seelog.Errorf("ip删除失败,失败ip为: %v,错误信息为: ",ip,err)
	}
	seelog.Infof("代理ip删除成功,ip为: %v",ip)
}


func (d *DbInfo) UpdateMoviePic(imgPath string,link string)  {

 d.Lock()
 d.PicUpdateCache = append(d.PicUpdateCache,map[string]string{
 	"img_path": imgPath,
 	"link": link,
 })
 d.Unlock()
}

var timerPic = time.Tick(10*time.Millisecond)

func (d *DbInfo) timeUpdatePic()  {
	go func() {
		for {
			<- timerPic

			d.Lock()
			 tempSave := make([]map[string]string,0)
			for _,item := range  d.PicUpdateCache { //todo 改为批量执行
				if _,ok := d.ExistsMovies.Load(item["link"]);  ok == true {

						updateSql := "update movies set img_path='"+item["img_path"]+"' where link='"+item["link"]+"'";
						_,err := d.Db.Exec(updateSql)
						seelog.Infof("============== 图片更新成功,图片地址为:%v",item["img_path"])
						if err != nil {
							fmt.Println(updateSql)
							seelog.Errorf("电影图片存储出错,出错番号为: %v,图片地址为%v",item["img_path"],err)
						}

				}else{
					tempSave = append(tempSave,item)
				}
			}
			d.PicUpdateCache = tempSave
			d.Unlock()
		}


	}()

}


func (d *DbInfo) UpdateActorPic(pic string,name string,birthday string)  {

	updateSql := "update actors set head_pic='"+pic+"' where name='"+name+"' and birthday='"+birthday+"'";
	fmt.Println(updateSql)
	_,err := d.Db.Exec(updateSql)
	if err != nil {
		seelog.Errorf("演员头像存储出错,出错演员为: %v,错误信息为%v",name,err)
	}

}
func (d *DbInfo)  work() {

	go func() {

		for{
			for table,data := range d.InsertData {
				if len(data) >= d.InsertLevelNumber[table] {
					d.Lock()
					insertData := d.InsertData[table][0:d.InsertLevelNumber[table]]
					d.InsertData[table] = d.InsertData[table][d.InsertLevelNumber[table]:]
					d.Unlock()
					d.insert(table,insertData);
				}
			}

			select {
			case <- timeLimiter:

			case <- d.InsertEndCh:
					goto stop
			}
			//fmt.Println("ffffffff------------------------------",len(d.InsertData["movies"]))

		}
	stop:
		fmt.Println(111)
		for table,data := range d.InsertData {
			if len(data) >0 {
				d.insert(table,data);
				seelog.Infof("程序执行最后的插入操作,插入条数为:%v",len(data))
			}

		}
		d.WriteMovieLog()
	}()
}

func  (d *DbInfo)WriteMovieLog()  {
	beforeNumber := strconv.Itoa(d.MovieInsertLog["before_number"])
	insertNumber := strconv.Itoa(d.MovieInsertLog["insert_number"])

	insertSql := "insert into movies_insert_log (`before_number`,`insert_number`,`created_at`) values("+beforeNumber+","+insertNumber+",'"+time.Now().Format("2006-01-02 15:04:05")+"')"
	_, err := d.Db.Exec(insertSql)

	if err != nil {
		seelog.Errorf(insertSql)
		seelog.Errorf("表%s插入数据出错，错误为:%v","movies_insert_log",err)
	}

}

func (d *DbInfo) insert(table string,insertData []map[string]string) {

	var buffer bytes.Buffer

	seelog.Tracef("表%v开始进行插入",table)


	buffer.WriteString("INSERT INTO " + table + " (")

	for _, field := range d.TableFields[table] {

		buffer.WriteString("`" + field + "`,")

	}
	buffer.Truncate(buffer.Len() - 1)

	buffer.WriteString(")VALUES ")

	for _, item := range insertData{
		buffer.WriteString("(")
		for _, field := range  d.TableFields[table] {

			transferValue := strings.Replace(item[field], "'", "\\'", -1)
			buffer.WriteString("'"+transferValue+"'")
			buffer.WriteString(",")
		}
		buffer.Truncate(buffer.Len() - 1)

		buffer.WriteString("),")
	}
	buffer.Truncate(buffer.Len() - 1)



	rs, err := d.Db.Exec(buffer.String())

	if err != nil {

		seelog.Errorf(buffer.String())
		seelog.Errorf("表%s插入数据出错，错误为:%v",table,err)
	}

	lines, err := rs.RowsAffected()
	defer func() {
		if err:= recover(); err != nil {
			seelog.Errorf("表%v保存出错,出错的行数为:%v,出错的数据为%#v",table,len(insertData),insertData)
		}
	}()

	if err != nil {
		seelog.Errorf("表%s获取插入行数出错%v", table, err)
	}
	atomic.AddInt64(&d.ActualInsertCount, lines)

	if table =="movies" && lines == int64(len(insertData)) {
		for _, item := range insertData{
			d.ExistsMovies.Store(item["link"],true)
		}
		d.Lock()
		d.MovieInsertLog["insert_number"] += int(lines)
		d.Unlock()

	}


	seelog.Infof("表%s插入的行数为%v", table, lines)

}
