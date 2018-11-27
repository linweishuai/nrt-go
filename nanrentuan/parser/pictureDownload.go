package parser

import (
	"spider/config"
	"spider/database"
)
import (
	"bytes"
	"github.com/cihub/seelog"
	"io"
	"os"
	"regexp"
	"spider/engine"
)

func PicDownload(contents []byte,db *database.DbInfo,imagePath string,filePath string,wheres map[string]string ) engine.ParseResult {
	if imagePath =="" {
		seelog.Errorf("图片路径为空%v",imagePath)
	}
	//图片正则
	reg, _ := regexp.Compile(`(\w|\d|_)*.jpg`)
	

	rs := reg.FindStringSubmatch(imagePath)

	if len(rs) < 1 {
		seelog.Errorf("图片路径为空%v",imagePath)
		return engine.ParseResult{}
	}
	name := rs[0]

	out, err := os.Create(filePath+name)
	if err != nil {
		seelog.Errorf("图片文件创建出错: %v",err)
	}

	_,err = io.Copy(out, bytes.NewReader(contents))

	if err!= nil {
		seelog.Errorf("图片拷贝出错")
	}
	out.Close()

	if(filePath == config.HEADS_IMG){
		db.UpdateActorPic(name,wheres["name"],wheres["birthday"])
	}else{
		db.UpdateMoviePic(name,wheres["link"])
	}

	return engine.ParseResult{}
}