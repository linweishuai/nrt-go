package helper

import (
	"fmt"
	"github.com/cihub/seelog"
	"os"
	"path/filepath"
	"regexp"
	"spider/config"
)

func SetLogger(fileName string) {
	if _, err := os.Stat(fileName); err == nil && false {
		logger, err := seelog.LoggerFromConfigAsFile(fileName)
		if err != nil {
			panic(err)
		}

		seelog.ReplaceLogger(logger)
	} else {
		configString := `<seelog>
                        <outputs formatid="main">
                            <filter levels="info">
                                <rollingfile type="size" filename="spider/logs/info.log" namemode="prefix" maxsize="102400" maxrolls="10" />
                            </filter>
 							<filter levels="error">
                                <rollingfile type="size" filename="spider/logs/error.log" namemode="prefix" maxsize="102400" maxrolls="10" />
                            </filter>
							<filter levels="trace">
                                <rollingfile type="size" filename="spider/logs/trace.log" namemode="prefix" maxsize="102400" maxrolls="10" />
                            </filter>
							<filter levels="warn">
                                <rollingfile type="size" filename="spider/logs/warn.log" namemode="prefix" maxsize="102400" maxrolls="10" />
                            </filter>
                            <console/>
                        </outputs>
					
                        <formats>
                            <format id="main" format="%Date %Time %RelFile  [%Line]  [%LEVEL] %Msg%n"/>
                        </formats>
                        </seelog>`
		logger, err := seelog.LoggerFromConfigAsString(configString)
		if err != nil {
			panic(err)
		}

		seelog.ReplaceLogger(logger)
	}



}

	func ImageExist(filePath string ,requestPath string)  (bool){
		reg, _ := regexp.Compile(`(\w|\d|_)*.jpg`)
		rs := reg.FindStringSubmatch(requestPath)
		fmt.Println(filePath,requestPath)
		if len(rs) < 1  {
			seelog.Errorf("图片的请求路径出错:%v",requestPath)
			return true
		}
		fileName := rs[0]
		filePath = filePath+fileName;

		exist,err :=  FileExists(filePath)
		if err != nil {
			seelog.Errorf("判断图片是否存在出错,出错路径为:%v",filePath)
		}
		return  exist

	}

	func FileExists(path string) (bool, error) {
		_, err := os.Stat(path)
		if err == nil {
			return true, nil
		}
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}



	func GetFileList(path string)  []string{
		rs := make([]string,0)
		err := filepath.Walk(path, func(path string, f os.FileInfo, err error) error {
			if ( f == nil ) {return err}
			if f.IsDir() {return nil}
			fileInfo, err := os.Stat(path)
			if fileInfo.Size() <= config.DAMAGE_SIZE  {

				reg, _ := regexp.Compile(`(\w|\d|_)*.jpg`)
				match := reg.FindStringSubmatch(path)
				rs = append(rs,match[0])
			}

			return nil
		})

		seelog.Infof("损坏的图片数量为:%v",len(rs))
		if err != nil {
			fmt.Printf("filepath.Walk() returned %v\n", err)
		}
		return rs

	}
