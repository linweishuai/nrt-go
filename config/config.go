package config

//const  ROOT = "D:/go_work/src/spider/"
var  ROOT = "/www/wwwroot/nrt-php.cn/storage/app/public/actors_image/"

var  IMAGES = ROOT+""

var  HEADS_IMG   =  IMAGES+"/heads/"

var  MOVIES_IMG  = IMAGES+"/movies/"

var  HOST = "http://nanrenvip.xyz"

const  FETCHER_INTERVAL = 10;//单位毫秒

const  AGENT_INTERVAL = 1;//代理请求轮询时间,单位秒

const  DAMAGE_SIZE = 32768;//代理请求轮询时间,单位秒

const  REQUEST_ERROR_NUMBER =20

var    PATTERN = 4 //当为1时只爬取代理,当为2时进行爬取代理和电影图片修正,当为3时进行爬取代理和重新下载损坏的图片,当为4时爬取代理,电影图片修正,爬取数据

var   DATABASE = "nrt"