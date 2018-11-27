package engine

import "spider/database"

type Request struct {
	Url        string
	ParserFunc func([]byte,*database.DbInfo) ParseResult
}

type ParseResult struct {
	Requests []Request
	Items    []interface{}
}

func NilParser([]byte,*database.DbInfo) ParseResult {
	return ParseResult{}
}
