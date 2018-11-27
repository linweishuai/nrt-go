package engine

//
//import (
//	"spider/fetcher"
//	"log"
//)
//
//type ConcurrentEngine struct {
//	Scheduler   Scheduler
//	WorkerCount int
//}
//type Scheduler interface {
//	Submit(Request)
//	ConfigureMasterWorkerChan(chan Request)
//}
//
//func (e *ConcurrentEngine) Run(seeds ...Request) {
//	in := make(chan Request)
//	out := make(chan ParseResult)
//
//	e.Scheduler.ConfigureMasterWorkerChan(in)
//
//	for i := 0; i < e.WorkerCount; i++ {
//		createWorker(in, out)
//	}
//	for _, r := range seeds {
//		e.Scheduler.Submit(r)
//	}
//	itemCount := 0
//	for parseResult := range out {
//		for _, item := range parseResult.Items {
//			itemCount++
//			log.Printf("get item %v %v", itemCount, item)
//		}
//		for _, request := range parseResult.Requests {
//			e.Scheduler.Submit(request)
//		}
//	}
//
//}
//func createWorker(in chan Request, out chan ParseResult) {
//
//	go func() {
//		for {
//			request := <-in
//			parseResult, err := work(request)
//			if err != nil {
//
//			}
//			out <- parseResult
//		}
//
//	}()
//}
//func work(r Request) (ParseResult, error) {
//
//	log.Printf("fetch url :%s", r.Url)
//
//	body, err := fetcher.Fetcher(r.Url)
//	if err != nil {
//		log.Printf("fetcher :error "+"fetching url %s: %v", r.Url, err)
//		return ParseResult{}, err
//	}
//	parseResult := r.ParserFunc(body)
//	return parseResult, err
//}
