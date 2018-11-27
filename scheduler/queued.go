package scheduler

import (
	"fmt"
	"github.com/cihub/seelog"
	"os"
	"spider/config"
	"spider/database"
	"spider/engine"
	"time"
)

type QueuedScheduler struct {
	requestChan chan engine.Request
	workerChan  chan chan engine.Request
}

func (s *QueuedScheduler) WorkerChan() chan engine.Request {
	return make(chan engine.Request)
}

func (s *QueuedScheduler) Submit(r engine.Request) {
	s.requestChan <- r
}

func (s *QueuedScheduler) WorkerReady(w chan engine.Request) {
	s.workerChan <- w
}

func (s *QueuedScheduler) Run(db *database.DbInfo) {
	s.workerChan = make(chan chan engine.Request)
	s.requestChan = make(chan engine.Request)

	go func() {
		var requestQ []engine.Request
		var workerQ []chan engine.Request
		timeContinue := time.Now().Unix()
		for {
			var activeRequest engine.Request
			var activeWorker chan engine.Request
			if len(requestQ) > 0 && len(workerQ) > 0 {
				activeRequest = requestQ[0]
				activeWorker = workerQ[0]
			}
			select {
			case r := <-s.requestChan:
				requestQ = append(requestQ, r)
				timeContinue = time.Now().Unix()
			case w := <-s.workerChan:
				workerQ = append(workerQ, w)
			case activeWorker <- activeRequest:
				workerQ = workerQ[1:]
				requestQ = requestQ[1:]
			default:
				nowTime := time.Now().Unix()

				if  nowTime - timeContinue > 60{

					if	config.PATTERN == 3{
						fmt.Println("循环结束------------------")
						db.DamageCh <- struct{}{}
					}

					if config.PATTERN != 3{
						fmt.Println("sssssssssssssssssssssssss")

						db.InsertEndCh <- struct{}{}
						fmt.Println("ttttttttttttttttt")
						time.AfterFunc(time.Second*60, func() {
							seelog.Infof("程序退出")
							os.Exit(1)
						})
					}

				}
			}
		}
	}()
}
