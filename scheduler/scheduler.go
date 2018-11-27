package scheduler

import "spider/engine"

type SimplerScheduler struct {
	workerChan chan engine.Request
}

func (s *SimplerScheduler) WorkerChan() chan engine.Request {
	return s.workerChan
}

func (s *SimplerScheduler) WorkerReady(chan engine.Request) {
}

func (s *SimplerScheduler) Run() {
	s.workerChan = make(chan engine.Request)
}

func (s *SimplerScheduler) Submit(r engine.Request) {
	go func() {
		s.workerChan <- r
	}()
}
