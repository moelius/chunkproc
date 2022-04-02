package chunkproc

import (
	"sync"
	"time"
)

type ProgressFunc func(progress Progress)

type Progress struct {
	workerIndex         int
	workersLastOffsets  []int
	Size                int
	Offset              int
	ProcessedGuaranteed int
	Processed           int
	Err                 error
}

func newProgressPoller(progressFunc ProgressFunc, duration time.Duration) progressPoller {
	return progressPoller{
		progressFunc: progressFunc,
		interval:     duration,
		progressChan: make(chan Progress),
		wg:           &sync.WaitGroup{},
	}
}

type progressPoller struct {
	interval     time.Duration
	progressFunc ProgressFunc
	progressChan chan Progress
	wg           *sync.WaitGroup
}

func (p progressPoller) processProgress(progress *Progress) {
	progress.ProcessedGuaranteed = 0
	for i, offset := range progress.workersLastOffsets {
		if i == 0 || offset < progress.ProcessedGuaranteed {
			progress.ProcessedGuaranteed = offset
		}
	}
	p.progressFunc(*progress)
}

func (p progressPoller) updateProgress(current *Progress, recent Progress) {
	if recent.Err != nil && current.Err == nil {
		current.Err = recent.Err
		return
	}
	current.Processed += recent.Processed
	current.workersLastOffsets[recent.workerIndex] = recent.Offset
	current.Offset = recent.Offset
}

func (p progressPoller) start(startOffset, fullSize int, concurrency int) {
	current := Progress{
		workersLastOffsets: make([]int, concurrency),
		Size:               fullSize,
		Processed:          startOffset,
	}
	p.wg.Add(1)
	go func() {
		ticker := time.NewTicker(p.interval)
		defer func() {
			ticker.Stop()
			p.wg.Done()
		}()
		for {
			select {
			case recent, ok := <-p.progressChan:
				if !ok {
					p.processProgress(&current)
					return
				}
				p.updateProgress(&current, recent)

			case <-ticker.C:
				p.processProgress(&current)
			}
		}
	}()
}

func (p progressPoller) stop() {
	close(p.progressChan)
	p.wg.Wait()
}
