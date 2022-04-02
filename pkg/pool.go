package chunkproc

import (
	"context"
	"sync"
	"sync/atomic"
)

type ProcessFunc func(workerIndex int, offset int, buffers Buffers) (n int, err error)

type worker struct {
	index   int
	pool    *pool
	buffers Buffers
}

func newWorker(pool *pool, index int, buffers Buffers) worker {
	return worker{
		pool:    pool,
		index:   index,
		buffers: buffers,
	}
}

func (w worker) start() {
	w.pool.workerWaitGroup.Add(1)
	go func() {
		for {
			select {
			case offset := <-w.pool.offsetChan:
				if _, err := w.pool.processFunc(w.index, offset, w.buffers); err != nil {
					w.sendErr(err)
				}
			case <-w.pool.ctx.Done():
				w.sendErr(w.pool.ctx.Err())
			case <-w.pool.stopWorkerChan:
				w.pool.workerWaitGroup.Done()
				return
			}
		}
	}()
}

func (w worker) sendErr(err error) {
	if atomic.CompareAndSwapUint32(w.pool.errSig, 0, 1) {
		w.pool.errorChan <- err
		close(w.pool.errorChan)
	}
}

type pool struct {
	maxWorkers      int
	buffersParams   []BufferParam
	processFunc     ProcessFunc
	offsetChan      chan int
	errorChan       chan error
	workerWaitGroup *sync.WaitGroup
	ctx             context.Context
	errSig          *uint32
	stopWorkerChan  chan struct{}
	stopped         bool
}

func newPool(ctx context.Context, processFunc ProcessFunc, maxWorkers int, buffersParams []BufferParam) pool {
	return pool{
		maxWorkers:      maxWorkers,
		offsetChan:      make(chan int),
		errorChan:       make(chan error, 1),
		processFunc:     processFunc,
		workerWaitGroup: &sync.WaitGroup{},
		ctx:             ctx,
		errSig:          new(uint32),
		stopWorkerChan:  make(chan struct{}),
		buffersParams:   buffersParams,
	}
}

func (p pool) start() {
	for workerIndex := 0; workerIndex < p.maxWorkers; workerIndex++ {
		newWorker(&p, workerIndex, newByteBuffers(p.buffersParams)).start()
	}
}

func (p *pool) stop() {
	if p.stopped {
		return
	}
	p.stopWorkers()
	close(p.offsetChan)
}

func (p pool) stopWorkers() {
	close(p.stopWorkerChan)
	p.workerWaitGroup.Wait()
}

func (p *pool) apply(offset int) error {
	if atomic.LoadUint32(p.errSig) == 1 {
		p.stop()
		p.stopped = true
		return <-p.errorChan
	}
	p.offsetChan <- offset
	return nil
}
