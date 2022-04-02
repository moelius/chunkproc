package chunkproc

import (
	"context"
	"time"
)

type Processor interface {
	Process(ctx context.Context, startOffset, chunkSize, fullSize, concurrency int, processFunc ProcessFunc) error
}

type processor struct {
	bufferParams BufferParams
}

func NewBufferedProcessor(bufferParams BufferParams) Processor {
	return processor{bufferParams: bufferParams}
}

func (p processor) Process(ctx context.Context, startOffset, chunkSize, fullSize, concurrency int, processFunc ProcessFunc) error {
	pool := newPool(ctx, processFunc, concurrency, p.bufferParams)
	pool.start()
	var err error
	for offset := startOffset; offset < fullSize; offset += chunkSize {
		if err = pool.apply(offset); err != nil {
			break
		}
	}
	pool.stop()
	return err
}

type processorWithProgress struct {
	proc             Processor
	progressFunc     ProgressFunc
	progressInterval time.Duration
}

func NewBufferedProcessorWithProgress(bufferParams BufferParams, progressFunc ProgressFunc, progressInterval time.Duration) Processor {
	return processorWithProgress{
		proc:             NewBufferedProcessor(bufferParams),
		progressFunc:     progressFunc,
		progressInterval: progressInterval,
	}
}

func (p processorWithProgress) Process(ctx context.Context, startOffset, chunkSize, fullSize, concurrency int, processFunc ProcessFunc) error {
	prog := newProgressPoller(p.progressFunc, p.progressInterval)
	processWithProgressFunc := func(workerIndex, offset int, buffers Buffers) (int, error) {
		n, err := processFunc(workerIndex, offset, buffers)
		prog.progressChan <- Progress{Offset: offset, Processed: n, Err: err}
		return n, err
	}
	prog.start(startOffset, fullSize, concurrency)
	err := p.proc.Process(ctx, startOffset, chunkSize, fullSize, concurrency, processWithProgressFunc)
	prog.stop()
	return err
}
