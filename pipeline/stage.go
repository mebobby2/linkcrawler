package pipeline

import (
	"context"
	"sync"

	"golang.org/x/xerrors"
)

type fifo struct {
	proc Processor
}

func FIFO(proc Processor) StageRunner {
	return fifo{proc: proc}
}

func (r fifo) Run(ctx context.Context, params StageParams) {
	for {
		select {
		case <-ctx.Done():
			return
		case payloadIn, ok := <-params.Input():
			if !ok {
				return
			}

			payloadOut, err := r.proc.Process(ctx, payloadIn)
			if err != nil {
				wrappedErr := xerrors.Errorf("pipeline stage %d: %w", params.StateIndex(), err)
				maybeEmitError(wrappedErr, params.Error())
				return
			}

			if payloadOut == nil {
				payloadIn.MarkAsProcessed()
				continue
			}

			select {
			case params.Output() <- payloadOut:
			case <-ctx.Done():
				return
			}
		}
	}
}

type fixedWorkerPool struct {
	fifos []StageRunner
}

func FixedWorkerPool(proc Processor, numWorkers int) StageRunner {
	if numWorkers <= 0 {
		panic("FixedWorkerPool: numWorkers must be > 0")
	}

	fifos := make([]StageRunner, numWorkers)
	for i := 0; i < numWorkers; i++ {
		fifos[i] = FIFO((proc))
	}

	// https://calvincheng919.medium.com/golang-pointers-f82717811812
	// * in a type declaration is saying the var holds a memory address to a string, or int etc
	// * everywhere else is saying give me the value of whatever the pointer is pointing to
	// & says give me the memory address of the var

	// https://stackoverflow.com/questions/23542989/pointers-vs-values-in-parameters-and-return-values
	// use pointers for big structs or structs you'll have to change, and otherwise pass values,
	// because getting things changed by surprise via a pointer is confusing.
	return &fixedWorkerPool{fifos: fifos}
}

func (p *fixedWorkerPool) Run(ctx context.Context, params StageParams) {
	var wg sync.WaitGroup

	for i := 0; i < len(p.fifos); i++ {
		wg.Add(1)
		go func(fifoIndex int) {
			p.fifos[fifoIndex].Run(ctx, params)
			wg.Done()
		}(i)
	}

	wg.Wait()
}
