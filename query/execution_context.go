package query

import (
	"context"
	"sync"
)

// ExecutionContext contains state that the query is currently executing with.
// 包装语句的当前执行状态信息
type ExecutionContext struct {
	// golang sdk默认的context实例
	context.Context

	// The statement ID of the executing query.
	// 一次query请求中，每个statement语句一个id
	statementID int

	// The query ID of the executing query.
	// 本次执行Task对应的id
	QueryID uint64

	// The query task information available to the StatementExecutor.
	// 当期query请求对应的Task
	task *Task

	// Output channel where results and errors should be sent.
	// 查询结果推送的channel
	Results chan *Result

	// Options used to start this query.
	// Executor的执行参数
	ExecutionOptions

	mu sync.RWMutex
	// 当请求执行结束，推送事件到该管道
	done chan struct{}
	err  error
}

// watch 监听Task执行终止的事件，包括Task终止、http请求终止，主要用于维护Context状态
func (ctx *ExecutionContext) watch() {
	ctx.done = make(chan struct{})
	if ctx.err != nil {
		close(ctx.done)
		return
	}

	go func() {
		defer close(ctx.done)

		var taskCtx <-chan struct{}
		if ctx.task != nil {
			taskCtx = ctx.task.closing
		}

		select {
		case <-taskCtx:
			// 监听Task.closing事件
			ctx.err = ctx.task.Error()
			if ctx.err == nil {
				ctx.err = ErrQueryInterrupted
			}
		case <-ctx.AbortCh:
			// http请求终止事件
			ctx.err = ErrQueryAborted
		case <-ctx.Context.Done():
			// context已经Done，直接按Context.Err()返回
			ctx.err = ctx.Context.Err()
		}
	}()
}

func (ctx *ExecutionContext) Done() <-chan struct{} {
	ctx.mu.RLock()
	if ctx.done != nil {
		defer ctx.mu.RUnlock()
		return ctx.done
	}
	ctx.mu.RUnlock()

	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ctx.done == nil {
		ctx.watch()
	}
	return ctx.done
}

func (ctx *ExecutionContext) Err() error {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.err
}

func (ctx *ExecutionContext) Value(key interface{}) interface{} {
	switch key {
	case monitorContextKey{}:
		return ctx.task
	}
	return ctx.Context.Value(key)
}

// send sends a Result to the Results channel and will exit if the query has
// been aborted.
// 投递一个Result数据，如果客户端已经中断（http中断），则返回错误
func (ctx *ExecutionContext) send(result *Result) error {
	result.StatementID = ctx.statementID
	select {
	case <-ctx.AbortCh:
		return ErrQueryAborted
	case ctx.Results <- result:
	}
	return nil
}

// Send sends a Result to the Results channel and will exit if the query has
// been interrupted or aborted.
func (ctx *ExecutionContext) Send(result *Result) error {
	result.StatementID = ctx.statementID
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ctx.Results <- result:
	}
	return nil
}
