package query

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

var (
	// ErrInvalidQuery is returned when executing an unknown query type.
	ErrInvalidQuery = errors.New("invalid query")

	// ErrNotExecuted is returned when a statement is not executed in a query.
	// This can occur when a previous statement in the same query has errored.
	ErrNotExecuted = errors.New("not executed")

	// ErrQueryInterrupted is an error returned when the query is interrupted.
	ErrQueryInterrupted = errors.New("query interrupted")

	// ErrQueryAborted is an error returned when the query is aborted.
	ErrQueryAborted = errors.New("query aborted")

	// ErrQueryEngineShutdown is an error sent when the query cannot be
	// created because the query engine was shutdown.
	ErrQueryEngineShutdown = errors.New("query engine shutdown")

	// ErrQueryTimeoutLimitExceeded is an error when a query hits the max time allowed to run.
	ErrQueryTimeoutLimitExceeded = errors.New("query-timeout limit exceeded")

	// ErrAlreadyKilled is returned when attempting to kill a query that has already been killed.
	ErrAlreadyKilled = errors.New("already killed")
)

// Statistics for the Executor
const (
	statQueriesActive          = "queriesActive"   // Number of queries currently being executed.
	statQueriesExecuted        = "queriesExecuted" // Number of queries that have been executed (started).
	statQueriesFinished        = "queriesFinished" // Number of queries that have finished.
	statQueryExecutionDuration = "queryDurationNs" // Total (wall) time spent executing queries.
	statRecoveredPanics        = "recoveredPanics" // Number of panics recovered by Query Executor.

	// PanicCrashEnv is the environment variable that, when set, will prevent
	// the handler from recovering any panics.
	PanicCrashEnv = "INFLUXDB_PANIC_CRASH"
)

// ErrDatabaseNotFound returns a database not found error for the given database name.
func ErrDatabaseNotFound(name string) error { return fmt.Errorf("database not found: %s", name) }

// ErrMaxSelectPointsLimitExceeded is an error when a query hits the maximum number of points.
func ErrMaxSelectPointsLimitExceeded(n, limit int) error {
	return fmt.Errorf("max-select-point limit exceeed: (%d/%d)", n, limit)
}

// ErrMaxConcurrentQueriesLimitExceeded is an error when a query cannot be run
// because the maximum number of queries has been reached.
func ErrMaxConcurrentQueriesLimitExceeded(n, limit int) error {
	return fmt.Errorf("max-concurrent-queries limit exceeded(%d, %d)", n, limit)
}

/********************************************************/
/* 授权：coarse-grained粗粒度的， fine-grained细粒度的      */
/* 具体参考Coarse-Grained vs. Fine-Grained Authorization */
/* 集群版的功能，OSS不支持                                 */
/********************************************************/

// CoarseAuthorizer determines if certain operations are authorized at the database level.
//
// It is supported both in OSS and Enterprise.
// database级别粗粒度权限控制
type CoarseAuthorizer interface {
	// AuthorizeDatabase indicates whether the given Privilege is authorized on the database with the given name.
	AuthorizeDatabase(p influxql.Privilege, name string) bool
}

type openCoarseAuthorizer struct{}

func (a openCoarseAuthorizer) AuthorizeDatabase(influxql.Privilege, string) bool { return true }

// OpenCoarseAuthorizer is a fully permissive implementation of CoarseAuthorizer.
var OpenCoarseAuthorizer openCoarseAuthorizer

// FineAuthorizer determines if certain operations are authorized at the series level.
//
// It is only supported in InfluxDB Enterprise. In OSS it always returns true.
// series级别细粒度权限控制
type FineAuthorizer interface {
	// AuthorizeSeriesRead determines if a series is authorized for reading
	AuthorizeSeriesRead(database string, measurement []byte, tags models.Tags) bool

	// AuthorizeSeriesWrite determines if a series is authorized for writing
	AuthorizeSeriesWrite(database string, measurement []byte, tags models.Tags) bool

	// IsOpen guarantees that the other methods of a FineAuthorizer always return true.
	IsOpen() bool
}

// OpenAuthorizer is the Authorizer used when authorization is disabled.
// It allows all operations.
type openAuthorizer struct{}

// OpenAuthorizer can be shared by all goroutines.
var OpenAuthorizer = openAuthorizer{}

// AuthorizeSeriesRead allows access to any series.
func (a openAuthorizer) AuthorizeSeriesRead(database string, measurement []byte, tags models.Tags) bool {
	return true
}

// AuthorizeSeriesWrite allows access to any series.
func (a openAuthorizer) AuthorizeSeriesWrite(database string, measurement []byte, tags models.Tags) bool {
	return true
}

func (a openAuthorizer) IsOpen() bool { return true }

// AuthorizeSeriesRead allows any query to execute.
func (a openAuthorizer) AuthorizeQuery(_ string, _ *influxql.Query) error { return nil }

// AuthorizerIsOpen returns true if the provided Authorizer is guaranteed to
// authorize anything. A nil Authorizer returns true for this function, and this
// function should be preferred over directly checking if an Authorizer is nil
// or not.
func AuthorizerIsOpen(a FineAuthorizer) bool {
	return a == nil || a.IsOpen()
}

/********************************************************/
/* Executor执行部分代码                                   */
/********************************************************/

// ExecutionOptions contains the options for executing a query.
type ExecutionOptions struct {
	// The database the query is running against.
	// 执行的目标数据库
	Database string

	// The retention policy the query is running against.
	// 执行的目标RP
	RetentionPolicy string

	// Authorizer handles series-level authorization
	// OSS版本有效，忽略
	Authorizer FineAuthorizer

	// CoarseAuthorizer handles database-level authorization
	// OSS版本有效，忽略
	CoarseAuthorizer CoarseAuthorizer

	// The requested maximum number of points to return in each result.
	// 结果集中一个chunk里最大的point数量，即models.Row实例中包装的point数量
	// 用户可指定，默认值：httpd.DefaultChunkSize = 10000
	ChunkSize int

	// If this query is being executed in a read-only context.
	// 如果用户以`GET`方式请求，则本次就以ReadOnly方式执行
	ReadOnly bool

	// Node to execute on.
	// OSS版本无效，忽略。参考`client.Query.NodeID`，该值来自于client
	NodeID uint64

	// Quiet suppresses non-essential output from the query executor.
	Quiet bool

	// AbortCh is a channel that signals when results are no longer desired by the caller.
	// 执行中断事件通知channel
	// 并不会发送任何数据到channel中，在http请求结束时close该channel，正在执行的任务可以检测到该事件
	AbortCh <-chan struct{}
}

type (
	iteratorsContextKey struct{}
	monitorContextKey   struct{}
)

// NewContextWithIterators returns a new context.Context with the *Iterators slice added.
// The query planner will add instances of AuxIterator to the Iterators slice.
// 把AuxIterator和当前context.Context，包装成新的context.Context返回
func NewContextWithIterators(ctx context.Context, itr *Iterators) context.Context {
	return context.WithValue(ctx, iteratorsContextKey{}, itr)
}

// StatementExecutor executes a statement within the Executor.
// 语句执行器，实现有两个：
//    1. query.TaskManager : 核心的查询逻辑
//    2. coordinator.StatementExecutor : 实现集群下的查询，会包装 query.TaskManager 实例
type StatementExecutor interface {
	// ExecuteStatement executes a statement. Results should be sent to the
	// results channel in the ExecutionContext.
	// 执行一个语句，result通过 ExecutionContext 中管道返回
	ExecuteStatement(ctx *ExecutionContext, stmt influxql.Statement) error
}

// StatementNormalizer normalizes a statement before it is executed.
// 语句规范化
type StatementNormalizer interface {
	// NormalizeStatement adds a default database and policy to the
	// measurements in the statement.
	// 添加默认database和RP到语句中的measurement里
	NormalizeStatement(stmt influxql.Statement, database, retentionPolicy string) error
}

// Executor executes every statement in an Query.
// 执行器执行一次查询中的每个语句
type Executor struct {
	// Used for executing a statement in the query.
	// 请求的语句执行器，目前实现只有 coordinator.StatementExecutor
	StatementExecutor StatementExecutor

	// Used for tracking running queries.
	// 用来跟踪运行的查询。
	// TODO TaskManager 实现了 StatementExecutor，这里两个对象实例是什么意思
	TaskManager *TaskManager

	// Logger to use for all logging.
	// Defaults to discarding all log output.
	Logger *zap.Logger

	// expvar-based stats.
	stats *Statistics
}

// NewExecutor returns a new instance of Executor.
func NewExecutor() *Executor {
	return &Executor{
		TaskManager: NewTaskManager(),
		Logger:      zap.NewNop(),
		stats:       &Statistics{},
	}
}

// Statistics keeps statistics related to the Executor.
// 保存 Executor 的统计信息
type Statistics struct {
	ActiveQueries          int64 // 实时正在执行
	ExecutedQueries        int64 // 已经提交的执行
	FinishedQueries        int64 // 已经完成的执行
	QueryExecutionDuration int64 // 执行总耗时
	RecoveredPanics        int64 // recover的panic数量
}

// Statistics returns statistics for periodic monitoring.
func (e *Executor) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "queryExecutor",
		Tags: tags,
		Values: map[string]interface{}{
			statQueriesActive:          atomic.LoadInt64(&e.stats.ActiveQueries),
			statQueriesExecuted:        atomic.LoadInt64(&e.stats.ExecutedQueries),
			statQueriesFinished:        atomic.LoadInt64(&e.stats.FinishedQueries),
			statQueryExecutionDuration: atomic.LoadInt64(&e.stats.QueryExecutionDuration),
			statRecoveredPanics:        atomic.LoadInt64(&e.stats.RecoveredPanics),
		},
	}}
}

// Close kills all running queries and prevents new queries from being attached.
func (e *Executor) Close() error {
	return e.TaskManager.Close()
}

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (e *Executor) WithLogger(log *zap.Logger) {
	e.Logger = log.With(zap.String("service", "query"))
	e.TaskManager.Logger = e.Logger
}

// ExecuteQuery executes each statement within a query.
// 执行一次查询的每条语句
func (e *Executor) ExecuteQuery(query *influxql.Query, opt ExecutionOptions, closing chan struct{}) <-chan *Result {
	// 结果是放到channel里的，有点意思
	results := make(chan *Result)
	go e.executeQuery(query, opt, closing, results)
	return results
}

// executeQuery 执行语句查询
func (e *Executor) executeQuery(query *influxql.Query, opt ExecutionOptions, closing <-chan struct{}, results chan *Result) {
	// 关闭Result channel，channel变为只读
	defer close(results)
	// panic检查，记录crash原因
	defer e.recover(query, results)

	// statistic统计信息
	atomic.AddInt64(&e.stats.ActiveQueries, 1)
	atomic.AddInt64(&e.stats.ExecutedQueries, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&e.stats.ActiveQueries, -1)
		atomic.AddInt64(&e.stats.FinishedQueries, 1)
		atomic.AddInt64(&e.stats.QueryExecutionDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	// 对请求进行事件监听，把请求终止事件绑定到ctx上
	// 事件包括：超时检测、客户端中断、Task close
	ctx, detach, err := e.TaskManager.AttachQuery(query, opt, closing)
	if err != nil {
		select {
		case results <- &Result{Err: err}:
		case <-opt.AbortCh:
		}
		return
	}
	// 从TaskManager的Task列表里清除并close
	defer detach()

	// Setup the execution context that will be used when executing statements.
	ctx.Results = results

	var i int
LOOP:
	for ; i < len(query.Statements); i++ {
		ctx.statementID = i
		stmt := query.Statements[i]

		// If a default database wasn't passed in by the caller, check the statement.
		defaultDB := opt.Database
		if defaultDB == "" {
			// 类似"xxx xxx ON MyDB"这种语句，实现HasDefaultDatabase接口，`DefaultDatabase()`就是"MyDB"
			if s, ok := stmt.(influxql.HasDefaultDatabase); ok {
				defaultDB = s.DefaultDatabase()
			}
		}

		// Do not let queries manually use the system measurements. If we find
		// one, return an error. This prevents a person from using the
		// measurement incorrectly and causing a panic.
		// 不要让请求手动使用系统的measurement，如果发现返回错误。
		// 这可以防止人错误使用measurement导致panic
		if stmt, ok := stmt.(*influxql.SelectStatement); ok {
			for _, s := range stmt.Sources {
				switch s := s.(type) {
				case *influxql.Measurement:
					if influxql.IsSystemName(s.Name) {
						command := "the appropriate meta command"
						switch s.Name {
						case "_fieldKeys":
							command = "SHOW FIELD KEYS"
						case "_measurements":
							command = "SHOW MEASUREMENTS"
						case "_series":
							command = "SHOW SERIES"
						case "_tagKeys":
							command = "SHOW TAG KEYS"
						case "_tags":
							command = "SHOW TAG VALUES"
						}
						results <- &Result{
							Err: fmt.Errorf("unable to use system source '%s': use %s instead", s.Name, command),
						}
						break LOOP
					}
				}
			}
		}

		// Rewrite statements, if necessary.
		// This can occur on meta read statements which convert to SELECT statements.
		// 对元数据查询转换成select语句（就是show语句转换成select）
		newStmt, err := RewriteStatement(stmt)
		if err != nil {
			results <- &Result{Err: err}
			break
		}
		stmt = newStmt

		// Normalize each statement if possible.
		// 因为实现目前只有 coordinator.StatementExecutor，这里一定是实现StatementNormalizer的
		if normalizer, ok := e.StatementExecutor.(StatementNormalizer); ok {
			// 规范语句，明确语句中的database和RP（通常是没有指定，使用defaultDB之类）
			if err := normalizer.NormalizeStatement(stmt, defaultDB, opt.RetentionPolicy); err != nil {
				// 对于不能规范化的语句，是要包装成错误返回给用户的，所以这里当做Result返回，并break终止执行
				if err := ctx.send(&Result{Err: err}); err == ErrQueryAborted {
					return
				}
				// 跳出循环，中断后续的语句执行，会导致整个查询中所有语句被标记成ErrNotExecuted状态
				break
			}
		}

		// Log each normalized statement.
		// 没看到Quiet的赋值操作，这里应该默认false
		if !ctx.Quiet {
			e.Logger.Info("Executing query", zap.Stringer("query", stmt))
		}

		// Send any other statements to the underlying statement executor.
		// 将通过Normalize处理的语句，交给基础的语句执行器执行（coordinator.StatementExecutor）
		err = e.StatementExecutor.ExecuteStatement(ctx, stmt)
		if err == ErrQueryInterrupted {
			// Query was interrupted so retrieve the real interrupt error from
			// the query task if there is one.
			// 如果执行异常，冲context中找出真正的err原因
			if qerr := ctx.Err(); qerr != nil {
				err = qerr
			}
		}

		// Send an error for this result if it failed for some reason.
		// 如果语句执行报错，则把错误包装成Result投递，并break中断执行
		if err != nil {
			if err := ctx.send(&Result{
				StatementID: i,
				Err:         err,
			}); err == ErrQueryAborted {
				return
			}
			// Stop after the first error.
			break
		}

		// Check if the query was interrupted during an uninterruptible statement.
		// 因为Context中维护Task完整状态，判断Task是否已经中断结束
		interrupted := false
		select {
		case <-ctx.Done():
			interrupted = true
		default:
			// Query has not been interrupted.
		}

		if interrupted {
			break
		}
	}

	// Send error results for any statements which were not executed.
	// 把循环剩余的query.Statements按ErrNotExecuted处理
	for ; i < len(query.Statements)-1; i++ {
		if err := ctx.send(&Result{
			StatementID: i,
			Err:         ErrNotExecuted,
		}); err == ErrQueryAborted {
			return
		}
	}
}

// Determines if the Executor will recover any panics or let them crash
// the server.
var willCrash bool

func init() {
	var err error
	if willCrash, err = strconv.ParseBool(os.Getenv(PanicCrashEnv)); err != nil {
		willCrash = false
	}
}

// recover 对将要导致crash的panic异常捕获，把异常信息推送到Result channel
func (e *Executor) recover(query *influxql.Query, results chan *Result) {
	if err := recover(); err != nil {
		atomic.AddInt64(&e.stats.RecoveredPanics, 1) // Capture the panic in _internal stats.
		e.Logger.Error(fmt.Sprintf("%s [panic:%s] %s", query.String(), err, debug.Stack()))
		results <- &Result{
			StatementID: -1,
			Err:         fmt.Errorf("%s [panic:%s]", query.String(), err),
		}

		if willCrash {
			e.Logger.Error(fmt.Sprintf("\n\n=====\nAll goroutines now follow:"))
			buf := debug.Stack()
			e.Logger.Error(fmt.Sprintf("%s", buf))
			os.Exit(1)
		}
	}
}

// Task is the internal data structure for managing queries.
// For the public use data structure that gets returned, see Task.
type Task struct {
	query     string
	database  string
	status    TaskStatus
	startTime time.Time
	closing   chan struct{}
	monitorCh chan error
	err       error
	mu        sync.Mutex
}

// Monitor starts a new goroutine that will monitor a query. The function
// will be passed in a channel to signal when the query has been finished
// normally. If the function returns with an error and the query is still
// running, the query will be terminated.
func (q *Task) Monitor(fn MonitorFunc) {
	go q.monitor(fn)
}

// Error returns any asynchronous error that may have occurred while executing
// the query.
func (q *Task) Error() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.err
}

func (q *Task) setError(err error) {
	q.mu.Lock()
	q.err = err
	q.mu.Unlock()
}

// monitor 通过执行回调方法MonitorFunc，检测结果如果有错误，推送到q.monitorCh
func (q *Task) monitor(fn MonitorFunc) {
	// 目前fn没有返回err的case
	if err := fn(q.closing); err != nil {
		select {
		case <-q.closing:
		case q.monitorCh <- err:
		}
	}
}

// close closes the query task closing channel if the query hasn't been previously killed.
func (q *Task) close() {
	q.mu.Lock()
	if q.status != KilledTask {
		// Set the status to killed to prevent closing the channel twice.
		q.status = KilledTask
		close(q.closing)
	}
	q.mu.Unlock()
}

// kill 标记任务状态为KilledTask，关闭q.closing channel
func (q *Task) kill() error {
	q.mu.Lock()
	if q.status == KilledTask {
		q.mu.Unlock()
		return ErrAlreadyKilled
	}
	q.status = KilledTask
	close(q.closing)
	q.mu.Unlock()
	return nil
}
