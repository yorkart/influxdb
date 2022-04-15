package query

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/pkg/tracing"
	"github.com/influxdata/influxdb/query/internal/gota"
	"github.com/influxdata/influxql"
	"golang.org/x/sync/errgroup"
)

var DefaultTypeMapper = influxql.MultiTypeMapper(
	FunctionTypeMapper{},
	MathTypeMapper{},
)

// SelectOptions are options that customize the select call.
// select语句执行参数
type SelectOptions struct {
	// Authorizer is used to limit access to data
	// OSS版本忽略
	Authorizer FineAuthorizer

	// Node to exclusively read from.
	// If zero, all nodes are used.
	// OSS版本忽略
	NodeID uint64

	// Maximum number of concurrent series.
	MaxSeriesN int

	// Maximum number of points to read from the query.
	// This requires the passed in context to have a Monitor that is
	// created using WithMonitor.
	MaxPointN int

	// Maximum number of buckets for a statement.
	MaxBucketsN int
}

// ShardMapper retrieves and maps shards into an IteratorCreator that can later be
// used for executing queries.
type ShardMapper interface {
	// MapShards 根据source、事件范围计算查询涉及的shard信息，并以 query.ShardGroup 接口返回
	// 用于指导调用方去构造Iterator
	MapShards(sources influxql.Sources, t influxql.TimeRange, opt SelectOptions) (ShardGroup, error)
}

// ShardGroup represents a shard or a collection of shards that can be accessed
// for creating iterators.
// When creating iterators, the resource used for reading the iterators should be
// separate from the resource used to map the shards. When the ShardGroup is closed,
// it should not close any resources associated with the created Iterator. Those
// resources belong to the Iterator and will be closed when the Iterator itself is
// closed.
// The query engine operates under this assumption and will close the shard group
// after creating the iterators, but before the iterators are actually read.
type ShardGroup interface {
	IteratorCreator
	influxql.FieldMapper
	io.Closer
}

// PreparedStatement is a prepared statement that is ready to be executed.
// 预编译语句
type PreparedStatement interface {
	// Select creates the Iterators that will be used to read the query.
	// 创建用于读取查询的Iterator，以Cursor形式返回给调用方进行迭代。
	Select(ctx context.Context) (Cursor, error)

	// Explain outputs the explain plan for this statement.
	// 输出语句解释的执行计划
	Explain() (string, error)

	// Close closes the resources associated with this prepared statement.
	// This must be called as the mapped shards may hold open resources such
	// as network connections.
	Close() error
}

// Prepare will compile the statement with the default compile options and
// then prepare the query.
// 将使用默认编译选项编译语句，然后准备查询
func Prepare(stmt *influxql.SelectStatement, shardMapper ShardMapper, opt SelectOptions) (PreparedStatement, error) {
	// 语句编译，提取、变换相关语句和属性
	c, err := Compile(stmt, CompileOptions{})
	if err != nil {
		return nil, err
	}
	// 涉及正则处理、类型校验、ShardGroup（IteratorCreator）计算等处理
	return c.Prepare(shardMapper, opt)
}

// Select compiles, prepares, and then initiates execution of the query using the
// default compile options.
// 使用默认的编译选项，选择编译、准备，然后开始执行查询。
func Select(ctx context.Context, stmt *influxql.SelectStatement, shardMapper ShardMapper, opt SelectOptions) (Cursor, error) {
	// 对语句进行编译
	s, err := Prepare(stmt, shardMapper, opt)
	if err != nil {
		return nil, err
	}
	// Must be deferred so it runs after Select.
	defer s.Close()
	return s.Select(ctx)
}

// preparedStatement 实现预编译select语句的执行
type preparedStatement struct {
	stmt *influxql.SelectStatement
	// opt  Iterator 的创建选项
	opt IteratorOptions
	// ic IteratorCreator 这里就是 ShardGroup 的实现 coordinator.LocalShardMapping
	ic interface {
		IteratorCreator
		io.Closer
	}
	// columns 最终projection的列名
	columns   []string
	maxPointN int
	// now 语句执行时的时间
	now time.Time
}

func (p *preparedStatement) Select(ctx context.Context) (Cursor, error) {
	// TODO(jsternberg): Remove this hacky method of propagating now.
	// Each level of the query should use a time range discovered during
	// compilation, but that requires too large of a refactor at the moment.
	// 简单粗暴实现now值的透传
	ctx = context.WithValue(ctx, "now", p.now)

	opt := p.opt
	opt.InterruptCh = ctx.Done()
	cur, err := buildCursor(ctx, p.stmt, p.ic, opt)
	if err != nil {
		return nil, err
	}

	// If a monitor exists and we are told there is a maximum number of points,
	// register the monitor function.
	// 添加point扫描限制的监控任务，
	if m := MonitorFromContext(ctx); m != nil {
		if p.maxPointN > 0 {
			monitor := PointLimitMonitor(cur, DefaultStatsInterval, p.maxPointN)
			m.Monitor(monitor)
		}
	}
	return cur, nil
}

func (p *preparedStatement) Close() error {
	return p.ic.Close()
}

// buildExprIterator creates an iterator for an expression.
func buildExprIterator(ctx context.Context, expr influxql.Expr, ic IteratorCreator, sources influxql.Sources, opt IteratorOptions, selector, writeMode bool) (Iterator, error) {
	opt.Expr = expr
	b := exprIteratorBuilder{
		ic:        ic,
		sources:   sources,
		opt:       opt,
		selector:  selector,
		writeMode: writeMode,
	}

	switch expr := expr.(type) {
	case *influxql.VarRef:
		return b.buildVarRefIterator(ctx, expr)
	case *influxql.Call:
		return b.buildCallIterator(ctx, expr)
	default:
		return nil, fmt.Errorf("invalid expression type: %T", expr)
	}
}

type exprIteratorBuilder struct {
	ic        IteratorCreator
	sources   influxql.Sources
	opt       IteratorOptions
	selector  bool
	writeMode bool
}

func (b *exprIteratorBuilder) buildVarRefIterator(ctx context.Context, expr *influxql.VarRef) (Iterator, error) {
	inputs := make([]Iterator, 0, len(b.sources))
	if err := func() error {
		for _, source := range b.sources {
			switch source := source.(type) {
			case *influxql.Measurement:
				input, err := b.ic.CreateIterator(ctx, source, b.opt)
				if err != nil {
					return err
				}
				inputs = append(inputs, input)
			case *influxql.SubQuery:
				subquery := subqueryBuilder{
					ic:   b.ic,
					stmt: source.Statement,
				}

				input, err := subquery.buildVarRefIterator(ctx, expr, b.opt)
				if err != nil {
					return err
				} else if input != nil {
					inputs = append(inputs, input)
				}
			}
		}
		return nil
	}(); err != nil {
		Iterators(inputs).Close()
		return nil, err
	}

	// Variable references in this section will always go into some call
	// iterator. Combine it with a merge iterator.
	itr := NewMergeIterator(inputs, b.opt)
	if itr == nil {
		itr = &nilFloatIterator{}
	}

	if b.opt.InterruptCh != nil {
		itr = NewInterruptIterator(itr, b.opt.InterruptCh)
	}
	return itr, nil
}

func (b *exprIteratorBuilder) buildCallIterator(ctx context.Context, expr *influxql.Call) (Iterator, error) {
	// TODO(jsternberg): Refactor this. This section needs to die in a fire.
	opt := b.opt
	// Eliminate limits and offsets if they were previously set. These are handled by the caller.
	opt.Limit, opt.Offset = 0, 0
	switch expr.Name {
	case "distinct":
		opt.Ordered = true
		input, err := buildExprIterator(ctx, expr.Args[0].(*influxql.VarRef), b.ic, b.sources, opt, b.selector, false)
		if err != nil {
			return nil, err
		}
		input, err = NewDistinctIterator(input, opt)
		if err != nil {
			return nil, err
		}
		return NewIntervalIterator(input, opt), nil
	case "sample":
		opt.Ordered = true
		input, err := buildExprIterator(ctx, expr.Args[0], b.ic, b.sources, opt, b.selector, false)
		if err != nil {
			return nil, err
		}
		size := expr.Args[1].(*influxql.IntegerLiteral)

		return newSampleIterator(input, opt, int(size.Val))
	case "holt_winters", "holt_winters_with_fit":
		opt.Ordered = true
		input, err := buildExprIterator(ctx, expr.Args[0], b.ic, b.sources, opt, b.selector, false)
		if err != nil {
			return nil, err
		}
		h := expr.Args[1].(*influxql.IntegerLiteral)
		m := expr.Args[2].(*influxql.IntegerLiteral)

		includeFitData := "holt_winters_with_fit" == expr.Name

		interval := opt.Interval.Duration
		// Redefine interval to be unbounded to capture all aggregate results
		opt.StartTime = influxql.MinTime
		opt.EndTime = influxql.MaxTime
		opt.Interval = Interval{}

		return newHoltWintersIterator(input, opt, int(h.Val), int(m.Val), includeFitData, interval)
	case "derivative", "non_negative_derivative", "difference", "non_negative_difference", "moving_average", "exponential_moving_average", "double_exponential_moving_average", "triple_exponential_moving_average", "relative_strength_index", "triple_exponential_derivative", "kaufmans_efficiency_ratio", "kaufmans_adaptive_moving_average", "chande_momentum_oscillator", "elapsed":
		if !opt.Interval.IsZero() {
			if opt.Ascending {
				opt.StartTime -= int64(opt.Interval.Duration)
			} else {
				opt.EndTime += int64(opt.Interval.Duration)
			}
		}
		opt.Ordered = true

		input, err := buildExprIterator(ctx, expr.Args[0], b.ic, b.sources, opt, b.selector, false)
		if err != nil {
			return nil, err
		}

		switch expr.Name {
		case "derivative", "non_negative_derivative":
			interval := opt.DerivativeInterval()
			isNonNegative := (expr.Name == "non_negative_derivative")
			return newDerivativeIterator(input, opt, interval, isNonNegative)
		case "elapsed":
			interval := opt.ElapsedInterval()
			return newElapsedIterator(input, opt, interval)
		case "difference", "non_negative_difference":
			isNonNegative := (expr.Name == "non_negative_difference")
			return newDifferenceIterator(input, opt, isNonNegative)
		case "moving_average":
			n := expr.Args[1].(*influxql.IntegerLiteral)
			if n.Val > 1 && !opt.Interval.IsZero() {
				if opt.Ascending {
					opt.StartTime -= int64(opt.Interval.Duration) * (n.Val - 1)
				} else {
					opt.EndTime += int64(opt.Interval.Duration) * (n.Val - 1)
				}
			}
			return newMovingAverageIterator(input, int(n.Val), opt)
		case "exponential_moving_average", "double_exponential_moving_average", "triple_exponential_moving_average", "relative_strength_index", "triple_exponential_derivative":
			n := expr.Args[1].(*influxql.IntegerLiteral)
			if n.Val > 1 && !opt.Interval.IsZero() {
				if opt.Ascending {
					opt.StartTime -= int64(opt.Interval.Duration) * (n.Val - 1)
				} else {
					opt.EndTime += int64(opt.Interval.Duration) * (n.Val - 1)
				}
			}

			nHold := -1
			if len(expr.Args) >= 3 {
				nHold = int(expr.Args[2].(*influxql.IntegerLiteral).Val)
			}

			warmupType := gota.WarmEMA
			if len(expr.Args) >= 4 {
				if warmupType, err = gota.ParseWarmupType(expr.Args[3].(*influxql.StringLiteral).Val); err != nil {
					return nil, err
				}
			}

			switch expr.Name {
			case "exponential_moving_average":
				return newExponentialMovingAverageIterator(input, int(n.Val), nHold, warmupType, opt)
			case "double_exponential_moving_average":
				return newDoubleExponentialMovingAverageIterator(input, int(n.Val), nHold, warmupType, opt)
			case "triple_exponential_moving_average":
				return newTripleExponentialMovingAverageIterator(input, int(n.Val), nHold, warmupType, opt)
			case "relative_strength_index":
				return newRelativeStrengthIndexIterator(input, int(n.Val), nHold, warmupType, opt)
			case "triple_exponential_derivative":
				return newTripleExponentialDerivativeIterator(input, int(n.Val), nHold, warmupType, opt)
			}
		case "kaufmans_efficiency_ratio", "kaufmans_adaptive_moving_average":
			n := expr.Args[1].(*influxql.IntegerLiteral)
			if n.Val > 1 && !opt.Interval.IsZero() {
				if opt.Ascending {
					opt.StartTime -= int64(opt.Interval.Duration) * (n.Val - 1)
				} else {
					opt.EndTime += int64(opt.Interval.Duration) * (n.Val - 1)
				}
			}

			nHold := -1
			if len(expr.Args) >= 3 {
				nHold = int(expr.Args[2].(*influxql.IntegerLiteral).Val)
			}

			switch expr.Name {
			case "kaufmans_efficiency_ratio":
				return newKaufmansEfficiencyRatioIterator(input, int(n.Val), nHold, opt)
			case "kaufmans_adaptive_moving_average":
				return newKaufmansAdaptiveMovingAverageIterator(input, int(n.Val), nHold, opt)
			}
		case "chande_momentum_oscillator":
			n := expr.Args[1].(*influxql.IntegerLiteral)
			if n.Val > 1 && !opt.Interval.IsZero() {
				if opt.Ascending {
					opt.StartTime -= int64(opt.Interval.Duration) * (n.Val - 1)
				} else {
					opt.EndTime += int64(opt.Interval.Duration) * (n.Val - 1)
				}
			}

			nHold := -1
			if len(expr.Args) >= 3 {
				nHold = int(expr.Args[2].(*influxql.IntegerLiteral).Val)
			}

			warmupType := gota.WarmupType(-1)
			if len(expr.Args) >= 4 {
				wt := expr.Args[3].(*influxql.StringLiteral).Val
				if wt != "none" {
					if warmupType, err = gota.ParseWarmupType(wt); err != nil {
						return nil, err
					}
				}
			}

			return newChandeMomentumOscillatorIterator(input, int(n.Val), nHold, warmupType, opt)
		}
		panic(fmt.Sprintf("invalid series aggregate function: %s", expr.Name))
	case "cumulative_sum":
		opt.Ordered = true
		input, err := buildExprIterator(ctx, expr.Args[0], b.ic, b.sources, opt, b.selector, false)
		if err != nil {
			return nil, err
		}
		return newCumulativeSumIterator(input, opt)
	case "integral":
		opt.Ordered = true
		input, err := buildExprIterator(ctx, expr.Args[0].(*influxql.VarRef), b.ic, b.sources, opt, false, false)
		if err != nil {
			return nil, err
		}
		interval := opt.IntegralInterval()
		return newIntegralIterator(input, opt, interval)
	case "top":
		if len(expr.Args) < 2 {
			return nil, fmt.Errorf("top() requires 2 or more arguments, got %d", len(expr.Args))
		}

		var input Iterator
		if len(expr.Args) > 2 {
			// Create a max iterator using the groupings in the arguments.
			dims := make(map[string]struct{}, len(expr.Args)-2+len(opt.GroupBy))
			for i := 1; i < len(expr.Args)-1; i++ {
				ref := expr.Args[i].(*influxql.VarRef)
				dims[ref.Val] = struct{}{}
			}
			for dim := range opt.GroupBy {
				dims[dim] = struct{}{}
			}

			call := &influxql.Call{
				Name: "max",
				Args: expr.Args[:1],
			}
			callOpt := opt
			callOpt.Expr = call
			callOpt.GroupBy = dims
			callOpt.Fill = influxql.NoFill

			builder := *b
			builder.opt = callOpt
			builder.selector = true
			builder.writeMode = false

			i, err := builder.callIterator(ctx, call, callOpt)
			if err != nil {
				return nil, err
			}
			input = i
		} else {
			// There are no arguments so do not organize the points by tags.
			builder := *b
			builder.opt.Expr = expr.Args[0]
			builder.selector = true
			builder.writeMode = false

			ref := expr.Args[0].(*influxql.VarRef)
			i, err := builder.buildVarRefIterator(ctx, ref)
			if err != nil {
				return nil, err
			}
			input = i
		}

		n := expr.Args[len(expr.Args)-1].(*influxql.IntegerLiteral)
		return newTopIterator(input, opt, int(n.Val), b.writeMode)
	case "bottom":
		if len(expr.Args) < 2 {
			return nil, fmt.Errorf("bottom() requires 2 or more arguments, got %d", len(expr.Args))
		}

		var input Iterator
		if len(expr.Args) > 2 {
			// Create a max iterator using the groupings in the arguments.
			dims := make(map[string]struct{}, len(expr.Args)-2)
			for i := 1; i < len(expr.Args)-1; i++ {
				ref := expr.Args[i].(*influxql.VarRef)
				dims[ref.Val] = struct{}{}
			}
			for dim := range opt.GroupBy {
				dims[dim] = struct{}{}
			}

			call := &influxql.Call{
				Name: "min",
				Args: expr.Args[:1],
			}
			callOpt := opt
			callOpt.Expr = call
			callOpt.GroupBy = dims
			callOpt.Fill = influxql.NoFill

			builder := *b
			builder.opt = callOpt
			builder.selector = true
			builder.writeMode = false

			i, err := builder.callIterator(ctx, call, callOpt)
			if err != nil {
				return nil, err
			}
			input = i
		} else {
			// There are no arguments so do not organize the points by tags.
			builder := *b
			builder.opt.Expr = expr.Args[0]
			builder.selector = true
			builder.writeMode = false

			ref := expr.Args[0].(*influxql.VarRef)
			i, err := builder.buildVarRefIterator(ctx, ref)
			if err != nil {
				return nil, err
			}
			input = i
		}

		n := expr.Args[len(expr.Args)-1].(*influxql.IntegerLiteral)
		return newBottomIterator(input, b.opt, int(n.Val), b.writeMode)
	}

	itr, err := func() (Iterator, error) {
		switch expr.Name {
		case "count":
			switch arg0 := expr.Args[0].(type) {
			case *influxql.Call:
				if arg0.Name == "distinct" {
					input, err := buildExprIterator(ctx, arg0, b.ic, b.sources, opt, b.selector, false)
					if err != nil {
						return nil, err
					}
					return newCountIterator(input, opt)
				}
			}
			fallthrough
		case "min", "max", "sum", "first", "last", "mean":
			return b.callIterator(ctx, expr, opt)
		case "median":
			opt.Ordered = true
			input, err := buildExprIterator(ctx, expr.Args[0].(*influxql.VarRef), b.ic, b.sources, opt, false, false)
			if err != nil {
				return nil, err
			}
			return newMedianIterator(input, opt)
		case "mode":
			input, err := buildExprIterator(ctx, expr.Args[0].(*influxql.VarRef), b.ic, b.sources, opt, false, false)
			if err != nil {
				return nil, err
			}
			return NewModeIterator(input, opt)
		case "stddev":
			input, err := buildExprIterator(ctx, expr.Args[0].(*influxql.VarRef), b.ic, b.sources, opt, false, false)
			if err != nil {
				return nil, err
			}
			return newStddevIterator(input, opt)
		case "spread":
			// OPTIMIZE(benbjohnson): convert to map/reduce
			input, err := buildExprIterator(ctx, expr.Args[0].(*influxql.VarRef), b.ic, b.sources, opt, false, false)
			if err != nil {
				return nil, err
			}
			return newSpreadIterator(input, opt)
		case "percentile":
			opt.Ordered = true
			input, err := buildExprIterator(ctx, expr.Args[0].(*influxql.VarRef), b.ic, b.sources, opt, false, false)
			if err != nil {
				return nil, err
			}
			var percentile float64
			switch arg := expr.Args[1].(type) {
			case *influxql.NumberLiteral:
				percentile = arg.Val
			case *influxql.IntegerLiteral:
				percentile = float64(arg.Val)
			}
			return newPercentileIterator(input, opt, percentile)
		default:
			return nil, fmt.Errorf("unsupported call: %s", expr.Name)
		}
	}()

	if err != nil {
		return nil, err
	}

	if !b.selector || !opt.Interval.IsZero() {
		itr = NewIntervalIterator(itr, opt)
		if !opt.Interval.IsZero() && opt.Fill != influxql.NoFill {
			itr = NewFillIterator(itr, expr, opt)
		}
	}
	if opt.InterruptCh != nil {
		itr = NewInterruptIterator(itr, opt.InterruptCh)
	}
	return itr, nil
}

func (b *exprIteratorBuilder) callIterator(ctx context.Context, expr *influxql.Call, opt IteratorOptions) (Iterator, error) {
	inputs := make([]Iterator, 0, len(b.sources))
	if err := func() error {
		for _, source := range b.sources {
			switch source := source.(type) {
			case *influxql.Measurement:
				input, err := b.ic.CreateIterator(ctx, source, opt)
				if err != nil {
					return err
				}
				inputs = append(inputs, input)
			case *influxql.SubQuery:
				// Identify the name of the field we are using.
				arg0 := expr.Args[0].(*influxql.VarRef)

				opt.Ordered = false
				input, err := buildExprIterator(ctx, arg0, b.ic, []influxql.Source{source}, opt, b.selector, false)
				if err != nil {
					return err
				}

				// Wrap the result in a call iterator.
				i, err := NewCallIterator(input, opt)
				if err != nil {
					input.Close()
					return err
				}
				inputs = append(inputs, i)
			}
		}
		return nil
	}(); err != nil {
		Iterators(inputs).Close()
		return nil, err
	}

	itr, err := Iterators(inputs).Merge(opt)
	if err != nil {
		Iterators(inputs).Close()
		return nil, err
	} else if itr == nil {
		itr = &nilFloatIterator{}
	}
	return itr, nil
}

func buildCursor(ctx context.Context, stmt *influxql.SelectStatement, ic IteratorCreator, opt IteratorOptions) (Cursor, error) {
	span := tracing.SpanFromContext(ctx)
	if span != nil {
		span = span.StartSpan("build_cursor")
		defer span.Finish()

		span.SetLabels("statement", stmt.String())
		ctx = tracing.NewContextWithSpan(ctx, span)
	}

	// 计算FillValue
	switch opt.Fill {
	case influxql.NumberFill:
		if v, ok := opt.FillValue.(int); ok {
			opt.FillValue = int64(v)
		}
	case influxql.PreviousFill:
		opt.FillValue = SkipDefault
	}

	// 用于输出列field
	fields := make([]*influxql.Field, 0, len(stmt.Fields)+1)
	// 判断是否输出时忽略time列，如果不忽略，则要把time列加进来
	if !stmt.OmitTime {
		// Add a field with the variable "time" if we have not omitted time.
		fields = append(fields, &influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "time",
				Type: influxql.Time,
			},
		})
	}

	// Iterate through each of the fields to add them to the value mapper.
	// 遍历每个字段，将它们添加到值映射器。
	// 遍历stmt.Fields，针对 influxql.Expr 的两种情况分析： influxql.VarRef ， influxql.Call
	valueMapper := newValueMapper()
	for _, f := range stmt.Fields {
		// 对输入f，计算出输出的f，添加到fields
		fields = append(fields, valueMapper.Map(f))

		// If the field is a top() or bottom() call, we need to also add
		// the extra variables if we are not writing into a target.
		if stmt.Target != nil {
			continue
		}

		switch expr := f.Expr.(type) {
		case *influxql.Call:
			if expr.Name == "top" || expr.Name == "bottom" {
				for i := 1; i < len(expr.Args)-1; i++ {
					nf := influxql.Field{Expr: expr.Args[i]}
					fields = append(fields, valueMapper.Map(&nf))
				}
			}
		}
	}

	// Set the aliases on each of the columns to what the final name should be.
	// 计算输出列的名称，绑定到输出的field上
	columns := stmt.ColumnNames()
	for i, f := range fields {
		f.Alias = columns[i]
	}

	// Retrieve the refs to retrieve the auxiliary fields.
	// 没有group by情况：计算 Auxiliary field，也就是projection时需要的tag或value
	var auxKeys []influxql.VarRef
	if len(valueMapper.refs) > 0 {
		opt.Aux = make([]influxql.VarRef, 0, len(valueMapper.refs))
		for ref := range valueMapper.refs {
			opt.Aux = append(opt.Aux, *ref)
		}
		sort.Sort(influxql.VarRefs(opt.Aux))

		auxKeys = make([]influxql.VarRef, len(opt.Aux))
		for i, ref := range opt.Aux {
			auxKeys[i] = valueMapper.symbols[ref.String()]
		}
	}

	// If there are no calls, then produce an auxiliary cursor.
	// 有refs，没有calls，可以理解为scan操作：select tag1,tag2,value from measurement
	if len(valueMapper.calls) == 0 {
		// If all of the auxiliary keys are of an unknown type,
		// do not construct the iterator and return a null cursor.
		// 此时，所有Type必须确定，否则返回空
		if !hasValidType(auxKeys) {
			return newNullCursor(fields), nil
		}

		itr, err := buildAuxIterator(ctx, ic, stmt.Sources, opt)
		if err != nil {
			return nil, err
		}

		// Create a slice with an empty first element.
		keys := []influxql.VarRef{{}}
		keys = append(keys, auxKeys...)

		scanner := NewIteratorScanner(itr, keys, opt.FillValue)
		return newScannerCursor(scanner, fields, opt), nil
	}

	// Check to see if this is a selector statement.
	// It is a selector if it is the only selector call and the call itself
	// is a selector.
	selector := len(valueMapper.calls) == 1
	// 如果只有一个函数调用， 判断在select部分，是否支持该函数（支持max，sum等）
	if selector {
		for call := range valueMapper.calls {
			if !influxql.IsSelector(call) {
				selector = false
			}
		}
	}

	// Produce an iterator for every single call and create an iterator scanner
	// associated with it.
	var g errgroup.Group
	var mu sync.Mutex
	scanners := make([]IteratorScanner, 0, len(valueMapper.calls))
	for call := range valueMapper.calls {
		call := call

		driver := valueMapper.table[call]
		if driver.Type == influxql.Unknown {
			// The primary driver of this call is of unknown type, so skip this.
			continue
		}

		g.Go(func() error {
			itr, err := buildFieldIterator(ctx, call, ic, stmt.Sources, opt, selector, stmt.Target != nil)
			if err != nil {
				return err
			}

			keys := make([]influxql.VarRef, 0, len(auxKeys)+1)
			keys = append(keys, driver)
			keys = append(keys, auxKeys...)

			scanner := NewIteratorScanner(itr, keys, opt.FillValue)

			mu.Lock()
			scanners = append(scanners, scanner)
			mu.Unlock()

			return nil
		})
	}

	// Close all scanners if any iterator fails.
	if err := g.Wait(); err != nil {
		for _, s := range scanners {
			s.Close()
		}
		return nil, err
	}

	if len(scanners) == 0 {
		return newNullCursor(fields), nil
	} else if len(scanners) == 1 {
		return newScannerCursor(scanners[0], fields, opt), nil
	}
	return newMultiScannerCursor(scanners, fields, opt), nil
}

func buildAuxIterator(ctx context.Context, ic IteratorCreator, sources influxql.Sources, opt IteratorOptions) (Iterator, error) {
	span := tracing.SpanFromContext(ctx)
	if span != nil {
		span = span.StartSpan("iterator_scanner")
		defer span.Finish()

		auxFieldNames := make([]string, len(opt.Aux))
		for i, ref := range opt.Aux {
			auxFieldNames[i] = ref.String()
		}
		span.SetLabels("auxiliary_fields", strings.Join(auxFieldNames, ", "))
		ctx = tracing.NewContextWithSpan(ctx, span)
	}

	inputs := make([]Iterator, 0, len(sources))
	if err := func() error {
		for _, source := range sources {
			switch source := source.(type) {
			case *influxql.Measurement:
				input, err := ic.CreateIterator(ctx, source, opt)
				if err != nil {
					return err
				}
				inputs = append(inputs, input)
			case *influxql.SubQuery:
				b := subqueryBuilder{
					ic:   ic,
					stmt: source.Statement,
				}

				input, err := b.buildAuxIterator(ctx, opt)
				if err != nil {
					return err
				} else if input != nil {
					inputs = append(inputs, input)
				}
			}
		}
		return nil
	}(); err != nil {
		Iterators(inputs).Close()
		return nil, err
	}

	// Merge iterators to read auxilary fields.
	input, err := Iterators(inputs).Merge(opt)
	if err != nil {
		Iterators(inputs).Close()
		return nil, err
	} else if input == nil {
		input = &nilFloatIterator{}
	}

	// Filter out duplicate rows, if required.
	if opt.Dedupe {
		// If there is no group by and it is a float iterator, see if we can use a fast dedupe.
		if itr, ok := input.(FloatIterator); ok && len(opt.Dimensions) == 0 {
			if sz := len(opt.Aux); sz > 0 && sz < 3 {
				input = newFloatFastDedupeIterator(itr)
			} else {
				input = NewDedupeIterator(itr)
			}
		} else {
			input = NewDedupeIterator(input)
		}
	}
	// Apply limit & offset.
	if opt.Limit > 0 || opt.Offset > 0 {
		input = NewLimitIterator(input, opt)
	}
	return input, nil
}

func buildFieldIterator(ctx context.Context, expr influxql.Expr, ic IteratorCreator, sources influxql.Sources, opt IteratorOptions, selector, writeMode bool) (Iterator, error) {
	span := tracing.SpanFromContext(ctx)
	if span != nil {
		span = span.StartSpan("iterator_scanner")
		defer span.Finish()

		labels := []string{"expr", expr.String()}
		if len(opt.Aux) > 0 {
			auxFieldNames := make([]string, len(opt.Aux))
			for i, ref := range opt.Aux {
				auxFieldNames[i] = ref.String()
			}
			labels = append(labels, "auxiliary_fields", strings.Join(auxFieldNames, ", "))
		}
		span.SetLabels(labels...)
		ctx = tracing.NewContextWithSpan(ctx, span)
	}

	input, err := buildExprIterator(ctx, expr, ic, sources, opt, selector, writeMode)
	if err != nil {
		return nil, err
	}

	// Apply limit & offset.
	if opt.Limit > 0 || opt.Offset > 0 {
		input = NewLimitIterator(input, opt)
	}
	return input, nil
}

// valueMapper influxql.SelectStatement.Fields 部分的值映射
// 也就是projection部分：
//   select sum("field0"), sum("field1") 中的"field0"、"field1"部分
//   select "host", "value" 中的"host", "value"部分
type valueMapper struct {
	// An index that maps a node's string output to its symbol so that all
	// nodes with the same signature are mapped the same.
	// 输入输出符号映射，按照map<influxql.Expr.String(), influxql.VarRef>方式映射，key为输入列，value为输出列描述
	// 两种情况：
	// 1. 没有group by时，influxql.Expr 为 influxql.VarRef 是按照 "{Val}::{Type}"方式拼接，示例：host::tag, value::float
	// 2. 存在group by时，influxql.Expr 为 influxql.Call 是按照 "FunctionName({Val}::{Type})"方式拼接，示例：sum(value::float)
	symbols map[string]influxql.VarRef
	// An index that maps a specific expression to a symbol. This ensures that
	// only expressions that were mapped get symbolized.
	// 输入输出映射，即原始输入列 influxql.Expr 到输出列 influxql.VarRef 的映射，
	// 两种情况：
	// 1. 没有group by时，即select "host", "value"形式，influxql.Expr 为 influxql.VarRef，所以key、value是一致都是 influxql.VarRef 类型
	// 2. 存在group by时，即select sum("value") 形式，influxql.Expr 为 influxql.Call，而sum("value")结果为float类型，
	//    所以value的 influxql.VarRef 应该为Float类型，而命名则是按val+seq顺序命名
	table map[influxql.Expr]influxql.VarRef
	// A collection of all of the calls in the table.
	// 当 influxql.Expr 为 influxql.Call 时，把 influxql.Expr 转换为 influxql.Call 保存在该属性中
	calls map[*influxql.Call]struct{}
	// A collection of all of the calls in the table.
	// 当 influxql.Expr 为 influxql.VarRef 时，把 influxql.Expr 转换为 influxql.VarRef 保存在该属性中
	refs map[*influxql.VarRef]struct{}
	i    int
}

func newValueMapper() *valueMapper {
	return &valueMapper{
		symbols: make(map[string]influxql.VarRef),
		table:   make(map[influxql.Expr]influxql.VarRef),
		calls:   make(map[*influxql.Call]struct{}),
		refs:    make(map[*influxql.VarRef]struct{}),
	}
}

func (v *valueMapper) Map(field *influxql.Field) *influxql.Field {
	clone := *field
	clone.Expr = influxql.CloneExpr(field.Expr)

	influxql.Walk(v, clone.Expr)
	// 根据输入field，查找对应输出列的influxql.VarRef，并替换作为输出field
	clone.Expr = influxql.RewriteExpr(clone.Expr, v.rewriteExpr)
	return &clone
}

func (v *valueMapper) Visit(n influxql.Node) influxql.Visitor {
	expr, ok := n.(influxql.Expr)
	if !ok {
		return v
	}

	key := expr.String()
	symbol, ok := v.symbols[key]
	if !ok {
		// This symbol has not been assigned yet.
		// If this is a call or expression, mark the node
		// as stored in the symbol table.
		switch n := n.(type) {
		case *influxql.Call:
			if isMathFunction(n) {
				return v
			}
			v.calls[n] = struct{}{}
		case *influxql.VarRef:
			v.refs[n] = struct{}{}
		default:
			return v
		}

		// Determine the symbol name and the symbol type.
		symbolName := fmt.Sprintf("val%d", v.i)
		valuer := influxql.TypeValuerEval{
			TypeMapper: DefaultTypeMapper,
		}
		typ, _ := valuer.EvalType(expr)

		symbol = influxql.VarRef{
			Val:  symbolName,
			Type: typ,
		}

		// Assign this symbol to the symbol table if it is not presently there
		// and increment the value index number.
		v.symbols[key] = symbol
		v.i++
	}
	// Store the symbol for this expression so we can later rewrite
	// the query correctly.
	v.table[expr] = symbol
	return nil
}

func (v *valueMapper) rewriteExpr(expr influxql.Expr) influxql.Expr {
	symbol, ok := v.table[expr]
	if !ok {
		return expr
	}
	return &symbol
}

// validateTypes 对类型进行验证（字段类型、函数参数返回值等）
func validateTypes(stmt *influxql.SelectStatement) error {
	valuer := influxql.TypeValuerEval{
		TypeMapper: influxql.MultiTypeMapper(
			FunctionTypeMapper{},
			MathTypeMapper{},
		),
	}
	for _, f := range stmt.Fields {
		if _, err := valuer.EvalType(f.Expr); err != nil {
			return err
		}
	}
	return nil
}

// hasValidType returns true if there is at least one non-unknown type
// in the slice.
func hasValidType(refs []influxql.VarRef) bool {
	for _, ref := range refs {
		if ref.Type != influxql.Unknown {
			return true
		}
	}
	return false
}
