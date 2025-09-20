package simulator

import (
	"fmt"
	"time"

	"retr0-kernel/optiquery/logical_plan"
)

type ExecutionMetrics struct {
	ExecutionTime   time.Duration          `json:"execution_time"`
	RowsProcessed   int64                  `json:"rows_processed"`
	RowsReturned    int64                  `json:"rows_returned"`
	CPUTime         time.Duration          `json:"cpu_time"`
	IOOperations    int64                  `json:"io_operations"`
	MemoryUsed      int64                  `json:"memory_used"`
	NetworkTraffic  int64                  `json:"network_traffic"`
	OperatorMetrics map[string]interface{} `json:"operator_metrics"`
	Connector       string                 `json:"connector"`
	SimulationOnly  bool                   `json:"simulation_only"`
}

type Simulator interface {
	SimulateExecution(plan *logical_plan.LogicalPlan, options map[string]interface{}) (*ExecutionMetrics, error)
}

func SimulateExecution(plan *logical_plan.LogicalPlan, connector string, options map[string]interface{}) (*ExecutionMetrics, error) {
	switch connector {
	case "postgres":
		simulator := NewPostgresSimulator()
		return simulator.SimulateExecution(plan, options)
	case "mongo":
		simulator := NewMongoSimulator()
		return simulator.SimulateExecution(plan, options)
	default:
		simulator := NewGenericSimulator()
		return simulator.SimulateExecution(plan, options)
	}
}

type GenericSimulator struct{}

func NewGenericSimulator() *GenericSimulator {
	return &GenericSimulator{}
}

func (gs *GenericSimulator) SimulateExecution(plan *logical_plan.LogicalPlan, options map[string]interface{}) (*ExecutionMetrics, error) {
	if plan == nil {
		return nil, fmt.Errorf("cannot simulate nil plan")
	}

	startTime := time.Now()

	metrics := &ExecutionMetrics{
		OperatorMetrics: make(map[string]interface{}),
		Connector:       "generic",
		SimulationOnly:  true,
	}

	err := gs.simulateNode(plan, metrics)
	if err != nil {
		return nil, err
	}

	metrics.ExecutionTime = time.Since(startTime)
	return metrics, nil
}

func (gs *GenericSimulator) simulateNode(plan *logical_plan.LogicalPlan, metrics *ExecutionMetrics) error {
	if plan == nil {
		return nil
	}

	for _, child := range plan.Children {
		err := gs.simulateNode(child, metrics)
		if err != nil {
			return err
		}
	}

	switch plan.NodeType {
	case logical_plan.NodeTypeScan:
		return gs.simulateScan(plan, metrics)
	case logical_plan.NodeTypeFilter:
		return gs.simulateFilter(plan, metrics)
	case logical_plan.NodeTypeProject:
		return gs.simulateProject(plan, metrics)
	case logical_plan.NodeTypeJoin:
		return gs.simulateJoin(plan, metrics)
	case logical_plan.NodeTypeAggregate:
		return gs.simulateAggregate(plan, metrics)
	case logical_plan.NodeTypeSort:
		return gs.simulateSort(plan, metrics)
	case logical_plan.NodeTypeLimit:
		return gs.simulateLimit(plan, metrics)
	default:
		return fmt.Errorf("unsupported node type for simulation: %s", plan.NodeType)
	}
}

func (gs *GenericSimulator) simulateScan(plan *logical_plan.LogicalPlan, metrics *ExecutionMetrics) error {

	estimatedRows := int64(1000)
	if plan.EstimatedRows != nil {
		estimatedRows = *plan.EstimatedRows
	}

	pagesRead := estimatedRows / 100
	if pagesRead < 1 {
		pagesRead = 1
	}

	metrics.IOOperations += pagesRead
	metrics.RowsProcessed += estimatedRows
	metrics.RowsReturned += estimatedRows
	metrics.MemoryUsed += estimatedRows * 100

	cpuTime := time.Duration(estimatedRows*10) * time.Microsecond
	metrics.CPUTime += cpuTime

	metrics.OperatorMetrics[plan.ID+"_scan"] = map[string]interface{}{
		"table_name":   plan.TableName,
		"rows_scanned": estimatedRows,
		"pages_read":   pagesRead,
		"scan_type":    "sequential",
	}

	return nil
}

func (gs *GenericSimulator) simulateFilter(plan *logical_plan.LogicalPlan, metrics *ExecutionMetrics) error {
	inputRows := int64(1000)
	if len(plan.Children) > 0 && plan.Children[0].EstimatedRows != nil {
		inputRows = *plan.Children[0].EstimatedRows
	}

	selectivity := 0.3
	outputRows := int64(float64(inputRows) * selectivity)

	metrics.RowsProcessed += inputRows
	metrics.RowsReturned = outputRows

	cpuTime := time.Duration(inputRows*5) * time.Microsecond
	metrics.CPUTime += cpuTime

	metrics.OperatorMetrics[plan.ID+"_filter"] = map[string]interface{}{
		"input_rows":  inputRows,
		"output_rows": outputRows,
		"selectivity": selectivity,
		"predicate":   "simplified_predicate",
	}

	return nil
}

func (gs *GenericSimulator) simulateProject(plan *logical_plan.LogicalPlan, metrics *ExecutionMetrics) error {
	inputRows := int64(1000)
	if len(plan.Children) > 0 && plan.Children[0].EstimatedRows != nil {
		inputRows = *plan.Children[0].EstimatedRows
	}

	metrics.RowsProcessed += inputRows
	metrics.RowsReturned = inputRows

	cpuTime := time.Duration(inputRows*2) * time.Microsecond
	metrics.CPUTime += cpuTime

	metrics.OperatorMetrics[plan.ID+"_project"] = map[string]interface{}{
		"input_rows":        inputRows,
		"output_rows":       inputRows,
		"projected_columns": len(plan.Projections),
	}

	return nil
}

func (gs *GenericSimulator) simulateJoin(plan *logical_plan.LogicalPlan, metrics *ExecutionMetrics) error {
	leftRows := int64(1000)
	rightRows := int64(1000)

	if len(plan.Children) >= 2 {
		if plan.Children[0].EstimatedRows != nil {
			leftRows = *plan.Children[0].EstimatedRows
		}
		if plan.Children[1].EstimatedRows != nil {
			rightRows = *plan.Children[1].EstimatedRows
		}
	}

	joinAlgorithm := "nested_loop"
	if physOp, exists := plan.Metadata["physical_operator"]; exists {
		if alg, ok := physOp.(string); ok {
			joinAlgorithm = alg
		}
	}

	var outputRows int64
	var cpuTime time.Duration
	var memoryUsed int64

	switch joinAlgorithm {
	case "nested_loop_join":

		comparisons := leftRows * rightRows
		cpuTime = time.Duration(comparisons*2) * time.Microsecond
		memoryUsed = leftRows * 100
		outputRows = int64(float64(leftRows*rightRows) * 0.1)

	case "hash_join":

		cpuTime = time.Duration((leftRows+rightRows)*10) * time.Microsecond
		memoryUsed = leftRows * 150
		outputRows = int64(float64(leftRows*rightRows) * 0.1)

	case "sort_merge_join":

		sortTime := time.Duration(leftRows*int64(logBase2(float64(leftRows)))+
			rightRows*int64(logBase2(float64(rightRows)))) * time.Microsecond * 5
		mergeTime := time.Duration((leftRows+rightRows)*5) * time.Microsecond
		cpuTime = sortTime + mergeTime
		memoryUsed = (leftRows + rightRows) * 100
		outputRows = int64(float64(leftRows*rightRows) * 0.1)

	default:

		cpuTime = time.Duration(leftRows*rightRows*2) * time.Microsecond
		memoryUsed = leftRows * 100
		outputRows = int64(float64(leftRows*rightRows) * 0.1)
	}

	metrics.RowsProcessed += leftRows + rightRows
	metrics.RowsReturned = outputRows
	metrics.CPUTime += cpuTime
	metrics.MemoryUsed += memoryUsed

	metrics.OperatorMetrics[plan.ID+"_join"] = map[string]interface{}{
		"left_rows":      leftRows,
		"right_rows":     rightRows,
		"output_rows":    outputRows,
		"join_algorithm": joinAlgorithm,
		"join_type":      string(plan.JoinType),
	}

	return nil
}

func (gs *GenericSimulator) simulateAggregate(plan *logical_plan.LogicalPlan, metrics *ExecutionMetrics) error {
	inputRows := int64(1000)
	if len(plan.Children) > 0 && plan.Children[0].EstimatedRows != nil {
		inputRows = *plan.Children[0].EstimatedRows
	}

	var outputRows int64
	if len(plan.GroupBy) == 0 {

		outputRows = 1
	} else {

		distinctGroups := float64(inputRows)
		for range plan.GroupBy {
			distinctGroups = distinctGroups * 0.7
		}
		outputRows = int64(distinctGroups)
		if outputRows < 1 {
			outputRows = 1
		}
		if outputRows > inputRows {
			outputRows = inputRows
		}
	}

	aggAlgorithm := "hash_aggregate"
	if physOp, exists := plan.Metadata["physical_operator"]; exists {
		if alg, ok := physOp.(string); ok {
			aggAlgorithm = alg
		}
	}

	var cpuTime time.Duration
	var memoryUsed int64

	switch aggAlgorithm {
	case "hash_aggregate":

		cpuTime = time.Duration(inputRows*15) * time.Microsecond
		memoryUsed = outputRows * 200

	case "sort_aggregate":

		sortTime := time.Duration(inputRows*int64(logBase2(float64(inputRows)))*10) * time.Microsecond
		aggTime := time.Duration(inputRows*5) * time.Microsecond
		cpuTime = sortTime + aggTime
		memoryUsed = inputRows * 100

	default:

		cpuTime = time.Duration(inputRows*15) * time.Microsecond
		memoryUsed = outputRows * 200
	}

	metrics.RowsProcessed += inputRows
	metrics.RowsReturned = outputRows
	metrics.CPUTime += cpuTime
	metrics.MemoryUsed += memoryUsed

	metrics.OperatorMetrics[plan.ID+"_aggregate"] = map[string]interface{}{
		"input_rows":          inputRows,
		"output_rows":         outputRows,
		"group_by_columns":    len(plan.GroupBy),
		"aggregate_functions": len(plan.Aggregates),
		"algorithm":           aggAlgorithm,
	}

	return nil
}

func (gs *GenericSimulator) simulateSort(plan *logical_plan.LogicalPlan, metrics *ExecutionMetrics) error {
	inputRows := int64(1000)
	if len(plan.Children) > 0 && plan.Children[0].EstimatedRows != nil {
		inputRows = *plan.Children[0].EstimatedRows
	}

	sortAlgorithm := "quicksort"
	if physOp, exists := plan.Metadata["physical_operator"]; exists {
		if alg, ok := physOp.(string); ok {
			sortAlgorithm = alg
		}
	} else {

		if inputRows > 100000 {
			sortAlgorithm = "external_sort"
		}
	}

	var cpuTime time.Duration
	var memoryUsed int64
	var ioOperations int64

	switch sortAlgorithm {
	case "quicksort":

		cpuTime = time.Duration(inputRows*int64(logBase2(float64(inputRows)))*20) * time.Microsecond
		memoryUsed = inputRows * 150
		ioOperations = 0

	case "external_sort":

		runSize := int64(10000)
		runs := (inputRows + runSize - 1) / runSize

		sortRunsTime := time.Duration(runs*runSize*int64(logBase2(float64(runSize)))*10) * time.Microsecond

		mergeTime := time.Duration(inputRows*int64(logBase2(float64(runs)))*5) * time.Microsecond

		cpuTime = sortRunsTime + mergeTime
		memoryUsed = runSize * 150
		ioOperations = inputRows * 3 / 100

	case "heapsort":

		cpuTime = time.Duration(inputRows*int64(logBase2(float64(inputRows)))*25) * time.Microsecond
		memoryUsed = inputRows * 120
		ioOperations = 0

	default:

		cpuTime = time.Duration(inputRows*int64(logBase2(float64(inputRows)))*20) * time.Microsecond
		memoryUsed = inputRows * 150
		ioOperations = 0
	}

	metrics.RowsProcessed += inputRows
	metrics.RowsReturned = inputRows
	metrics.CPUTime += cpuTime
	metrics.MemoryUsed += memoryUsed
	metrics.IOOperations += ioOperations

	metrics.OperatorMetrics[plan.ID+"_sort"] = map[string]interface{}{
		"input_rows":   inputRows,
		"output_rows":  inputRows,
		"sort_columns": len(plan.OrderBy),
		"algorithm":    sortAlgorithm,
		"runs_created": (inputRows + 9999) / 10000,
	}

	return nil
}

func (gs *GenericSimulator) simulateLimit(plan *logical_plan.LogicalPlan, metrics *ExecutionMetrics) error {
	inputRows := int64(1000)
	if len(plan.Children) > 0 && plan.Children[0].EstimatedRows != nil {
		inputRows = *plan.Children[0].EstimatedRows
	}

	outputRows := inputRows
	if plan.LimitCount != nil {
		limit := *plan.LimitCount
		if plan.OffsetCount != nil {
			offset := *plan.OffsetCount

			processedRows := offset + limit
			if processedRows > inputRows {
				processedRows = inputRows
			}
			if processedRows > offset {
				outputRows = processedRows - offset
			} else {
				outputRows = 0
			}
			metrics.RowsProcessed += processedRows
		} else {

			if limit < inputRows {
				outputRows = limit
				metrics.RowsProcessed += limit
			} else {
				metrics.RowsProcessed += inputRows
			}
		}
	} else {
		metrics.RowsProcessed += inputRows
	}

	cpuTime := time.Duration(metrics.RowsProcessed*1) * time.Microsecond
	metrics.CPUTime += cpuTime
	metrics.RowsReturned = outputRows

	metrics.OperatorMetrics[plan.ID+"_limit"] = map[string]interface{}{
		"input_rows":        inputRows,
		"output_rows":       outputRows,
		"limit":             plan.LimitCount,
		"offset":            plan.OffsetCount,
		"early_termination": outputRows < inputRows,
	}

	return nil
}

type PostgresSimulator struct {
	GenericSimulator
}

func NewPostgresSimulator() *PostgresSimulator {
	return &PostgresSimulator{}
}

func (ps *PostgresSimulator) SimulateExecution(plan *logical_plan.LogicalPlan, options map[string]interface{}) (*ExecutionMetrics, error) {
	metrics, err := ps.GenericSimulator.SimulateExecution(plan, options)
	if err != nil {
		return nil, err
	}

	metrics.Connector = "postgres"

	ps.applyPostgresOptimizations(plan, metrics)

	return metrics, nil
}

func (ps *PostgresSimulator) applyPostgresOptimizations(plan *logical_plan.LogicalPlan, metrics *ExecutionMetrics) {

	if plan == nil {
		return
	}

	switch plan.NodeType {
	case logical_plan.NodeTypeJoin:

		if physOp, exists := plan.Metadata["physical_operator"]; exists {
			if alg, ok := physOp.(string); ok && alg == "hash_join" {

				metrics.CPUTime = time.Duration(float64(metrics.CPUTime) * 0.85)
			}
		}

	case logical_plan.NodeTypeAggregate:

		metrics.CPUTime = time.Duration(float64(metrics.CPUTime) * 0.9)

	case logical_plan.NodeTypeScan:

		metrics.IOOperations = int64(float64(metrics.IOOperations) * 0.8)
	}

	for _, child := range plan.Children {
		ps.applyPostgresOptimizations(child, metrics)
	}
}

type MongoSimulator struct {
	GenericSimulator
}

func NewMongoSimulator() *MongoSimulator {
	return &MongoSimulator{}
}

func (ms *MongoSimulator) SimulateExecution(plan *logical_plan.LogicalPlan, options map[string]interface{}) (*ExecutionMetrics, error) {
	metrics, err := ms.GenericSimulator.SimulateExecution(plan, options)
	if err != nil {
		return nil, err
	}

	metrics.Connector = "mongo"

	ms.applyMongoOptimizations(plan, metrics)

	return metrics, nil
}

func (ms *MongoSimulator) applyMongoOptimizations(plan *logical_plan.LogicalPlan, metrics *ExecutionMetrics) {
	if plan == nil {
		return
	}

	switch plan.NodeType {
	case logical_plan.NodeTypeScan:

		metrics.NetworkTraffic += metrics.RowsProcessed * 300

	case logical_plan.NodeTypeAggregate:

		metrics.CPUTime = time.Duration(float64(metrics.CPUTime) * 0.7)

	case logical_plan.NodeTypeJoin:

		metrics.CPUTime = time.Duration(float64(metrics.CPUTime) * 1.3)
		metrics.NetworkTraffic += metrics.RowsProcessed * 200
	}

	for _, child := range plan.Children {
		ms.applyMongoOptimizations(child, metrics)
	}
}

func logBase2(x float64) float64 {
	if x <= 1 {
		return 1
	}
	result := 0.0
	for x > 1 {
		x /= 2
		result++
	}
	return result
}
