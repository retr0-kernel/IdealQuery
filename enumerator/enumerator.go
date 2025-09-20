package enumerator

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"retr0-kernel/optiquery/catalog"
	"retr0-kernel/optiquery/cost_model"
	"retr0-kernel/optiquery/logical_plan"
)

type PlanEnumerator struct {
	costModel  cost_model.CostModel
	catalogMgr *catalog.CatalogManager
	maxPlans   int
}

func NewPlanEnumerator(catalogMgr *catalog.CatalogManager) *PlanEnumerator {
	return &PlanEnumerator{
		costModel:  cost_model.NewSimpleCostModel(),
		catalogMgr: catalogMgr,
		maxPlans:   1000,
	}
}

type EnumerationResult struct {
	BestPlan     *logical_plan.LogicalPlan   `json:"best_plan"`
	AllPlans     []*logical_plan.LogicalPlan `json:"all_plans"`
	PlanCount    int                         `json:"plan_count"`
	BestCost     float64                     `json:"best_cost"`
	EnumStrategy string                      `json:"enum_strategy"`
	SearchSpace  int                         `json:"search_space_size"`
	PruningStats PruningStatistics           `json:"pruning_stats"`
}

type PruningStatistics struct {
	PlansGenerated int `json:"plans_generated"`
	PlansPruned    int `json:"plans_pruned"`
	PlansEvaluated int `json:"plans_evaluated"`
}

type TableInfo struct {
	Name               string
	Cardinality        int64
	SelectivityFactors map[string]float64
}

type JoinGraph struct {
	Tables     []TableInfo
	Edges      []JoinEdge
	Predicates map[string]*logical_plan.Predicate
}

type JoinEdge struct {
	Left        string
	Right       string
	Selectivity float64
	JoinType    logical_plan.JoinType
	Condition   *logical_plan.JoinCondition
}

func (pe *PlanEnumerator) EnumeratePlans(plan *logical_plan.LogicalPlan) (*EnumerationResult, error) {
	if plan == nil {
		return nil, fmt.Errorf("cannot enumerate plans for nil plan")
	}

	tables := pe.extractTables(plan)

	if len(tables) <= 1 {

		alternatives := pe.generateSingleTableAlternatives(plan)
		return pe.selectBestPlan(append([]*logical_plan.LogicalPlan{plan}, alternatives...), "single_table")
	} else if len(tables) <= 4 {

		return pe.enumerateWithDP(plan, tables)
	} else {

		return pe.enumerateWithGreedy(plan, tables)
	}
}

func (pe *PlanEnumerator) enumerateWithDP(plan *logical_plan.LogicalPlan, tables []string) (*EnumerationResult, error) {

	joinGraph := pe.buildJoinGraph(plan, tables)

	plans := pe.generateDPJoinOrders(joinGraph, plan)

	allPlans := []*logical_plan.LogicalPlan{}
	for _, logicalPlan := range plans {
		physicalAlternatives := pe.generatePhysicalAlternatives(logicalPlan)
		allPlans = append(allPlans, physicalAlternatives...)
	}

	return pe.selectBestPlan(allPlans, "dynamic_programming")
}

func (pe *PlanEnumerator) generateDPJoinOrders(joinGraph *JoinGraph, originalPlan *logical_plan.LogicalPlan) []*logical_plan.LogicalPlan {
	tables := make([]string, len(joinGraph.Tables))
	for i, table := range joinGraph.Tables {
		tables[i] = table.Name
	}

	n := len(tables)
	if n <= 1 {
		return []*logical_plan.LogicalPlan{originalPlan}
	}

	dp := make(map[int]*logical_plan.LogicalPlan)

	for i := 0; i < n; i++ {
		mask := 1 << i
		dp[mask] = logical_plan.NewScanNode(tables[i], "")
	}

	for size := 2; size <= n; size++ {

		subsets := pe.generateSubsets(n, size)

		for _, subset := range subsets {
			bestPlan := pe.findBestJoinForSubset(subset, dp, joinGraph, tables)
			if bestPlan != nil {
				dp[subset] = bestPlan
			}
		}
	}

	plans := []*logical_plan.LogicalPlan{}
	fullMask := (1 << n) - 1

	if finalPlan, exists := dp[fullMask]; exists {
		plans = append(plans, finalPlan)

		for mask, plan := range dp {
			if popcount(mask) >= n-1 {
				plans = append(plans, plan)
			}
		}
	}

	return pe.removeDuplicatePlans(plans)
}

func (pe *PlanEnumerator) findBestJoinForSubset(subset int, dp map[int]*logical_plan.LogicalPlan, joinGraph *JoinGraph, tables []string) *logical_plan.LogicalPlan {
	var bestPlan *logical_plan.LogicalPlan
	bestCost := math.Inf(1)

	for leftMask := subset; leftMask > 0; leftMask = (leftMask - 1) & subset {
		rightMask := subset ^ leftMask

		if rightMask == 0 || leftMask == subset {
			continue
		}

		leftPlan, leftExists := dp[leftMask]
		rightPlan, rightExists := dp[rightMask]

		if !leftExists || !rightExists {
			continue
		}

		joinEdge := pe.findJoinEdge(leftMask, rightMask, joinGraph, tables)
		if joinEdge == nil {
			continue
		}

		joinPlan := logical_plan.NewJoinNode(leftPlan, rightPlan, joinEdge.JoinType, joinEdge.Condition)

		cost, err := pe.costModel.EstimateCost(joinPlan, pe.catalogMgr)
		if err != nil {
			continue
		}

		if cost.TotalCost < bestCost {
			bestCost = cost.TotalCost
			bestPlan = joinPlan
		}

		swappedJoin := logical_plan.NewJoinNode(rightPlan, leftPlan, joinEdge.JoinType, pe.swapJoinCondition(joinEdge.Condition))
		swappedCost, err := pe.costModel.EstimateCost(swappedJoin, pe.catalogMgr)
		if err == nil && swappedCost.TotalCost < bestCost {
			bestCost = swappedCost.TotalCost
			bestPlan = swappedJoin
		}
	}

	return bestPlan
}

func (pe *PlanEnumerator) enumerateWithGreedy(plan *logical_plan.LogicalPlan, tables []string) (*EnumerationResult, error) {
	joinGraph := pe.buildJoinGraph(plan, tables)

	plans := []*logical_plan.LogicalPlan{}

	sizeBasedPlan := pe.createSizeBasedJoinOrder(joinGraph)
	if sizeBasedPlan != nil {
		plans = append(plans, sizeBasedPlan)
	}

	selectivityBasedPlan := pe.createSelectivityBasedJoinOrder(joinGraph)
	if selectivityBasedPlan != nil {
		plans = append(plans, selectivityBasedPlan)
	}

	cardinalityBasedPlan := pe.createCardinalityBasedJoinOrder(joinGraph)
	if cardinalityBasedPlan != nil {
		plans = append(plans, cardinalityBasedPlan)
	}

	mixedPlan := pe.createMixedHeuristicJoinOrder(joinGraph)
	if mixedPlan != nil {
		plans = append(plans, mixedPlan)
	}

	allPlans := []*logical_plan.LogicalPlan{}
	for _, logicalPlan := range plans {
		physicalAlternatives := pe.generatePhysicalAlternatives(logicalPlan)
		allPlans = append(allPlans, physicalAlternatives...)
	}

	return pe.selectBestPlan(allPlans, "greedy")
}

func (pe *PlanEnumerator) buildJoinGraph(plan *logical_plan.LogicalPlan, tables []string) *JoinGraph {
	joinGraph := &JoinGraph{
		Tables:     make([]TableInfo, 0, len(tables)),
		Edges:      make([]JoinEdge, 0),
		Predicates: make(map[string]*logical_plan.Predicate),
	}

	for _, tableName := range tables {
		tableInfo := TableInfo{
			Name:               tableName,
			Cardinality:        pe.getTableCardinality(tableName),
			SelectivityFactors: make(map[string]float64),
		}
		joinGraph.Tables = append(joinGraph.Tables, tableInfo)
	}

	pe.extractJoinConditions(plan, joinGraph)

	return joinGraph
}

func (pe *PlanEnumerator) extractJoinConditions(plan *logical_plan.LogicalPlan, joinGraph *JoinGraph) {
	if plan == nil {
		return
	}

	if plan.NodeType == logical_plan.NodeTypeJoin && plan.JoinCondition != nil {

		leftTable := pe.extractTableFromExpression(plan.JoinCondition.Left)
		rightTable := pe.extractTableFromExpression(plan.JoinCondition.Right)

		if leftTable != "" && rightTable != "" {
			edge := JoinEdge{
				Left:        leftTable,
				Right:       rightTable,
				Selectivity: pe.estimateJoinSelectivity(plan.JoinCondition),
				JoinType:    plan.JoinType,
				Condition:   plan.JoinCondition,
			}
			joinGraph.Edges = append(joinGraph.Edges, edge)
		}
	}

	for _, child := range plan.Children {
		pe.extractJoinConditions(child, joinGraph)
	}
}

func (pe *PlanEnumerator) createSelectivityBasedJoinOrder(joinGraph *JoinGraph) *logical_plan.LogicalPlan {
	if len(joinGraph.Tables) < 2 {
		return nil
	}

	edges := make([]JoinEdge, len(joinGraph.Edges))
	copy(edges, joinGraph.Edges)

	sort.Slice(edges, func(i, j int) bool {
		return edges[i].Selectivity < edges[j].Selectivity
	})

	usedTables := make(map[string]bool)
	var currentPlan *logical_plan.LogicalPlan

	for _, edge := range edges {
		leftPlan := pe.getOrCreateTablePlan(edge.Left, usedTables)
		rightPlan := pe.getOrCreateTablePlan(edge.Right, usedTables)

		if currentPlan == nil {
			currentPlan = logical_plan.NewJoinNode(leftPlan, rightPlan, edge.JoinType, edge.Condition)
		} else {

			if pe.planContainsTable(currentPlan, edge.Left) && !pe.planContainsTable(currentPlan, edge.Right) {
				currentPlan = logical_plan.NewJoinNode(currentPlan, rightPlan, edge.JoinType, edge.Condition)
			} else if pe.planContainsTable(currentPlan, edge.Right) && !pe.planContainsTable(currentPlan, edge.Left) {
				currentPlan = logical_plan.NewJoinNode(currentPlan, leftPlan, edge.JoinType, edge.Condition)
			} else if !pe.planContainsTable(currentPlan, edge.Left) && !pe.planContainsTable(currentPlan, edge.Right) {

				newJoin := logical_plan.NewJoinNode(leftPlan, rightPlan, edge.JoinType, edge.Condition)
				currentPlan = logical_plan.NewJoinNode(currentPlan, newJoin, logical_plan.JoinTypeInner, pe.createDefaultJoinCondition(edge.Left, edge.Right))
			}
		}

		usedTables[edge.Left] = true
		usedTables[edge.Right] = true
	}

	for _, table := range joinGraph.Tables {
		if !usedTables[table.Name] {
			tablePlan := logical_plan.NewScanNode(table.Name, "")
			if currentPlan == nil {
				currentPlan = tablePlan
			} else {
				currentPlan = logical_plan.NewJoinNode(currentPlan, tablePlan, logical_plan.JoinTypeInner, pe.createDefaultJoinCondition("", table.Name))
			}
		}
	}

	return currentPlan
}

func (pe *PlanEnumerator) createCardinalityBasedJoinOrder(joinGraph *JoinGraph) *logical_plan.LogicalPlan {
	if len(joinGraph.Tables) < 2 {
		return nil
	}

	tables := make([]TableInfo, len(joinGraph.Tables))
	copy(tables, joinGraph.Tables)

	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Cardinality < tables[j].Cardinality
	})

	var currentPlan *logical_plan.LogicalPlan

	for i, table := range tables {
		tablePlan := logical_plan.NewScanNode(table.Name, "")

		if i == 0 {
			currentPlan = tablePlan
		} else {

			joinCondition := pe.findJoinConditionForTables(currentPlan, table.Name, joinGraph)
			if joinCondition == nil {
				joinCondition = pe.createDefaultJoinCondition("", table.Name)
			}

			currentPlan = logical_plan.NewJoinNode(currentPlan, tablePlan, logical_plan.JoinTypeInner, joinCondition)
		}
	}

	return currentPlan
}

func (pe *PlanEnumerator) createMixedHeuristicJoinOrder(joinGraph *JoinGraph) *logical_plan.LogicalPlan {
	if len(joinGraph.Tables) < 2 {
		return nil
	}

	type TableScore struct {
		Table TableInfo
		Score float64
	}

	scores := make([]TableScore, len(joinGraph.Tables))

	for i, table := range joinGraph.Tables {
		score := 0.0

		if table.Cardinality > 0 {
			score += 1000000.0 / float64(table.Cardinality)
		}

		joinCount := pe.countJoinsForTable(table.Name, joinGraph)
		score += float64(joinCount * 100)

		avgSelectivity := pe.averageSelectivityForTable(table.Name, joinGraph)
		score += (1.0 - avgSelectivity) * 500

		scores[i] = TableScore{Table: table, Score: score}
	}

	sort.Slice(scores, func(i, j int) bool {
		return scores[i].Score > scores[j].Score
	})

	var currentPlan *logical_plan.LogicalPlan

	for i, scored := range scores {
		tablePlan := logical_plan.NewScanNode(scored.Table.Name, "")

		if i == 0 {
			currentPlan = tablePlan
		} else {
			joinCondition := pe.findJoinConditionForTables(currentPlan, scored.Table.Name, joinGraph)
			if joinCondition == nil {
				joinCondition = pe.createDefaultJoinCondition("", scored.Table.Name)
			}

			currentPlan = logical_plan.NewJoinNode(currentPlan, tablePlan, logical_plan.JoinTypeInner, joinCondition)
		}
	}

	return currentPlan
}

func (pe *PlanEnumerator) createSizeBasedJoinOrder(joinGraph *JoinGraph) *logical_plan.LogicalPlan {
	return pe.createCardinalityBasedJoinOrder(joinGraph)
}

func (pe *PlanEnumerator) getTableCardinality(tableName string) int64 {
	table, err := pe.catalogMgr.GetTable(tableName)
	if err != nil {
		return 1000
	}
	return table.RowCount
}

func (pe *PlanEnumerator) extractTableFromExpression(expr *logical_plan.Expression) string {
	if expr == nil || expr.Type != "column" {
		return ""
	}

	if value, ok := expr.Value.(string); ok {
		parts := strings.Split(value, ".")
		if len(parts) == 2 {
			return parts[0]
		}
	}

	return ""
}

func (pe *PlanEnumerator) estimateJoinSelectivity(condition *logical_plan.JoinCondition) float64 {
	if condition == nil {
		return 0.1
	}

	switch condition.Operator {
	case "=":
		return 0.1
	case "<", ">", "<=", ">=":
		return 0.33
	default:
		return 0.5
	}
}

func (pe *PlanEnumerator) generateSubsets(n, size int) []int {
	var subsets []int
	pe.generateSubsetsRecursive(0, n, size, 0, &subsets)
	return subsets
}

func (pe *PlanEnumerator) generateSubsetsRecursive(start, n, size, current int, subsets *[]int) {
	if size == 0 {
		*subsets = append(*subsets, current)
		return
	}

	for i := start; i < n; i++ {
		pe.generateSubsetsRecursive(i+1, n, size-1, current|(1<<i), subsets)
	}
}

func popcount(x int) int {
	count := 0
	for x != 0 {
		count++
		x &= x - 1
	}
	return count
}

func (pe *PlanEnumerator) findJoinEdge(leftMask, rightMask int, joinGraph *JoinGraph, tables []string) *JoinEdge {
	leftTables := pe.maskToTables(leftMask, tables)
	rightTables := pe.maskToTables(rightMask, tables)

	for _, edge := range joinGraph.Edges {
		if (contains(leftTables, edge.Left) && contains(rightTables, edge.Right)) ||
			(contains(leftTables, edge.Right) && contains(rightTables, edge.Left)) {
			return &edge
		}
	}

	return nil
}

func (pe *PlanEnumerator) maskToTables(mask int, tables []string) []string {
	var result []string
	for i := 0; i < len(tables); i++ {
		if mask&(1<<i) != 0 {
			result = append(result, tables[i])
		}
	}
	return result
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func (pe *PlanEnumerator) swapJoinCondition(condition *logical_plan.JoinCondition) *logical_plan.JoinCondition {
	if condition == nil {
		return nil
	}

	return &logical_plan.JoinCondition{
		Left:     condition.Right,
		Right:    condition.Left,
		Operator: condition.Operator,
	}
}

func (pe *PlanEnumerator) getOrCreateTablePlan(tableName string, usedTables map[string]bool) *logical_plan.LogicalPlan {
	usedTables[tableName] = true
	return logical_plan.NewScanNode(tableName, "")
}

func (pe *PlanEnumerator) planContainsTable(plan *logical_plan.LogicalPlan, tableName string) bool {
	if plan == nil {
		return false
	}

	if plan.NodeType == logical_plan.NodeTypeScan && plan.TableName == tableName {
		return true
	}

	for _, child := range plan.Children {
		if pe.planContainsTable(child, tableName) {
			return true
		}
	}

	return false
}

func (pe *PlanEnumerator) createDefaultJoinCondition(leftTable, rightTable string) *logical_plan.JoinCondition {

	leftCol := "id"
	rightCol := "id"

	if leftTable != "" {
		leftCol = leftTable + "_id"
	}
	if rightTable != "" {
		rightCol = rightTable + "_id"
	}

	return &logical_plan.JoinCondition{
		Left:     logical_plan.NewColumnExpression(leftTable, leftCol),
		Right:    logical_plan.NewColumnExpression(rightTable, rightCol),
		Operator: "=",
	}
}

func (pe *PlanEnumerator) findJoinConditionForTables(plan *logical_plan.LogicalPlan, tableName string, joinGraph *JoinGraph) *logical_plan.JoinCondition {

	for _, edge := range joinGraph.Edges {
		if edge.Left == tableName || edge.Right == tableName {
			return edge.Condition
		}
	}
	return nil
}

func (pe *PlanEnumerator) countJoinsForTable(tableName string, joinGraph *JoinGraph) int {
	count := 0
	for _, edge := range joinGraph.Edges {
		if edge.Left == tableName || edge.Right == tableName {
			count++
		}
	}
	return count
}

func (pe *PlanEnumerator) averageSelectivityForTable(tableName string, joinGraph *JoinGraph) float64 {
	totalSelectivity := 0.0
	count := 0

	for _, edge := range joinGraph.Edges {
		if edge.Left == tableName || edge.Right == tableName {
			totalSelectivity += edge.Selectivity
			count++
		}
	}

	if count == 0 {
		return 0.5
	}

	return totalSelectivity / float64(count)
}

func (pe *PlanEnumerator) removeDuplicatePlans(plans []*logical_plan.LogicalPlan) []*logical_plan.LogicalPlan {
	seen := make(map[string]bool)
	unique := []*logical_plan.LogicalPlan{}

	for _, plan := range plans {
		signature := pe.getPlanSignature(plan)
		if !seen[signature] {
			seen[signature] = true
			unique = append(unique, plan)
		}
	}

	return unique
}

func (pe *PlanEnumerator) getPlanSignature(plan *logical_plan.LogicalPlan) string {
	if plan == nil {
		return "nil"
	}

	signature := string(plan.NodeType)
	if plan.TableName != "" {
		signature += ":" + plan.TableName
	}

	for _, child := range plan.Children {
		signature += "(" + pe.getPlanSignature(child) + ")"
	}

	return signature
}

func (pe *PlanEnumerator) generateSingleTableAlternatives(plan *logical_plan.LogicalPlan) []*logical_plan.LogicalPlan {
	return pe.generatePhysicalAlternatives(plan)
}

func (pe *PlanEnumerator) selectBestPlan(plans []*logical_plan.LogicalPlan, strategy string) (*EnumerationResult, error) {
	if len(plans) == 0 {
		return nil, fmt.Errorf("no plans to evaluate")
	}

	var bestPlan *logical_plan.LogicalPlan
	bestCost := math.Inf(1)
	evaluatedCount := 0

	for _, plan := range plans {
		if evaluatedCount >= pe.maxPlans {
			break
		}

		cost, err := pe.costModel.EstimateCost(plan, pe.catalogMgr)
		if err != nil {
			continue
		}

		if cost.TotalCost < bestCost {
			bestCost = cost.TotalCost
			bestPlan = plan
		}
		evaluatedCount++
	}

	if bestPlan == nil {
		return nil, fmt.Errorf("no valid plan found")
	}

	return &EnumerationResult{
		BestPlan:     bestPlan,
		AllPlans:     plans,
		PlanCount:    len(plans),
		BestCost:     bestCost,
		EnumStrategy: strategy,
		SearchSpace:  len(plans),
		PruningStats: PruningStatistics{
			PlansGenerated: len(plans),
			PlansPruned:    0,
			PlansEvaluated: evaluatedCount,
		},
	}, nil
}

func (pe *PlanEnumerator) generatePhysicalAlternatives(plan *logical_plan.LogicalPlan) []*logical_plan.LogicalPlan {
	var alternatives []*logical_plan.LogicalPlan

	if plan == nil {
		return alternatives
	}

	alternatives = append(alternatives, plan)

	planCopy := plan.Clone()

	switch plan.NodeType {
	case logical_plan.NodeTypeJoin:

		hashJoinPlan := planCopy.Clone()
		if hashJoinPlan.Metadata == nil {
			hashJoinPlan.Metadata = make(map[string]interface{})
		}
		hashJoinPlan.Metadata["physical_operator"] = "hash_join"
		alternatives = append(alternatives, hashJoinPlan)

		sortMergeJoinPlan := planCopy.Clone()
		if sortMergeJoinPlan.Metadata == nil {
			sortMergeJoinPlan.Metadata = make(map[string]interface{})
		}
		sortMergeJoinPlan.Metadata["physical_operator"] = "sort_merge_join"
		alternatives = append(alternatives, sortMergeJoinPlan)

		nestedLoopJoinPlan := planCopy.Clone()
		if nestedLoopJoinPlan.Metadata == nil {
			nestedLoopJoinPlan.Metadata = make(map[string]interface{})
		}
		nestedLoopJoinPlan.Metadata["physical_operator"] = "nested_loop_join"
		alternatives = append(alternatives, nestedLoopJoinPlan)

	case logical_plan.NodeTypeAggregate:

		hashAggPlan := planCopy.Clone()
		if hashAggPlan.Metadata == nil {
			hashAggPlan.Metadata = make(map[string]interface{})
		}
		hashAggPlan.Metadata["physical_operator"] = "hash_aggregate"
		alternatives = append(alternatives, hashAggPlan)

		sortAggPlan := planCopy.Clone()
		if sortAggPlan.Metadata == nil {
			sortAggPlan.Metadata = make(map[string]interface{})
		}
		sortAggPlan.Metadata["physical_operator"] = "sort_aggregate"
		alternatives = append(alternatives, sortAggPlan)

	case logical_plan.NodeTypeSort:

		quicksortPlan := planCopy.Clone()
		if quicksortPlan.Metadata == nil {
			quicksortPlan.Metadata = make(map[string]interface{})
		}
		quicksortPlan.Metadata["physical_operator"] = "quicksort"
		alternatives = append(alternatives, quicksortPlan)

		externalSortPlan := planCopy.Clone()
		if externalSortPlan.Metadata == nil {
			externalSortPlan.Metadata = make(map[string]interface{})
		}
		externalSortPlan.Metadata["physical_operator"] = "external_sort"
		alternatives = append(alternatives, externalSortPlan)

	case logical_plan.NodeTypeScan:

		seqScanPlan := planCopy.Clone()
		if seqScanPlan.Metadata == nil {
			seqScanPlan.Metadata = make(map[string]interface{})
		}
		seqScanPlan.Metadata["scan_type"] = "sequential"
		alternatives = append(alternatives, seqScanPlan)

		table, err := pe.catalogMgr.GetTable(plan.TableName)
		if err == nil && len(table.Indexes) > 0 {
			indexScanPlan := planCopy.Clone()
			if indexScanPlan.Metadata == nil {
				indexScanPlan.Metadata = make(map[string]interface{})
			}
			indexScanPlan.Metadata["scan_type"] = "index"
			indexScanPlan.Metadata["index_name"] = table.Indexes[0].Name
			alternatives = append(alternatives, indexScanPlan)
		}
	}

	for i, child := range plan.Children {
		childAlternatives := pe.generatePhysicalAlternatives(child)

		for _, childAlt := range childAlternatives {
			if childAlt != child {
				for _, baseAlt := range alternatives {
					newPlan := baseAlt.Clone()
					if i < len(newPlan.Children) {
						newPlan.Children[i] = childAlt
						alternatives = append(alternatives, newPlan)
					}
				}
			}
		}
	}

	return pe.removeDuplicatePlans(alternatives)
}

func (pe *PlanEnumerator) extractJoinNodes(plan *logical_plan.LogicalPlan) []*logical_plan.LogicalPlan {
	var joins []*logical_plan.LogicalPlan

	if plan == nil {
		return joins
	}

	if plan.NodeType == logical_plan.NodeTypeJoin {
		joins = append(joins, plan)
	}

	for _, child := range plan.Children {
		childJoins := pe.extractJoinNodes(child)
		joins = append(joins, childJoins...)
	}

	return joins
}

func (pe *PlanEnumerator) extractTables(plan *logical_plan.LogicalPlan) []string {
	var tables []string
	seen := make(map[string]bool)

	pe.extractTablesRecursive(plan, &tables, seen)
	return tables
}

func (pe *PlanEnumerator) extractTablesRecursive(plan *logical_plan.LogicalPlan, tables *[]string, seen map[string]bool) {
	if plan == nil {
		return
	}

	if plan.NodeType == logical_plan.NodeTypeScan && plan.TableName != "" {
		if !seen[plan.TableName] {
			*tables = append(*tables, plan.TableName)
			seen[plan.TableName] = true
		}
	}

	for _, child := range plan.Children {
		pe.extractTablesRecursive(child, tables, seen)
	}
}
