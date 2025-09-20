package optimizer

import (
	"fmt"

	"retr0-kernel/optiquery/catalog"
	"retr0-kernel/optiquery/cost_model"
	"retr0-kernel/optiquery/logical_plan"
)

type CostBasedOptimizer struct {
	costModel  cost_model.CostModel
	catalogMgr *catalog.CatalogManager
}

func NewCostBasedOptimizer(catalogMgr *catalog.CatalogManager) *CostBasedOptimizer {
	return &CostBasedOptimizer{
		costModel:  cost_model.NewSimpleCostModel(),
		catalogMgr: catalogMgr,
	}
}

func OptimizeWithCost(plan *logical_plan.LogicalPlan) (*logical_plan.LogicalPlan, *ExplainResult, error) {

	catalogMgr := catalog.NewCatalogManager()
	optimizer := NewCostBasedOptimizer(catalogMgr)
	return optimizer.Optimize(plan)
}

func (cbo *CostBasedOptimizer) Optimize(plan *logical_plan.LogicalPlan) (*logical_plan.LogicalPlan, *ExplainResult, error) {
	if plan == nil {
		return nil, nil, fmt.Errorf("cannot optimize nil plan")
	}

	explain := &ExplainResult{
		AppliedRules: []string{},
		Steps:        []OptimizationStep{},
		Statistics:   OptimizationStatistics{},
	}

	ruleOptimizedPlan, ruleExplain, err := OptimizeWithRules(plan)
	if err != nil {
		return nil, explain, err
	}

	explain.AppliedRules = append(explain.AppliedRules, ruleExplain.AppliedRules...)
	explain.Steps = append(explain.Steps, ruleExplain.Steps...)

	costOptimizedPlan, err := cbo.applyCostBasedOptimizations(ruleOptimizedPlan)
	if err != nil {
		return nil, explain, err
	}

	finalCost, err := cbo.costModel.EstimateCost(costOptimizedPlan, cbo.catalogMgr)
	if err != nil {
		return nil, explain, err
	}

	cbo.propagateCostEstimates(costOptimizedPlan)

	explain.AppliedRules = append(explain.AppliedRules, "CostBasedOptimization")
	explain.Steps = append(explain.Steps, OptimizationStep{
		RuleName:    "CostBasedOptimization",
		BeforePlan:  ruleOptimizedPlan,
		AfterPlan:   costOptimizedPlan,
		Description: fmt.Sprintf("Applied cost-based optimization (final cost: %.2f)", finalCost.TotalCost),
	})

	explain.Statistics.TotalRulesApplied = len(explain.AppliedRules)
	return costOptimizedPlan, explain, nil
}

func (cbo *CostBasedOptimizer) applyCostBasedOptimizations(plan *logical_plan.LogicalPlan) (*logical_plan.LogicalPlan, error) {
	optimizedPlan := plan.Clone()

	optimizedPlan = cbo.optimizeJoinOrder(optimizedPlan)

	optimizedPlan = cbo.selectPhysicalOperators(optimizedPlan)

	return optimizedPlan, nil
}

func (cbo *CostBasedOptimizer) optimizeJoinOrder(plan *logical_plan.LogicalPlan) *logical_plan.LogicalPlan {
	if plan == nil {
		return nil
	}

	if plan.NodeType == logical_plan.NodeTypeJoin && len(plan.Children) == 2 {
		leftChild := plan.Children[0]
		rightChild := plan.Children[1]

		currentCost, err := cbo.costModel.EstimateCost(plan, cbo.catalogMgr)
		if err != nil {
			return plan
		}

		swappedPlan := logical_plan.NewJoinNode(rightChild, leftChild, plan.JoinType, plan.JoinCondition)
		swappedCost, err := cbo.costModel.EstimateCost(swappedPlan, cbo.catalogMgr)
		if err != nil {
			return plan
		}

		if swappedCost.TotalCost < currentCost.TotalCost {
			plan = swappedPlan
		}
	}

	for i, child := range plan.Children {
		plan.Children[i] = cbo.optimizeJoinOrder(child)
	}

	return plan
}

func (cbo *CostBasedOptimizer) selectPhysicalOperators(plan *logical_plan.LogicalPlan) *logical_plan.LogicalPlan {
	if plan == nil {
		return nil
	}

	switch plan.NodeType {
	case logical_plan.NodeTypeJoin:
		leftCard, _ := cbo.costModel.EstimateCardinality(plan.Children[0], cbo.catalogMgr)
		rightCard, _ := cbo.costModel.EstimateCardinality(plan.Children[1], cbo.catalogMgr)

		if leftCard < 1000 && rightCard < 1000 {
			plan.Metadata["physical_operator"] = "nested_loop_join"
		} else if leftCard < rightCard {

			plan.Metadata["physical_operator"] = "hash_join"
			plan.Metadata["build_side"] = "left"
		} else {
			plan.Metadata["physical_operator"] = "hash_join"
			plan.Metadata["build_side"] = "right"
		}

		if leftCard > 1000000 && rightCard > 1000000 {
			plan.Metadata["physical_operator"] = "sort_merge_join"
		}

	case logical_plan.NodeTypeAggregate:
		cardinality, _ := cbo.costModel.EstimateCardinality(plan, cbo.catalogMgr)
		if len(plan.GroupBy) == 0 {

			plan.Metadata["physical_operator"] = "hash_aggregate"
		} else if cardinality < 10000 {
			plan.Metadata["physical_operator"] = "hash_aggregate"
		} else {

			plan.Metadata["physical_operator"] = "sort_aggregate"
		}

	case logical_plan.NodeTypeSort:
		cardinality, _ := cbo.costModel.EstimateCardinality(plan, cbo.catalogMgr)
		if cardinality < 100000 {
			plan.Metadata["physical_operator"] = "quicksort"
		} else {
			plan.Metadata["physical_operator"] = "external_sort"
		}

	case logical_plan.NodeTypeScan:

		if plan.Metadata == nil {
			plan.Metadata = make(map[string]interface{})
		}
		plan.Metadata["scan_type"] = "sequential"

	}

	for i, child := range plan.Children {
		plan.Children[i] = cbo.selectPhysicalOperators(child)
	}

	return plan
}

func (cbo *CostBasedOptimizer) propagateCostEstimates(plan *logical_plan.LogicalPlan) {
	if plan == nil {
		return
	}

	for _, child := range plan.Children {
		cbo.propagateCostEstimates(child)
	}

	cost, err := cbo.costModel.EstimateCost(plan, cbo.catalogMgr)
	if err == nil {
		plan.EstimatedCost = &cost.TotalCost
		plan.EstimatedRows = &cost.Cardinality
	}
}
