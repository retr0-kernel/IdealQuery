package cost_model

import (
	"math"

	"retr0-kernel/optiquery/catalog"
	"retr0-kernel/optiquery/logical_plan"
)

type CostModel interface {
	EstimateCost(plan *logical_plan.LogicalPlan, catalog *catalog.CatalogManager) (*CostEstimate, error)
	EstimateCardinality(plan *logical_plan.LogicalPlan, catalog *catalog.CatalogManager) (int64, error)
}

type CostEstimate struct {
	TotalCost   float64 `json:"total_cost"`
	CPUCost     float64 `json:"cpu_cost"`
	IOCost      float64 `json:"io_cost"`
	NetworkCost float64 `json:"network_cost"`
	MemoryCost  float64 `json:"memory_cost"`
	Cardinality int64   `json:"cardinality"`
}

type SimpleCostModel struct {
	SeqScanCostPerPage    float64
	RandomScanCostPerPage float64
	CPUCostPerTuple       float64
	JoinCostFactor        float64
	SortCostFactor        float64
	HashCostFactor        float64
}

func NewSimpleCostModel() *SimpleCostModel {
	return &SimpleCostModel{
		SeqScanCostPerPage:    1.0,
		RandomScanCostPerPage: 4.0,
		CPUCostPerTuple:       0.01,
		JoinCostFactor:        1.5,
		SortCostFactor:        2.0,
		HashCostFactor:        1.2,
	}
}

func (cm *SimpleCostModel) EstimateCost(plan *logical_plan.LogicalPlan, catalogMgr *catalog.CatalogManager) (*CostEstimate, error) {
	if plan == nil {
		return &CostEstimate{}, nil
	}

	switch plan.NodeType {
	case logical_plan.NodeTypeScan:
		return cm.estimateScanCost(plan, catalogMgr)
	case logical_plan.NodeTypeFilter:
		return cm.estimateFilterCost(plan, catalogMgr)
	case logical_plan.NodeTypeProject:
		return cm.estimateProjectCost(plan, catalogMgr)
	case logical_plan.NodeTypeJoin:
		return cm.estimateJoinCost(plan, catalogMgr)
	case logical_plan.NodeTypeAggregate:
		return cm.estimateAggregateCost(plan, catalogMgr)
	case logical_plan.NodeTypeSort:
		return cm.estimateSortCost(plan, catalogMgr)
	case logical_plan.NodeTypeLimit:
		return cm.estimateLimitCost(plan, catalogMgr)
	default:

		cardinality, _ := cm.EstimateCardinality(plan, catalogMgr)
		return &CostEstimate{
			TotalCost:   float64(cardinality) * cm.CPUCostPerTuple,
			CPUCost:     float64(cardinality) * cm.CPUCostPerTuple,
			Cardinality: cardinality,
		}, nil
	}
}

func (cm *SimpleCostModel) EstimateCardinality(plan *logical_plan.LogicalPlan, catalogMgr *catalog.CatalogManager) (int64, error) {
	if plan == nil {
		return 0, nil
	}

	switch plan.NodeType {
	case logical_plan.NodeTypeScan:
		table, err := catalogMgr.GetTable(plan.TableName)
		if err != nil {
			return 1000, nil
		}
		return table.RowCount, nil

	case logical_plan.NodeTypeFilter:
		if len(plan.Children) == 0 {
			return 0, nil
		}
		childCardinality, err := cm.EstimateCardinality(plan.Children[0], catalogMgr)
		if err != nil {
			return 0, err
		}

		selectivity := cm.estimateSelectivity(plan.Predicate, catalogMgr)
		return int64(float64(childCardinality) * selectivity), nil

	case logical_plan.NodeTypeProject:
		if len(plan.Children) == 0 {
			return 0, nil
		}
		return cm.EstimateCardinality(plan.Children[0], catalogMgr)

	case logical_plan.NodeTypeJoin:
		if len(plan.Children) < 2 {
			return 0, nil
		}
		leftCard, err := cm.EstimateCardinality(plan.Children[0], catalogMgr)
		if err != nil {
			return 0, err
		}
		rightCard, err := cm.EstimateCardinality(plan.Children[1], catalogMgr)
		if err != nil {
			return 0, err
		}

		switch plan.JoinType {
		case logical_plan.JoinTypeCross:
			return leftCard * rightCard, nil
		case logical_plan.JoinTypeInner:

			return int64(float64(leftCard*rightCard) * 0.1), nil
		case logical_plan.JoinTypeLeft:
			return leftCard, nil
		case logical_plan.JoinTypeRight:
			return rightCard, nil
		case logical_plan.JoinTypeFull:
			return leftCard + rightCard, nil
		default:
			return int64(float64(leftCard*rightCard) * 0.1), nil
		}

	case logical_plan.NodeTypeAggregate:
		if len(plan.Children) == 0 {
			return 1, nil
		}
		childCard, err := cm.EstimateCardinality(plan.Children[0], catalogMgr)
		if err != nil {
			return 0, err
		}

		if len(plan.GroupBy) == 0 {
			return 1, nil
		}

		return int64(float64(childCard) * 0.1), nil

	case logical_plan.NodeTypeSort:
		if len(plan.Children) == 0 {
			return 0, nil
		}
		return cm.EstimateCardinality(plan.Children[0], catalogMgr)

	case logical_plan.NodeTypeLimit:
		if plan.LimitCount != nil {
			childCard, err := cm.EstimateCardinality(plan.Children[0], catalogMgr)
			if err != nil {
				return 0, err
			}
			limit := *plan.LimitCount
			if childCard < limit {
				return childCard, nil
			}
			return limit, nil
		}
		if len(plan.Children) == 0 {
			return 0, nil
		}
		return cm.EstimateCardinality(plan.Children[0], catalogMgr)

	default:
		return 1000, nil
	}
}

func (cm *SimpleCostModel) estimateScanCost(plan *logical_plan.LogicalPlan, catalogMgr *catalog.CatalogManager) (*CostEstimate, error) {
	table, err := catalogMgr.GetTable(plan.TableName)
	if err != nil {

		return &CostEstimate{
			TotalCost:   1000.0,
			IOCost:      800.0,
			CPUCost:     200.0,
			Cardinality: 1000,
		}, nil
	}

	pages := float64(table.RowCount) / 100.0
	if pages < 1 {
		pages = 1
	}

	ioCost := pages * cm.SeqScanCostPerPage
	cpuCost := float64(table.RowCount) * cm.CPUCostPerTuple

	return &CostEstimate{
		TotalCost:   ioCost + cpuCost,
		IOCost:      ioCost,
		CPUCost:     cpuCost,
		Cardinality: table.RowCount,
	}, nil
}

func (cm *SimpleCostModel) estimateFilterCost(plan *logical_plan.LogicalPlan, catalogMgr *catalog.CatalogManager) (*CostEstimate, error) {
	if len(plan.Children) == 0 {
		return &CostEstimate{}, nil
	}

	childCost, err := cm.EstimateCost(plan.Children[0], catalogMgr)
	if err != nil {
		return nil, err
	}

	selectivity := cm.estimateSelectivity(plan.Predicate, catalogMgr)
	outputCardinality := int64(float64(childCost.Cardinality) * selectivity)

	filterCpuCost := float64(childCost.Cardinality) * cm.CPUCostPerTuple * 0.5

	return &CostEstimate{
		TotalCost:   childCost.TotalCost + filterCpuCost,
		CPUCost:     childCost.CPUCost + filterCpuCost,
		IOCost:      childCost.IOCost,
		NetworkCost: childCost.NetworkCost,
		MemoryCost:  childCost.MemoryCost,
		Cardinality: outputCardinality,
	}, nil
}

func (cm *SimpleCostModel) estimateProjectCost(plan *logical_plan.LogicalPlan, catalogMgr *catalog.CatalogManager) (*CostEstimate, error) {
	if len(plan.Children) == 0 {
		return &CostEstimate{}, nil
	}

	childCost, err := cm.EstimateCost(plan.Children[0], catalogMgr)
	if err != nil {
		return nil, err
	}

	projectionCpuCost := float64(childCost.Cardinality) * cm.CPUCostPerTuple * 0.1

	return &CostEstimate{
		TotalCost:   childCost.TotalCost + projectionCpuCost,
		CPUCost:     childCost.CPUCost + projectionCpuCost,
		IOCost:      childCost.IOCost,
		NetworkCost: childCost.NetworkCost,
		MemoryCost:  childCost.MemoryCost,
		Cardinality: childCost.Cardinality,
	}, nil
}

func (cm *SimpleCostModel) estimateJoinCost(plan *logical_plan.LogicalPlan, catalogMgr *catalog.CatalogManager) (*CostEstimate, error) {
	if len(plan.Children) < 2 {
		return &CostEstimate{}, nil
	}

	leftCost, err := cm.EstimateCost(plan.Children[0], catalogMgr)
	if err != nil {
		return nil, err
	}

	rightCost, err := cm.EstimateCost(plan.Children[1], catalogMgr)
	if err != nil {
		return nil, err
	}

	joinCpuCost := float64(leftCost.Cardinality*rightCost.Cardinality) * cm.CPUCostPerTuple * cm.JoinCostFactor

	outputCardinality, _ := cm.EstimateCardinality(plan, catalogMgr)

	return &CostEstimate{
		TotalCost:   leftCost.TotalCost + rightCost.TotalCost + joinCpuCost,
		CPUCost:     leftCost.CPUCost + rightCost.CPUCost + joinCpuCost,
		IOCost:      leftCost.IOCost + rightCost.IOCost,
		NetworkCost: leftCost.NetworkCost + rightCost.NetworkCost,
		MemoryCost:  leftCost.MemoryCost + rightCost.MemoryCost,
		Cardinality: outputCardinality,
	}, nil
}

func (cm *SimpleCostModel) estimateAggregateCost(plan *logical_plan.LogicalPlan, catalogMgr *catalog.CatalogManager) (*CostEstimate, error) {
	if len(plan.Children) == 0 {
		return &CostEstimate{}, nil
	}

	childCost, err := cm.EstimateCost(plan.Children[0], catalogMgr)
	if err != nil {
		return nil, err
	}

	aggCpuCost := float64(childCost.Cardinality) * cm.CPUCostPerTuple * cm.HashCostFactor
	outputCardinality, _ := cm.EstimateCardinality(plan, catalogMgr)

	return &CostEstimate{
		TotalCost:   childCost.TotalCost + aggCpuCost,
		CPUCost:     childCost.CPUCost + aggCpuCost,
		IOCost:      childCost.IOCost,
		NetworkCost: childCost.NetworkCost,
		MemoryCost:  childCost.MemoryCost + float64(childCost.Cardinality)*0.1,
		Cardinality: outputCardinality,
	}, nil
}

func (cm *SimpleCostModel) estimateSortCost(plan *logical_plan.LogicalPlan, catalogMgr *catalog.CatalogManager) (*CostEstimate, error) {
	if len(plan.Children) == 0 {
		return &CostEstimate{}, nil
	}

	childCost, err := cm.EstimateCost(plan.Children[0], catalogMgr)
	if err != nil {
		return nil, err
	}

	if childCost.Cardinality <= 1 {
		return childCost, nil
	}

	sortCpuCost := float64(childCost.Cardinality) * math.Log2(float64(childCost.Cardinality)) * cm.CPUCostPerTuple * cm.SortCostFactor

	return &CostEstimate{
		TotalCost:   childCost.TotalCost + sortCpuCost,
		CPUCost:     childCost.CPUCost + sortCpuCost,
		IOCost:      childCost.IOCost,
		NetworkCost: childCost.NetworkCost,
		MemoryCost:  childCost.MemoryCost + float64(childCost.Cardinality)*0.2,
		Cardinality: childCost.Cardinality,
	}, nil
}

func (cm *SimpleCostModel) estimateLimitCost(plan *logical_plan.LogicalPlan, catalogMgr *catalog.CatalogManager) (*CostEstimate, error) {
	if len(plan.Children) == 0 {
		return &CostEstimate{}, nil
	}

	childCost, err := cm.EstimateCost(plan.Children[0], catalogMgr)
	if err != nil {
		return nil, err
	}

	outputCardinality, _ := cm.EstimateCardinality(plan, catalogMgr)

	if plan.LimitCount != nil && *plan.LimitCount < childCost.Cardinality {
		reductionFactor := float64(*plan.LimitCount) / float64(childCost.Cardinality)
		return &CostEstimate{
			TotalCost:   childCost.TotalCost * reductionFactor,
			CPUCost:     childCost.CPUCost * reductionFactor,
			IOCost:      childCost.IOCost * reductionFactor,
			NetworkCost: childCost.NetworkCost * reductionFactor,
			MemoryCost:  childCost.MemoryCost * reductionFactor,
			Cardinality: outputCardinality,
		}, nil
	}

	return childCost, nil
}

func (cm *SimpleCostModel) estimateSelectivity(predicate *logical_plan.Predicate, catalogMgr *catalog.CatalogManager) float64 {
	if predicate == nil || predicate.Expression == nil {
		return 1.0
	}

	expr := predicate.Expression
	switch expr.Value {
	case "=":
		return 0.1
	case "<", ">", "<=", ">=":
		return 0.33
	case "LIKE":
		return 0.2
	case "IN":
		return 0.3
	case "IS NULL":
		return 0.05
	case "IS NOT NULL":
		return 0.95
	default:
		return 0.5
	}
}
