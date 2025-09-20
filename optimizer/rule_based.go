package optimizer

import (
	"fmt"

	"retr0-kernel/optiquery/logical_plan"
)

type OptimizationRule interface {
	Apply(plan *logical_plan.LogicalPlan) (*logical_plan.LogicalPlan, bool, error)
	Name() string
}

type ExplainResult struct {
	AppliedRules []string               `json:"applied_rules"`
	Steps        []OptimizationStep     `json:"steps"`
	Statistics   OptimizationStatistics `json:"statistics"`
}

type OptimizationStep struct {
	RuleName    string                    `json:"rule_name"`
	BeforePlan  *logical_plan.LogicalPlan `json:"before_plan"`
	AfterPlan   *logical_plan.LogicalPlan `json:"after_plan"`
	Description string                    `json:"description"`
}

type OptimizationStatistics struct {
	TotalRulesApplied    int     `json:"total_rules_applied"`
	EstimatedImprovement float64 `json:"estimated_improvement"`
}

type RuleBasedOptimizer struct {
	rules []OptimizationRule
}

func NewRuleBasedOptimizer() *RuleBasedOptimizer {
	return &RuleBasedOptimizer{
		rules: []OptimizationRule{
			&PredicatePushdownRule{},
			&ProjectionPushdownRule{},
			&ConstantFoldingRule{},
			&JoinReorderingRule{},
		},
	}
}

func OptimizeWithRules(plan *logical_plan.LogicalPlan) (*logical_plan.LogicalPlan, *ExplainResult, error) {
	optimizer := NewRuleBasedOptimizer()
	return optimizer.Optimize(plan)
}

func (rbo *RuleBasedOptimizer) Optimize(plan *logical_plan.LogicalPlan) (*logical_plan.LogicalPlan, *ExplainResult, error) {
	if plan == nil {
		return nil, nil, fmt.Errorf("cannot optimize nil plan")
	}

	explain := &ExplainResult{
		AppliedRules: []string{},
		Steps:        []OptimizationStep{},
		Statistics:   OptimizationStatistics{},
	}

	currentPlan := plan.Clone()
	totalRulesApplied := 0

	maxIterations := 10
	for iteration := 0; iteration < maxIterations; iteration++ {
		changed := false

		for _, rule := range rbo.rules {
			beforePlan := currentPlan.Clone()
			optimizedPlan, ruleApplied, err := rule.Apply(currentPlan)
			if err != nil {
				return nil, explain, fmt.Errorf("error applying rule %s: %w", rule.Name(), err)
			}

			if ruleApplied {
				explain.AppliedRules = append(explain.AppliedRules, rule.Name())
				explain.Steps = append(explain.Steps, OptimizationStep{
					RuleName:    rule.Name(),
					BeforePlan:  beforePlan,
					AfterPlan:   optimizedPlan,
					Description: fmt.Sprintf("Applied %s rule", rule.Name()),
				})

				currentPlan = optimizedPlan
				totalRulesApplied++
				changed = true
			}
		}

		if !changed {
			break
		}
	}

	explain.Statistics.TotalRulesApplied = totalRulesApplied
	return currentPlan, explain, nil
}

type PredicatePushdownRule struct{}

func (r *PredicatePushdownRule) Name() string {
	return "PredicatePushdown"
}

func (r *PredicatePushdownRule) Apply(plan *logical_plan.LogicalPlan) (*logical_plan.LogicalPlan, bool, error) {
	return r.applyRecursive(plan)
}

func (r *PredicatePushdownRule) applyRecursive(plan *logical_plan.LogicalPlan) (*logical_plan.LogicalPlan, bool, error) {
	if plan == nil {
		return nil, false, nil
	}

	changed := false

	if plan.NodeType == logical_plan.NodeTypeFilter && len(plan.Children) == 1 {
		child := plan.Children[0]

		switch child.NodeType {
		case logical_plan.NodeTypeProject:

			if canPushFilterBelowProject(plan.Predicate, child) {

				newFilter := logical_plan.NewFilterNode(child.Children[0], plan.Predicate)
				newProject := logical_plan.NewProjectNode(newFilter, child.Projections)
				plan = newProject
				changed = true
			}
		case logical_plan.NodeTypeJoin:

			leftPushable, rightPushable := canPushFilterToJoinSides(plan.Predicate, child)
			if leftPushable || rightPushable {

				changed = true
			}
		}
	}

	for i, child := range plan.Children {
		optimizedChild, childChanged, err := r.applyRecursive(child)
		if err != nil {
			return nil, false, err
		}
		if childChanged {
			plan.Children[i] = optimizedChild
			changed = true
		}
	}

	return plan, changed, nil
}

type ProjectionPushdownRule struct{}

func (r *ProjectionPushdownRule) Name() string {
	return "ProjectionPushdown"
}

func (r *ProjectionPushdownRule) Apply(plan *logical_plan.LogicalPlan) (*logical_plan.LogicalPlan, bool, error) {
	return r.applyRecursive(plan)
}

func (r *ProjectionPushdownRule) applyRecursive(plan *logical_plan.LogicalPlan) (*logical_plan.LogicalPlan, bool, error) {
	if plan == nil {
		return nil, false, nil
	}

	changed := false

	if plan.NodeType == logical_plan.NodeTypeProject && len(plan.Children) == 1 {
		child := plan.Children[0]

		if isRedundantProjection(plan.Projections) {
			plan = child
			changed = true
		}
	}

	for i, child := range plan.Children {
		optimizedChild, childChanged, err := r.applyRecursive(child)
		if err != nil {
			return nil, false, err
		}
		if childChanged {
			plan.Children[i] = optimizedChild
			changed = true
		}
	}

	return plan, changed, nil
}

type ConstantFoldingRule struct{}

func (r *ConstantFoldingRule) Name() string {
	return "ConstantFolding"
}

func (r *ConstantFoldingRule) Apply(plan *logical_plan.LogicalPlan) (*logical_plan.LogicalPlan, bool, error) {

	return plan, false, nil
}

type JoinReorderingRule struct{}

func (r *JoinReorderingRule) Name() string {
	return "JoinReordering"
}

func (r *JoinReorderingRule) Apply(plan *logical_plan.LogicalPlan) (*logical_plan.LogicalPlan, bool, error) {

	return plan, false, nil
}

func canPushFilterBelowProject(predicate *logical_plan.Predicate, projectNode *logical_plan.LogicalPlan) bool {

	return true
}

func canPushFilterToJoinSides(predicate *logical_plan.Predicate, joinNode *logical_plan.LogicalPlan) (bool, bool) {

	return false, false
}

func isRedundantProjection(projections []logical_plan.Column) bool {
	return len(projections) == 1 && projections[0].Name == "*"
}
