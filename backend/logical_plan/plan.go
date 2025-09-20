package logical_plan

import (
	"fmt"
	"strings"
)

type NodeType string

const (
	NodeTypeScan      NodeType = "scan"
	NodeTypeFilter    NodeType = "filter"
	NodeTypeProject   NodeType = "project"
	NodeTypeJoin      NodeType = "join"
	NodeTypeAggregate NodeType = "aggregate"
	NodeTypeSort      NodeType = "sort"
	NodeTypeLimit     NodeType = "limit"
	NodeTypeUnion     NodeType = "union"
	NodeTypeSubquery  NodeType = "subquery"
)

type JoinType string

const (
	JoinTypeInner JoinType = "inner"
	JoinTypeLeft  JoinType = "left"
	JoinTypeRight JoinType = "right"
	JoinTypeFull  JoinType = "full"
	JoinTypeCross JoinType = "cross"
)

type AggregateType string

const (
	AggregateCount AggregateType = "count"
	AggregateSum   AggregateType = "sum"
	AggregateAvg   AggregateType = "avg"
	AggregateMin   AggregateType = "min"
	AggregateMax   AggregateType = "max"
)

type Expression struct {
	Type     string       `json:"type"`
	Value    interface{}  `json:"value"`
	Left     *Expression  `json:"left,omitempty"`
	Right    *Expression  `json:"right,omitempty"`
	Args     []Expression `json:"args,omitempty"`
	DataType string       `json:"data_type,omitempty"`
}

type Column struct {
	Table string `json:"table,omitempty"`
	Name  string `json:"name"`
	Alias string `json:"alias,omitempty"`
}

type Predicate struct {
	Expression *Expression `json:"expression"`
}

type JoinCondition struct {
	Left     *Expression `json:"left"`
	Right    *Expression `json:"right"`
	Operator string      `json:"operator"`
}

type AggregateFunction struct {
	Type   AggregateType `json:"type"`
	Column *Expression   `json:"column,omitempty"`
	Alias  string        `json:"alias,omitempty"`
}

type OrderBy struct {
	Expression *Expression `json:"expression"`
	Ascending  bool        `json:"ascending"`
}

type LogicalPlan struct {
	ID       string         `json:"id"`
	NodeType NodeType       `json:"node_type"`
	Children []*LogicalPlan `json:"children,omitempty"`

	TableName string `json:"table_name,omitempty"`
	Alias     string `json:"alias,omitempty"`

	Predicate *Predicate `json:"predicate,omitempty"`

	Projections []Column `json:"projections,omitempty"`

	JoinType      JoinType       `json:"join_type,omitempty"`
	JoinCondition *JoinCondition `json:"join_condition,omitempty"`

	GroupBy    []Column            `json:"group_by,omitempty"`
	Aggregates []AggregateFunction `json:"aggregates,omitempty"`

	OrderBy []OrderBy `json:"order_by,omitempty"`

	LimitCount  *int64 `json:"limit_count,omitempty"`
	OffsetCount *int64 `json:"offset_count,omitempty"`

	EstimatedRows *int64   `json:"estimated_rows,omitempty"`
	EstimatedCost *float64 `json:"estimated_cost,omitempty"`

	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

func NewScanNode(tableName, alias string) *LogicalPlan {
	return &LogicalPlan{
		ID:        generateID(),
		NodeType:  NodeTypeScan,
		TableName: tableName,
		Alias:     alias,
		Metadata:  make(map[string]interface{}),
	}
}

func NewFilterNode(child *LogicalPlan, predicate *Predicate) *LogicalPlan {
	return &LogicalPlan{
		ID:        generateID(),
		NodeType:  NodeTypeFilter,
		Children:  []*LogicalPlan{child},
		Predicate: predicate,
		Metadata:  make(map[string]interface{}),
	}
}

func NewProjectNode(child *LogicalPlan, projections []Column) *LogicalPlan {
	return &LogicalPlan{
		ID:          generateID(),
		NodeType:    NodeTypeProject,
		Children:    []*LogicalPlan{child},
		Projections: projections,
		Metadata:    make(map[string]interface{}),
	}
}

func NewJoinNode(left, right *LogicalPlan, joinType JoinType, condition *JoinCondition) *LogicalPlan {
	return &LogicalPlan{
		ID:            generateID(),
		NodeType:      NodeTypeJoin,
		Children:      []*LogicalPlan{left, right},
		JoinType:      joinType,
		JoinCondition: condition,
		Metadata:      make(map[string]interface{}),
	}
}

func NewAggregateNode(child *LogicalPlan, groupBy []Column, aggregates []AggregateFunction) *LogicalPlan {
	return &LogicalPlan{
		ID:         generateID(),
		NodeType:   NodeTypeAggregate,
		Children:   []*LogicalPlan{child},
		GroupBy:    groupBy,
		Aggregates: aggregates,
		Metadata:   make(map[string]interface{}),
	}
}

func NewSortNode(child *LogicalPlan, orderBy []OrderBy) *LogicalPlan {
	return &LogicalPlan{
		ID:       generateID(),
		NodeType: NodeTypeSort,
		Children: []*LogicalPlan{child},
		OrderBy:  orderBy,
		Metadata: make(map[string]interface{}),
	}
}

func NewLimitNode(child *LogicalPlan, limit *int64, offset *int64) *LogicalPlan {
	return &LogicalPlan{
		ID:          generateID(),
		NodeType:    NodeTypeLimit,
		Children:    []*LogicalPlan{child},
		LimitCount:  limit,
		OffsetCount: offset,
		Metadata:    make(map[string]interface{}),
	}
}

func (lp *LogicalPlan) Clone() *LogicalPlan {
	clone := &LogicalPlan{
		ID:       generateID(),
		NodeType: lp.NodeType,

		TableName: lp.TableName,
		Alias:     lp.Alias,
		JoinType:  lp.JoinType,

		Projections: make([]Column, len(lp.Projections)),
		GroupBy:     make([]Column, len(lp.GroupBy)),
		Aggregates:  make([]AggregateFunction, len(lp.Aggregates)),
		OrderBy:     make([]OrderBy, len(lp.OrderBy)),

		LimitCount:    lp.LimitCount,
		OffsetCount:   lp.OffsetCount,
		EstimatedRows: lp.EstimatedRows,
		EstimatedCost: lp.EstimatedCost,

		Predicate:     clonePredicate(lp.Predicate),
		JoinCondition: cloneJoinCondition(lp.JoinCondition),

		Metadata: make(map[string]interface{}),
	}

	copy(clone.Projections, lp.Projections)
	copy(clone.GroupBy, lp.GroupBy)
	copy(clone.Aggregates, lp.Aggregates)
	copy(clone.OrderBy, lp.OrderBy)

	for k, v := range lp.Metadata {
		clone.Metadata[k] = v
	}

	clone.Children = make([]*LogicalPlan, len(lp.Children))
	for i, child := range lp.Children {
		clone.Children[i] = child.Clone()
	}

	return clone
}

func (lp *LogicalPlan) String() string {
	return lp.toStringWithIndent(0)
}

func (lp *LogicalPlan) toStringWithIndent(indent int) string {
	indentStr := strings.Repeat("  ", indent)
	var result strings.Builder

	result.WriteString(fmt.Sprintf("%s%s", indentStr, string(lp.NodeType)))

	switch lp.NodeType {
	case NodeTypeScan:
		result.WriteString(fmt.Sprintf(" [table=%s", lp.TableName))
		if lp.Alias != "" {
			result.WriteString(fmt.Sprintf(" as %s", lp.Alias))
		}
		result.WriteString("]")
	case NodeTypeFilter:
		result.WriteString(" [predicate=...]")
	case NodeTypeProject:
		result.WriteString(fmt.Sprintf(" [columns=%d]", len(lp.Projections)))
	case NodeTypeJoin:
		result.WriteString(fmt.Sprintf(" [type=%s]", string(lp.JoinType)))
	case NodeTypeAggregate:
		result.WriteString(fmt.Sprintf(" [groupBy=%d, aggregates=%d]", len(lp.GroupBy), len(lp.Aggregates)))
	case NodeTypeSort:
		result.WriteString(fmt.Sprintf(" [orderBy=%d]", len(lp.OrderBy)))
	case NodeTypeLimit:
		if lp.LimitCount != nil {
			result.WriteString(fmt.Sprintf(" [limit=%d", *lp.LimitCount))
			if lp.OffsetCount != nil {
				result.WriteString(fmt.Sprintf(", offset=%d", *lp.OffsetCount))
			}
			result.WriteString("]")
		}
	}

	if lp.EstimatedRows != nil || lp.EstimatedCost != nil {
		result.WriteString(" [")
		if lp.EstimatedRows != nil {
			result.WriteString(fmt.Sprintf("rows=%d", *lp.EstimatedRows))
		}
		if lp.EstimatedCost != nil {
			if lp.EstimatedRows != nil {
				result.WriteString(", ")
			}
			result.WriteString(fmt.Sprintf("cost=%.2f", *lp.EstimatedCost))
		}
		result.WriteString("]")
	}

	for _, child := range lp.Children {
		result.WriteString("\n")
		result.WriteString(child.toStringWithIndent(indent + 1))
	}

	return result.String()
}

type PlanVisitor interface {
	VisitScan(*LogicalPlan) error
	VisitFilter(*LogicalPlan) error
	VisitProject(*LogicalPlan) error
	VisitJoin(*LogicalPlan) error
	VisitAggregate(*LogicalPlan) error
	VisitSort(*LogicalPlan) error
	VisitLimit(*LogicalPlan) error
	VisitUnion(*LogicalPlan) error
	VisitSubquery(*LogicalPlan) error
}

func (lp *LogicalPlan) Accept(visitor PlanVisitor) error {
	var err error

	switch lp.NodeType {
	case NodeTypeScan:
		err = visitor.VisitScan(lp)
	case NodeTypeFilter:
		err = visitor.VisitFilter(lp)
	case NodeTypeProject:
		err = visitor.VisitProject(lp)
	case NodeTypeJoin:
		err = visitor.VisitJoin(lp)
	case NodeTypeAggregate:
		err = visitor.VisitAggregate(lp)
	case NodeTypeSort:
		err = visitor.VisitSort(lp)
	case NodeTypeLimit:
		err = visitor.VisitLimit(lp)
	case NodeTypeUnion:
		err = visitor.VisitUnion(lp)
	case NodeTypeSubquery:
		err = visitor.VisitSubquery(lp)
	}

	if err != nil {
		return err
	}

	for _, child := range lp.Children {
		if err := child.Accept(visitor); err != nil {
			return err
		}
	}

	return nil
}

func clonePredicate(p *Predicate) *Predicate {
	if p == nil {
		return nil
	}
	return &Predicate{
		Expression: cloneExpression(p.Expression),
	}
}

func cloneJoinCondition(jc *JoinCondition) *JoinCondition {
	if jc == nil {
		return nil
	}
	return &JoinCondition{
		Left:     cloneExpression(jc.Left),
		Right:    cloneExpression(jc.Right),
		Operator: jc.Operator,
	}
}

func cloneExpression(e *Expression) *Expression {
	if e == nil {
		return nil
	}

	clone := &Expression{
		Type:     e.Type,
		Value:    e.Value,
		DataType: e.DataType,
		Left:     cloneExpression(e.Left),
		Right:    cloneExpression(e.Right),
	}

	if e.Args != nil {
		clone.Args = make([]Expression, len(e.Args))
		for i, arg := range e.Args {
			clone.Args[i] = *cloneExpression(&arg)
		}
	}

	return clone
}

var idCounter int64

func generateID() string {
	idCounter++
	return fmt.Sprintf("node_%d", idCounter)
}

func NewColumnExpression(table, column string) *Expression {
	value := column
	if table != "" {
		value = table + "." + column
	}
	return &Expression{
		Type:  "column",
		Value: value,
	}
}

func NewLiteralExpression(value interface{}) *Expression {
	return &Expression{
		Type:  "literal",
		Value: value,
	}
}

func NewBinaryOpExpression(operator string, left, right *Expression) *Expression {
	return &Expression{
		Type:  "binary_op",
		Value: operator,
		Left:  left,
		Right: right,
	}
}

func NewFunctionExpression(funcName string, args []Expression) *Expression {
	return &Expression{
		Type:  "function",
		Value: funcName,
		Args:  args,
	}
}
