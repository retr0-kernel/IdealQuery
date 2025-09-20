package parser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"retr0-kernel/optiquery/logical_plan"
)

type SQLParser struct {
	tokens []string
	pos    int
}

func ParseSQL(query string) (*logical_plan.LogicalPlan, error) {
	parser := &SQLParser{}
	return parser.Parse(query)
}

func ParseMongo(query string) (*logical_plan.LogicalPlan, error) {

	return nil, fmt.Errorf("MongoDB parsing not yet implemented")
}

func ParseAthena(query string) (*logical_plan.LogicalPlan, error) {

	return nil, fmt.Errorf("Athena parsing not yet implemented")
}

func (p *SQLParser) Parse(query string) (*logical_plan.LogicalPlan, error) {

	p.tokens = tokenize(query)
	p.pos = 0

	if len(p.tokens) == 0 {
		return nil, fmt.Errorf("empty query")
	}

	switch strings.ToUpper(p.tokens[0]) {
	case "SELECT":
		return p.parseSelect()
	default:
		return nil, fmt.Errorf("unsupported query type: %s", p.tokens[0])
	}
}

func (p *SQLParser) parseSelect() (*logical_plan.LogicalPlan, error) {
	if !p.consumeToken("SELECT") {
		return nil, fmt.Errorf("expected SELECT")
	}

	projections, err := p.parseProjections()
	if err != nil {
		return nil, err
	}

	if !p.consumeToken("FROM") {
		return nil, fmt.Errorf("expected FROM")
	}

	fromPlan, err := p.parseFromClause()
	if err != nil {
		return nil, err
	}

	currentPlan := fromPlan

	if p.peekToken() != "" && strings.ToUpper(p.peekToken()) == "WHERE" {
		p.consumeToken("WHERE")
		predicate, err := p.parsePredicate()
		if err != nil {
			return nil, err
		}
		currentPlan = logical_plan.NewFilterNode(currentPlan, predicate)
	}

	if p.peekToken() != "" && strings.ToUpper(p.peekToken()) == "GROUP" {
		if p.consumeToken("GROUP") && p.consumeToken("BY") {
			groupBy, aggregates, err := p.parseGroupBy(projections)
			if err != nil {
				return nil, err
			}
			currentPlan = logical_plan.NewAggregateNode(currentPlan, groupBy, aggregates)
		}
	}

	if p.peekToken() != "" && strings.ToUpper(p.peekToken()) == "ORDER" {
		if p.consumeToken("ORDER") && p.consumeToken("BY") {
			orderBy, err := p.parseOrderBy()
			if err != nil {
				return nil, err
			}
			currentPlan = logical_plan.NewSortNode(currentPlan, orderBy)
		}
	}

	if p.peekToken() != "" && strings.ToUpper(p.peekToken()) == "LIMIT" {
		p.consumeToken("LIMIT")
		limit, err := p.parseLimit()
		if err != nil {
			return nil, err
		}
		currentPlan = logical_plan.NewLimitNode(currentPlan, limit, nil)
	}

	if !isSelectAll(projections) {
		currentPlan = logical_plan.NewProjectNode(currentPlan, projections)
	}

	return currentPlan, nil
}

func (p *SQLParser) parseProjections() ([]logical_plan.Column, error) {
	var projections []logical_plan.Column

	for {
		token := p.nextToken()
		if token == "" {
			break
		}

		if token == "*" {
			projections = append(projections, logical_plan.Column{Name: "*"})
		} else {

			parts := strings.Split(token, ".")
			if len(parts) == 2 {
				projections = append(projections, logical_plan.Column{
					Table: parts[0],
					Name:  parts[1],
				})
			} else {
				projections = append(projections, logical_plan.Column{
					Name: token,
				})
			}
		}

		if p.peekToken() == "," {
			p.consumeToken(",")
			continue
		}

		nextToken := strings.ToUpper(p.peekToken())
		if nextToken == "FROM" || nextToken == "" {
			break
		}
	}

	return projections, nil
}

func (p *SQLParser) parseFromClause() (*logical_plan.LogicalPlan, error) {
	tableName := p.nextToken()
	if tableName == "" {
		return nil, fmt.Errorf("expected table name")
	}

	var alias string
	nextToken := p.peekToken()
	if nextToken != "" && !isKeyword(nextToken) && nextToken != "," {
		alias = p.nextToken()
	}

	leftPlan := logical_plan.NewScanNode(tableName, alias)

	for {
		token := strings.ToUpper(p.peekToken())
		if !strings.Contains(token, "JOIN") {
			break
		}

		joinType, err := p.parseJoinType()
		if err != nil {
			return nil, err
		}

		rightTableName := p.nextToken()
		if rightTableName == "" {
			return nil, fmt.Errorf("expected table name after JOIN")
		}

		var rightAlias string
		if !isKeyword(p.peekToken()) && p.peekToken() != "" {
			rightAlias = p.nextToken()
		}

		rightPlan := logical_plan.NewScanNode(rightTableName, rightAlias)

		if !p.consumeToken("ON") {
			return nil, fmt.Errorf("expected ON after JOIN")
		}

		joinCondition, err := p.parseJoinCondition()
		if err != nil {
			return nil, err
		}

		leftPlan = logical_plan.NewJoinNode(leftPlan, rightPlan, joinType, joinCondition)
	}

	return leftPlan, nil
}

func (p *SQLParser) parseJoinType() (logical_plan.JoinType, error) {
	token := strings.ToUpper(p.nextToken())

	switch token {
	case "JOIN", "INNER":
		if token == "INNER" {
			p.consumeToken("JOIN")
		}
		return logical_plan.JoinTypeInner, nil
	case "LEFT":
		if p.consumeToken("OUTER") {
			p.consumeToken("JOIN")
		} else {
			p.consumeToken("JOIN")
		}
		return logical_plan.JoinTypeLeft, nil
	case "RIGHT":
		if p.consumeToken("OUTER") {
			p.consumeToken("JOIN")
		} else {
			p.consumeToken("JOIN")
		}
		return logical_plan.JoinTypeRight, nil
	case "FULL":
		if p.consumeToken("OUTER") {
			p.consumeToken("JOIN")
		} else {
			p.consumeToken("JOIN")
		}
		return logical_plan.JoinTypeFull, nil
	case "CROSS":
		p.consumeToken("JOIN")
		return logical_plan.JoinTypeCross, nil
	default:
		return "", fmt.Errorf("unsupported join type: %s", token)
	}
}

func (p *SQLParser) parseJoinCondition() (*logical_plan.JoinCondition, error) {

	leftExpr := p.nextToken()
	operator := p.nextToken()
	rightExpr := p.nextToken()

	if leftExpr == "" || operator == "" || rightExpr == "" {
		return nil, fmt.Errorf("invalid join condition")
	}

	return &logical_plan.JoinCondition{
		Left:     logical_plan.NewColumnExpression("", leftExpr),
		Right:    logical_plan.NewColumnExpression("", rightExpr),
		Operator: operator,
	}, nil
}

func (p *SQLParser) parsePredicate() (*logical_plan.Predicate, error) {

	column := p.nextToken()
	operator := p.nextToken()
	value := p.nextToken()

	if column == "" || operator == "" || value == "" {
		return nil, fmt.Errorf("invalid predicate")
	}

	var parsedValue interface{}
	if intVal, err := strconv.Atoi(value); err == nil {
		parsedValue = intVal
	} else if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
		parsedValue = floatVal
	} else {

		parsedValue = strings.Trim(value, "'\"")
	}

	predicate := &logical_plan.Predicate{
		Expression: logical_plan.NewBinaryOpExpression(
			operator,
			logical_plan.NewColumnExpression("", column),
			logical_plan.NewLiteralExpression(parsedValue),
		),
	}

	return predicate, nil
}

func (p *SQLParser) parseGroupBy(projections []logical_plan.Column) ([]logical_plan.Column, []logical_plan.AggregateFunction, error) {
	var groupBy []logical_plan.Column
	var aggregates []logical_plan.AggregateFunction

	for {
		token := p.nextToken()
		if token == "" {
			break
		}

		groupBy = append(groupBy, logical_plan.Column{Name: token})

		if p.peekToken() == "," {
			p.consumeToken(",")
			continue
		}

		break
	}

	for _, proj := range projections {
		if strings.Contains(strings.ToUpper(proj.Name), "COUNT") ||
			strings.Contains(strings.ToUpper(proj.Name), "SUM") ||
			strings.Contains(strings.ToUpper(proj.Name), "AVG") ||
			strings.Contains(strings.ToUpper(proj.Name), "MIN") ||
			strings.Contains(strings.ToUpper(proj.Name), "MAX") {

			var aggType logical_plan.AggregateType
			switch {
			case strings.Contains(strings.ToUpper(proj.Name), "COUNT"):
				aggType = logical_plan.AggregateCount
			case strings.Contains(strings.ToUpper(proj.Name), "SUM"):
				aggType = logical_plan.AggregateSum
			case strings.Contains(strings.ToUpper(proj.Name), "AVG"):
				aggType = logical_plan.AggregateAvg
			case strings.Contains(strings.ToUpper(proj.Name), "MIN"):
				aggType = logical_plan.AggregateMin
			case strings.Contains(strings.ToUpper(proj.Name), "MAX"):
				aggType = logical_plan.AggregateMax
			}

			aggregates = append(aggregates, logical_plan.AggregateFunction{
				Type:  aggType,
				Alias: proj.Alias,
			})
		}
	}

	return groupBy, aggregates, nil
}

func (p *SQLParser) parseOrderBy() ([]logical_plan.OrderBy, error) {
	var orderBy []logical_plan.OrderBy

	for {
		token := p.nextToken()
		if token == "" {
			break
		}

		ascending := true

		if strings.ToUpper(p.peekToken()) == "DESC" {
			ascending = false
			p.nextToken()
		} else if strings.ToUpper(p.peekToken()) == "ASC" {
			p.nextToken()
		}

		orderBy = append(orderBy, logical_plan.OrderBy{
			Expression: logical_plan.NewColumnExpression("", token),
			Ascending:  ascending,
		})

		if p.peekToken() == "," {
			p.consumeToken(",")
			continue
		}

		break
	}

	return orderBy, nil
}

func (p *SQLParser) parseLimit() (*int64, error) {
	token := p.nextToken()
	if token == "" {
		return nil, fmt.Errorf("expected limit value")
	}

	limit, err := strconv.ParseInt(token, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid limit value: %s", token)
	}

	return &limit, nil
}

func (p *SQLParser) nextToken() string {
	if p.pos >= len(p.tokens) {
		return ""
	}
	token := p.tokens[p.pos]
	p.pos++
	return token
}

func (p *SQLParser) peekToken() string {
	if p.pos >= len(p.tokens) {
		return ""
	}
	return p.tokens[p.pos]
}

func (p *SQLParser) consumeToken(expected string) bool {
	token := p.peekToken()
	if strings.ToUpper(token) == strings.ToUpper(expected) {
		p.nextToken()
		return true
	}
	return false
}

func tokenize(query string) []string {

	re := regexp.MustCompile(`\w+|[(),.=<>!]+|'[^']*'|"[^"]*"`)
	tokens := re.FindAllString(query, -1)

	var cleanTokens []string
	for _, token := range tokens {
		trimmed := strings.TrimSpace(token)
		if trimmed != "" {
			cleanTokens = append(cleanTokens, trimmed)
		}
	}

	return cleanTokens
}

func isKeyword(token string) bool {
	keywords := []string{
		"SELECT", "FROM", "WHERE", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS",
		"ON", "GROUP", "BY", "ORDER", "LIMIT", "HAVING", "UNION", "AND", "OR", "NOT",
		"IN", "EXISTS", "BETWEEN", "LIKE", "IS", "NULL", "ASC", "DESC", "DISTINCT",
		"COUNT", "SUM", "AVG", "MIN", "MAX", "AS", "INTO", "VALUES", "INSERT",
		"UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TABLE", "INDEX", "VIEW",
	}

	upper := strings.ToUpper(token)
	for _, keyword := range keywords {
		if upper == keyword {
			return true
		}
	}
	return false
}

func isSelectAll(projections []logical_plan.Column) bool {
	return len(projections) == 1 && projections[0].Name == "*"
}
