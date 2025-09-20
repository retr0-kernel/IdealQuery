import React from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Separator } from '@/components/ui/separator'
import { useQueryStore } from '@/stores/queryStore'
import { formatCost, formatRows } from '@/lib/utils'

export function NodeInspector() {
    const { selectedNode } = useQueryStore()

    if (!selectedNode) {
        return (
            <Card>
                <CardContent className="flex items-center justify-center h-48">
                    <p className="text-muted-foreground">Select a node in the plan to inspect details</p>
                </CardContent>
            </Card>
        )
    }

    const renderNodeDetails = () => {
        switch (selectedNode.node_type) {
            case 'scan':
                return (
                    <div className="space-y-3">
                        <div>
                            <label className="text-sm font-medium">Table</label>
                            <p className="text-sm text-muted-foreground">{selectedNode.table_name}</p>
                        </div>
                        {selectedNode.alias && (
                            <div>
                                <label className="text-sm font-medium">Alias</label>
                                <p className="text-sm text-muted-foreground">{selectedNode.alias}</p>
                            </div>
                        )}
                        {selectedNode.metadata?.scan_type && (
                            <div>
                                <label className="text-sm font-medium">Scan Type</label>
                                <Badge variant="outline">{selectedNode.metadata.scan_type}</Badge>
                            </div>
                        )}
                        {selectedNode.metadata?.index_name && (
                            <div>
                                <label className="text-sm font-medium">Index</label>
                                <p className="text-sm text-muted-foreground">{selectedNode.metadata.index_name}</p>
                            </div>
                        )}
                    </div>
                )

            case 'filter':
                return (
                    <div className="space-y-3">
                        <div>
                            <label className="text-sm font-medium">Predicate</label>
                            <div className="p-2 bg-gray-50 rounded text-sm font-mono">
                                {renderPredicate(selectedNode.predicate)}
                            </div>
                        </div>
                    </div>
                )

            case 'project':
                return (
                    <div className="space-y-3">
                        <div>
                            <label className="text-sm font-medium">Projections</label>
                            <div className="space-y-1">
                                {selectedNode.projections?.map((proj, idx) => (
                                    <div key={idx} className="text-sm text-muted-foreground">
                                        {proj.table ? `${proj.table}.${proj.name}` : proj.name}
                                        {proj.alias && ` AS ${proj.alias}`}
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>
                )

            case 'join':
                return (
                    <div className="space-y-3">
                        <div>
                            <label className="text-sm font-medium">Join Type</label>
                            <Badge variant="outline">{selectedNode.join_type || 'inner'}</Badge>
                        </div>
                        {selectedNode.join_condition && (
                            <div>
                                <label className="text-sm font-medium">Join Condition</label>
                                <div className="p-2 bg-gray-50 rounded text-sm font-mono">
                                    {renderJoinCondition(selectedNode.join_condition)}
                                </div>
                            </div>
                        )}
                        {selectedNode.metadata?.physical_operator && (
                            <div>
                                <label className="text-sm font-medium">Physical Operator</label>
                                <Badge variant="outline">{selectedNode.metadata.physical_operator}</Badge>
                            </div>
                        )}
                        {selectedNode.metadata?.build_side && (
                            <div>
                                <label className="text-sm font-medium">Build Side</label>
                                <Badge variant="outline">{selectedNode.metadata.build_side}</Badge>
                            </div>
                        )}
                    </div>
                )

            case 'aggregate':
                return (
                    <div className="space-y-3">
                        {selectedNode.group_by && selectedNode.group_by.length > 0 && (
                            <div>
                                <label className="text-sm font-medium">Group By</label>
                                <div className="space-y-1">
                                    {selectedNode.group_by.map((col, idx) => (
                                        <div key={idx} className="text-sm text-muted-foreground">
                                            {col.table ? `${col.table}.${col.name}` : col.name}
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}
                        {selectedNode.aggregates && selectedNode.aggregates.length > 0 && (
                            <div>
                                <label className="text-sm font-medium">Aggregates</label>
                                <div className="space-y-1">
                                    {selectedNode.aggregates.map((agg, idx) => (
                                        <div key={idx} className="text-sm text-muted-foreground">
                                            {agg.type.toUpperCase()}
                                            {agg.column && `(${agg.column.value || agg.column})`}
                                            {agg.alias && ` AS ${agg.alias}`}
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}
                        {selectedNode.metadata?.physical_operator && (
                            <div>
                                <label className="text-sm font-medium">Algorithm</label>
                                <Badge variant="outline">{selectedNode.metadata.physical_operator}</Badge>
                            </div>
                        )}
                    </div>
                )

            case 'sort':
                return (
                    <div className="space-y-3">
                        {selectedNode.order_by && (
                            <div>
                                <label className="text-sm font-medium">Order By</label>
                                <div className="space-y-1">
                                    {selectedNode.order_by.map((order, idx) => (
                                        <div key={idx} className="text-sm text-muted-foreground">
                                            {order.expression?.value || 'column'} {order.ascending ? 'ASC' : 'DESC'}
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}
                        {selectedNode.metadata?.physical_operator && (
                            <div>
                                <label className="text-sm font-medium">Sort Algorithm</label>
                                <Badge variant="outline">{selectedNode.metadata.physical_operator}</Badge>
                            </div>
                        )}
                    </div>
                )

            case 'limit':
                return (
                    <div className="space-y-3">
                        {selectedNode.limit_count && (
                            <div>
                                <label className="text-sm font-medium">Limit</label>
                                <p className="text-sm text-muted-foreground">{selectedNode.limit_count}</p>
                            </div>
                        )}
                        {selectedNode.offset_count && (
                            <div>
                                <label className="text-sm font-medium">Offset</label>
                                <p className="text-sm text-muted-foreground">{selectedNode.offset_count}</p>
                            </div>
                        )}
                    </div>
                )

            default:
                return (
                    <div>
                        <p className="text-sm text-muted-foreground">
                            No specific details available for this node type.
                        </p>
                    </div>
                )
        }
    }

    const renderPredicate = (predicate) => {
        if (!predicate?.expression) return 'N/A'
        const expr = predicate.expression

        if (expr.type === 'binary_op') {
            const left = expr.left?.value || 'column'
            const right = expr.right?.value || 'value'
            return `${left} ${expr.value} ${right}`
        }

        return JSON.stringify(expr, null, 2)
    }

    const renderJoinCondition = (condition) => {
        if (!condition) return 'N/A'
        const left = condition.left?.value || 'left'
        const right = condition.right?.value || 'right'
        return `${left} ${condition.operator} ${right}`
    }

    return (
        <Card>
            <CardHeader>
                <CardTitle className="flex items-center gap-2">
                    Node Inspector
                    <Badge variant="outline">{selectedNode.node_type}</Badge>
                </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
                {/* Basic Info */}
                <div className="grid grid-cols-2 gap-4">
                    <div>
                        <label className="text-sm font-medium">Node ID</label>
                        <p className="text-sm text-muted-foreground font-mono">{selectedNode.id}</p>
                    </div>
                    <div>
                        <label className="text-sm font-medium">Type</label>
                        <p className="text-sm text-muted-foreground capitalize">{selectedNode.node_type}</p>
                    </div>
                </div>

                <Separator />

                {/* Cost Information */}
                {(selectedNode.estimated_cost !== undefined || selectedNode.estimated_rows !== undefined) && (
                    <>
                        <div>
                            <h4 className="font-medium mb-2">Cost Estimates</h4>
                            <div className="grid grid-cols-2 gap-4">
                                {selectedNode.estimated_rows !== undefined && (
                                    <div>
                                        <label className="text-sm font-medium">Estimated Rows</label>
                                        <p className="text-sm text-muted-foreground">{formatRows(selectedNode.estimated_rows)}</p>
                                    </div>
                                )}
                                {selectedNode.estimated_cost !== undefined && (
                                    <div>
                                        <label className="text-sm font-medium">Estimated Cost</label>
                                        <p className="text-sm text-muted-foreground">{formatCost(selectedNode.estimated_cost)}</p>
                                    </div>
                                )}
                            </div>
                        </div>
                        <Separator />
                    </>
                )}

                {/* Node-specific Details */}
                <div>
                    <h4 className="font-medium mb-2">Details</h4>
                    {renderNodeDetails()}
                </div>

                {/* Metadata */}
                {selectedNode.metadata && Object.keys(selectedNode.metadata).length > 0 && (
                    <>
                        <Separator />
                        <div>
                            <h4 className="font-medium mb-2">Metadata</h4>
                            <div className="space-y-2">
                                {Object.entries(selectedNode.metadata).map(([key, value]) => (
                                    <div key={key}>
                                        <label className="text-sm font-medium capitalize">{key.replace(/_/g, ' ')}</label>
                                        <p className="text-sm text-muted-foreground">
                                            {typeof value === 'object' ? JSON.stringify(value) : String(value)}
                                        </p>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </>
                )}
            </CardContent>
        </Card>
    )
}