import React from 'react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, LineChart, Line } from 'recharts'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Badge } from '@/components/ui/badge'
import { useQueryStore } from '@/stores/queryStore'
import { formatCost, formatRows, formatDuration } from '@/lib/utils'

export function CostChart() {
    const { optimizationResults, simulationResults } = useQueryStore()

    if (!optimizationResults && !simulationResults) {
        return (
            <Card>
                <CardContent className="flex items-center justify-center h-48">
                    <p className="text-muted-foreground">No optimization results to compare</p>
                </CardContent>
            </Card>
        )
    }

    const costData = []

    if (optimizationResults?.rule) {
        costData.push({
            name: 'Rule-Based',
            estimatedCost: optimizationResults.rule.optimizedPlan?.estimated_cost || 0,
            estimatedRows: optimizationResults.rule.optimizedPlan?.estimated_rows || 0,
            rulesApplied: optimizationResults.rule.explain?.applied_rules?.length || 0
        })
    }

    if (optimizationResults?.cost) {
        costData.push({
            name: 'Cost-Based',
            estimatedCost: optimizationResults.cost.optimizedPlan?.estimated_cost || 0,
            estimatedRows: optimizationResults.cost.optimizedPlan?.estimated_rows || 0,
            rulesApplied: optimizationResults.cost.explain?.applied_rules?.length || 0
        })
    }

    const simulationData = []

    if (simulationResults) {
        if (simulationResults.original) {
            simulationData.push({
                name: 'Original',
                executionTime: simulationResults.original.execution_time / 1000000, // Convert to ms
                cpuTime: simulationResults.original.cpu_time / 1000000,
                ioOperations: simulationResults.original.io_operations,
                memoryUsed: simulationResults.original.memory_used / 1024 / 1024, // Convert to MB
                rowsProcessed: simulationResults.original.rows_processed,
                rowsReturned: simulationResults.original.rows_returned
            })
        }

        if (simulationResults.rule) {
            simulationData.push({
                name: 'Rule-Based',
                executionTime: simulationResults.rule.execution_time / 1000000,
                cpuTime: simulationResults.rule.cpu_time / 1000000,
                ioOperations: simulationResults.rule.io_operations,
                memoryUsed: simulationResults.rule.memory_used / 1024 / 1024,
                rowsProcessed: simulationResults.rule.rows_processed,
                rowsReturned: simulationResults.rule.rows_returned
            })
        }

        if (simulationResults.cost) {
            simulationData.push({
                name: 'Cost-Based',
                executionTime: simulationResults.cost.execution_time / 1000000,
                cpuTime: simulationResults.cost.cpu_time / 1000000,
                ioOperations: simulationResults.cost.io_operations,
                memoryUsed: simulationResults.cost.memory_used / 1024 / 1024,
                rowsProcessed: simulationResults.cost.rows_processed,
                rowsReturned: simulationResults.cost.rows_returned
            })
        }
    }

    const calculateImprovement = (original, optimized, key) => {
        if (!original || !optimized || !original[key] || !optimized[key]) return null
        const improvement = ((original[key] - optimized[key]) / original[key]) * 100
        return improvement.toFixed(1)
    }

    const getImprovementBadge = (improvement) => {
        if (!improvement) return null
        const value = parseFloat(improvement)
        if (value > 0) {
            return <Badge className="bg-green-100 text-green-800">{improvement}% faster</Badge>
        } else if (value < 0) {
            return <Badge className="bg-red-100 text-red-800">{Math.abs(value)}% slower</Badge>
        }
        return <Badge variant="outline">Same</Badge>
    }

    return (
        <Card>
            <CardHeader>
                <CardTitle>Performance Analysis</CardTitle>
            </CardHeader>
            <CardContent>
                <Tabs defaultValue="estimates" className="w-full">
                    <TabsList className="grid w-full grid-cols-3">
                        <TabsTrigger value="estimates">Cost Estimates</TabsTrigger>
                        <TabsTrigger value="simulation">Simulation Results</TabsTrigger>
                        <TabsTrigger value="comparison">Improvement</TabsTrigger>
                    </TabsList>

                    <TabsContent value="estimates" className="space-y-4">
                        {costData.length > 0 ? (
                            <>
                                <div className="h-64">
                                    <ResponsiveContainer width="100%" height="100%">
                                        <BarChart data={costData}>
                                            <CartesianGrid strokeDasharray="3 3" />
                                            <XAxis dataKey="name" />
                                            <YAxis yAxisId="cost" orientation="left" />
                                            <YAxis yAxisId="rows" orientation="right" />
                                            <Tooltip
                                                formatter={(value, name) => [
                                                    name === 'estimatedCost' ? formatCost(value) : formatRows(value),
                                                    name === 'estimatedCost' ? 'Estimated Cost' : 'Estimated Rows'
                                                ]}
                                            />
                                            <Legend />
                                            <Bar yAxisId="cost" dataKey="estimatedCost" fill="#3b82f6" name="Estimated Cost" />
                                            <Bar yAxisId="rows" dataKey="estimatedRows" fill="#10b981" name="Estimated Rows" />
                                        </BarChart>
                                    </ResponsiveContainer>
                                </div>

                                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                    {costData.map((item, index) => (
                                        <Card key={index}>
                                            <CardContent className="p-4">
                                                <h4 className="font-medium mb-2">{item.name} Optimization</h4>
                                                <div className="space-y-2 text-sm">
                                                    <div className="flex justify-between">
                                                        <span>Estimated Cost:</span>
                                                        <span className="font-mono">{formatCost(item.estimatedCost)}</span>
                                                    </div>
                                                    <div className="flex justify-between">
                                                        <span>Estimated Rows:</span>
                                                        <span className="font-mono">{formatRows(item.estimatedRows)}</span>
                                                    </div>
                                                    {item.rulesApplied > 0 && (
                                                        <div className="flex justify-between">
                                                            <span>Rules Applied:</span>
                                                            <Badge variant="outline">{item.rulesApplied}</Badge>
                                                        </div>
                                                    )}
                                                </div>
                                            </CardContent>
                                        </Card>
                                    ))}
                                </div>
                            </>
                        ) : (
                            <p className="text-muted-foreground text-center py-8">No cost estimation data available</p>
                        )}
                    </TabsContent>

                    <TabsContent value="simulation" className="space-y-4">
                        {simulationData.length > 0 ? (
                            <>
                                <div className="h-64">
                                    <ResponsiveContainer width="100%" height="100%">
                                        <LineChart data={simulationData}>
                                            <CartesianGrid strokeDasharray="3 3" />
                                            <XAxis dataKey="name" />
                                            <YAxis />
                                            <Tooltip formatter={(value, name) => [formatDuration(value), 'Execution Time (ms)']} />
                                            <Legend />
                                            <Line type="monotone" dataKey="executionTime" stroke="#3b82f6" strokeWidth={2} />
                                            <Line type="monotone" dataKey="cpuTime" stroke="#10b981" strokeWidth={2} />
                                        </LineChart>
                                    </ResponsiveContainer>
                                </div>

                                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                                    {simulationData.map((item, index) => (
                                        <Card key={index}>
                                            <CardContent className="p-4">
                                                <h4 className="font-medium mb-2">{item.name}</h4>
                                                <div className="space-y-2 text-sm">
                                                    <div className="flex justify-between">
                                                        <span>Execution Time:</span>
                                                        <span className="font-mono">{formatDuration(item.executionTime)}</span>
                                                    </div>
                                                    <div className="flex justify-between">
                                                        <span>CPU Time:</span>
                                                        <span className="font-mono">{formatDuration(item.cpuTime)}</span>
                                                    </div>
                                                    <div className="flex justify-between">
                                                        <span>I/O Operations:</span>
                                                        <span className="font-mono">{item.ioOperations}</span>
                                                    </div>
                                                    <div className="flex justify-between">
                                                        <span>Memory Used:</span>
                                                        <span className="font-mono">{item.memoryUsed.toFixed(1)} MB</span>
                                                    </div>
                                                    <div className="flex justify-between">
                                                        <span>Rows Processed:</span>
                                                        <span className="font-mono">{formatRows(item.rowsProcessed)}</span>
                                                    </div>
                                                    <div className="flex justify-between">
                                                        <span>Rows Returned:</span>
                                                        <span className="font-mono">{formatRows(item.rowsReturned)}</span>
                                                    </div>
                                                </div>
                                            </CardContent>
                                        </Card>
                                    ))}
                                </div>
                            </>
                        ) : (
                            <p className="text-muted-foreground text-center py-8">No simulation data available</p>
                        )}
                    </TabsContent>

                    <TabsContent value="comparison" className="space-y-4">
                        {simulationData.length > 1 ? (
                            <div className="space-y-4">
                                <h3 className="text-lg font-medium">Performance Improvements</h3>

                                {simulationData.slice(1).map((optimized, index) => {
                                    const original = simulationData[0]
                                    const execImprovement = calculateImprovement(original, optimized, 'executionTime')
                                    const cpuImprovement = calculateImprovement(original, optimized, 'cpuTime')
                                    const ioImprovement = calculateImprovement(original, optimized, 'ioOperations')
                                    const memoryImprovement = calculateImprovement(original, optimized, 'memoryUsed')

                                    return (
                                        <Card key={index}>
                                            <CardContent className="p-4">
                                                <h4 className="font-medium mb-3">{optimized.name} vs Original</h4>
                                                <div className="grid grid-cols-2 gap-4">
                                                    <div className="space-y-2">
                                                        <div className="flex justify-between items-center">
                                                            <span className="text-sm">Execution Time:</span>
                                                            {getImprovementBadge(execImprovement)}
                                                        </div>
                                                        <div className="flex justify-between items-center">
                                                            <span className="text-sm">CPU Time:</span>
                                                            {getImprovementBadge(cpuImprovement)}
                                                        </div>
                                                    </div>
                                                    <div className="space-y-2">
                                                        <div className="flex justify-between items-center">
                                                            <span className="text-sm">I/O Operations:</span>
                                                            {getImprovementBadge(ioImprovement)}
                                                        </div>
                                                        <div className="flex justify-between items-center">
                                                            <span className="text-sm">Memory Usage:</span>
                                                            {getImprovementBadge(memoryImprovement)}
                                                        </div>
                                                    </div>
                                                </div>
                                            </CardContent>
                                        </Card>
                                    )
                                })}
                            </div>
                        ) : (
                            <p className="text-muted-foreground text-center py-8">
                                Need at least two simulation results to compare performance
                            </p>
                        )}
                    </TabsContent>
                </Tabs>
            </CardContent>
        </Card>
    )
}
