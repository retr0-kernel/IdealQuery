import React, { useEffect, useRef, useState } from 'react'
import * as d3 from 'd3'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ZoomIn, ZoomOut, RotateCcw, Download } from 'lucide-react'
import { useQueryStore } from '@/stores/queryStore'
import { formatCost, formatRows, cn } from '@/lib/utils'

const NODE_COLORS = {
    scan: '#3b82f6',    
    filter: '#f59e0b',  
    project: '#10b981', 
    join: '#ef4444',    
    aggregate: '#8b5cf6', // violet
    sort: '#06b6d4',    
    limit: '#84cc16',   
    union: '#ec4899',   
    subquery: '#6b7280' 
}

export function PlanVisualization() {
    const { parseResult, optimizationResults, selectedNode, setSelectedNode } = useQueryStore()
    const [activeView, setActiveView] = useState('original')
    const svgRef = useRef()
    const [transform, setTransform] = useState({ x: 0, y: 0, k: 1 })

    const plans = {
        original: parseResult?.logicalPlan,
        rule: optimizationResults?.rule?.optimizedPlan,
        cost: optimizationResults?.cost?.optimizedPlan
    }

    const currentPlan = plans[activeView]

    useEffect(() => {
        if (currentPlan && svgRef.current) {
            renderPlan(currentPlan)
        }
    }, [currentPlan, activeView])

    const renderPlan = (plan) => {
        if (!plan || !svgRef.current) return

      
        d3.select(svgRef.current).selectAll('*').remove()

        const svg = d3.select(svgRef.current)
        const width = 800
        const height = 600

        svg.attr('width', width).attr('height', height)

      
        const zoom = d3.zoom()
            .scaleExtent([0.1, 3])
            .on('zoom', (event) => {
                const { x, y, k } = event.transform
                setTransform({ x, y, k })
                g.attr('transform', event.transform)
            })

        svg.call(zoom)

        const g = svg.append('g')

      
        const root = d3.hierarchy(plan, d => d.children)

      
        const treeLayout = d3.tree()
            .size([width - 100, height - 100])
            .separation((a, b) => (a.parent === b.parent ? 1 : 2) / a.depth)

        treeLayout(root)

      
        root.each(d => {
            d.y = d.depth * 120 + 50
            d.x = d.x + 50
        })

      
        const links = g.selectAll('.link')
            .data(root.links())
            .enter()
            .append('path')
            .attr('class', 'link')
            .attr('d', d3.linkVertical()
                .x(d => d.x)
                .y(d => d.y)
            )
            .attr('fill', 'none')
            .attr('stroke', '#6b7280')
            .attr('stroke-width', 2)

      
        const nodes = g.selectAll('.node')
            .data(root.descendants())
            .enter()
            .append('g')
            .attr('class', 'node')
            .attr('transform', d => `translate(${d.x},${d.y})`)
            .style('cursor', 'pointer')
            .on('click', (event, d) => {
                setSelectedNode(d.data)
            })

      
        nodes.append('rect')
            .attr('width', 120)
            .attr('height', 60)
            .attr('x', -60)
            .attr('y', -30)
            .attr('rx', 8)
            .attr('fill', d => NODE_COLORS[d.data.node_type] || '#6b7280')
            .attr('stroke', d => selectedNode?.id === d.data.id ? '#000' : 'none')
            .attr('stroke-width', 2)

      
        nodes.append('text')
            .attr('text-anchor', 'middle')
            .attr('dy', '-5')
            .attr('fill', 'white')
            .attr('font-size', '12px')
            .attr('font-weight', 'bold')
            .text(d => d.data.node_type.toUpperCase())

      
        nodes.append('text')
            .attr('text-anchor', 'middle')
            .attr('dy', '10')
            .attr('fill', 'white')
            .attr('font-size', '10px')
            .text(d => {
                if (d.data.table_name) return d.data.table_name
                if (d.data.node_type === 'join') return d.data.join_type || 'inner'
                return ''
            })

      
        nodes.append('text')
            .attr('text-anchor', 'middle')
            .attr('dy', '25')
            .attr('fill', 'white')
            .attr('font-size', '9px')
            .text(d => {
                const cost = d.data.estimated_cost
                const rows = d.data.estimated_rows
                if (cost !== undefined && rows !== undefined) {
                    return `${formatRows(rows)} rows, ${formatCost(cost)} cost`
                }
                return ''
            })

      
        g.attr('transform', `translate(${transform.x},${transform.y}) scale(${transform.k})`)
    }

    const handleZoomIn = () => {
        const svg = d3.select(svgRef.current)
        svg.transition().call(
            d3.zoom().transform,
            d3.zoomTransform(svg.node()).scale(transform.k * 1.5)
        )
    }

    const handleZoomOut = () => {
        const svg = d3.select(svgRef.current)
        svg.transition().call(
            d3.zoom().transform,
            d3.zoomTransform(svg.node()).scale(transform.k * 0.75)
        )
    }

    const handleResetZoom = () => {
        const svg = d3.select(svgRef.current)
        svg.transition().call(
            d3.zoom().transform,
            d3.zoomIdentity
        )
    }

    const handleExportSVG = () => {
        const svgElement = svgRef.current
        const serializer = new XMLSerializer()
        const svgString = serializer.serializeToString(svgElement)
        const blob = new Blob([svgString], { type: 'image/svg+xml' })
        const url = URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url
        a.download = `query_plan_${activeView}_${new Date().toISOString().split('T')[0]}.svg`
        a.click()
        URL.revokeObjectURL(url)
    }

    if (!currentPlan) {
        return (
            <Card>
                <CardContent className="flex items-center justify-center h-64">
                    <p className="text-muted-foreground">No query plan to visualize. Run a query first.</p>
                </CardContent>
            </Card>
        )
    }

    return (
        <Card>
            <CardHeader>
                <div className="flex items-center justify-between">
                    <CardTitle>Query Plan Visualization</CardTitle>
                    <div className="flex items-center gap-2">
                        <Button variant="outline" size="sm" onClick={handleZoomIn}>
                            <ZoomIn className="w-4 h-4" />
                        </Button>
                        <Button variant="outline" size="sm" onClick={handleZoomOut}>
                            <ZoomOut className="w-4 h-4" />
                        </Button>
                        <Button variant="outline" size="sm" onClick={handleResetZoom}>
                            <RotateCcw className="w-4 h-4" />
                        </Button>
                        <Button variant="outline" size="sm" onClick={handleExportSVG}>
                            <Download className="w-4 h-4" />
                        </Button>
                    </div>
                </div>
            </CardHeader>
            <CardContent>
                <Tabs value={activeView} onValueChange={setActiveView}>
                    <TabsList className="grid w-full grid-cols-3">
                        <TabsTrigger value="original">Original</TabsTrigger>
                        <TabsTrigger value="rule" disabled={!plans.rule}>
                            Rule-Based
                            {optimizationResults?.rule && (
                                <Badge variant="secondary" className="ml-2">
                                    {optimizationResults.rule.explain?.applied_rules?.length || 0} rules
                                </Badge>
                            )}
                        </TabsTrigger>
                        <TabsTrigger value="cost" disabled={!plans.cost}>
                            Cost-Based
                            {optimizationResults?.cost && (
                                <Badge variant="secondary" className="ml-2">
                                    optimized
                                </Badge>
                            )}
                        </TabsTrigger>
                    </TabsList>

                    <TabsContent value={activeView} className="mt-4">
                        <div className="relative border rounded-lg overflow-hidden bg-gray-50">
                            <svg ref={svgRef} className="w-full h-96" />

                            {/* Legend */}
                            <div className="absolute top-4 left-4 bg-white p-3 rounded-lg shadow-lg">
                                <h4 className="font-semibold text-sm mb-2">Node Types</h4>
                                <div className="grid grid-cols-2 gap-2 text-xs">
                                    {Object.entries(NODE_COLORS).map(([type, color]) => (
                                        <div key={type} className="flex items-center gap-2">
                                            <div
                                                className="w-3 h-3 rounded"
                                                style={{ backgroundColor: color }}
                                            />
                                            <span className="capitalize">{type}</span>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        </div>
                    </TabsContent>
                </Tabs>
            </CardContent>
        </Card>
    )
}