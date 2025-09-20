import React, { useEffect } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { Toaster } from 'react-hot-toast'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { QueryEditor } from '@/components/QueryEditor'
import { PlanVisualization } from '@/components/PlanVisualization'
import { NodeInspector } from '@/components/NodeInspector'
import { CostChart } from '@/components/CostChart'
import { CatalogPanel } from '@/components/CatalogPanel'
import { useQueryStore } from '@/stores/queryStore'
import { useCatalogStore } from '@/stores/catalogStore'
import { Database, BarChart3, Eye, Code, Github, HelpCircle } from 'lucide-react'

const queryClient = new QueryClient({
    defaultOptions: {
        queries: {
            staleTime: 1000 * 60 * 5, // 5 minutes
            retry: 1,
        },
    },
})

function App() {
    const { activeTab, setActiveTab, parseResult, optimizationResults } = useQueryStore()
    const { initialize } = useCatalogStore()

    useEffect(() => {
        initialize()
    }, [initialize])

    const hasResults = parseResult || optimizationResults

    return (
        <QueryClientProvider client={queryClient}>
            <div className="min-h-screen bg-background">
                {/* Header */}
                <header className="border-b bg-white sticky top-0 z-50">
                    <div className="container mx-auto px-4 py-4">
                        <div className="flex items-center justify-between">
                            <div className="flex items-center gap-3">
                                <div className="flex items-center gap-2">
                                    <Database className="w-8 h-8 text-primary" />
                                    <h1 className="text-2xl font-bold">OptiQuery</h1>
                                </div>
                                <Badge variant="outline">Query Optimizer & Visualizer</Badge>
                            </div>

                            <div className="flex items-center gap-2">
                                <Button variant="ghost" size="sm">
                                    <HelpCircle className="w-4 h-4 mr-2" />
                                    Help
                                </Button>
                                <Button variant="ghost" size="sm">
                                    <Github className="w-4 h-4 mr-2" />
                                    GitHub
                                </Button>
                            </div>
                        </div>
                    </div>
                </header>

                {/* Main Content */}
                <main className="container mx-auto px-4 py-6">
                    <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
                        {/* Left Sidebar - Catalog */}
                        <div className="lg:col-span-1">
                            <CatalogPanel />
                        </div>

                        {/* Main Content Area */}
                        <div className="lg:col-span-3">
                            <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
                                <TabsList className="grid w-full grid-cols-4">
                                    <TabsTrigger value="editor" className="flex items-center gap-2">
                                        <Code className="w-4 h-4" />
                                        Editor
                                    </TabsTrigger>
                                    <TabsTrigger value="visualization" disabled={!hasResults} className="flex items-center gap-2">
                                        <Eye className="w-4 h-4" />
                                        Plans
                                        {hasResults && <Badge variant="secondary" className="ml-1">✓</Badge>}
                                    </TabsTrigger>
                                    <TabsTrigger value="inspector" disabled={!hasResults} className="flex items-center gap-2">
                                        <Database className="w-4 h-4" />
                                        Inspector
                                    </TabsTrigger>
                                    <TabsTrigger value="analysis" disabled={!hasResults} className="flex items-center gap-2">
                                        <BarChart3 className="w-4 h-4" />
                                        Analysis
                                    </TabsTrigger>
                                </TabsList>

                                <TabsContent value="editor" className="mt-6">
                                    <QueryEditor />
                                </TabsContent>

                                <TabsContent value="visualization" className="mt-6">
                                    <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
                                        <div className="xl:col-span-2">
                                            <PlanVisualization />
                                        </div>
                                        <div className="xl:col-span-1">
                                            <NodeInspector />
                                        </div>
                                    </div>
                                </TabsContent>

                                <TabsContent value="inspector" className="mt-6">
                                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                                        <NodeInspector />
                                        <OptimizationSummary />
                                    </div>
                                </TabsContent>

                                <TabsContent value="analysis" className="mt-6">
                                    <CostChart />
                                </TabsContent>
                            </Tabs>
                        </div>
                    </div>
                </main>

                {/* Footer */}
                <footer className="border-t bg-gray-50 mt-12">
                    <div className="container mx-auto px-4 py-6">
                        <div className="flex items-center justify-between text-sm text-muted-foreground">
                            <p>© 2024 OptiQuery - Teaching-grade Query Optimizer</p>
                            <p>Built with React, D3.js, and Go</p>
                        </div>
                    </div>
                </footer>
            </div>

            <Toaster
                position="top-right"
                toastOptions={{
                    duration: 4000,
                    style: {
                        background: '#363636',
                        color: '#fff',
                    },
                }}
            />
        </QueryClientProvider>
    )
}

function OptimizationSummary() {
    const { optimizationResults } = useQueryStore()

    if (!optimizationResults) {
        return (
            <Card>
                <CardContent className="flex items-center justify-center h-48">
                    <p className="text-muted-foreground">No optimization results available</p>
                </CardContent>
            </Card>
        )
    }

    return (
        <Card>
            <CardHeader>
                <CardTitle>Optimization Summary</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
                {optimizationResults.rule && (
                    <div className="space-y-2">
                        <h4 className="font-medium">Rule-Based Optimization</h4>
                        <div className="text-sm space-y-1">
                            <p>Rules Applied: {optimizationResults.rule.explain?.applied_rules?.length || 0}</p>
                            <div className="space-y-1">
                                {optimizationResults.rule.explain?.applied_rules?.map((rule, idx) => (
                                    <Badge key={idx} variant="outline" className="mr-1">{rule}</Badge>
                                ))}
                            </div>
                        </div>
                    </div>
                )}

                {optimizationResults.cost && (
                    <div className="space-y-2">
                        <h4 className="font-medium">Cost-Based Optimization</h4>
                        <div className="text-sm space-y-1">
                            <p>Strategy: Dynamic Programming + Greedy Heuristics</p>
                            <p>Physical Operators: Selected based on cardinality estimates</p>
                        </div>
                    </div>
                )}
            </CardContent>
        </Card>
    )
}

export default App