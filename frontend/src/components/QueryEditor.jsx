import React, { useState, useEffect } from 'react'
import Editor from '@monaco-editor/react'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Play, Save, Upload, Download, RotateCcw, Database } from 'lucide-react'
import { useQueryStore } from '@/stores/queryStore'
import { useCatalogStore } from '@/stores/catalogStore'
import toast from 'react-hot-toast'

const SAMPLE_QUERIES = {
    sql: [
        {
            name: "Simple Filter",
            query: "SELECT name, age FROM customers WHERE age > 25;"
        },
        {
            name: "Join Query",
            query: `SELECT c.name, o.total_amount, o.order_date
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.country = 'USA'
ORDER BY o.total_amount DESC;`
        },
        {
            name: "Aggregation",
            query: `SELECT 
  country, 
  COUNT(*) as customer_count,
  AVG(age) as avg_age
FROM customers 
GROUP BY country
ORDER BY customer_count DESC;`
        },
        {
            name: "Complex Multi-Join",
            query: `SELECT 
  c.name,
  c.country,
  COUNT(o.order_id) as order_count,
  SUM(o.total_amount) as total_spent,
  AVG(o.total_amount) as avg_order_value
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
LEFT JOIN products p ON o.product_name = p.name
WHERE c.age BETWEEN 25 AND 40
GROUP BY c.customer_id, c.name, c.country
HAVING COUNT(o.order_id) > 0
ORDER BY total_spent DESC
LIMIT 10;`
        }
    ],
    mongo: [
        {
            name: "Aggregation Pipeline",
            query: `[
  { $match: { age: { $gt: 25 } } },
  { $group: { 
      _id: "$country", 
      count: { $sum: 1 }, 
      avgAge: { $avg: "$age" } 
    } },
  { $sort: { count: -1 } }
]`
        }
    ]
}

export function QueryEditor() {
    const {
        currentQuery,
        dialect,
        isLoading,
        error,
        setQuery,
        setDialect,
        runFullWorkflow,
        clearResults,
        resetState
    } = useQueryStore()

    const { tables, loadSampleData } = useCatalogStore()
    const [selectedSample, setSelectedSample] = useState('')

    const handleEditorChange = (value) => {
        setQuery(value || '')
    }

    const handleRunQuery = async () => {
        if (!currentQuery.trim()) {
            toast.error('Please enter a query')
            return
        }

        try {
            await runFullWorkflow('postgres')
            toast.success('Query processed successfully!')
        } catch (error) {
            toast.error(`Error: ${error.message}`)
        }
    }

    const handleLoadSample = (sampleQuery) => {
        setQuery(sampleQuery)
        setSelectedSample('')
    }

    const handleLoadSampleData = async () => {
        try {
            await loadSampleData()
            toast.success('Sample data loaded successfully!')
        } catch (error) {
            toast.error(`Failed to load sample data: ${error.message}`)
        }
    }

    const handleSaveQuery = () => {
        const blob = new Blob([currentQuery], { type: 'text/plain' })
        const url = URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url
        a.download = `query_${new Date().toISOString().split('T')[0]}.${dialect}`
        a.click()
        URL.revokeObjectURL(url)
    }

    const handleLoadQuery = () => {
        const input = document.createElement('input')
        input.type = 'file'
        input.accept = '.sql,.mongo,.txt'
        input.onchange = (e) => {
            const file = e.target.files?.[0]
            if (file) {
                const reader = new FileReader()
                reader.onload = (e) => {
                    const content = e.target?.result
                    if (typeof content === 'string') {
                        setQuery(content)
                        toast.success('Query loaded successfully!')
                    }
                }
                reader.readAsText(file)
            }
        }
        input.click()
    }

    return (
        <div className="space-y-4">
            <Card>
                <CardHeader className="pb-3">
                    <div className="flex items-center justify-between">
                        <CardTitle className="text-lg">Query Editor</CardTitle>
                        <div className="flex items-center gap-2">
                            <Select value={dialect} onValueChange={setDialect}>
                                <SelectTrigger className="w-32">
                                    <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="sql">SQL</SelectItem>
                                    <SelectItem value="mongo">MongoDB</SelectItem>
                                    <SelectItem value="athena">Athena</SelectItem>
                                </SelectContent>
                            </Select>
                        </div>
                    </div>
                </CardHeader>
                <CardContent className="space-y-4">
                    {/* Toolbar */}
                    <div className="flex items-center justify-between gap-2 flex-wrap">
                        <div className="flex items-center gap-2">
                            <Button
                                onClick={handleRunQuery}
                                disabled={isLoading || !currentQuery.trim()}
                                className="bg-green-600 hover:bg-green-700"
                            >
                                <Play className="w-4 h-4 mr-2" />
                                {isLoading ? 'Processing...' : 'Run Query'}
                            </Button>

                            <Button variant="outline" onClick={clearResults}>
                                <RotateCcw className="w-4 h-4 mr-2" />
                                Clear
                            </Button>

                            <Button variant="outline" onClick={resetState}>
                                Reset All
                            </Button>
                        </div>

                        <div className="flex items-center gap-2">
                            <Button variant="outline" size="sm" onClick={handleSaveQuery}>
                                <Download className="w-4 h-4 mr-2" />
                                Save
                            </Button>

                            <Button variant="outline" size="sm" onClick={handleLoadQuery}>
                                <Upload className="w-4 h-4 mr-2" />
                                Load
                            </Button>

                            {tables.length === 0 && (
                                <Button variant="outline" size="sm" onClick={handleLoadSampleData}>
                                    <Database className="w-4 h-4 mr-2" />
                                    Load Sample Data
                                </Button>
                            )}
                        </div>
                    </div>

                    {/* Sample Queries */}
                    <div className="flex items-center gap-2">
                        <span className="text-sm text-muted-foreground">Sample queries:</span>
                        <Select value={selectedSample} onValueChange={setSelectedSample}>
                            <SelectTrigger className="w-48">
                                <SelectValue placeholder="Choose a sample..." />
                            </SelectTrigger>
                            <SelectContent>
                                {SAMPLE_QUERIES[dialect]?.map((sample, index) => (
                                    <SelectItem key={index} value={sample.query}>
                                        {sample.name}
                                    </SelectItem>
                                ))}
                            </SelectContent>
                        </Select>
                        {selectedSample && (
                            <Button
                                variant="outline"
                                size="sm"
                                onClick={() => handleLoadSample(selectedSample)}
                            >
                                Load Sample
                            </Button>
                        )}
                    </div>

                    {/* Editor */}
                    <div className="border rounded-lg overflow-hidden">
                        <Editor
                            height="300px"
                            language={dialect === 'sql' ? 'sql' : 'javascript'}
                            value={currentQuery}
                            onChange={handleEditorChange}
                            theme="vs-dark"
                            options={{
                                minimap: { enabled: false },
                                fontSize: 14,
                                lineNumbers: 'on',
                                wordWrap: 'on',
                                automaticLayout: true,
                                scrollBeyondLastLine: false,
                                formatOnPaste: true,
                                formatOnType: true,
                            }}
                        />
                    </div>

                    {/* Error Display */}
                    {error && (
                        <div className="p-3 bg-red-50 border border-red-200 rounded-lg">
                            <p className="text-red-800 text-sm">{error}</p>
                        </div>
                    )}

                    {/* Table Info */}
                    {tables.length > 0 && (
                        <div className="p-3 bg-blue-50 border border-blue-200 rounded-lg">
                            <p className="text-blue-800 text-sm">
                                <strong>Available tables:</strong> {tables.join(', ')}
                            </p>
                        </div>
                    )}
                </CardContent>
            </Card>
        </div>
    )
}