import React, { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { Separator } from '@/components/ui/separator'
import {
    Database,
    Table,
    Plus,
    Search,
    BarChart3,
    Key,
    Hash
} from 'lucide-react'
import { RefreshCw as Refresh } from 'lucide-react'
import { useCatalogStore } from '@/stores/catalogStore'
import { formatRows } from '@/lib/utils'
import toast from 'react-hot-toast'

export function CatalogPanel() {
    const {
        tables,
        selectedTable,
        tableStats,
        isLoading,
        loadTables,
        loadTableStats,
        selectTable,
        loadSampleData
    } = useCatalogStore()

    const [searchTerm, setSearchTerm] = useState('')

    const filteredTables = tables.filter(table =>
        table.toLowerCase().includes(searchTerm.toLowerCase())
    )

    const handleRefresh = async () => {
        try {
            await loadTables()
            toast.success('Tables refreshed')
        } catch (error) {
            toast.error('Failed to refresh tables')
        }
    }

    const handleLoadSampleData = async () => {
        try {
            await loadSampleData()
            toast.success('Sample data loaded')
        } catch (error) {
            toast.error('Failed to load sample data')
        }
    }

    const handleTableSelect = async (tableName) => {
        selectTable(tableName)
        if (!tableStats[tableName]) {
            try {
                await loadTableStats(tableName)
            } catch (error) {
                toast.error(`Failed to load stats for ${tableName}`)
            }
        }
    }

    const selectedTableStats = selectedTable ? tableStats[selectedTable] : null

    return (
        <div className="space-y-4">
            {/* Database Info */}
            <Card>
                <CardHeader className="pb-3">
                    <CardTitle className="flex items-center gap-2 text-lg">
                        <Database className="w-5 h-5" />
                        Database Catalog
                    </CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                    <div className="flex items-center gap-2">
                        <Button
                            variant="outline"
                            size="sm"
                            onClick={handleRefresh}
                            disabled={isLoading}
                        >
                            <Refresh className="w-4 h-4 mr-2" />
                            Refresh
                        </Button>

                        {tables.length === 0 && (
                            <Button
                                variant="outline"
                                size="sm"
                                onClick={handleLoadSampleData}
                                disabled={isLoading}
                            >
                                <Plus className="w-4 h-4 mr-2" />
                                Load Sample
                            </Button>
                        )}
                    </div>

                    <div className="relative">
                        <Search className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
                        <Input
                            placeholder="Search tables..."
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                            className="pl-8"
                        />
                    </div>
                </CardContent>
            </Card>

            {/* Tables List */}
            <Card>
                <CardHeader className="pb-3">
                    <CardTitle className="flex items-center justify-between text-base">
                        <div className="flex items-center gap-2">
                            <Table className="w-4 h-4" />
                            Tables
                        </div>
                        <Badge variant="outline">{filteredTables.length}</Badge>
                    </CardTitle>
                </CardHeader>
                <CardContent>
                    {filteredTables.length > 0 ? (
                        <div className="space-y-2">
                            {filteredTables.map((table) => (
                                <div
                                    key={table}
                                    className={`p-2 rounded-lg border cursor-pointer transition-colors ${
                                        selectedTable === table
                                            ? 'bg-primary/10 border-primary'
                                            : 'hover:bg-gray-50'
                                    }`}
                                    onClick={() => handleTableSelect(table)}
                                >
                                    <div className="flex items-center justify-between">
                                        <span className="font-medium text-sm">{table}</span>
                                        {tableStats[table] && (
                                            <Badge variant="secondary" className="text-xs">
                                                {formatRows(tableStats[table].row_count)} rows
                                            </Badge>
                                        )}
                                    </div>
                                </div>
                            ))}
                        </div>
                    ) : (
                        <div className="text-center py-4 text-muted-foreground">
                            {tables.length === 0 ? 'No tables found' : 'No matching tables'}
                        </div>
                    )}
                </CardContent>
            </Card>

            {/* Table Details */}
            {selectedTableStats && (
                <Card>
                    <CardHeader className="pb-3">
                        <CardTitle className="flex items-center gap-2 text-base">
                            <BarChart3 className="w-4 h-4" />
                            Table Details
                        </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        <div>
                            <h4 className="font-medium mb-2">{selectedTable}</h4>
                            <div className="grid grid-cols-1 gap-2 text-sm">
                                <div className="flex justify-between">
                                    <span>Rows:</span>
                                    <span className="font-mono">{formatRows(selectedTableStats.row_count)}</span>
                                </div>
                                <div className="flex justify-between">
                                    <span>Columns:</span>
                                    <span className="font-mono">{selectedTableStats.columns?.length || 0}</span>
                                </div>
                                <div className="flex justify-between">
                                    <span>Indexes:</span>
                                    <span className="font-mono">{selectedTableStats.indexes?.length || 0}</span>
                                </div>
                            </div>
                        </div>

                        <Separator />

                        {/* Columns */}
                        {selectedTableStats.columns && (
                            <div>
                                <h5 className="font-medium mb-2">Columns</h5>
                                <div className="space-y-1 max-h-32 overflow-y-auto">
                                    {selectedTableStats.columns.map((column, idx) => (
                                        <div key={idx} className="text-xs p-2 bg-gray-50 rounded">
                                            <div className="flex items-center justify-between">
                                                <span className="font-medium">{column.name}</span>
                                                <Badge variant="outline" className="text-xs">
                                                    {column.data_type}
                                                </Badge>
                                            </div>
                                            {column.ndv && (
                                                <div className="text-muted-foreground mt-1">
                                                    NDV: {formatRows(column.ndv)}
                                                </div>
                                            )}
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}

                        {/* Indexes */}
                        {selectedTableStats.indexes && selectedTableStats.indexes.length > 0 && (
                            <>
                                <Separator />
                                <div>
                                    <h5 className="font-medium mb-2">Indexes</h5>
                                    <div className="space-y-1">
                                        {selectedTableStats.indexes.map((index, idx) => (
                                            <div key={idx} className="text-xs p-2 bg-gray-50 rounded">
                                                <div className="flex items-center gap-2">
                                                    {index.unique ? <Key className="w-3 h-3" /> : <Hash className="w-3 h-3" />}
                                                    <span className="font-medium">{index.name}</span>
                                                </div>
                                                <div className="text-muted-foreground mt-1">
                                                    Columns: {index.columns?.join(', ')}
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            </>
                        )}
                    </CardContent>
                </Card>
            )}
        </div>
    )
}
