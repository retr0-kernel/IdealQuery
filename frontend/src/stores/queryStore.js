import { create } from 'zustand'
import { apiClient } from '../lib/api'

export const useQueryStore = create((set, get) => ({
  
    currentQuery: '',
    dialect: 'sql',
    isLoading: false,
    error: null,

  
    parseResult: null,
    optimizationResults: null,
    simulationResults: null,

  
    activeTab: 'editor',
    selectedNode: null,
    comparisonMode: 'side-by-side',

  
    setQuery: (query) => set({ currentQuery: query }),
    setDialect: (dialect) => set({ dialect }),
    setActiveTab: (tab) => set({ activeTab: tab }),
    setSelectedNode: (node) => set({ selectedNode: node }),
    setComparisonMode: (mode) => set({ comparisonMode: mode }),

  
    parseQuery: async () => {
        const { currentQuery, dialect } = get()
        if (!currentQuery.trim()) return

        set({ isLoading: true, error: null })
        try {
            const result = await apiClient.parseQuery(dialect, currentQuery)
            set({ parseResult: result, isLoading: false })
            return result
        } catch (error) {
            set({ error: error.message, isLoading: false })
            throw error
        }
    },

    optimizeQuery: async (strategy = 'cost') => {
        const { parseResult } = get()
        if (!parseResult?.logicalPlan) throw new Error('No plan to optimize')

        set({ isLoading: true, error: null })
        try {
            const result = await apiClient.optimizeQuery(strategy, parseResult.logicalPlan)
            set({
                optimizationResults: { ...get().optimizationResults, [strategy]: result },
                isLoading: false
            })
            return result
        } catch (error) {
            set({ error: error.message, isLoading: false })
            throw error
        }
    },

    runFullWorkflow: async (connector = 'postgres') => {
        const { currentQuery, dialect } = get()
        if (!currentQuery.trim()) return

        set({ isLoading: true, error: null, parseResult: null, optimizationResults: null, simulationResults: null })

        try {
            const results = await apiClient.fullWorkflow(dialect, currentQuery, connector)
            set({
                parseResult: { logicalPlan: results.original.plan },
                optimizationResults: {
                    rule: { optimizedPlan: results.ruleOptimized.plan, explain: results.ruleOptimized.explain },
                    cost: { optimizedPlan: results.costOptimized.plan, explain: results.costOptimized.explain }
                },
                simulationResults: {
                    original: results.original.simulation,
                    rule: results.ruleOptimized.simulation,
                    cost: results.costOptimized.simulation
                },
                isLoading: false,
                activeTab: 'visualization'
            })
            return results
        } catch (error) {
            set({ error: error.message, isLoading: false })
            throw error
        }
    },

    clearResults: () => set({
        parseResult: null,
        optimizationResults: null,
        simulationResults: null,
        selectedNode: null,
        error: null
    }),

    resetState: () => set({
        currentQuery: '',
        dialect: 'sql',
        isLoading: false,
        error: null,
        parseResult: null,
        optimizationResults: null,
        simulationResults: null,
        activeTab: 'editor',
        selectedNode: null,
        comparisonMode: 'side-by-side'
    })
}))