import { create } from 'zustand'
import { apiClient } from '../lib/api'

export const useCatalogStore = create((set, get) => ({
  
    tables: [],
    selectedTable: null,
    tableStats: {},
    isLoading: false,
    error: null,
    isInitialized: false,

  
    loadTables: async () => {
        set({ isLoading: true, error: null })
        try {
            const response = await apiClient.getTables()
            set({ tables: response.tables || [], isLoading: false, isInitialized: true })
        } catch (error) {
            set({ error: error.message, isLoading: false })
        }
    },

    loadTableStats: async (tableName) => {
        set({ isLoading: true, error: null })
        try {
            const stats = await apiClient.getTableStats(tableName)
            set({
                tableStats: { ...get().tableStats, [tableName]: stats },
                isLoading: false
            })
            return stats
        } catch (error) {
            set({ error: error.message, isLoading: false })
            throw error
        }
    },

    loadSampleData: async () => {
        set({ isLoading: true, error: null })
        try {
            await apiClient.loadSampleData()
          
            await get().loadTables()
            set({ isLoading: false })
        } catch (error) {
            set({ error: error.message, isLoading: false })
            throw error
        }
    },

    selectTable: (tableName) => {
        set({ selectedTable: tableName })
      
        if (tableName && !get().tableStats[tableName]) {
            get().loadTableStats(tableName)
        }
    },

    addTable: async (schema) => {
        set({ isLoading: true, error: null })
        try {
            await apiClient.addTable(schema)
            await get().loadTables() // Reload tables
            set({ isLoading: false })
        } catch (error) {
            set({ error: error.message, isLoading: false })
            throw error
        }
    },

  
    initialize: async () => {
        if (get().isInitialized) return

        try {
            await get().loadTables()
          
            if (get().tables.length === 0) {
                await get().loadSampleData()
            }
        } catch (error) {
            console.error('Failed to initialize catalog:', error)
        }
    }
}))