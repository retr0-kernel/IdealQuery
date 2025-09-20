import axios from 'axios'

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8080'

const api = axios.create({
    baseURL: API_BASE_URL,
    timeout: 30000,
    headers: {
        'Content-Type': 'application/json',
    },
})

// Request interceptor
api.interceptors.request.use(
    (config) => {
      
        return config
    },
    (error) => {
        return Promise.reject(error)
    }
)

// Response interceptor
api.interceptors.response.use(
    (response) => {
        return response.data
    },
    (error) => {
        console.error('API Error:', error)
        return Promise.reject(error)
    }
)

// API endpoints
export const apiClient = {
  
    health: () => api.get('/health'),

  
    loadSampleData: () => api.post('/api/load-sample-data'),
    getTables: () => api.get('/api/catalog/tables'),
    getTableStats: (tableName) => api.get(`/api/catalog/table/${tableName}/stats`),
    addTable: (schema) => api.post('/api/catalog/table', schema),
    updateTableStats: (tableName, stats) => api.post(`/api/catalog/table/${tableName}/stats`, stats),

  
    parseQuery: (dialect, query) => api.post('/api/parse', { dialect, query }),
    optimizeQuery: (strategy, logicalPlan) => api.post('/api/optimize', { strategy, logicalPlan }),
    simulateExecution: (plan, connector, options = {}) => api.post('/api/simulate', { plan, connector, options }),

  
    processQuery: async (dialect, query, optimizationStrategy = 'cost') => {
        const parseResult = await api.post('/api/parse', { dialect, query })
        const optimizeResult = await api.post('/api/optimize', {
            strategy: optimizationStrategy,
            logicalPlan: parseResult.logicalPlan
        })
        return { parseResult, optimizeResult }
    },

    fullWorkflow: async (dialect, query, connector = 'postgres') => {
        const parseResult = await api.post('/api/parse', { dialect, query })

        const [ruleOptimization, costOptimization] = await Promise.all([
            api.post('/api/optimize', { strategy: 'rule', logicalPlan: parseResult.logicalPlan }),
            api.post('/api/optimize', { strategy: 'cost', logicalPlan: parseResult.logicalPlan })
        ])

        const [originalSimulation, ruleSimulation, costSimulation] = await Promise.all([
            api.post('/api/simulate', { plan: parseResult.logicalPlan, connector }),
            api.post('/api/simulate', { plan: ruleOptimization.optimizedPlan, connector }),
            api.post('/api/simulate', { plan: costOptimization.optimizedPlan, connector })
        ])

        return {
            original: { plan: parseResult.logicalPlan, simulation: originalSimulation },
            ruleOptimized: { plan: ruleOptimization.optimizedPlan, simulation: ruleSimulation, explain: ruleOptimization.explain },
            costOptimized: { plan: costOptimization.optimizedPlan, simulation: costSimulation, explain: costOptimization.explain }
        }
    }
}

export default api