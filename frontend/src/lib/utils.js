import { clsx } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs) {
    return twMerge(clsx(inputs))
}

export function formatCost(cost) {
    if (cost === null || cost === undefined) return 'N/A'
    if (cost < 1) return cost.toFixed(3)
    if (cost < 1000) return cost.toFixed(2)
    if (cost < 1000000) return `${(cost / 1000).toFixed(1)}K`
    return `${(cost / 1000000).toFixed(1)}M`
}

export function formatRows(rows) {
    if (rows === null || rows === undefined) return 'N/A'
    if (rows < 1000) return rows.toString()
    if (rows < 1000000) return `${(rows / 1000).toFixed(1)}K`
    if (rows < 1000000000) return `${(rows / 1000000).toFixed(1)}M`
    return `${(rows / 1000000000).toFixed(1)}B`
}

export function formatDuration(ms) {
    if (ms < 1000) return `${ms}ms`
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
    return `${(ms / 60000).toFixed(1)}m`
}

export function generateId() {
    return Math.random().toString(36).substr(2, 9)
}

export function debounce(func, wait) {
    let timeout
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout)
            func(...args)
        }
        clearTimeout(timeout)
        timeout = setTimeout(later, wait)
    }
}

export function deepClone(obj) {
    return JSON.parse(JSON.stringify(obj))
}