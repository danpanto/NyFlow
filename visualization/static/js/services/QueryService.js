import { LRUCache } from "../LRUCache.js";
import { filterService } from "./FilterService.js";
import { SUPPORTED_VARIABLES } from "../queryVariables.js";


class QueryService extends EventTarget {
    constructor() {
        super();
        this._cache = new LRUCache(500);
        this._data = {};
        this._loading = false;

        this._listenerCounts = {};
        SUPPORTED_VARIABLES.forEach(v => this._listenerCounts[v] = 0);

        SUPPORTED_VARIABLES.forEach(variable => {
            Object.defineProperty(this, variable, {
                get: () => {
                    return this._data[variable] || null;
                },
                enumerable: true
            });
        });

        this._debounceTimer = null;
        this._onFilterChange = this._onFilterChange.bind(this);

        filterService.addListener("date", this._onFilterChange);
        filterService.addListener("vendors", this._onFilterChange);
        filterService.addListener("zones", this._onFilterChange);
    }

    get activeVariables() {
        return Object.keys(this._listenerCounts).filter(v => this._listenerCounts[v] > 0);
    }

    _getStandardFilterState() {
        if (!filterService.dateRange) return null;

        return {
            date: filterService.dateRange,
            vendors: Array.from(filterService.vendors).sort()
        };
    }

    _getAggFilterState() {
        if (!filterService.dateRange) return null;

        const minDate = new Date(filterService.dateRange.min);
        let maxDate = new Date(filterService.dateRange.max);

        const diffMs = maxDate.getTime() - minDate.getTime();
        const diffHours = diffMs / (1000 * 60 * 60);

        if (diffHours < 24) {
            maxDate = new Date(minDate.getTime() + (24 * 60 * 60 * 1000));
        }

        const finalDiffHours = (maxDate.getTime() - minDate.getTime()) / (1000 * 60 * 60);
        const finalDiffDays = finalDiffHours / 24;

        let time_grouping;
        if (finalDiffDays <= 7) {
            time_grouping = "hour";
        } else if (finalDiffDays <= 365) {
            time_grouping = "day";
        } else {
            time_grouping = "week";
        }

        const zones = Array.from(filterService.zones);

        return {
            date: { min: minDate.toISOString(), max: maxDate.toISOString() },
            vendors: Array.from(filterService.vendors).sort(),
            zones: zones.sort((a, b) => a - b),
            time_grouping: time_grouping
        };
    }
    _getVariableCacheKey(filterState, variable) {
        return JSON.stringify({ ...filterState, variable });
    }

    _onFilterChange() {
        this._query();
    }

    async _query() {
        const activeVars = this.activeVariables;
        if (activeVars.length === 0) return;

        const standardVars = activeVars.filter(v => !v.startsWith('agg-'));
        const aggVars = activeVars.filter(v => v.startsWith('agg-'));

        await Promise.all([
            this._fetchGroup(standardVars, this._getStandardFilterState(), false),
            this._fetchGroup(aggVars, this._getAggFilterState(), true)
        ]);
    }

    async _fetchGroup(variables, filterState, isAgg) {
        if (variables.length === 0) return;

        const variablesToFetch = [];
        const cachedVarsToBroadcast = [];

        variables.forEach(variable => {
            const key = this._getVariableCacheKey(filterState, variable);
            const cachedData = this._cache.get(key);

            if (cachedData) {
                this._data[variable] = cachedData;
                cachedVarsToBroadcast.push(variable);
            } else {
                variablesToFetch.push(variable);
            }
        });

        // Broadcast cached instantly
        if (cachedVarsToBroadcast.length > 0) {
            this._broadcast(cachedVarsToBroadcast, false);
        }

        if (variablesToFetch.length === 0) return;

        this._loading = true;
        this._broadcast(variablesToFetch, true);

        try {
            // If it's an agg variable, strip "agg-" for the backend request
            const backendVariables = isAgg 
                ? variablesToFetch.map(v => v.replace('agg-', '')) 
                : variablesToFetch;

            const payload = {
                ...filterState,
                variables: backendVariables
            };

            const response = await fetch('/api/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            
            const result = await response.json();
            
            if (result.status === "ok" && result.data) {
                variablesToFetch.forEach(variable => {
                    // Re-map the backend's variable name back to the frontend's variable name
                    const backendKey = isAgg ? variable.replace('agg-', '') : variable;
                    const responseData = result.data[backendKey];

                    if (responseData !== undefined) {
                        const cacheKey = this._getVariableCacheKey(filterState, variable);
                        this._cache.set(cacheKey, responseData);
                        this._data[variable] = responseData;
                    }
                });
            } else {
                console.error("API returned error:", result.msg);
            }
        } catch (err) {
            console.error("Query failed:", err);
        } finally {
            this._loading = false;
            this._broadcast(variablesToFetch, false); 
        }
    }

    _broadcast(variables, isLoading) {
        variables.forEach(variable => {
            this.dispatchEvent(new CustomEvent(`query-${variable}`, {
                detail: { 
                    value: this._data[variable] || null,
                    loading: isLoading 
                }
            }));
        });
    }

    addListener(variable, callback) {
        if (this._listenerCounts[variable] === undefined) {
            // Only warn if it's not an aggregation variable
            if (!variable.startsWith('agg-')) {
                console.warn(`Variable '${variable}' is not in SUPPORTED_VARIABLES.`);
            }
            this._listenerCounts[variable] = 0;
            
            // Setup dynamic getter for the new variable
            if (!this.hasOwnProperty(variable)) {
                Object.defineProperty(this, variable, {
                    get: () => this._data[variable] || null,
                    enumerable: true
                });
            }
        }

        this._listenerCounts[variable]++;
        const internalWrapper = (e) => callback(e.detail.value, e.detail.loading);
        this.addEventListener(`query-${variable}`, internalWrapper);

        // Determine which filter state to use based on the variable prefix
        const isAgg = variable.startsWith('agg-');
        const filterState = isAgg ? this._getAggFilterState() : this._getStandardFilterState();
        const key = this._getVariableCacheKey(filterState, variable);
        const cachedData = this._cache.get(key);

        if (cachedData !== undefined) {
            this._data[variable] = cachedData;
            callback(cachedData, false);
        } else if (this._listenerCounts[variable] === 1) {
            this._onFilterChange();
        }

        return () => {
            this.removeEventListener(`query-${variable}`, internalWrapper);
            this._listenerCounts[variable] = Math.max(0, this._listenerCounts[variable] - 1);
        };
    }
}

export const queryService = new QueryService();
