import { LRUCache } from "../LRUCache.js";
import { filterService } from "./FilterService.js";

export const SUPPORTED_VARIABLES = [
    "total_trips",
    "total_price", 
    "mean_price",
    "total_tip",
    "mean_tip",
    "mean_distance",
    "mean_duration",
    "mean_tip_time",
    "mean_tip_dis",
    "mean_price_time",
    "mean_price_dis"
];

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
    }

    get activeVariables() {
        return Object.keys(this._listenerCounts).filter(v => this._listenerCounts[v] > 0);
    }


    _getFilterState() {
        return {
            date: filterService.dateRange,
            vendors: Array.from(filterService.vendors).sort()
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

        const filterState = this._getFilterState();

        const variablesToFetch = [];
        const cachedVarsToBroadcast = [];

        activeVars.forEach(variable => {
            const key = this._getVariableCacheKey(filterState, variable);
            const cachedData = this._cache.get(key);

            if (cachedData) {
                this._data[variable] = cachedData;
                cachedVarsToBroadcast.push(variable);
            } else {
                variablesToFetch.push(variable);
            }
        });


        // 4. Immediately broadcast variables we found in the cache
        if (cachedVarsToBroadcast.length > 0) {
            this._broadcast(cachedVarsToBroadcast, false);
        }

        // If everything was cached, we are done!
        if (variablesToFetch.length === 0) return;

        // 5. Fetch ONLY the missing variables from the backend
        this._loading = true;
        this._broadcast(variablesToFetch, true); // Tell only missing charts they are loading

        try {
            const payload = {
                ...filterState,
                variables: variablesToFetch // e.g., ["total_amount"] (count was cached)
            };

            const response = await fetch('/api/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            
            const result = await response.json();
            
            if (result.status === "ok" && result.data) {
                variablesToFetch.forEach(variable => {
                    if (result.data[variable] !== undefined) {
                        const key = this._getVariableCacheKey(filterState, variable);
                        this._cache.set(key, result.data[variable]);
                        this._data[variable] = result.data[variable];
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

    // 7. Pass explicit loading state so cached charts don't show spinners
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
            console.warn(`Variable '${variable}' is not in SUPPORTED_VARIABLES.`);
            this._listenerCounts[variable] = 0;
        }

        this._listenerCounts[variable]++;
        const internalWrapper = (e) => callback(e.detail.value, e.detail.loading);
        this.addEventListener(`query-${variable}`, internalWrapper);

        const filterState = this._getFilterState();
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
