import { DataQueryLayer } from "./DataQueryLayer.js";
import { filterService } from "./services/FilterService.js";
import { VARIABLE_CONFIG } from "./queryVariables.js";
import "./components/HouseIncomeControlPanel.js";
import { LRUCache } from "./LRUCache.js";

export class HouseIncomeLayer extends DataQueryLayer {
    constructor(mapManager, backend) {
        super(mapManager, backend, "house_income");

        // Replace the generic control panel
        this.controlPanel = document.createElement("house-income-control-panel");
        this.cache = new LRUCache(50); // Only 14 possible years, 50 is more than enough

        // Teal-to-gold gradient to visually distinguish from rent layer
        this.gradient.currentTheme = ["#0d3b66", "#1a6b9a", "#2ecc71", "#f39c12", "#f1c40f"];
    }

    async bind() {
        document.body.appendChild(this.drawer);
        document.body.appendChild(this.controlPanel);
        document.body.appendChild(this.uiWrapper);

        const config = VARIABLE_CONFIG[this.variable] || {};
        const layerName = config.longName || this.variable;
        document.title = `${this.baseAppName} • ${layerName}`;

        const layerTitleElement = document.querySelector('.layer-title-text');
        if (layerTitleElement) layerTitleElement.textContent = `• ${layerName}`;

        this.mapManager.toggleLayer(this.variable, true);

        await this._loadData();

        this.unsubscribeSelectZone = filterService.addListener("zones", () => {
            this.onSelectedZone(filterService.lastZone);
        });

        this.unsubscribeDate = filterService.addListener("date", () => {
            this._loadData();
        });
    }

    async _loadData() {
        try {
            const year = this.controlPanel.selectedYear ?? 2024;
            const cacheKey = `year-${year}`;

            let cachedData = this.cache.get(cacheKey);

            if (!cachedData) {
                const response = await fetch('/api/house-income', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ year })
                });
                const result = await response.json();

                if (result.status === "ok") {
                    cachedData = result.data;
                    this.cache.set(cacheKey, cachedData);
                } else {
                    console.error("House income API error:", result);
                    return;
                }
            }

            // Filter out zero / null values (no data)
            const filteredData = {};
            for (const key in cachedData) {
                if (cachedData[key] > 0) {
                    filteredData[key] = cachedData[key];
                }
            }

            this.data = filteredData;
            this.mapController.update({ query: this.data });
            const { min, max, absoluteMax } = this.mapController.dataBounds;
            this.legend.update(this.gradient, min, max, absoluteMax);
            this.onSelectedZone(filterService.lastZone);

        } catch (e) {
            console.error("Error loading house income data", e);
        }
    }

    unbind() {
        this.drawer.remove();
        this.controlPanel.remove();
        this.uiWrapper.remove();
        this.mapManager.toggleLayer(this.variable, false);

        const layerTitleElement = document.querySelector('.layer-title-text');
        if (layerTitleElement) layerTitleElement.textContent = "";

        if (this.unsubscribeSelectZone) {
            this.unsubscribeSelectZone();
            this.unsubscribeSelectZone = null;
        }
        if (this.unsubscribeDate) {
            this.unsubscribeDate();
            this.unsubscribeDate = null;
        }

        document.title = this.baseAppName;
    }
}
