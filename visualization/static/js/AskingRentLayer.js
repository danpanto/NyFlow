import { DataQueryLayer } from "./DataQueryLayer.js";
import { filterService } from "./services/FilterService.js";
import { VARIABLE_CONFIG } from "./queryVariables.js";
import "./components/AskingRentControlPanel.js";
import { LRUCache } from "./LRUCache.js";

export class AskingRentLayer extends DataQueryLayer {
    constructor(mapManager, backend) {
        super(mapManager, backend, "asking_rent");

        // Remove old generic control panel overriding it
        this.controlPanel = document.createElement("asking-rent-control-panel");
        this.cache = new LRUCache(200);
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

        await this._loadStaticData();

        this.unsubscribeSelectZone = filterService.addListener("zones", () => {
            this.onSelectedZone(filterService.lastZone);
        });

        this.unsubscribeDate = filterService.addListener("date", () => {
            this._loadStaticData();
        });
    }

    async _loadStaticData() {
        try {
            const ds = filterService.dateRange || { min: new Date("2010-01-01T00:00:00"), max: new Date("2010-01-31T23:59:59") };

            const fmt = (d) => {
                const dt = d instanceof Date ? d : new Date(d);
                return dt.toISOString().split('.')[0];
            };

            const payload = {
                variables: ["asking_rent"],
                vendors: filterService.vendors ? Array.from(filterService.vendors) : [],
                date: { min: fmt(ds.min), max: fmt(ds.max) },
                zones: filterService.zones ? Array.from(filterService.zones) : []
            };

            const cacheKey = JSON.stringify(payload);
            let cachedData = this.cache.get(cacheKey);

            if (!cachedData) {
                const response = await fetch('/api/asking-rent', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const result = await response.json();

                if (result.status === "ok") {
                    cachedData = result.data;
                    this.cache.set(cacheKey, cachedData);
                } else {
                    return;
                }
            }
            
            this.data = cachedData;
            this.mapController.update({ query: this.data });
            const { min, max, absoluteMax } = this.mapController.dataBounds;
            this.legend.update(this.gradient, min, max, absoluteMax);
            this.onSelectedZone(filterService.lastZone);

        } catch (e) {
            console.error("Error loading asking rent data", e);
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
